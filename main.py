import asyncio
import json
import time
from dataclasses import dataclass, field

import astrbot.api.message_components as Comp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.event.filter import (
    event_message_type,
    EventMessageType,
)
from astrbot.api.star import Context, Star

@dataclass
class BufferedMessage:
    timestamp: float
    sender_id: str
    sender_name: str
    components: list

@dataclass
class SessionBuffer:
    messages: list[BufferedMessage] = field(
        default_factory=list
    )
    flush_task: asyncio.Task | None = None
    flush_event: asyncio.Event | None = None


class FactAggregatorPlugin(Star):

    def __init__(
        self,
        context: Context,
        config: AstrBotConfig
    ):
        super().__init__(context)

        self.config = config

        self.buffers = {}

        logger.info(
            "消息聚合（SmoothChat）已加载"
        )
        
    def check_group_enabled(
        self,
        event: AstrMessageEvent
    ) -> bool:

        # 私聊不受群聊名单影响
        if event.is_private_chat():
            return True

        mode = self.config.get(
            "group_mode",
            "off"
        )

        groups = {
            str(item).strip()
            for item in self.config.get(
                "group_list",
                []
            )
            if str(item).strip()
        }

        group_id = event.get_group_id()

        if group_id is None:
            self.debug(
                "[FACT] 无法获取群号，默认放行"
            )
            return True

        group_id = str(group_id).strip()

        if mode == "whitelist":
            enabled = group_id in groups

        elif mode == "blacklist":
            enabled = group_id not in groups

        else:
            enabled = True

        self.debug(
            f"[FACT] group_id={group_id}, "
            f"group_mode={mode}, "
            f"group_enabled={enabled}"
        )

        return enabled
    
    def check_group_trigger(
        self,
        event: AstrMessageEvent
    ) -> bool:

        # 私聊不受群聊触发模式影响
        if event.is_private_chat():
            return True

        mode = self.config.get(
            "group_trigger_mode",
            "all"
        )

        # 所有符合条件的群消息均可进入聚合
        if mode == "all":
            return True

        # 仅明确提及或唤醒机器人时进入聚合
        if mode == "mention":
            try:
                enabled = bool(
                    event.is_at_or_wake_command()
                )

                self.debug(
                    f"[FACT] group_trigger_mode=mention, "
                    f"triggered={enabled}"
                )

                return enabled

            except Exception:
                logger.exception(
                    "[FACT] failed to detect mention"
                )

                return False

        # 未知配置值默认使用 all
        self.debug(
            f"[FACT] unknown group_trigger_mode={mode}, "
            "fallback to all"
        )

        return True

    def _reconstruct_event(
        self,
        event: AstrMessageEvent,
        text: str
    ):
        event.message_str = text

        message_obj = getattr(
            event,
            "message_obj",
            None
        )

        if message_obj is None:
            return

        try:
            message_obj.message_str = text
        except Exception:
            pass

        try:
            message_obj.message = [
                Comp.Plain(text)
            ]
        except Exception:
            logger.exception(
                "[SmoothChat] failed to rebuild message chain"
            )

        raw_message = getattr(
            message_obj,
            "raw_message",
            None
        )

        if isinstance(raw_message, dict):
        try:
            raw_message["message"] = [
                {
                    "type": "text",
                    "data": {
                        "text": text
                    }
                }
            ]

            raw_message["raw_message"] = text

        except Exception:
            logger.exception(
                "[SmoothChat] failed to rebuild raw message"
            )

    def _build_merged_text(
        self,
        messages: list[BufferedMessage]
    ) -> str:
        lines = []

        for message in messages:
            for component in message.components:
                if component.get(
                    "component_type"
                ) != "Plain":
                    continue

                text = component.get(
                    "text",
                    ""
                ).strip()

                if text:
                    lines.append(text)

        return "\n".join(lines).strip()

    def _should_flush_immediately(
        self,
        components
    ) -> bool:

        endings = tuple(
            str(item)
            for item in self.config.get(
                "auto_flush_endings",
                []
            )
            if str(item)
        )

        if not endings:
            return False

        for component in components:
            text = getattr(
                component,
                "text",
                ""
            ).rstrip()

            if text.endswith(endings):
                return True

        return False

    def _get_buffer_stats(
        self,
        buffer: SessionBuffer
    ) -> tuple[int, int]:

        message_count = len(
            buffer.messages
        )

        char_count = 0

        for buffered_message in buffer.messages:
            for component in buffered_message.components:
                if component.get(
                    "component_type"
                ) == "Plain":
                    char_count += len(
                        component.get(
                            "text",
                            ""
                        )
                    )

        return message_count, char_count


    def _buffer_limit_reached(
        self,
        buffer: SessionBuffer
    ) -> bool:

        message_count, char_count = (
            self._get_buffer_stats(buffer)
        )

        max_messages = int(
            self.config.get(
                "max_buffer_messages",
                30
            )
        )

        max_chars = int(
            self.config.get(
                "max_buffer_chars",
                8000
            )
        )

        message_limit_reached = (
            max_messages > 0
            and message_count >= max_messages
        )

        char_limit_reached = (
            max_chars > 0
            and char_count >= max_chars
        )

        self.debug(
            f"[SmoothChat] buffer_messages={message_count}, "
            f"buffer_chars={char_count}"
        )

        return (
            message_limit_reached
            or char_limit_reached
        )

    def debug(self, msg):

        if self.config.get(
            "debug_log",
            False
        ):
            logger.info(msg)

    async def terminate(self):

        for buffer in self.buffers.values():

            if buffer.flush_task:
                buffer.flush_task.cancel()

            if (
                buffer.flush_event
                and not buffer.flush_event.is_set()
            ):
                buffer.flush_event.set()

        self.buffers.clear()

    @event_message_type(
        EventMessageType.ALL,
        priority=999999
    )
    async def on_message(
        self,
        event: AstrMessageEvent
    ):
        msg = event.get_message_str()

        # 空消息过滤
        if not msg or not msg.strip():
            return

        raw_components = event.get_messages()

        # AstrBot 命令优先放行
        prefixes = tuple(
            str(item)
            for item in self.config.get(
                "command_prefixes",
                ["/"]
            )
            if str(item)
        )

        if prefixes:
            for component in raw_components:
                text = getattr(
                    component,
                    "text",
                    ""
                ).strip()

                if text.startswith(prefixes):
                    self.debug(
                        "[SmoothChat] command bypass"
                    )
                    return

        # 群聊白名单和黑名单
        if not self.check_group_enabled(event):
            self.debug(
                "[SmoothChat] group bypass"
            )
            return

        key = (
            event.unified_msg_origin,
            str(event.get_sender_id())
        )

        # 首条消息需要符合触发条件，后续消息继续加入已有缓冲区
        if (
            key not in self.buffers
            and not self.check_group_trigger(event)
        ):
            self.debug(
                "[SmoothChat] group trigger bypass"
            )
            return

        try:
            components = [
                self.serialize_component(component)
                for component in raw_components
            ]

            current_message = BufferedMessage(
                timestamp=time.time(),
                sender_id=str(
                    event.get_sender_id()
                ),
                sender_name=event.get_sender_name(),
                components=components
            )

            # 后续消息加入现有缓冲区
            if key in self.buffers:
                buffer = self.buffers[key]

                buffer.messages.append(
                    current_message
                )

                if buffer.flush_task:
                    buffer.flush_task.cancel()

                should_flush = (
                    self._should_flush_immediately(
                        raw_components
                    )
                    or self._buffer_limit_reached(
                        buffer
                    )
                )

                if should_flush:
                    self.debug(
                        "[SmoothChat] immediate flush"
                    )

                    if (
                        buffer.flush_event
                        and not buffer.flush_event.is_set()
                    ):
                        buffer.flush_event.set()

                else:
                    buffer.flush_task = (
                        asyncio.create_task(
                            self._delayed_flush(key)
                        )
                    )

                # 后续消息不能独立进入 AstrBot
                event.stop_event()
                return

            # 第一条消息创建缓冲会话
            flush_event = asyncio.Event()

            buffer = SessionBuffer(
                messages=[
                    current_message
                ],
                flush_event=flush_event
            )

            self.buffers[key] = buffer

            should_flush = (
                self._should_flush_immediately(
                    raw_components
                )
                or self._buffer_limit_reached(
                    buffer
                )
            )

            if should_flush:
                flush_event.set()
            else:
                buffer.flush_task = (
                    asyncio.create_task(
                        self._delayed_flush(key)
                    )
                )

            self.debug(
                "[SmoothChat] first event waiting"
            )

            # 保持首条消息的事件生命周期
            await flush_event.wait()

            session = self.buffers.pop(
                key,
                None
            )

            if session is None:
                event.stop_event()
                return

            if session.flush_task:
                session.flush_task.cancel()

            merged_text = self._build_merged_text(
                session.messages
            )

            if not merged_text:
                event.stop_event()
                return

            self.debug(
                f"[SmoothChat] merged_text={merged_text!r}"
            )

            # 将聚合结果写回第一条事件
            self._reconstruct_event(
                event,
                merged_text
            )

            self.debug(
                "[SmoothChat] event reconstructed"
            )

            # 这里不要调用 event.stop_event()
            # 正常返回，使 AstrBot 继续原生处理链

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "[SmoothChat] failed to aggregate message"
            )

            # 出错时避免残留缓冲区
            key = (
                event.unified_msg_origin,
                str(event.get_sender_id())
            )

            session = self.buffers.pop(
                key,
                None
            )

            if session:
                if session.flush_task:
                    session.flush_task.cancel()

                if (
                    session.flush_event
                    and not session.flush_event.is_set()
                ):
                    session.flush_event.set()
            
    async def _delayed_flush(
        self,
        key
    ):
        try:
            flush_timeout = float(
                self.config.get(
                    "flush_timeout",
                    5.0
                )
            )

            await asyncio.sleep(
                max(0.1, flush_timeout)
            )

            buffer = self.buffers.get(key)

            if (
                buffer
                and buffer.flush_event
                and not buffer.flush_event.is_set()
            ):
                self.debug(
                    "[SmoothChat] timeout flush"
                )

                buffer.flush_event.set()

        except asyncio.CancelledError:
            pass

        except Exception:
            logger.exception(
                "[SmoothChat] flush timer failed"
            )

    def serialize_component(
        self,
        comp
    ):

        result = {
            "component_type":
            type(comp).__name__
        }

        try:

            if hasattr(comp, "__dict__"):

                for k, v in comp.__dict__.items():

                    try:

                        json.dumps(v)

                        result[k] = v

                    except Exception:

                        result[k] = str(v)

        except Exception:

            pass

        return result
