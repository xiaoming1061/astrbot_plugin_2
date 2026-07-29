import asyncio
import json
import time
from astrbot.api import AstrBotConfig
from dataclasses import dataclass, field

import astrbot.api.message_components as Comp

from astrbot.api import logger
from astrbot.api.event import (
    AstrMessageEvent,
    MessageChain,
)
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
            self.config.get(
                "command_prefixes",
                ["/"]
            )
        )

        if prefixes:
            for comp in raw_components:
                text = getattr(
                    comp,
                    "text",
                    ""
                ).strip()

                if text.startswith(prefixes):
                    self.debug(
                        "[FACT] command bypass"
                    )
                    return

        # 群聊白名单/黑名单只作用于普通消息
        if not self.check_group_enabled(event):
            self.debug(
                "[FACT] group bypass"
            )
            return

        try:
            components = [
                self.serialize_component(comp)
                for comp in raw_components
            ]

            key = (
                event.unified_msg_origin,
                str(event.get_sender_id())
            )

            if key not in self.buffers:
                self.buffers[key] = SessionBuffer()

            buffer = self.buffers[key]

            buffer.messages.append(
                BufferedMessage(
                    timestamp=time.time(),
                    sender_id=str(
                        event.get_sender_id()
                    ),
                    sender_name=event.get_sender_name(),
                    components=components
                )
            )

            self.debug(
                f"[FACT] buffer_size={len(buffer.messages)}"
            )

            # 检测是否由固定结束符立即提交
            should_flush = False

            endings = tuple(
                self.config.get(
                    "auto_flush_endings",
                    []
                )
            )

            if endings:
                for comp in raw_components:
                    text = getattr(
                        comp,
                        "text",
                        ""
                    ).rstrip()

                    if text.endswith(endings):
                        should_flush = True
                        break

            # 取消之前的等待任务
            if buffer.flush_task:
                buffer.flush_task.cancel()

            if should_flush:
                self.debug(
                    "[FACT] auto flush"
                )

                buffer.flush_task = asyncio.create_task(
                    self._flush(key)
                )

            else:
                buffer.flush_task = asyncio.create_task(
                    self._delayed_flush(key)
                )

            # 阻断 AstrBot 对普通消息的默认回复
            event.stop_event()

            self.debug(
                "[FACT] chat intercepted"
            )

        except Exception:
            logger.exception(
                "[FACT] failed to buffer message"
            )
            
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

            await self._flush(key)

        except asyncio.CancelledError:

            pass

        except Exception as e:

            logger.exception(e)

    async def _flush(
        self,
        key
    ):
        buffer = self.buffers.get(key)

        if not buffer:
            return

        if not buffer.messages:
            return

        messages = buffer.messages

        del self.buffers[key]

        batch = self._build_batch(
            key,
            messages
        )

        logger.info(
            "\n========== FACT BATCH ==========\n"
        )

        logger.info(
            json.dumps(
                batch,
                ensure_ascii=False,
                indent=2
            )
        )

        logger.info(
            "\n===============================\n"
        )

        try:

            logger.info(
                "[FACT] entering _process_batch"
            )

            await self._process_batch(
                batch
            )

            logger.info(
                "[FACT] _process_batch finished"
            )

        except Exception:
            logger.exception(
                "[FACT] process failed"
            )

    async def _process_batch(
        self,
        batch
    ):
        logger.info("[FACT] start")

        umo = batch["session"]["umo"]

        provider_id = await (
            self.context
            .get_current_chat_provider_id(
                umo=umo
            )
        )

        prompt = self._build_fact_prompt(
            batch
        )

        contexts = None

        try:

            cid = await (
                self.context.conversation_manager
                .get_curr_conversation_id(
                    umo
                )
            )

            logger.info(
                f"[FACT] cid={cid}"
            )

            if cid:

                conv = await (
                    self.context.conversation_manager
                    .get_conversation(
                        umo,
                        cid
                    )
                )

                logger.info(
                    f"[FACT] history_len={len(conv.history)}"
                )

                if conv.history:

                    history_limit = self.config.get(
                        "history_limit",
                        20
                    )

                    history_limit = int(
                        self.config.get(
                            "history_limit",
                            20
                        )
                    )

                    history = json.loads(
                        conv.history
                    )

                    if history_limit > 0:
                        contexts = history[-history_limit:]
                    else:
                        contexts = []

                    self.debug(
                        f"[FACT] contexts={len(contexts)}"
                    )

        except Exception:
            logger.exception(
                "[FACT] load history failed"
            )

        logger.info(
            f"[FACT] provider={provider_id}"
        )

        resp = await self.context.llm_generate(
            chat_provider_id=provider_id,
            contexts=contexts,
            prompt=prompt
        )

        answer = resp.completion_text

        self.debug(
            f"[FACT] answer={answer}"
        )

        chain = MessageChain()

        chain.chain.append(
            Comp.Plain(answer)
        )

        await self.context.send_message(
            umo,
            chain
        )

        logger.info(
            "[FACT] done"
        )

    def _fact_system_prompt(
            self
        ):

            return """
    你将收到一种特殊格式：

    <FACT_CONTEXT>
    ...
    </FACT_CONTEXT>

    规则：

    1. FACT_CONTEXT 内部内容表示用户短时间内连续发送的消息。
    2. 应将这些内容视为一次连续输入。
    3. 不要分析 FACT_CONTEXT 标签。
    4. 不要解释 FACT_CONTEXT 机制。
    5. 不要总结 FACT_CONTEXT 结构。
    6. 保持你当前的人格和对话风格。
    7. 像正常聊天一样理解并回应用户。
    """

    def _build_fact_prompt(
        self,
        batch
    ):

        return (
            self._fact_system_prompt()
            + "\n\n"
            + self._render_fact_context(
                batch
            )
        )

    def _render_fact_context(
        self,
        batch
    ):

        lines = []

        lines.append(
            "<FACT_CONTEXT>"
        )

        lines.append("")

        for msg in batch["messages"]:

            for comp in msg["components"]:

                component_type = comp.get(
                    "component_type"
                )

                if component_type == "Plain":

                    text = comp.get(
                        "text",
                        ""
                    )

                    if text.strip():

                        lines.append(
                            text
                        )

                elif component_type == "Image":

                    lines.append(
                        "[IMAGE]"
                    )

                elif component_type == "At":

                    lines.append(
                        "[AT]"
                    )

                elif component_type == "Record":

                    lines.append(
                        "[VOICE]"
                    )

                elif component_type == "Video":

                    lines.append(
                        "[VIDEO]"
                    )

                elif component_type == "File":

                    lines.append(
                        "[FILE]"
                    )

            lines.append("")

        lines.append(
            "</FACT_CONTEXT>"
        )

        return "\n".join(lines)

    def _build_batch(
        self,
        key,
        messages
    ):

        first_ts = messages[0].timestamp

        return {
            "schema_version": "3.3",
            "type": "fact_batch",
            "session": {
                "umo": key[0],
                "sender_id": key[1]
            },
            "messages": [
                {
                    "message_index": idx,
                    "offset": round(
                        msg.timestamp - first_ts,
                        3
                    ),
                    "sender_id": msg.sender_id,
                    "sender_name": msg.sender_name,
                    "components": msg.components
                }
                for idx, msg in enumerate(messages)
            ]
        }

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
