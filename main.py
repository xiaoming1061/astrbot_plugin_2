# SmoothChat - AstrBot message aggregation plugin
# Copyright (C) 2026 xiao_ming1001
#
# This file includes code and design adapted from:
# astrbot_plugin_continuous_message
# https://github.com/aliveriver/astrbot_plugin_continuous_message
# Copyright (C) aliveriver
#
# The adapted portions include:
# - native event reconstruction;
# - event lifecycle handling used to return aggregated messages to
#   the AstrBot processing pipeline;
# - preservation and reconstruction of native image components.
#
# Modified for SmoothChat on 2026-07-30.
# SmoothChat adds group-chat aggregation, per-user buffer isolation,
# group allowlists and blocklists, trigger modes, automatic flush
# endings, buffer size limits, and lightweight native image component
# preservation without image downloading, localization, conversion,
# card parsing, or link enrichment.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with this program. If not, see:
# https://www.gnu.org/licenses/
#
# SPDX-License-Identifier: AGPL-3.0-or-later

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

    # 序列化组件，用于统计文本和构建聚合内容
    components: list

    # AstrBot 原生图片组件，用于事件重建
    image_components: list = field(
        default_factory=list
    )

@dataclass
class SessionBuffer:
    messages: list[BufferedMessage] = field(
        default_factory=list
    )
    flush_task: asyncio.Task | None = None
    flush_event: asyncio.Event | None = None


class SmoothChatPlugin(Star):

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
                "[SmoothChat] 无法获取群号，默认放行"
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
            f"[SmoothChat] group_id={group_id}, "
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
                    f"[SmoothChat] group_trigger_mode=mention, "
                    f"triggered={enabled}"
                )

                return enabled

            except Exception:
                logger.exception(
                    "[SmoothChat] failed to detect mention"
                )

                return False

        # 未知配置值默认使用 all
        self.debug(
            f"[SmoothChat] unknown group_trigger_mode={mode}, "
            "fallback to all"
        )

        return True

    def _reconstruct_event(
        self,
        event: AstrMessageEvent,
        text: str,
        image_components: list
    ) -> None:
        """
        将聚合后的文本与原生图片组件写回 AstrBot 消息事件，
        使重建后的消息继续进入 AstrBot 原生处理管线。

        Native event and image-component reconstruction in this method
        is adapted from astrbot_plugin_continuous_message:
        https://github.com/aliveriver/astrbot_plugin_continuous_message

        Modified for SmoothChat's lightweight group-chat aggregation
        and per-user buffer isolation architecture.
        """
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
            chain = []

            if text:
                chain.append(
                    Comp.Plain(text)
                )

            chain.extend(
                image_components
            )

            message_obj.message = chain

        except Exception:
            logger.exception(
                "[SmoothChat] failed to rebuild message chain"
            )

        raw_message = getattr(
            message_obj,
            "raw_message",
            None
        )

        if not isinstance(raw_message, dict):
            return

        try:
            raw_segments = []

            if text:
                raw_segments.append(
                    {
                        "type": "text",
                        "data": {
                            "text": text
                        }
                    }
                )

            for image in image_components:
                image_ref = (
                    getattr(image, "url", None)
                    or getattr(image, "file", None)
                )

                if not image_ref:
                    continue

                image_ref = str(image_ref)

                image_data = {
                    "file": image_ref
                }

                if image_ref.startswith(
                    ("http://", "https://")
                ):
                    image_data["url"] = image_ref

                raw_segments.append(
                    {
                        "type": "image",
                        "data": image_data
                    }
                )

            raw_message["message"] = raw_segments
            raw_message["raw_message"] = text

        except Exception:
            logger.exception(
                "[SmoothChat] failed to rebuild raw message"
            )

    @staticmethod
    def _is_image_component(
        component
    ) -> bool:
        return type(component).__name__ == "Image"

    def _collect_image_components(
        self,
        messages: list[BufferedMessage]
    ) -> list:
        images = []

        for message in messages:
            images.extend(
                message.image_components
            )

        return images

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
        raw_components = event.get_messages()

        has_text = bool(
            msg and msg.strip()
        )

        has_image = any(
            self._is_image_component(component)
            for component in raw_components
        )

        # 没有文字也没有图片才视为空消息
        if not has_text and not has_image:
            return

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
                    command_key = (
                        event.unified_msg_origin,
                        str(event.get_sender_id())
                    )

                    buffer = self.buffers.get(
                        command_key
                    )

                    if (
                        buffer
                        and buffer.flush_event
                        and not buffer.flush_event.is_set()
                    ):
                        if buffer.flush_task:
                            buffer.flush_task.cancel()

                        buffer.flush_event.set()

                        self.debug(
                            "[SmoothChat] active buffer flushed before command"
                        )

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

            image_components = [
                component
                for component in raw_components
                if self._is_image_component(component)
            ]

            current_message = BufferedMessage(
                timestamp=time.time(),
                sender_id=str(
                    event.get_sender_id()
                ),
                sender_name=event.get_sender_name(),
                components=components,
                image_components=image_components
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

            merged_images = self._collect_image_components(
                session.messages
            )

            if not merged_text and not merged_images:
                event.stop_event()
                return
            
            self.debug(
                f"[SmoothChat] merged_text={merged_text!r}"
            )

            # 将聚合结果写回第一条事件
            self._reconstruct_event(
                event,
                merged_text,
                merged_images
            )

            self.debug(
                f"[SmoothChat] event reconstructed, "
                f"text_length={len(merged_text)}, "
                f"image_count={len(merged_images)}"
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
