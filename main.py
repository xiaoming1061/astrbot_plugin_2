import asyncio
import json
import time
import inspect
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
            "Fact Layer Loaded"
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

        # 使用原始组件判断命令
        prefixes = tuple(
            self.config["command_prefixes"]
        )

        for comp in event.get_messages():

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

        msg = msg.strip()

        try:

            components = [
                self.serialize_component(c)
                for c in event.get_messages()
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

            logger.info(
                f"[BUFFER] size={len(buffer.messages)}"
            )

            if buffer.flush_task:
                buffer.flush_task.cancel()

            buffer.flush_task = asyncio.create_task(
                self._delayed_flush(key)
            )

            event.stop_event()

            logger.info(
                "[FACT] chat intercepted"
            )

        except Exception as e:
            logger.exception(e)

    async def _delayed_flush(
        self,
        key
    ):
        try:

            await asyncio.sleep(
                self.config["flush_timeout"]
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

                    contexts = json.loads(
                        conv.history
                    )[-history_limit:]

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
