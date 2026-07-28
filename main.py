import asyncio
import json
import time
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


FLUSH_TIMEOUT = 5.0


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

    def __init__(self, context: Context):
        super().__init__(context)

        self.buffers = {}

        logger.info(
            "Fact Layer V3.3 Final Loaded"
        )

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

            #
            # 阻断AstrBot默认LLM流程
            #
            event.stop_event()

        except Exception as e:

            logger.exception(e)

    async def _delayed_flush(
        self,
        key
    ):

        try:

            await asyncio.sleep(
                FLUSH_TIMEOUT
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

            await self._process_batch(
                batch
            )

        except Exception as e:

            logger.exception(e)

    async def _process_batch(
        self,
        batch
    ):

        umo = batch["session"]["umo"]

        provider_id = (
            await self.context
            .get_current_chat_provider_id(
                umo=umo
            )
        )

        prompt = self._build_fact_prompt(
            batch
        )

        logger.info(
            f"[LLM] provider={provider_id}"
        )

        logger.info(
            "\n========== FACT PROMPT ==========\n"
        )

        logger.info(prompt)

        logger.info(
            "\n================================\n"
        )

        llm_resp = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
        )

        answer = llm_resp.completion_text

        logger.info(
            f"[LLM RESPONSE] {answer}"
        )

        message_chain = MessageChain()

        message_chain.chain.append(
            Comp.Plain(answer)
        )

        await self.context.send_message(
            umo,
            message_chain
        )

        logger.info(
            "[SEND] success"
        )

    def _build_fact_prompt(
        self,
        batch
    ):

        return (
            "<FACT_DATA>"
            + json.dumps(
                batch,
                ensure_ascii=False,
                separators=(",", ":")
            )
            + "</FACT_DATA>"
        )

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
