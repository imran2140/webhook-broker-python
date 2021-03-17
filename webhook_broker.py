from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from functools import update_wrapper, cached_property
from typing import Any, Callable, Dict, Optional

import httpx
try:
    from fastapi import Body, FastAPI, HTTPException, Request
except ImportError:
    FastAPI = None  # type: ignore


@dataclass
class RequestContext:
    request_id: Optional[str]


@dataclass
class BaseBroadcastRequest:
    processor_name: str
    broker: WebhookBroker
    request_id: Optional[str] = None

    def with_request_id(self, request_id: str) -> BaseBroadcastRequest:
        self.request_id = request_id
        return self


class BroadcastRequest(BaseBroadcastRequest):
    def send(self, **payload) -> httpx.Response:
        return self.broker.broadcast(self.processor_name, self.request_id, payload)


class AsyncBroadcastRequest(BaseBroadcastRequest):
    async def send(self, **payload) -> httpx.Response:
        return await self.broker.broadcast_async(self.processor_name, self.request_id, payload)


class WebhookBroker:
    def __init__(
        self,
        endpoint: str,
        channel_id: str,
        channel_token: str,
        producer_id: str,
        producer_token: str,
        consumer_path: str,
    ):
        self.endpoint = endpoint
        self.channel_id = channel_id
        self.channel_token = channel_token
        self.channel_url = f'{self.endpoint}/channel/{self.channel_id}'
        self.broadcast_url = f'{self.channel_url}/broadcast'
        self.producer_id = producer_id
        self.producer_token = producer_token
        self.consumer_path = consumer_path
        self.processors: Dict[str, Callable] = {}

    @classmethod
    def consumer(
        cls,
        endpoint: str,
        consumer_path: str,
    ):
        return cls(
            endpoint=endpoint, 
            consumer_path=consumer_path,
            channel_id='',
            channel_token='',
            producer_id='',
            producer_token='',
        )

    @classmethod
    def producer(
        cls,
        endpoint: str,
        channel_id: str,
        channel_token: str,
        producer_id: str,
        producer_token: str,
    ):
        return cls(
            endpoint=endpoint, 
            channel_id=channel_id,
            channel_token=channel_token,
            producer_id=producer_id,
            producer_token=producer_token,
            consumer_path='',
        )

    @cached_property
    def can_consume(self) -> bool:
        return bool(self.consumer_path)

    @cached_property
    def can_produce(self) -> bool:
        return bool(self.channel_id and self.channel_token and self.producer_id and self.producer_token)

    @cached_property
    def channel_url(self) -> str:
        if not self.channel_id:
            return ''

        return f'{self.endpoint}/channel/{self.channel_id}'

    @cached_property
    def broadcast_url(self) -> str:
        if not self.channel_url:
            return ''

        return f'{self.channel_url}/broadcast'

    @cached_property
    def client(self):
        return httpx.Client()

    @cached_property
    def async_client(self):
        return httpx.AsyncClient()

    def broadcast(self, processor_name: str, request_id: Optional[str], payload: Dict[str, Any]) -> httpx.Response:
        request = self._build_request(processor_name, request_id, payload)
        response = self.client.send(request)
        return response

    async def broadcast_async(self, processor_name: str, request_id: Optional[str], payload: Dict[str, Any]) -> httpx.Response:
        request = self._build_request(processor_name, request_id, payload)
        response = await self.async_client.send(request)
        return response

    def _build_request(self, processor_name: str, request_id: Optional[str], payload: Dict[str, Any]) -> httpx.Request:
        if not self.can_produce:
            raise TypeError('Not configured as producer')

        headers = {
            'X-Broker-Producer-ID': self.producer_id,
            'X-Broker-Producer-Token': self.producer_token,
            'X-Broker-Channel-Token': self.channel_token,
        }
        if request_id:
            headers['X-Request-Id'] = request_id

        broadcast_payload = {
            'processor_name': processor_name,
            'payload': payload,
        }

        return httpx.Request('POST', self.broadcast_url, json=broadcast_payload, headers=headers)

    if FastAPI:
        def register_fastapi(self, app: FastAPI):
            if not self.can_consume:
                raise TypeError('Not configured as consumer')

            async def controller(request: Request, processor_name: str = Body(...), payload: Dict[str, Any] = Body(...)):
                if processor_name not in self.processors:
                    raise HTTPException(status_code=400, detail="Processor not found")

                processor = self.processors[processor_name]
                context = RequestContext(
                    request_id=request.headers.get('x-request-id'),
                )
                if processor.awaitable:
                    await processor(context, **payload)
                else:
                    processor(context, **payload)

            app.post(self.consumer_path)(controller)

    def processor(self, fn: Callable) -> Processor:
        if not self.can_consume:
            raise TypeError('Not configured as consumer')

        processor_obj: Processor = update_wrapper(Processor(self, fn), fn)    # type: ignore
        self.processors[processor_obj.name] = processor_obj
        return processor_obj

    def __del__(self):
        self.client.close()
        asyncio.get_event_loop().run_until_complete(self.async_client.aclose())


class Processor:
    def __init__(self, broker: WebhookBroker, fn: Callable):
        self.name = f'{fn.__module__}.{fn.__qualname__}'
        self.broker = broker
        self.fn = fn
        self.awaitable = inspect.iscoroutinefunction(self.fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def broadcast(self) -> BroadcastRequest:
        return BroadcastRequest(self.name, self.broker)

    def broadcast_async(self) -> AsyncBroadcastRequest:
        return AsyncBroadcastRequest(self.name, self.broker)
