import asyncio
import json

import pytest
from fastapi import FastAPI
from httpx import Client, AsyncClient
from pytest_httpx import to_response

from webhook_broker import WebhookBroker


@pytest.fixture(scope='module')
def fastapi_app():
    yield FastAPI()


@pytest.fixture(scope='module')
def broker():
    yield WebhookBroker(
        endpoint='http://localhost:8080',
        channel_id='sample-channel',
        channel_token='sample-channel-token',
        producer_id='sample-producer',
        producer_token='sample-producer-token',
        consumer_path='/consumer',
    )


def test_webhook_broker_producer_consumer(broker, fastapi_app):
    assert broker.can_produce == True
    assert broker.can_consume == True

    broker.register_fastapi(fastapi_app)

    @broker.processor
    def do_nothing(context):
        pass


def test_webhook_broker_producer(fastapi_app):
    broker = WebhookBroker.producer(
        endpoint='http://localhost:8080',
        channel_id='sample-channel',
        channel_token='sample-channel-token',
        producer_id='sample-producer',
        producer_token='sample-producer-token',
    )
    assert broker.can_produce == True
    assert broker.can_consume == False

    with pytest.raises(TypeError):
        broker.register_fastapi(fastapi_app)

    with pytest.raises(TypeError):
        @broker.processor
        def do_nothing(context):
            pass


def test_webhook_broker_consumer(fastapi_app):
    broker = WebhookBroker.consumer(
        endpoint='http://localhost:8080',
        consumer_path='/consumer',
    )
    assert broker.can_produce == False
    assert broker.can_consume == True

    broker.register_fastapi(fastapi_app)

    @broker.processor
    def do_nothing(context):
        pass


@pytest.mark.asyncio
async def test_broadcast(broker, fastapi_app, httpx_mock):
    broker.register_fastapi(fastapi_app)

    @broker.processor
    def do_nothing(context):
        pass

    def broadcast_callback(request, ext):
        assert request.method == 'POST'
        assert request.url == 'http://localhost:8080/channel/sample-channel/broadcast'

        request_body_decoded = json.loads(request.read())
        assert request_body_decoded == {
            'processor_name': 'test_webhook_broker.test_broadcast.<locals>.do_nothing',
            'payload': {'x': 1, 'y': 2},
        }
        request.headers.get('content-type') == 'application/json'
        request.headers.get('x-broker-producer-id') == 'sample-producer'
        request.headers.get('x-broker-producer-token') == 'sample-producer-token'
        request.headers.get('x-broker-channel-token') == 'sample-channel-token'
        return to_response(status_code=201)

    httpx_mock.add_callback(broadcast_callback)

    await do_nothing.broadcast_async().send(x=1, y=2)
    do_nothing.broadcast().send(x=1, y=2)


@pytest.mark.asyncio
async def test_processor(broker, fastapi_app):
    fake_payload = {'x': 1, 'y': 2}

    broker.register_fastapi(fastapi_app)

    @broker.processor
    async def sum(context, **payload):
        assert context.request_id == 'fake-sum-request'
        assert payload == fake_payload

    @broker.processor
    def mul(context, **payload):
        assert context.request_id == 'fake-mul-request'
        assert payload == fake_payload

    async with AsyncClient(app=fastapi_app, base_url='http://localhost') as client:
        for processor_name in ('sum', 'mul'):
            await client.post(
                broker.consumer_path,
                json={
                    'processor_name': f'test_webhook_broker.test_processor.<locals>.{processor_name}',
                    'payload': fake_payload,
                },
                headers={'X-Request-ID': f'fake-{processor_name}-request'},
            )
