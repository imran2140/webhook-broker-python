import random
from typing import Optional

from fastapi import FastAPI, Request

from webhook_broker import WebhookBroker, RequestContext


app = FastAPI()

broker = WebhookBroker(
    endpoint='http://localhost:8080',
    channel_id='sample-channel',
    channel_token='sample-channel-token',
    producer_id='sample-producer',
    producer_token='sample-producer-token',
    consumer_path='/consumer',
)
broker.register_fastapi(app)


@app.get('/')
async def index(request: Request) -> dict:
    x, y = random.randint(1, 10), random.randint(1, 10)
    request_id = request.headers.get('x-request-id')
    await sum.broadcast_async().send(x=1, y=2)
    await mul.broadcast_async().send(x=1, y=2)
    return {'x': x, 'y': y}


@broker.processor
async def sum(context: RequestContext, /, x: int, y: int) -> dict:
    result = {'sum': x + y}
    print(context.request_id, result)
    return result


@broker.processor
def mul(context: RequestContext, /, x: int, y: int) -> dict:
    result = {'mul': x * y}
    print(context.request_id, result)
    return result
