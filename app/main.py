import asyncio, aio_pika, os, json
from functools import partial

from ml.recommendal_system import Skynet


async def generate(body):
    predictor = Skynet()
    answer = {}
    if "item_id" in body:
        succedaneum = predictor.recommend_succedaneum(body["item_id"])
        answer["succedaneum"] = succedaneum
    if "seen" in body:
        supplement = predictor.recommend_supplement(body["seen"])
        answer["supplement"] = supplement
    return answer


async def on_message(exchange: aio_pika.Exchange, message: aio_pika.IncomingMessage):
    async with message.process():
        body = json.loads(message.body)
        answer = await generate(body)

        await exchange.publish(
            aio_pika.Message(
                body=json.dumps(answer).encode(), correlation_id=message.correlation_id
            ),
            routing_key=message.reply_to,
        )


async def main(loop):
    mq_host = os.environ.get("MQ_HOST")
    connection = await aio_pika.connect_robust(mq_host, loop=loop)
    queue_name = "rpc_queue"
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name)
    await queue.consume(partial(on_message, channel.default_exchange))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()
