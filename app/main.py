import asyncio, aio_pika, os, json
from ml.recommendal_system import Skynet

async def generate(body):
    predictor = Skynet()
    answer = {}
    if "item_id" in body:
        succedaneum = predictor.recommend_succedaneum(body["item_id"])
        answer["succedaneum"] = succedaneum
    if "seen" in body:
        supplement = await predictor.recommend_supplement(body["seen"])
        answer["supplement"] = supplement
    return answer


async def main(loop):
    mq_host = os.environ.get('MQ_HOST')
    connection = await aio_pika.connect_robust(mq_host, loop=loop)
    async with connection:
        queue_name = "requests_queue"
        routing_key = "answers_queue"
        channel = await connection.channel()

        queue = await channel.declare_queue(
            queue_name,
            auto_delete=True
        )   

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body)
                    answer = await generate(body)
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(answer).encode()
                        ),
                        routing_key=routing_key
                    )
                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
