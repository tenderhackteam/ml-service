import asyncio
import aio_pika
import json
import os

async def main(loop):
    connection = await aio_pika.connect_robust(
        os.environ.get('MQ_HOST'), loop=loop
    )

    async with connection:
        routing_key = "requests_queue"
        queue_name = "answers_queue"
        message = {
            "seen": [1257331, 1205312, 1228720],
            "compare": [4],
            "cart": [5, 6],
            "item_id": 1228720,
            "top_n": 10
        }

        channel = await connection.channel()

        queue = await channel.declare_queue(
            queue_name,
            auto_delete=True
        )

        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode()
            ),
            routing_key=routing_key
        )
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(message.body)
                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
