import asyncio, aioredis, pickle, os, json

async def main():
    redis = aioredis.from_url(
        os.environ.get('REDIS_HOST'), encoding="utf-8", decode_responses=True
    )
    with open('./assets/top3.pickle', 'rb') as handle:
        top3 = json.dumps(pickle.load(handle))
        await redis.set("top3", top3)
    with open('./assets/top8.pickle', 'rb') as handle:
        top8 = json.dumps(pickle.load(handle))
        await redis.set("top8", top8)
    with open('./assets/top5.pickle', 'rb') as handle:
        top5 = json.dumps(pickle.load(handle))
        await redis.set("top5", top5)


asyncio.run(main())
