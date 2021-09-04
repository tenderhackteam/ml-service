import pandas as pd
from typing import Dict
import json
import pickle
import aioredis
import os


async def clean_json(json_str: str) -> Dict:
    aska = json_str[::-1]
    edge_inda = - aska.index("}")
    if edge_inda != 0:
        json_str = json_str[:edge_inda] + ']'
    else:
        json_str += ']'
    return json.loads(json_str)


async def get_characts(obj):
    characts = set()
    if not pd.isna(obj['Характеристики СТЕ']):
        for charact in await clean_json(obj['Характеристики СТЕ']):
            try:
                characts.add((charact['Name'], charact['Value']))
            except KeyError:
                pass
    return characts


async def one_based_connected(id: int, topn: int):
    redis = aioredis.from_url(
        os.environ.get('REDIS_HOST'), encoding="utf-8", decode_responses=True
    )

    obj = id2obj[id]
    characts = await get_characts(obj)
    candidates = {}
    raw_top8 = await redis.get("top8")
    top8 = json.loads(raw_top8)
    for category in top8[obj['Категория']]:
        category_candidates = categories[category]
        for cand in category_candidates:
            connected_characts = await get_characts(id2obj[cand])
            candidates[cand] = (len(characts & connected_characts)) ** (0.5) * 1 / 8

    raw_top5 = await redis.get("top5")
    top5 = json.loads(raw_top5)
    for category in top5[obj['Категория']]:
        category_candidates = categories[category]
        for cand in category_candidates:
            connected_characts = await get_characts(id2obj[cand])
            candidates[cand] = (len(characts & connected_characts)) ** (0.5) * 1 / 5

    raw_top3 = await redis.get("top3")
    top3 = json.loads(raw_top3)
    for category in top3[obj['Категория']]:
        category_candidates = categories[category]
        for cand in category_candidates:
            connected_characts = await get_characts(id2obj[cand])
            candidates[cand] = (len(characts & connected_characts)) ** (0.5) * 1 / 3

    if not pd.isna(obj['Другая продукция в контрактах']) and len(obj['Другая продукция в контрактах'].strip()) > 0:
        st_others = obj['Другая продукция в контрактах']
        edge_inda = -st_others[::-1].index("}")
        if edge_inda != 0:
            st_others = st_others[:edge_inda] + ']'
        else:
            st_others += ']'
        st_data = json.loads(st_others)

        for prod in st_data:
            try:
                connected_obj = prod['OtherSkuId']
                connected_characts = await get_characts(decoder[connected_obj])
                candidates[connected_obj] = (len(characts & connected_characts)) ** (0.5)
            except KeyError:
                pass

    sorted_candidates = sorted(candidates, key=lambda x: -candidates[x])
    return sorted_candidates[:topn]


with open('./assets/id2obj.pickle', 'rb') as handle:
    id2obj = pickle.load(handle)

with open('./assets/categories.pickle', 'rb') as handle:
    categories = pickle.load(handle)

with open('./assets/decoder.pickle', 'rb') as handle:
    decoder = pickle.load(handle)

# USAGE EXAMPLE
#ids = one_based_connected(34172198, 10)
#print(ids[0])
