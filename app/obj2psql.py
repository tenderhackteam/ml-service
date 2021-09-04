import pickle, asyncio, aioredis, os, json, pandas
import numpy as np
from sqlalchemy import select
from db.db_setup import Item, Category, session

async def main():
    redis = aioredis.from_url(
        os.environ.get('REDIS_HOST'), encoding="utf-8", decode_responses=True
    )

    with open('./assets/id2obj.pickle', 'rb') as handle:
        id2obj_raw = pandas.DataFrame(pickle.load(handle))
        id2obj_raw = id2obj_raw.replace({np.nan: None})
        id2obj = id2obj_raw.to_dict()
        # print(id2obj[20528973])
        for i in list(id2obj.keys()):
            item = id2obj[i]
            print(item["Идентификатор СТЕ"])
            regions = []
            flag = False
            result = session.execute(select(Category).where(Category.name == item["Категория"]))
            results = result.all()
            if len(results) == 0:
                category = Category(name=item["Категория"])
                flag = True
            else:
                category = results[0]
            for region in json.loads(item["Регионы поставки"]):
                regions.append(region["Name"])
            new_item = Item(
                cte_id=item["Идентификатор СТЕ"],
                cte_name=item["Наименование СТЕ"],
                description=item["Описание"],
                cte_props=item["Характеристики СТЕ"],
                regions=regions,
                made_contracts=item["Кол-во заключенных контрактов"],
                suppliers=item["Поставщики"],
                country=item["Страна происхождения"],
                other_items_in_contracts=item["Другая продукция в контрактах"],
                cpgz_id=item["Идентификатор КПГЗ"],
                cpgz_code=item["Код КПГЗ"],
                model=item["Модель"],
                price=item["Цена"]
            )
            await redis.set(i, item["Просмотры"])
            category.items.append(new_item)
            if flag:
                session.add(category)
            session.commit()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()
