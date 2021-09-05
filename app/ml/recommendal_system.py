import pandas as pd
from typing import Dict
import json
import pickle
import xgboost as xgb
import random
from sklearn.model_selection import train_test_split
from typing import Dict, List, Tuple, Set
import math
import os, aioredis


class Skynet():

    def __init__(self, fold_path: str = "./assets/"):

        self.fold_path = fold_path
        self.model_xgb = xgb.Booster()
        self.redis = aioredis.from_url(
            os.environ.get('REDIS_HOST'), encoding="utf-8", decode_responses=True
        )

        with open(self.fold_path + 'id2obj.pickle', 'rb') as handle:
            self.id2obj = pickle.load(handle)

        with open(self.fold_path + 'categories.pickle', 'rb') as handle:
            self.categories = pickle.load(handle)

        with open(self.fold_path + 'connected_keys.pickle', 'rb') as handle:
            self.connected_keys = pickle.load(handle)

        with open(self.fold_path + 'decoder.pickle', 'rb') as handle:
            self.decoder = pickle.load(handle)

        with open(self.fold_path + 'top3.pickle', 'rb') as handle:
            self.top3 = pickle.load(handle)

        with open(self.fold_path + 'top5.pickle', 'rb') as handle:
            self.top5 = pickle.load(handle)

        with open(self.fold_path + 'top8.pickle', 'rb') as handle:
            self.top8 = pickle.load(handle)

        with open(self.fold_path + 'median_cost.pickle', 'rb') as handle:
            self.mid_cost = pickle.load(handle)

        with open(self.fold_path + 'charact_dict.pickle', 'rb') as handle:
            self.all_characts = pickle.load(handle)

        with open(self.fold_path + 'connected.pickle', 'rb') as handle:
            self.connected = pickle.load(handle)

        with open(self.fold_path + 'needed_categories.pickle', 'rb') as handle:
            self.needed_categories = pickle.load(handle)
        
        self.model_xgb.load_model(fold_path + "xgb_regressor")

    def clean_json(self, json_str: str) -> Dict:
        aska = json_str[::-1]
        edge_inda = - aska.index("}")
        if edge_inda != 0:
            json_str = json_str[:edge_inda] + ']'
        else:
            json_str += ']'
        return json.loads(json_str)

    def get_cost(self, obj) -> float:
        costs = obj['Цена']
        if pd.isna(costs) or not costs:
            return 0
        else:
            all_costs = []
            for cost in self.clean_json(costs):
                all_costs.append(cost['Cost'])
            return all_costs[len(all_costs) // 2]

    def get_characts(self, obj) -> Set[Tuple[str]]:
        characts = set()
        if not pd.isna(obj['Характеристики СТЕ']):
            for charact in self.clean_json(obj['Характеристики СТЕ']):
                try:
                    characts.add((charact['Name'], charact['Value']))
                except KeyError:
                    pass
        return characts

    async def one_based_connected(self, id: int, topn: int) -> List[int]:
        obj = self.id2obj[id]
        characts = self.get_characts(obj)
        candidates = {}

        # raw_top8 = await self.redis.get("top8")
        # self.top8 = json.loads(raw_top8)

        # raw_top5 = await self.redis.get("top5")
        # self.top5 = json.loads(raw_top5)

        # raw_top3 = await self.redis.get("top3")
        # self.top3 = json.loads(raw_top3)

        for category in self.top8[obj['Категория']]:
            category_candidates = self.categories[category]
            for cand in category_candidates:
                connected_characts = self.get_characts(self.id2obj[cand])
                candidates[cand] = math.log(max(1, len(characts & connected_characts))) / 8

        for category in self.top5[obj['Категория']]:
            category_candidates = self.categories[category]
            for cand in category_candidates:
                connected_characts = self.get_characts(self.id2obj[cand])
                candidates[cand] = math.log(max(1, len(characts & connected_characts))) / 5

        for category in self.top3[obj['Категория']]:
            category_candidates = self.categories[category]
            for cand in category_candidates:
                connected_characts = self.get_characts(self.id2obj[cand])
                candidates[cand] = math.log(max(1, len(characts & connected_characts))) / 3

        if not pd.isna(obj['Другая продукция в контрактах']) and len(obj['Другая продукция в контрактах'].strip()) > 0:
            st_others = obj['Другая продукция в контрактах']
            st_others = self.clean_json(st_others)

            for prod in st_others:
                try:
                    connected_obj = prod['OtherSkuId']
                    connected_characts = self.get_characts(self.decoder[connected_obj])
                    candidates[connected_obj] = math.log(max(1, len(characts & connected_characts)))
                except KeyError:
                    pass

        sorted_candidates = sorted(candidates, key=lambda x: -candidates[x])
        return sorted_candidates[:topn]

    def get_edge_features(self, p1_ind: int, p2_ind: int) -> List:
        try:
            p1 = self.id2obj[p1_ind]
            p2 = self.id2obj[p2_ind]
        except KeyError:
            return [0 for i in range(124)]

        features = []

        try:
            connected1 = self.connected[p1['Категория']]
            connected2 = self.connected[p2['Категория']]
            common = len(set(connected1) & set(connected2))
            features.append(common)
        except KeyError:
            features.append(0)

        if pd.isna(p2['Кол-во заключенных контрактов']):
            features.append(0)
        else:
            features.append(p2['Кол-во заключенных контрактов'])

        if pd.isna(p2['Просмотры']):
            features.append(0)
        else:
            features.append(p2['Просмотры'])

        difa1 = self.get_cost(p1) - self.mid_cost[p1['Категория']]
        difa2 = self.get_cost(p2) - self.mid_cost[p2['Категория']]
        features.append(abs(difa1 - difa2))

        characts1 = self.get_characts(p1)
        characts2 = self.get_characts(p2)
        same_characts = characts1 & characts2
        one_hotted = [0 for i in range(len(self.needed_categories))]
        for ch_name, ch_val in same_characts:
            if ch_name in self.needed_categories:
                one_hotted[self.needed_categories[ch_name]] = 1
        features += one_hotted

        return features

    def get_connected_goods(self, id: int) -> Set[int]:
        try:
            obj = self.id2obj[id]
        except KeyError:
            return set()
        if pd.isna(obj['Другая продукция в контрактах']) or not obj['Другая продукция в контрактах']:
            return set()
        res_set = set()
        for i in self.clean_json(obj['Другая продукция в контрактах']):
            res_set.add(i['OtherSkuId'])
        return res_set

    def get_edge_prob(self, p1_ind: int, p2_ind: int) -> float:
        dat = [self.get_edge_features(p1_ind, p2_ind)]
        features = pd.DataFrame(dat,
                                columns=['common', 'contracts', 'views', 'tilt'] + list(self.needed_categories.keys()))
        return self.model_xgb.predict(xgb.DMatrix(features))[0]

    async def get_unsorted_rec_edges(self, checked: List[int]) -> List[Tuple[int]]:
        possible_edges = []
        for good in checked[-5:]:
            for good_cand in await self.one_based_connected(good, 10):
                possible_edges.append((good, good_cand))
        return possible_edges

    def get_unsorted_rec_edges_succedaneum(self, good: int) -> List[Tuple[int]]:
        possible_edges = []
        for good_cand in self.one_based_connected_succedaneum(good):
            possible_edges.append((good, good_cand))
        return possible_edges

    def rang_edges(self, edges: List[Tuple[int]]) -> List[Tuple[int]]:
        return sorted(edges, key=lambda x: - self.get_edge_prob(x[0], x[1]))

    def one_based_connected_succedaneum(self, id: int, topn: int = 20) -> List[int]:
        obj = self.id2obj[id]
        characts = self.get_characts(obj)
        candidates = {}
        category = obj['Категория']

        category_candidates = self.categories[category]

        for cand in category_candidates:
            if cand != id:
                connected_characts = self.get_characts(self.id2obj[cand])
                candidates[cand] = math.log(max(1, len(characts & connected_characts))) / 2

        if not pd.isna(obj['Другая продукция в контрактах']) and len(obj['Другая продукция в контрактах'].strip()) > 0:
            st_others = obj['Другая продукция в контрактах']
            st_others = self.clean_json(st_others)

            for prod in st_others:
                try:
                    connected_obj = prod['OtherSkuId']
                    connected_characts = self.get_characts(self.decoder[connected_obj])
                    candidates[connected_obj] = math.log(max(1, len(characts & connected_characts)))
                except KeyError:
                    pass

        sorted_candidates = sorted(candidates, key=lambda x: -candidates[x])
        return sorted_candidates[:topn]

    async def recommend_supplement(self, last_ids: List[int], topn=15):
        candidates_edges = await self.get_unsorted_rec_edges(last_ids)
        recommended_edges = self.rang_edges(candidates_edges)[:topn]
        recommended_goods = [i[1] for i in recommended_edges]
        unique = []
        go_set = set()
        for good in recommended_goods:
            if good not in go_set:
                go_set.add(good)
                unique.append(good)
        return unique

    def recommend_succedaneum(self, last_id: int, topn: int = 10) -> List[int]:
        candidates_edges = self.get_unsorted_rec_edges_succedaneum(last_id)
        recommended_edges = self.rang_edges(candidates_edges)[:topn]
        recommended_goods = [i[1] for i in recommended_edges]
        unique = []
        go_set = set()
        for good in recommended_goods:
            if good not in go_set:
                go_set.add(good)
                unique.append(good)
        return unique

# USAGE EXAMPLE
# predictor = Skynet()
# supplement = predictor.recommend_supplement([1257331, 1205312, 1228720])
# succedaneum = predictor.recommend_succedaneum(1228720)
# print(supplement)
# print(succedaneum)
