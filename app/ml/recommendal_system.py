import pandas as pd
from typing import Dict
import json
import pickle
import xgboost as xgb
import random
from sklearn.model_selection import train_test_split
from typing import Dict, List, Tuple, Set
import math


class Skynet():

    def __init__(self, fold_path: str = "./assets/"):

        self.fold_path = fold_path
        self.model_xgb = xgb.Booster()
        self.model_xgb.load_model(fold_path + "xgb_regressor")

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

    def clean_json(self, json_str: str) -> Dict:
        aska = json_str[::-1]
        edge_inda = - aska.index("}")
        if edge_inda != 0:
            json_str = json_str[:edge_inda] + ']'
        else:
            json_str += ']'
        return json.loads(json_str)

    def get_cost(self, obj) -> float:
        costs = obj['????????']
        if pd.isna(costs) or not costs:
            return 0
        else:
            all_costs = []
            for cost in self.clean_json(costs):
                all_costs.append(cost['Cost'])
            return all_costs[len(all_costs) // 2]

    def get_characts(self, obj) -> Set[Tuple[str]]:
        characts = set()
        if not pd.isna(obj['???????????????????????????? ??????']):
            for charact in self.clean_json(obj['???????????????????????????? ??????']):
                try:
                    characts.add((charact['Name'], charact['Value']))
                except KeyError:
                    pass
        return characts

    def one_based_connected(self, id: int, topn: int) -> List[int]:
        try:
            obj = self.id2obj[id]
        except KeyError:
            return [34172198 for i in range(topn)]
        characts = self.get_characts(obj)
        candidates = {}

        for category in self.top8[obj['??????????????????']]:
            if category != obj['??????????????????'] or len(self.top8[obj['??????????????????']]) < 2:
                category_candidates = self.categories[category]
                for cand in category_candidates:
                    if cand != id:
                        try:
                            connected_characts = self.get_characts(
                                self.id2obj[cand])
                        except KeyError:
                            connected_characts = set()
                        candidates[cand] = math.log(
                            max(1, len(characts & connected_characts))) / 8

        for category in self.top5[obj['??????????????????']]:
            if category != obj['??????????????????'] or len(self.top5[obj['??????????????????']]) < 2:
                category_candidates = self.categories[category]
                for cand in category_candidates:
                    if cand != id:
                        try:
                            connected_characts = self.get_characts(
                                self.id2obj[cand])
                        except KeyError:
                            connected_characts = set()
                        candidates[cand] = math.log(
                            max(1, len(characts & connected_characts))) / 5

        for category in self.top3[obj['??????????????????']]:
            if category != obj['??????????????????'] or len(self.top3[obj['??????????????????']]) < 2:
                category_candidates = self.categories[category]
                for cand in category_candidates:
                    if cand != id:
                        try:
                            connected_characts = self.get_characts(
                                self.id2obj[cand])
                        except KeyError:
                            connected_characts = set()
                        candidates[cand] = math.log(
                            max(1, len(characts & connected_characts))) / 3

        if not pd.isna(obj['???????????? ?????????????????? ?? ????????????????????']) and len(obj['???????????? ?????????????????? ?? ????????????????????'].strip()) > 0:
            st_others = obj['???????????? ?????????????????? ?? ????????????????????']
            st_others = self.clean_json(st_others)

            for prod in st_others:
                try:
                    connected_obj = prod['OtherSkuId']
                    connected_characts = self.get_characts(
                        self.decoder[connected_obj])
                    candidates[connected_obj] = math.log(
                        max(1, len(characts & connected_characts)))
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
            connected1 = self.connected[p1['??????????????????']]
            connected2 = self.connected[p2['??????????????????']]
            common = len(set(connected1) & set(connected2))
            features.append(common)
        except KeyError:
            features.append(0)

        if pd.isna(p2['??????-???? ?????????????????????? ????????????????????']):
            features.append(0)
        else:
            features.append(p2['??????-???? ?????????????????????? ????????????????????'])

        if pd.isna(p2['??????????????????']):
            features.append(0)
        else:
            features.append(p2['??????????????????'])

        difa1 = self.get_cost(p1) - self.mid_cost[p1['??????????????????']]
        difa2 = self.get_cost(p2) - self.mid_cost[p2['??????????????????']]
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
        if pd.isna(obj['???????????? ?????????????????? ?? ????????????????????']) or not obj['???????????? ?????????????????? ?? ????????????????????']:
            return set()
        res_set = set()
        for i in self.clean_json(obj['???????????? ?????????????????? ?? ????????????????????']):
            res_set.add(i['OtherSkuId'])
        return res_set

    def get_edge_prob(self, p1_ind: int, p2_ind: int) -> float:
        dat = [self.get_edge_features(p1_ind, p2_ind)]
        features = pd.DataFrame(dat,
                                columns=['common', 'contracts', 'views', 'tilt'] + list(self.needed_categories.keys()))
        return self.model_xgb.predict(xgb.DMatrix(features))[0]

    def get_unsorted_rec_edges(self, checked: List[int]) -> List[Tuple[int]]:
        possible_edges = []
        for good in checked[-5:]:
            for good_cand in self.one_based_connected(good, 10):
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
        try:
            obj = self.id2obj[id]
        except KeyError:
            return [34172198 for i in range(topn)]
        characts = self.get_characts(obj)
        candidates = {}
        category = obj['??????????????????']

        category_candidates = self.categories[category]

        for cand in category_candidates:
            if cand != id:
                try:
                    connected_characts = self.get_characts(self.id2obj[cand])
                except KeyError:
                    connected_characts = set()
                candidates[cand] = math.log(
                    max(1, len(characts & connected_characts))) / 2

        if not pd.isna(obj['???????????? ?????????????????? ?? ????????????????????']) and len(obj['???????????? ?????????????????? ?? ????????????????????'].strip()) > 0:
            st_others = obj['???????????? ?????????????????? ?? ????????????????????']
            st_others = self.clean_json(st_others)

            for prod in st_others:
                try:
                    connected_obj = prod['OtherSkuId']
                    connected_characts = self.get_characts(
                        self.decoder[connected_obj])
                    candidates[connected_obj] = math.log(
                        max(1, len(characts & connected_characts)))
                except KeyError:
                    pass

        sorted_candidates = sorted(candidates, key=lambda x: -candidates[x])
        return sorted_candidates[:topn]

    def recommend_supplement(self, last_ids: List[int], topn=15):
        candidates_edges = self.get_unsorted_rec_edges(last_ids)
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
