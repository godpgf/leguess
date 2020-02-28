from .base_match import BaseMatch
import numpy as np


class ItemBaseMatch(BaseMatch):
    def __init__(self, i2i_dict, popular_rank):
        self.i2i_dict = i2i_dict
        self.popular_rank = popular_rank
        # self.sort_dict = {i: sorted(i_w.items(), key=lambda c: c[1], reverse=True) for i, i_w in i2i_dict.items()}

    def match_item(self, data_source_factory, match_size):
        # 创建数据源，需要的数据名是行为序列id和排除序列id
        data_source = data_source_factory(["p_act_ids", "exclude_ids"])
        data_iter = iter(data_source)
        match_item_array = np.zeros([len(data_source), match_size], np.int32)
        match_percent_array = np.zeros([len(data_source), match_size], np.float32)
        line_id = 0
        while True:
            try:
                data_value_list = next(data_iter)
                act_ids, exclude_ids = data_value_list[0], data_value_list[1]
                match_list = self._match_item(act_ids, exclude_ids, match_size)
                # 召回
                last_p = 1.0
                for i in range(len(match_list)):
                    match_item_array[line_id][i] = match_list[i][0]
                    match_percent_array[line_id][i] = match_list[i][1]
                    last_p = match_list[i][1]
                # 如果数量不足，补充排行榜--------------------------
                if len(match_list) < match_size:
                    exclude_ids = set(list(exclude_ids) + match_list)
                    id = len(match_list)
                    pid = 0
                    while id < match_size:
                        if self.popular_rank[pid][0] in exclude_ids:
                            pid+=1
                            continue
                        match_item_array[line_id][id] = self.popular_rank[pid][0]
                        match_percent_array[line_id][id] = self.popular_rank[pid][1] * last_p
                        id += 1
                line_id += 1
            except StopIteration as e:
                break
        return match_item_array, match_percent_array

    def _match_item(self, act_ids, exclude_ids, match_size):
        exclude_ids = set(exclude_ids)
        rank = dict()
        id = 0
        while len(rank) < match_size:
            # 正常推荐
            has_value = False
            for i in act_ids:
                if i in self.i2i_dict:
                    sort_list = self.i2i_dict[i]["sort"]
                    if id < len(sort_list):
                        j, wj = sort_list[id]
                        if j not in exclude_ids:
                            rank[j] = rank.get(j, 0) + wj
                        has_value = True
            if has_value is False:
                break
            id += 1
        return sorted(rank.items(), key=lambda c: c[1], reverse=True)[:match_size]
