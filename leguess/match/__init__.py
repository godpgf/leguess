from .itembase_match import ItemBaseMatch
from .modelbase_match import ModelBaseMatch


# 合并多个召回结果
def merge_match(match_res_list):
    assert len(match_res_list) > 0
    if len(match_res_list) == 1:
        return match_res_list[0]
    match_size = len(match_res_list[0])
    match_set = set()
    match_list = []
    id = 0
    while len(match_list) < match_size:
        assert id <= len(match_list)
        for match_res in match_res_list:
            if match_res[id] not in match_set:
                match_set.add(match_res[id])
                match_list.append(match_res[id])
                if len(match_list) == match_size:
                    break
        id += 1
    return match_list


# 切分相似行为，以便编码成vector
def split_near_act(act_list, l_skip, r_skip):
    near_act_list = []
    target_act_list = []
    for i in range(len(act_list)):
        near_acts = []
        for j in range(max(0, i - l_skip), i):
            near_acts.append(act_list[j])
        for j in range(i+1, min(len(act_list), i + r_skip + 1)):
            near_acts.append(act_list[j])
        if len(near_acts) > 0:
            target_act_list.append(act_list[i])
            near_act_list.append(near_acts)
    return target_act_list, near_act_list
