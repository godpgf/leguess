from .itembase_match import ItemBaseMatch
from .modelbase_match import ModelBaseMatch


def merge_match(match_res_list):
    assert len(match_res_list) > 0
    if len(match_res_list) == 1:
        return match_res_list[0]
    match_size = len(match_res_list[0])
    match_set = set()
    match_list = []
    id = 0
    while len(match_list) < match_size:
        for match_res in match_res_list:
            if match_res[id] not in match_set:
                match_set.add(match_res[id])
                match_list.append(match_res[id])
                if len(match_list) == match_size:
                    break
        id += 1
    return match_list
