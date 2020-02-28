from leguess.db import *
from .hr import HR
import numpy as np


def eval_recm(hdfs_db, user_profile_db, test_act_list_path, recm_call_back, top_list, pred_act_type="download", pred_channel="jingxuan", exclude_act_type_list=["show"], offline_time=1*60*60, filter_item_set=None):
    # 评估召回和排序的命中率---------------------------------------------------------------
    offline_match_hr = HR(top_list)
    offline_rank_hr = HR([top_list[0]])
    online_match_hr = HR(top_list)
    online_rank_hr = HR([top_list[0]])
    default_match_hr = HR([top_list[0]])

    # 读取测试数据
    def _eval_recm(user, act_list, channel_list, org_list, timestamp_list):
        item_size = 0
        for act, channel in zip(act_list, channel_list):
            if act.split('@')[1] == pred_act_type and channel == pred_channel and (filter_item_set is None or act.split("@")[0] in filter_item_set):
                item_size += 1
        for act, channel, org, timestamp in zip(act_list, channel_list, org_list, timestamp_list):
            if act.split('@')[1] == pred_act_type and channel == pred_channel and (filter_item_set is None or act.split("@")[0] in filter_item_set):
                # 开始召回
                act_list, channel_list, org_list, timestamp_list = user_profile_db.get_act_list(user)
                tag_list = user_profile_db.get_tag_list(user)

                if len(act_list) == 0:
                    match_item, rank_percent = recm_call_back(user, act_list, channel_list, org_list, timestamp_list,
                                                              tag_list, timestamp, True)
                    default_match_hr.add_sample(match_item, act.split("@")[0], 1.0/item_size)
                else:
                    if timestamp - timestamp_list[-1] > offline_time:
                        match_item, rank_percent = recm_call_back(user, act_list, channel_list, org_list,
                                                                  timestamp_list,
                                                                  tag_list, timestamp, True)
                        offline_match_hr.add_sample(match_item, act.split("@")[0], 1.0/item_size)
                        ids = np.argsort(-rank_percent)
                        offline_rank_hr.add_sample(match_item[ids], act.split("@")[0], 1.0 / item_size)
                    else:
                        match_item, rank_percent = recm_call_back(user, act_list, channel_list, org_list,
                                                                  timestamp_list,
                                                                  tag_list, timestamp, False)
                        online_match_hr.add_sample(match_item, act.split("@")[0], 1.0/item_size)
                        ids = np.argsort(-rank_percent)
                        online_rank_hr.add_sample(match_item[ids], act.split("@")[0], 1.0 / item_size)

            user_profile_db.push_act(user, act, channel, org, timestamp)
    hdfs_db.read_user_act_list(test_act_list_path, _eval_recm, exclude_act_type_list=exclude_act_type_list)
    print("match[default,offline,online]------------------------------------------")
    default_match_hr.print_eval()
    offline_match_hr.print_eval()
    online_match_hr.print_eval()
    print("rank[offline,online]-------------------------------------------")
    offline_rank_hr.print_eval()
    online_rank_hr.print_eval()


