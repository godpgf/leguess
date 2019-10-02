from leguess.db import *
from .hr import HR
import numpy as np


def eval_recm(hdfs_db, user_profile_db, test_act_list_path, recm_call_back, top_list, pred_act_type="download", pred_channel="default", exclude_act_type_list=["show"], filter_item_set=None):
    # 评估召回和排序的命中率---------------------------------------------------------------
    match_hr = HR(top_list)
    rank_hr = HR([top_list[0]])

    # 读取测试数据
    def _eval_recm(user, act_list, channel_list, timestamp_list):
        item_size = 0
        for act, channel in zip(act_list, channel_list):
            if act.endswith(pred_act_type) and channel == pred_channel and (filter_item_set is None or act.split("@")[0] in filter_item_set):
                item_size += 1
        for act, channel, timestamp in zip(act_list, channel_list, timestamp_list):
            if act.endswith(pred_act_type) and channel == pred_channel and (filter_item_set is None or act.split("@")[0] in filter_item_set):
                # 开始召回
                act_list, channel_list, timestamp_list = user_profile_db.get_act_list(user)
                tag_list = user_profile_db.get_tag_list(user)
                match_item, rank_percent = recm_call_back(user, act_list, channel_list, timestamp_list, tag_list, timestamp)
                match_hr.add_sample(match_item, act.split("@")[0], 1.0/item_size)
                ids = np.argsort(-rank_percent)
                rank_hr.add_sample(match_item[ids], act.split("@")[0], 1.0/item_size)
            user_profile_db.push_act(user, act, channel, timestamp)
    hdfs_db.read_user_act_list(test_act_list_path, _eval_recm, exclude_act_type_list=exclude_act_type_list)
    print("match------------------------------------------")
    match_hr.print_eval()
    print("rank-------------------------------------------")
    rank_hr.print_eval()


