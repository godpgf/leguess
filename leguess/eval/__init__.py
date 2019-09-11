from leguess.db import *
from .hr import HR


def eval_match(hdfs_db, act_list_path, tag_list_path, test_act_list_path, match_call_back, top_list, pred_act_type="download", pred_channel="default", exclude_act_type_list=["show"]):
    user_profile_db = MemoryUserProfileDB()
    # 读取用户的历史行为
    hdfs_db.read_user_act_list(act_list_path,
                               lambda user, act_list, channel_list, timestamp_list: user_profile_db.refresh_act_list(
                                   user, act_list, channel_list, timestamp_list), exclude_act_type_list=exclude_act_type_list)
    # 读取用户历史标签
    hdfs_db.read_user_tag_list(tag_list_path, lambda user, tag_list: user_profile_db.refresh_tag_list(user, tag_list))

    hr = HR(top_list)

    # 读取测试数据
    def _eval_match(user, act_list, channel_list, timestamp_list):
        item_size = 0
        for act, channel in zip(act_list, channel_list):
            if act.endswith(pred_act_type) and channel == pred_channel:
                item_size += 1
        for act, channel, timestamp in zip(act_list, channel_list, timestamp_list):
            if act.endswith(pred_act_type) and channel == pred_channel:
                # 开始召回
                act_list, channel_list, timestamp_list = user_profile_db.get_act_list(user)
                tag_list = user_profile_db.get_tag_list(user)
                pred_item_list = match_call_back(user, act_list, channel_list, timestamp_list, tag_list, timestamp)
                hr.add_sample(pred_item_list, act.split("@")[0], 1.0/item_size)
            user_profile_db.push_act(user, act, channel, timestamp)
    hdfs_db.read_user_act_list(test_act_list_path, _eval_match)
    hr.print_eval()
