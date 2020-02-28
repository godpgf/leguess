class UserProfileDB(object):
    def __init__(self, act_filter=None):
        self.act_filter = act_filter

    def get_act_list(self, user_id):
        # 返回行为序列，频道序列，时间序列
        return None, None, None, None

    def get_tag_list(self, user_id):
        # 返回标签序列
        return None

    def refresh_tag_list(self, user_id, tag_list):
        pass

    def push_act(self, user_id, act_name, timestamp):
        pass

    def refresh_act_list(self, user_id, act_list, channel_list, timestamp_list):
        pass


class MemoryUserProfileDB(UserProfileDB):
    def __init__(self, act_filter=None):
        super(MemoryUserProfileDB, self).__init__(act_filter)
        self.user_act_dict = {}
        self.user_tag_dict = {}

    def get_act_list(self, user_id):
        return self.user_act_dict.get(user_id, (None, None, None, None))

    def get_tag_list(self, user_id):
        return self.user_tag_dict.get(user_id, None)

    def refresh_tag_list(self, user_id, tag_list):
        self.user_tag_dict[user_id] = tag_list

    def push_act(self, user_id, act_name, channel, org, timestamp):
        if self.act_filter is not None and self.act_filter(self.user_act_dict.get(user_id, []), act_name) is False:
            return False
        if user_id not in self.user_act_dict:
            self.user_act_dict[user_id] = ([act_name], [channel], [org], [timestamp])
        else:
            self.user_act_dict[user_id][0].append(act_name)
            self.user_act_dict[user_id][1].append(channel)
            self.user_act_dict[user_id][2].append(org)
            self.user_act_dict[user_id][3].append(timestamp)
        return True

    def refresh_act_list(self, user_id, act_list, channel_list, org_list, timestamp_list):
        a_list, c_list, o_list, t_list = [], [], [], []
        for a, c, o, t in zip(act_list, channel_list, org_list, timestamp_list):
            if self.act_filter is not None and self.act_filter(a_list, a) is False:
                continue
            a_list.append(a)
            c_list.append(c)
            o_list.append(o)
            t_list.append(t)
        self.user_act_dict[user_id] = (a_list, c_list, o_list, t_list)


class RedisUserProfileDB(UserProfileDB):
    # 以后做
    pass

