class UserProfileDB(object):
    def get_act_list(self, user_id):
        return None, None

    def get_tag_list(self, user_id):
        return None

    def refresh_tag_list(self, user_id, tag_list):
        pass

    def push_act(self, user_id, act_name, timestamp):
        pass

    def refresh_act_list(self, user_id, act_list, timestamp_list):
        pass


class MemoryUserProfileDB(UserProfileDB):
    def __init__(self):
        self.user_act_dict = {}
        self.user_tag_dict = {}

    def get_act_list(self, user_id):
        return self.user_act_dict.get(user_id, (None, None))

    def get_tag_list(self, user_id):
        return self.user_tag_dict.get(user_id, None)

    def refresh_tag_list(self, user_id, tag_list):
        self.user_tag_dict[user_id] = tag_list

    def push_act(self, user_id, act_name, timestamp):
        if user_id not in self.user_act_dict:
            self.user_act_dict[user_id] = ([act_name], [timestamp])
        else:
            self.user_act_dict[user_id][0].append(act_name)
            self.user_act_dict[user_id][1].append(timestamp)

    def refresh_act_list(self, user_id, act_list, timestamp_list):
        self.user_act_dict[user_id] = (act_list, timestamp_list)


class RedisUserProfileDB(UserProfileDB):
    # 以后做
    pass

