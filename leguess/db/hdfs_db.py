from hdfs import InsecureClient
import numpy as np


class HDFSDB(object):
    def __init__(self, http_host, root="/", timeout=10000):
        self.client = InsecureClient(http_host, root=root, timeout=timeout)

    def read_data(self, path, call_back):
        file_list = []
        for parent, dirnames, filenames in self.client.walk(path):
            for file in filenames:
                if file.startswith('part'):
                    print("%s/%s"%(parent,file))
                    file_list.append("%s/%s" % (parent, file))

        for file in file_list:
            with self.client.read(file) as fs:
                lines = fs.read().decode().split('\n')
                for line in lines:
                    if len(line) < 2:
                        continue
                    call_back(line)

    def read_line_2_id(self, path):
        # 读取行到id的映射，id必须在行尾
        # 特殊的，id为0表示缺失，id为1表示其他。hdfs文件中id是从1开始
        line_2_id = {}

        def _read_line_2_id(line):
            tmp = line.split('\x01')
            # hdfs里的id从1开始，这里的1却表示Other，应该从2开始，所以“int(tmp[-1]) + 1”
            line_2_id["@".join(tmp[:-1])] = int(tmp[-1]) + 1
        self.read_data(path, _read_line_2_id)
        id_2_line = [''] * (len(line_2_id)+2)
        id_2_line[0] = "None"
        id_2_line[1] = "Other"
        for s, id in line_2_id.items():
            id_2_line[id] = s
        return line_2_id, id_2_line

    def read_i_2_i(self, path, ri_index=2, li_2_id=None, ri_2_id=None):
        i2i_dict = {}

        def _read_i_2_i(line):
            tmp = line.split('\x01')
            li = "@".join(tmp[:ri_index])
            ri = "@".join(tmp[ri_index:-1])
            w = float(tmp[-1])
            if li_2_id is not None:
                if li not in li_2_id:
                    return
                li = li_2_id[li]
            if ri_2_id is not None:
                if ri not in ri_2_id:
                    return
                ri = ri_2_id[ri]
            if li not in i2i_dict:
                i2i_dict[li] = {ri: w}
            else:
                i2i_dict[li][ri] = w
        self.read_data(path, _read_i_2_i)
        return i2i_dict

    def read_user_act_list(self, path, call_back, exclude_act_type_list=None, channel_filter=None, user_filter=None, delta_time=None):
        # 如果一个用户两个行为之间的时间差距太大delta_time，将被切开
        # 第一列是user_id，倒数第二列是channel，最后一列是timestamp
        user_list = []
        act_list = []
        channel_list = []
        timestamp_list = []

        def clean_all_list():
            act_list.clear()
            channel_list.clear()
            timestamp_list.clear()

        def _read_user_profile_db(line):
            tmp = line.split("\x01")
            act = "@".join(tmp[1:-2])
            if exclude_act_type_list is not None:
                for exclude_act_type in exclude_act_type_list:
                    if act.endswith(exclude_act_type):
                        return
            user = tmp[0]
            channel = tmp[-2]
            timestamp = int(tmp[-1])
            if user_filter is not None and not user_filter(user):
                return
            if channel_filter is not None and channel not in channel_filter:
                return
            if len(user_list) > 0:
                assert user >= user_list[-1]
                if user > user_list[-1]:
                    call_back(user_list[-1], act_list, channel_list, timestamp_list)
                    clean_all_list()
                    user_list.clear()
                    user_list.append(user)
                else:
                    assert timestamp >= timestamp_list[-1]
                    if delta_time is not None and timestamp - timestamp_list[-1] > delta_time:
                        call_back(user_list[-1], act_list, channel_list, timestamp_list)
                        clean_all_list()
            else:
                user_list.append(user)
            act_list.append(act)
            channel_list.append(channel)
            timestamp_list.append(timestamp)
        self.read_data(path, _read_user_profile_db)
        call_back(user_list[-1], act_list, channel_list, timestamp_list)

    def read_user_tag_list(self, path, call_back):
        def _read_user_tag_list(line):
            tmp = line.split('\x01')
            tmp[-1] = tmp[-1].replace("\r", "")
            call_back(tmp[0], tmp[1:])
        self.read_data(path, _read_user_tag_list)
