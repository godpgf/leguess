import numpy as np


class HR(object):
    def __init__(self, top_list):
        self.top_list = top_list
        # 命中数量
        self.hit_cnt = np.zeros(len(top_list), dtype=np.float32)
        self.all_cnt = 0.0
        # 命中种类
        self.hit_set = []
        # 所有种类
        self.all_set = set()
        for _ in range(len(top_list)):
            self.hit_set.append(set())

    def add_sample(self, pred_item_list, real_item, weight=1):
        for i in range(self.top_list[-1]):
            if i < len(pred_item_list) and pred_item_list[i] == real_item:
                for id, top_n in enumerate(self.top_list):
                    if i < top_n:
                        self.hit_cnt[id] += weight
                        self.hit_set[id].add(real_item)
                break

        self.all_cnt += weight
        self.all_set.add(real_item)

        if int(self.all_cnt + 1) % 100 == 0:
            print(self.hit_cnt / self.all_cnt)

    def print_eval(self):
        hr = ["命中率"]
        hr.extend(["%.4f" % (h/self.all_cnt) for h in self.hit_cnt])
        cover = ["覆盖率"]
        cover.extend(["%.4f" % (len(h)/len(self.all_set)) for h in self.hit_set])
        print("\t\t\t".join(hr))
        print("\t\t\t".join(cover))
