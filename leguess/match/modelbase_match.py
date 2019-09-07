from .base_match import BaseMatch
from pixelchar import *
import numpy as np


class ModelBaseMatch(BaseMatch):
    def __init__(self, resource_path, model_path, meta_path, p_label_name='p_item_percent'):
        with open(resource_path, 'r') as f:
            model_text = f.read()
        self.model = CharClassificationModel(DataMeta.load(meta_path), model_text)
        self.model.load(model_path)
        self.p_label_name = p_label_name

    def match_item(self, data_source_factory, match_size):
        pred = self.model.predict(data_source_factory, self.p_label_name)
        data_source = data_source_factory(["exclude_ids"])
        line_id = 0
        data_iter = iter(data_source)
        match_item_array = np.zeros([len(data_source), match_size], np.int32)
        match_percent_array = np.zeros([len(data_source), match_size], np.float32)
        while True:
            try:
                data_value_list = next(data_iter)
                exclude_ids = set(data_value_list[0])
                cur_pred = pred[line_id]
                sid = np.argsort(-cur_pred)
                colume_id = 0
                for id in sid:
                    if id not in exclude_ids and id > 1:
                        match_item_array[line_id][colume_id] = id
                        match_percent_array[line_id][colume_id] = cur_pred[id]
                        colume_id += 1
                        if colume_id >= 512:
                            break
                line_id += 1
            except StopIteration as e:
                break
        return match_item_array, match_percent_array
