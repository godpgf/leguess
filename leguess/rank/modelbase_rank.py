from .base_rank import BaseRank
from pixelchar import *


class ModelBaseRank(BaseRank):
    def __init__(self, resource_path, model_path, meta_path, p_label_name='predict'):
        with open(resource_path, 'r') as f:
            model_text = f.read()
        self.model = CharClassificationModel(DataMeta.load(meta_path), model_text)
        self.model.load(model_path)
        self.p_label_name = p_label_name

    def rank_item(self, data_source_factory):
        return self.model.predict(data_source_factory, self.p_label_name)
