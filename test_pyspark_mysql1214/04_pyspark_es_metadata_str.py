#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：tfec_user_profile 
@File    ：03_pyspark_es_metadata_map.py
@Author  ：itcast
@Date    ：2022/12/14 17:29 
'''


# inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender
# inType:Elasticsearch
# esNodes:up01:9200
# esIndex:tfec_tbl_users
# esType:_doc
# selectFields:id,gender

class ESMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str

    def __init__(self, inType,
                 esNodes,
                 esIndex,
                 esType,
                 selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esIndex = esIndex
        self.esType = esType
        self.selectFields = selectFields

    def __str__(self):
        return f"ESMeta=inType:{self.inType},self.esNodes:{self.esNodes},self.esIndex:{self.esIndex},self.esType:{self.esType},self.selectFields:{self.selectFields}"


if __name__ == '__main__':
    es_str = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'
    kv = es_str.split('##')
    es_map = {}
    for s in kv:
        es_meta = s.split('=')
        es_map[es_meta[0]] = es_meta[1]

    esMeta = ESMeta(**es_map)
    print(esMeta)
    print(esMeta.esIndex)
