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
    es_str = {'inType': 'Elasticsearch',
              'esNodes': '192.168.88.166:9200',
              'esIndex': 'uprofile-policy_client',
              'esType': '_doc',
              'selectFields': 'user_id,birthday'}
    esMeta = ESMeta(**es_str)
    print(esMeta)
    print(esMeta.esIndex)
