# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from pymysql import connect


class MongoPipeline(object):
    def open_spider(self, spider):
        self.client = pymongo.MongoClient()

    def process_item(self, item, spider):
        self.client.room.lianjia.insert(item)
        return item

    def close_spider(self, spider):
        self.client.close()


class MysqlPipeline(object):
    def open_spider(self, spider):
        self.client = connect(host='localhost', port=3306, user='root', password='root', db='room', charset="utf8")
        self.cursor = self.client.cursor()

    def process_item(self, item, spider):
        args = [item["total"],
                item["unitPriceValue"],
                item["xiao_qu"],
                item["qu_yu"],

                item["hu_xing"],
                item["lou_ceng"],
                item["mian_ji"],
                item["zhuang_xiu"],
                item["gong_nuan"],
                item["chan_quan"],

                item["yong_tu"],
                item["nian_xian"],
                item["di_ya"] ]
        sql = 'insert into t_lianjia VALUES (0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.cursor.execute(sql, args)
        self.client.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.client.close()
