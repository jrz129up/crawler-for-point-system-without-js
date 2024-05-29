import json
import os
import random
import re
import time

import execjs
import pandas as pd
import requests
from loguru import logger

with open("sdk.js", "r", encoding="utf-8") as f:
    js = f.read()
js_complied = execjs.compile(js)


def encrypt(plain_data):
    return js_complied.call("encrypt", plain_data)


def decrypt(encrypt_data):
    return js_complied.call("decrypt", encrypt_data)


def to_csv(data_set, filename="data.csv"):
    df = pd.DataFrame(data_set)
    df.to_csv(filename, index=False, encoding="utf_8_sig")


def append_to_csv(dataset, filename):
    df = pd.DataFrame(dataset)
    df.to_csv(filename, mode='a', header=False, index=False, encoding="utf_8_sig")


class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.rank_levels = [5, 4, 3] 

    def get_ranking_list(self, rank_level, page):
        headers = {headers}
        url = "yrl"
        data = encrypt(json.dumps({
        payload for the format of json
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_ranking_list(raw_json)

    @staticmethod
    def parse_ranking_list(raw_json):
        result_list = []
        records = raw_json["data"]["records"]
        for record in records:
            result = {
                "村庄ID": record["villageId"],
                "村庄名称": record["villageName"],
                "村庄长名称": record["longName"],
                "村庄排名": record["ranking"],
                "星级榜": record["star"],
            }
            result_list.append(result)
        return result_list

    other similar staticmethod
    
    def crawl(self):
        save_csv_filename = "csv"
        for rank_level in self.rank_levels:
            logger.info(f"{rank_level}")
            record_count = 1
            page = 1
            while True:
                village_list = self.get_ranking_list(rank_level=rank_level, page=page)
                if not village_list:
                    break
                logger.info(f'采集:{rank_level} 页码：{page} 结果总数：{len(village_list)}')
                data_list = []
                for village in village_list:
                    template = {
                        "村庄ID": "",
                        "村庄名称": "",
                        "村庄长名称": "",
                        other templates
                    }
                    logger.info(f'#{record_count} 采集村庄详细信息，村庄ID: {village["村庄ID"]}')
                    village_id = village["村庄ID"]
                    village_regulation = self.get_village_regulation(village_id)
                    get other data 

                    template.update(village)
                    template.update(village_regulation)
                    update other data

                    data_list.append(template)
                    record_count += 1
                if not os.path.exists(save_csv_filename):
                    to_csv(data_list, save_csv_filename)
                else:
                    append_to_csv(data_list, save_csv_filename)
                logger.info(f'采集：{rank_level} 页码{page}，存储到: {save_csv_filename}')
                page += 1
        logger.success(f'done')

    def test(self):
        crawler = Crawler()
        print(crawler.get_score_rule("number"))


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
