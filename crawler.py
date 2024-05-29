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
    """
    数据保存为csv
    :param data_set:
    :param filename:
    :return:
    """
    df = pd.DataFrame(data_set)
    df.to_csv(filename, index=False, encoding="utf_8_sig")


def append_to_csv(dataset, filename):
    """
    将数据追加到指定csv
    :param dataset:
    :param filename:
    :return:
    """
    df = pd.DataFrame(dataset)
    df.to_csv(filename, mode='a', header=False, index=False, encoding="utf_8_sig")


class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.rank_levels = [5, 4, 3]  # 星级榜：五星、四星、三星

    def get_ranking_list(self, rank_level, page):
        """
        采集星级榜
        :param rank_level: 星级
        :param page: 页码
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "d6wZUUhHdKQWs8",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701335594000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "X-Request-Id": "3f83fc9e-b375-466d-b883-b0864948d5ed",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-NTRiMzRlOTgtM2E5Mi00ODQ5LWEwMjUtYmE5ZDYxNzkyYmU3-MTk0MjVkMTUtZWI0NC00OTU2LWIwYmUtMTk1MWIyNjk2ZTY0-1-YWVnaXM=-MS4zOC40-bW9kdWxlQy9wYWdlcy9pbmRleC9pbmRleA==-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJEoWmId3eeKvpiXocBpVl+LP6aEqcCP6pj4YLwBMVLki",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/182/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/starVillage/getVillageRankDT"
        data = encrypt(json.dumps({
            "areaId": 0,
            "star": rank_level,
            "current": page,
            "size": 20
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

    def get_village_regulation(self, village_id):
        """
        采集村规民约
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "WnVdYtgCoVsfbj",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "application/json",
            "timestamp": "1701417939000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "true",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-MWVhNWFiZDUtNWE3Ny00NmI0LTlkOTgtZTUwYjkzYjZlZGJh-Y2FkY2EzODAtMGUzMS00MTQwLWFhNzItZjEyNzI3ZDM5YWVl-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJNAE/UmyPELBfws8+V2EoWpBRYeAzj9l34+1NqCkbnNR",
            "X-Request-Id": "2aeb12c5-3167-4be3-b972-0b46e8d64a7c",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/village/provision-detail/getFirstProvision"
        params = {
            "villageId": village_id
        }
        response = self.session.get(url, headers=headers, params=params)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_village_regulation(raw_json)

    @staticmethod
    def parse_village_regulation(raw_json):
        raw_json = raw_json["data"]
        result = {
            "省": raw_json["provinceName"],
            "市": raw_json["cityName"],
            "县": raw_json["countyName"],
            "镇": raw_json["townName"],
            "村": raw_json["villageName"],
            "村规民约章数": int(re.match(r'共(\d+)章(\d+)条', raw_json["detailCount"]).group(1)) if raw_json["detailCount"] else "",
            "村规民约条数": int(re.match(r'共(\d+)章(\d+)条', raw_json["detailCount"]).group(2)) if raw_json["detailCount"] else "",
            "村规民约查看数": int(raw_json["viewCount"].replace("查看", "")) if raw_json["viewCount"] else "",
            "村规民约点赞数": int(raw_json["thumbUpNumber"].replace("点赞", ""))if raw_json["thumbUpNumber"] else "",
        }
        return result

    def get_score_rule(self, village_id):
        """
        采集积分规则
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "5lhJWnbudjNNFO",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "application/json",
            "timestamp": "1701418927000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "true",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-ODAxYjkyMjMtZTQ1Ny00ZmFmLTk5ZmEtMmQwYTQwNjQ0NTM0-ZjkyMjE3ZGEtMTcwOS00ZTA5LThmZTMtNTBlMzNkZTllNjNm-1-YWVnaXM=-MS4zOC40-bW9kdWxlQS9wYWdlcy92aWxsYWdlX3J1bGUvZXhoaWJpdGlvbl9kZXRhaWxfcnVsZS9leGhpYml0aW9uX2RldGFpbF9ydWxl-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJAXPfHrgiOV4jXMmZw8oo6hV5QsnVUseKLFlrycBEDAJ",
            "X-Request-Id": "b342224b-7415-4230-8a48-5820c2ca0700",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/score/score-detail/home-page"
        params = {
            "villageId": village_id,
            "revisionId": ""
        }
        response = self.session.get(url, headers=headers, params=params)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_score_rule(raw_json)

    @staticmethod
    def parse_score_rule(raw_json):
        # 用于处理异常情况：批量校验一次限制500条事件
        if "批量校验一次限制500条事件" in raw_json["message"]:
            return {}
        raw_json = raw_json["data"]
        result = {
            "积分规则章数": int(re.match(r'共(\d+)章(\d+)条', raw_json["typeDetailTotal"]).group(1)) if raw_json["typeDetailTotal"] else "",
            "积分规则条数": int(re.match(r'共(\d+)章(\d+)条', raw_json["typeDetailTotal"]).group(2)) if raw_json["typeDetailTotal"] else "",
            "积分规则查看数": raw_json["viewNum"],
            "积分规则点赞数": raw_json["thumbUpNumber"],
        }
        return result

    def get_review_rules(self, village_id):
        """
        采集评议规则层数
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "nUsL9YAjX6YwT9",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701419493000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YWMwNGMwZWUtN2E3MC00NzkzLWFlYjItYTIxOTllMmM4YWVk-MmRjZDY1YmItYzUxMi00YmZlLWEyZmEtMzA4OWJmOTFlZWRm-1-YWVnaXM=-MS4zOC40-bW9kdWxlQS9wYWdlcy9yZXZpZXdfYWRtaW4vcmV2aWV3X2FkbWlu-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJJsaisXkZ0WZ69nVLIF/W42l856YlemNAmJNa7zX589r",
            "X-Request-Id": "66d04fe2-cf10-4cec-aedc-9064c39d523d",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/resident/resident-role-info/role-user-info-list"
        data = encrypt(json.dumps({
            "role": "gl",
            "villageId": village_id,
            "secretKey": "bWmTzH4uwO1NpVxp96u+7biPIBp2j2S9xD0hWoQkZZSB+kig3wjxoQa+FO1CmVmMehtb/iojDlaJLYrkX7SA61ineSTbxaXX6fyNN1wUO2pbtcw7fddXF0IUD5OHXZI6WsCF5f2MvuHhdykPoc1PjKFAKwlkAF53IcDhwUZYsZo="
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_review_rules(raw_json)

    @staticmethod
    def parse_review_rules(raw_json):
        result = {
            "评议规则层数": len(raw_json["data"])
        }
        return result

    def get_score_board(self, village_id):
        """
        采集积分看板数据
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "29Ppn990wxHRth",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701420041000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YTQ2ZTBiNDktNWRmNy00YjY5LTkzMzItNWFkODlmZDQzZTMx-NDI4YjgzMTItOWUzOS00N2JiLTk2YzAtYzcwMGM0ODU4Yzg3-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJPFKAs0A1RtCuFv5az+banNilWBl6OfSZg1SA25jYrZM",
            "X-Request-Id": "a01daaeb-6480-464a-aac8-4c2c336e5081",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/residentScore/find_village_score_index_data"
        data = encrypt(json.dumps({
            "villageId": village_id,
            "isYouKe": True
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_score_board(raw_json)

    @staticmethod
    def parse_score_board(raw_json):
        raw_json = raw_json["data"]
        score_type_list = raw_json["scoreTyp"]
        if not raw_json["score"]:
            return {}
        result = {
            "全村总积分": raw_json["score"]["totalVillageScore"],
            "累计加分": raw_json["score"]["totalVillageScoreAdd"],
            "累计减分": raw_json["score"]["totalVillageScoreSub"],
        }
        number_map = ['一', '二', '三', '四', '五', '六', '七', '八']
        sorted_score_type_list = sorted(score_type_list, key=lambda x: x["sumScore"], reverse=True)
        for _id, score_type in enumerate(sorted_score_type_list):
            score_type_data = {
                f"第{number_map[_id]}积分事项": score_type["scoreTypeContent"],
                f"第{number_map[_id]}积分事项分数": score_type["sumScore"],
                f"第{number_map[_id]}积分事项来源": score_type["scoreDetail"][0],
            }
            result.update(score_type_data)
        return result

    def get_score_board_cumulative_start_time(self, village_id):
        """
        采集积分看板数据累计开始时间（点击积分看板后可看到累计开始时间）
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "mY1CWZ3DUaIrPj",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701430937000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YmJlNzIxMjktN2IwMi00ZTQ4LTgzZjQtMWRiOTBlYzI5MmQw-NDViZTM2ZGMtZTJkNy00NGM1LTgyYTUtOWI2YmIxZTFjYzQz-1-YWVnaXM=-MS4zOC40-bW9kdWxlRy9wYWdlcy9zY29yZXB1YmxpY2l0eS9zY29yZV9ib2FyZC9zY29yZV9ib2FyZA==-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJNfe6kNqEuaLKfewDIXqOXqIg9pUW0GseVBBQ1wkZDbs",
            "X-Request-Id": "b70de287-9678-40cb-b320-b397f690ff8d",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/think/api/score/village_score_data"
        data = encrypt(json.dumps({
            "villageId": village_id
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_score_board_cumulative_start_time(raw_json)

    @staticmethod
    def parse_score_board_cumulative_start_time(raw_json):
        start_time = raw_json["data"]["score"]["monthStart"]
        result = {
            "开始时间": start_time
        }
        return result

    def get_group_rank_list(self, village_id):
        """
        采集积分排行
        :param village_id:
        :return:
        """
        result = {
            "党员排行": self.get_party_member_group_rank_list(village_id),
            "个人排行": self.get_person_group_rank_list(village_id),
            "家庭排行": self.get_family_group_rank_list(village_id),
            "院落/小组排行": self.get_squad_group_rank_list(village_id),
        }
        return result

    def get_party_member_group_rank_list(self, village_id):
        """
        采集积分排行-党员排行
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "34IkHtIkWZoadC",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701421986000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-ZDAxMDNkNzMtZGJlNi00MjUzLWE2NWEtM2MwM2NhNzFkZWE0-NTc0OWNmYmQtOTlkYS00NzhjLWEyNzgtM2ZhZmVkZmNjYzMx-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJBa9fs5VOIE/futKejwehfoYQRi33YKsUAixF7B+/fLk",
            "X-Request-Id": "874e9c45-28c4-4fde-a860-21c057700780",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/residentScore/allRankingList"
        data = encrypt(json.dumps({
            "villageId": village_id,
            "isParty": "party",
            "pageSize": 10,
            "currentPage": 1
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_party_member_group_rank_list(raw_json)

    @staticmethod
    def parse_party_member_group_rank_list(raw_json):
        records = raw_json["data"]["records"]
        scores = ",".join([str(record["score"]) for record in records])
        return scores

    def get_squad_group_rank_list(self, village_id):
        """
        采集积分排行-院落/小组排行
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "LyQ8m9cDnxNLcx",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701423559000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YjZmMzBmNGMtMmVmNS00Y2Q4LThiMzYtMWQ5MTgwZjg4MWFj-YjkwOWJlZjMtNDdhYi00MTY5LWE2MTAtNGRhMGZmZmZlYTky-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJODB3eKCzWYAH9c0Lh1bxuGemR/UDSXpjjznNSjopna4",
            "X-Request-Id": "fc5725cd-7538-42a2-b2b9-e5666c63a9a8",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/village/yard-info/yard-info-ranking"
        data = encrypt(json.dumps({
            "villageId": village_id,
            "currentPage": 1,
            "pageSize": 10
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_squad_group_rank_list(raw_json)

    @staticmethod
    def parse_squad_group_rank_list(raw_json):
        records = raw_json["data"]["ipage"]["records"]
        scores = ",".join([str(record["totalScore"]) for record in records])
        return scores

    def get_person_group_rank_list(self, village_id):
        """
        采集积分排行-个人排行
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "S5TIS1kQvb9stW",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701422919000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-N2Y5OThmMWItMmFkZS00MWU5LWE3ZDctZmM2MzVmMGM4ZTQ2-MDIzMzJmYTQtNTI2MC00YzVkLThiOWMtYTQ2ZDNhZTIyNGE1-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJG+EYeZNMVKsMXymPw75NBtMmNL12MV1AWnq/OI2Wh20",
            "X-Request-Id": "8d1a92aa-baa5-45a5-ae9a-db5e35a36a9b",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/residentScore/allRankingList"
        data = encrypt(json.dumps({
            "villageId": village_id,
            "isParty": "masses",
            "pageSize": 10,
            "currentPage": 1
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_person_group_rank_list(raw_json)

    @staticmethod
    def parse_person_group_rank_list(raw_json):
        records = raw_json["data"]["records"]
        scores = ",".join([str(record["score"]) for record in records])
        return scores

    def get_family_group_rank_list(self, village_id):
        """
        采集积分排行-家庭排行
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "rKODH8XIyCdB8H",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701423124000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-NzUwODk5M2QtNGRkNy00NjdiLTg2YzEtODE0ODViNDEyNDI3-NzJlODkzZWEtNjhkOC00MjJmLWFmZjMtYzU1NGVlYTU0ZWQ2-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJDXAsTVoGVWrTinFYRKlbwLPpZsvB8/sQ1PcH/6QoUxs",
            "X-Request-Id": "8d8c51ae-c19e-4ca8-867c-070006ecba64",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/resident/family-info/family-score-ranking"
        data = encrypt(json.dumps({
            "villageId": village_id,
            "currentPage": 1,
            "pageSize": 10
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_family_group_rank_list(raw_json)

    @staticmethod
    def parse_family_group_rank_list(raw_json):
        records = raw_json["data"]["ipage"]["records"]
        scores = ",".join([str(record["totalScore"]) for record in records])
        return scores

    def get_score_store_product_list(self, village_id):
        """
        采集积分超市商品信息
        :param village_id:
        :return:
        """
        def get_score_store_product_list_by_page(vid, _page):
            headers = {
                "Host": "gyz-jifen.ssv.qq.com",
                "Connection": "keep-alive",
                "nonce": "DBj4W6SUNfdNSR",
                "Authorization": "",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
                "Content-Type": "text/plain",
                "timestamp": "1701428487000",
                "MinAppName": "0",
                "xweb_xhr": "1",
                "EncryPtionSwitch": "",
                "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
                "sw8": "1-ZTY1YmNjMGItMDdkZC00YmYwLTllY2MtMWQ0ODk4ZjIxMWIy-ZjNjZjkxNzAtNmRiNC00Yzg0LTkyMDgtNDhhZDc0MzI0NTU2-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
                "sign": "ynODP3+N3C85FM0SkQ/DJDrgfwg09psq4H5APZd53WQzbMJzhYGhC0xXSveqd+dJ",
                "X-Request-Id": "dab30d36-2a97-4857-833b-7e02dd9108ff",
                "Accept": "*/*",
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
                "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9"
            }
            url = "https://gyz-jifen.ssv.qq.com/shop/goods/queryShopGoodList"
            data = encrypt(json.dumps({
                "villageId": vid,
                "currentPage": page,
                "pageSize": 15
            }, separators=(',', ':')))
            response = self.session.post(url, headers=headers, data=data)
            decrypted_data = decrypt(response.text)
            raw_json = json.loads(decrypted_data)
            return self.parse_score_store_product_list(raw_json)

        score_store_product_info = ""
        page = 1
        while True:
            product_info = get_score_store_product_list_by_page(village_id, page)
            if not product_info:
                break
            if page == 1:
                score_store_product_info += product_info
            else:
                score_store_product_info += f';{product_info}'
            page += 1

        result = {
            "积分商品": score_store_product_info,
            "积分商品总分": sum([float(score) for score in score_store_product_info.split(";")]) if score_store_product_info else "",
            "积分商品总数": len(score_store_product_info.split(";")) if score_store_product_info else ""
        }

        return result

    @staticmethod
    def parse_score_store_product_list(raw_json):
        records = raw_json["data"]["records"]
        goods_info = ";".join([f'{record["goodsIntegral"]}' for record in records])
        return goods_info

    def get_party_group_service_center_member(self, village_id):
        """
        采集党群服务中心「党总支成员」和「村委会成员」的数量
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "xXW9W31OZ9Jjus",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "application/json",
            "timestamp": "1701429735000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "true",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YzBhMjk0NjMtOTZkMS00NDliLTkxODEtZThiN2RjMTdkZGMz-Mjc5YTQyOTktNjE3NS00ZWI1LTkyOTQtYzA4MzhmZGViZjgw-1-YWVnaXM=-MS4zOC40-bW9kdWxlRy9wYWdlcy93b3JrL3NlcnZlcl9vcmdhbml6YXRpb24vc2VydmVyX29yZ2FuaXphdGlvbg==-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJM7A8ugcsqnJQny3xDXychB/ZmMcwZESiN4wCqLOWYVp",
            "X-Request-Id": "5c2e903b-b351-447c-b828-40f149cb0340",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/partyplate/queryPartyResidentInfoList"
        params = {
            "villageId": village_id
        }
        response = self.session.get(url, headers=headers, params=params)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_party_group_service_center_member(raw_json)

    @staticmethod
    def parse_party_group_service_center_member(raw_json):
        result = {}
        records = raw_json["data"]
        for record in records:
            if record["partyMassPlate"]["name"] == "党委":
                result["党总支成员人数"] = len(record["residentList"])
            if record["partyMassPlate"]["name"] == "村委":
                result["村委会成员人数"] = len(record["residentList"])
        return result

    def get_village_millstone(self, village_id):
        """
        采集发展历程
        :param village_id:
        :return:
        """
        headers = {
            "Host": "gyz-jifen.ssv.qq.com",
            "Connection": "keep-alive",
            "nonce": "aiiCjQM2SUJ4Gz",
            "Authorization": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x6309080b)XWEB/8461",
            "Content-Type": "text/plain",
            "timestamp": "1701431696000",
            "MinAppName": "0",
            "xweb_xhr": "1",
            "EncryPtionSwitch": "",
            "AuthorizationOpenid": "ynODP3+N3C85FM0SkQ/DJBvz+yzgmNGA3on5eSc9oyI=",
            "sw8": "1-YjZmNTFlNjUtNGY4NS00YzBhLThmY2MtOTk2ZGNhOWRhODFl-OGU3ZWE4NGUtNzg0Zi00Zjg5LTljODktYzMyOGQ3OGEzNDg1-1-YWVnaXM=-MS4zOC40-cGFnZXMvaW5kZXgvaW5kZXg=-Z3l6LWppZmVuLnNzdi5xcS5jb20=",
            "sign": "ynODP3+N3C85FM0SkQ/DJAa84aJqbhgozyQXrNpoUV/zzFPxGIO4zzarPNTFfL+K",
            "X-Request-Id": "c09fc35f-9dfe-4e67-aee9-46146ff203f1",
            "Accept": "*/*",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://servicewechat.com/wxe6cb55a776e77467/183/page-frame.html",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        url = "https://gyz-jifen.ssv.qq.com/score/block/getVillageMilestone"
        data = encrypt(json.dumps({
            "villageId": village_id
        }, separators=(',', ':')))
        response = self.session.post(url, headers=headers, data=data)
        decrypted_data = decrypt(response.text)
        raw_json = json.loads(decrypted_data)
        return self.parse_village_millstone(raw_json)

    @staticmethod
    def parse_village_millstone(raw_json):
        result = {
            "户数": "",
            "人数": "",
            "兑换人数": "",
        }
        records = raw_json["data"]["records"]
        for record in records:
            if record["type"] == 3:
                result.update({
                    "户数": int(re.search(r'共计(\d+)户(\d+)人', record["content"]).group(1)),
                    "人数": int(re.search(r'共计(\d+)户(\d+)人', record["content"]).group(2)),
                })
            if record["type"] == 4:
                result.update({
                    "兑换人数": int(re.search(r'目前已有(\d+)人兑换', record["content"]).group(1)),
                })
        return result

    def crawl(self):
        save_csv_filename = "村级数据5.25.csv"
        for rank_level in self.rank_levels:
            logger.info(f"开始采集星级榜：{rank_level}")
            record_count = 1
            page = 1
            while True:
                village_list = self.get_ranking_list(rank_level=rank_level, page=page)
                if not village_list:
                    break
                logger.info(f'采集星级榜：{rank_level} 页码：{page} 结果总数：{len(village_list)}')
                data_list = []
                for village in village_list:
                    template = {
                        "村庄ID": "",
                        "村庄名称": "",
                        "村庄长名称": "",
                        "村庄排名": "",
                        "星级榜": "",
                        "省": "",
                        "市": "",
                        "县": "",
                        "镇": "",
                        "村": "",
                        "村规民约章数": "",
                        "村规民约条数": "",
                        "村规民约查看数": "",
                        "村规民约点赞数": "",
                        "积分规则章数": "",
                        "积分规则条数": "",
                        "积分规则查看数": "",
                        "积分规则点赞数": "",
                        "评议规则层数": "",
                        "全村总积分": "",
                        "累计加分": "",
                        "累计减分": "",
                        "第一积分事项": "",
                        "第一积分事项分数": "",
                        "第一积分事项来源": "",
                        "第二积分事项": "",
                        "第二积分事项分数": "",
                        "第二积分事项来源": "",
                        "第三积分事项": "",
                        "第三积分事项分数": "",
                        "第三积分事项来源": "",
                        "第四积分事项": "",
                        "第四积分事项分数": "",
                        "第四积分事项来源": "",
                        "第五积分事项": "",
                        "第五积分事项分数": "",
                        "第五积分事项来源": "",
                        "开始时间": "",
                        "党员排行": "",
                        "个人排行": "",
                        "家庭排行": "",
                        "院落/小组排行": "",
                        "积分商品": "",
                        "积分商品总分": "",
                        "积分商品总数": "",
                        "党总支成员人数": "",
                        "村委会成员人数": "",
                        "户数": "",
                        "人数": "",
                        "兑换人数": "",
                    }
                    logger.info(f'#{record_count} 采集村庄详细信息，村庄ID: {village["村庄ID"]}')
                    village_id = village["村庄ID"]
                    # 采集村规民约
                    village_regulation = self.get_village_regulation(village_id)
                    logger.info(f'采集村规民约 村庄ID: {village["村庄ID"]} 结果: {village_regulation}')
                    # 采集积分规则
                    score_rule = self.get_score_rule(village_id)
                    logger.info(f'采集积分规则 村庄ID: {village["村庄ID"]} 结果: {score_rule}')
                    # 采集评议规则层数
                    review_rules = self.get_review_rules(village_id)
                    logger.info(f'采集评议规则层数 村庄ID: {village["村庄ID"]} 结果: {review_rules}')
                    # 采集积分看板数据
                    score_board = self.get_score_board(village_id)
                    logger.info(f'采集积分看板数据 村庄ID: {village["村庄ID"]} 结果: {score_board}')
                    # 采集积分看板数据累计开始时间
                    score_board_cumulative_start_time = self.get_score_board_cumulative_start_time(village_id)
                    logger.info(f'采集积分看板数据累计开始时间 村庄ID: {village["村庄ID"]} 结果: {score_board_cumulative_start_time}')
                    # 采集积分排行
                    group_rank_list = self.get_group_rank_list(village_id)
                    logger.info(f'采集积分排行 村庄ID: {village["村庄ID"]} 结果: {group_rank_list}')
                    # 采集积分超市商品信息
                    score_store_product_list = self.get_score_store_product_list(village_id)
                    logger.info(f'采集积分超市商品信息 村庄ID: {village["村庄ID"]} 结果: {score_store_product_list}')
                    # 采集党群服务中心「党总支成员」和「村委会成员」的数量
                    party_group_service_center_member = self.get_party_group_service_center_member(village_id)
                    logger.info(f'采集党群服务中心「党总支成员」和「村委会成员」的数量 村庄ID: {village["村庄ID"]} 结果: {party_group_service_center_member}')
                    # 采集发展历程
                    village_millstone = self.get_village_millstone(village_id)
                    logger.info(f'采集发展历程 村庄ID: {village["村庄ID"]} 结果: {village_millstone}')

                    template.update(village)
                    template.update(village_regulation)
                    template.update(score_rule)
                    template.update(review_rules)
                    template.update(score_board)
                    template.update(score_board_cumulative_start_time)
                    template.update(group_rank_list)
                    template.update(score_store_product_list)
                    template.update(party_group_service_center_member)
                    template.update(village_millstone)

                    data_list.append(template)
                    record_count += 1
                if not os.path.exists(save_csv_filename):
                    to_csv(data_list, save_csv_filename)
                else:
                    append_to_csv(data_list, save_csv_filename)
                logger.info(f'采集星级榜：{rank_level} 页码{page}，采集完毕，存储到csv: {save_csv_filename}')
                page += 1
        logger.success(f'采集结束')

    def test(self):
        crawler = Crawler()
        print(crawler.get_score_rule("12255"))


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
    # crawler.test()
