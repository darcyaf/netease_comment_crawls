# -*- coding:utf8 -*-
import scrapy
import re
import logging
import json
import base64
import requests
from Crypto.Cipher import AES
from scrapy.http import Request, FormRequest
from mongodb_project import MongoUtils

logger = logging.getLogger(__name__)


class MusicSpider(scrapy.Spider):
    name = "163_spider"
    allowed_domains = ["music.163.com"]
    start_urls = []
    headers = {
        'Cookie': 'appver=1.5.0.75771;',
        'Referer': 'http://music.163.com/'
    }

    # 163配置
    post_data = {
        'params': 'CBLdjiE9VGyoIIXDZknaIx1RTkQUckueXd2iihJMwJEI9hWUek1S3rrsZVFeeARUaD3bW3lRFWk5fvlNvzNWrJ7TgVGaiyWDyKcx7JQcRxNuOqYz5pce6daITH59SjT1',
        'encSecKey': '82886d43fb2c2daae7941b1f6a4290fa92506b0817006d3c2516ce3aff5127b169cba1443809e429f38c3094a7b60c801fbf9079266237723f8046ee79ef0ebdf1d96be51ae60e16bf7b5a048d9c6b3e786bae2bddfd29b4bb7e365de1df1107e26777811a01e308763d528a8fccfd17a7a439541a8b66d801f17d38921e64e9'
    }
    limit = 30  # 每种口味的单页歌单数量
    page_num = 50  # 每种口味要爬取几页歌单

    # 歌单id缓存，防止重复插入。除此还可以使用playlist_buffer、comment_buffer做缓存，然后insert_many
    playlist_id_buffer = []
    db = MongoUtils.MongoDB().db

    def AES_encrypt(self,text, key, iv):
        pad = 16 - len(text) % 16
        text = text + pad * chr(pad)
        encryptor = AES.new(key, AES.MODE_CBC, iv)
        encrypt_text = encryptor.encrypt(text)
        encrypt_text = base64.b64encode(encrypt_text)
        return encrypt_text.decode()

    def get_postdata(self,i):
        first_param = "{rid:\"\", \"offset\":\""+str(i)+"\", total:\"true\", limit:\"100\", csrf_token:\"\"}"
        second_param = "010001"
        third_param = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
        forth_param = "0CoJUm6Qyw8W8jud"
        iv = "0102030405060708"
        first_key = forth_param
        second_key = 16 * 'F'
        h_encText =self.AES_encrypt(first_param, first_key, iv)
        h_encText =self.AES_encrypt(h_encText, second_key, iv)

        encSecKey = "257348aecb5e556c066de214e531faadd1c55d814f9be95fd06d6bff9f4c7a41f831f6394d5a3fd2e3881736d94a02ca919d952872e7d0a50ebfa1769a7a62d512f5f1ca21aec60bc3819a9c3ffca5eca9a0dba6d6f7249b06f5965ecfff3695b54e1c28f3f624750ed39e7de08fc8493242e26dbc4484a01c76f739e135637c"
        data = {
             "params":h_encText,
             "encSecKey": encSecKey
        }
        return data

    def start_requests(self):
        """start_requests方法【必须】返回一个可迭代对象，该对象包含了spider用于爬取的第一个Request"""
        playlist_url='http://music.163.com/discover/playlist?cat=%E4%BC%A4%E6%84%9F';
        for offset in range(0, self.page_num * self.limit, self.limit):
            full_url =playlist_url + '&order=hot&limit='+str(self.limit)+'&offset=' + str(offset)
            logger.debug('Getting playlist url:' + full_url)
            yield Request(full_url, callback=self.in_get_playlist)

    # 获取歌单列表doc
    def pre_get_playlist(self, response):
        pass

    #获取搜有歌单
    def in_get_playlist(self, response):
        playlist_url = 'http://music.163.com/api/playlist/detail?id='
        playlist_ids = response.xpath('//ul/li/div/div/a/@data-res-id').extract()
        for id in playlist_ids:
            if re.match('^\d{4,}\d$', id) and id not in self.playlist_id_buffer:
                self.playlist_id_buffer.append(id)
                yield Request(playlist_url + str(id), callback=self.post_get_playlist)

    def post_get_playlist(self, response):
        collection = self.db.playlist
        result = json.loads(response.body, encoding='utf-8')['result']

        # inserted = collection.update({'id': result['id']}, result, upsert=True)  # upsert=True表示insert or update
        # logger.info('Update or Insert to playlist database[%s]' % (str(inserted),))
        if result['id'] not in self.playlist_id_buffer:
            collection.insert(result)

        for song in result['tracks']:
            artists = []
            for detail in song['artists']:
                artists.append(detail['name'])
            comment_url = 'http://music.163.com/weapi/v1/resource/comments/%s/?csrf_token=' % (song['commentThreadId'],)
            # 使用FormRequest来进行POST登陆，或者使用下面的方式登陆
            post_data = self.get_postdata(0)
            response = requests.post(comment_url, headers=self.headers, data=post_data)
            total=json.loads(response.content)["total"]
            for pg in range(total//100):
                post_data = self.get_postdata(pg)
                yield FormRequest(comment_url, formdata=post_data, callback=self.parse,
                                  meta={'m_id': song['id'], 'm_name': song['name'], 'artists': artists})

    def parseComments(self,oldcomments):
        comments=[]
        for old_comment in oldcomments:
            content=old_comment["content"]
            if ("分开" in content) or ("分手" in content) or ("挽留" in content) or ("离婚" in content) or ("前男友" in content) or ("前女友" in content):
                comment={}
                comment["content"]=content
                comment["user_id"]=old_comment["user"]["userId"]
                comment["nick_name"]=old_comment["user"]["nickname"]
                comment["time"]=old_comment["time"]
                comments.append(comment)
        return {"comments":comments}

    def parse(self, response):
        collection = self.db.comment
        comment_body_before = json.loads(response.body, encoding='utf-8')
        # {m_id,m_name,artists,comments:["user_id","nick_name","contents","time"]}
        comment_body=self.parseComments(comment_body_before["comments"])
        if comment_body["comments"]:
            music_id = response.meta['m_id']
            comment_body['m_id'] = music_id
            comment_body['m_name'] = response.meta['m_name']
            comment_body['artists'] = response.meta['artists']
            print("song id:"+str(music_id)+"\tname:"+comment_body["m_name"])
            collection.update({'id': music_id}, comment_body, upsert=True)
        # logger.info('Update or Insert to Mongodb[%s]' % (str(inserted),))
        yield
