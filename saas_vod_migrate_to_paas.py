#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/1/19 14:15
# @Author  : peilin.wu
# @Site    : 
# @File    : saas_vod_migrate_to_paas.py
# @Software: PyCharm

# 迁移点播数据类型统计
# python2.7 saas_vod_migrate_to_paas.py --config /testapp/config/test_vod_v2_script/config.ini -c type -i ./migration.csv
# 迁移点播数据
# time python2.7 saas_vod_migrate_to_paas.py --config "/testapp/config/test_vod_v2_script/config.ini" -i "./test.csv" -y "./mappinf_file.csv" -f "/testapp/test_vod_v2_script/flash_covert_h5.js" -d "/testapp/test_vod_v2_script/mapping.json" -a "6147b94a"

import optparse

from gevent import monkey;

monkey.patch_all()
import gevent
import Queue
import binascii
import csv
import os
import random
import subprocess
import sys
import threading
import time

from common.configuration import config
from common import loggers as loggers
from collections import namedtuple
from db_vod_manage import DbVodManage
from vio.vio import VioHander
from mysql_ext import MySqlBus

log_path = config['global'].get('log_dir', "/testapp/logs/test_vod_v2_script/").strip()
log_name = "saas_vod_migrate_to_paas"
logger = loggers.get_logger(log_path, log_name)

M = ['record_id', 'webinar_id', 'user_id', 'room_id', 'name', 'video_path', 'msg_path', 'quality', 'duration',
     'storage', 'source',
     'source_path', 'record_status', 'transcode_status', 'created_at', 'paas_record_id']

RUNNING = True

TYPES = namedtuple('t', ['exist', 'record', 'upload'])
types = TYPES(0, 1, 2)

ranges = str(config['global'].get("storages", "oss").strip())
if ranges != None and len(ranges) > 0:
    src_file_bucket_config = dict()
    dest_file_bucket_config = dict()
    for storage in str(ranges).split(","):
        if storage.strip() == "oss":
            dest_file_bucket_config["oss_bucket"] = str(config["global"]["oss_mp4_bucket"])
        elif storage.strip() == "cos":
            dest_file_bucket_config["cos_bucket"] = str(config["global"]["oss_mp4_bucket"])
        else:
            continue
    dest_file_vio = VioHander(range=ranges, bucket_config=dest_file_bucket_config)

db_obj_mig = MySqlBus(host=config["global"].get("migration_db_host"),
                      port=int(config["global"].get("migration_db_port")), db=config["global"].get("migration_db_name"),
                      user=config["global"].get("migration_db_user"),
                      password=config["global"].get("migration_db_password"),
                      logger=logger)


class VodV2Status(object):
    """
    v2 点播状态,vod_info.status
    """
    # 初始化
    INIT = 0
    # 已发布
    PUBLISHED = 1
    # 回放生成成功
    RECORD_SUCCEED = 2
    # 回放生成失败
    RECORD_FAILED = 3
    # 审核中
    AUDITING = 4
    # 审核成功
    AUDIT_SUCCEED = 5
    # 审核失败
    AUDIT_FAILED = 6
    # 转码中
    TRANSCODING = 7
    # 转码失败
    TRANSCODE_FAILED = 8
    # 转码部分成功
    TRANSCODE_PART_SUCC = 9


class TaskQueue(object):
    def __init__(self, max_size=0, mapping_file=None):
        self.max_size = max_size
        self.mapping_file = mapping_file
        self.tq = Queue.Queue(self.max_size)

    def push_task_to_queue(self, task):
        """放一个数据到队列"""
        while True:
            if self.get_queue_size() >= self.max_size:
                time.sleep(0.5)
                continue

            self.tq.put(task)
            break

    def get_task_from_queue(self):
        """从队列获取一条数据"""
        try:
            return self.tq.get()
        except Exception as e:
            sys.exc_clear()
            # logger.exception(e)

    def get_queue_size(self):
        return self.tq.qsize()


def csv_to_json(input_file):
    """csv -> json格式转换"""
    tasks = list()
    keys = list()
    try:
        with open(input_file) as f:
            render = csv.reader(f)  # reader(迭代器对象)--> 迭代器对象
            header_row = next(render)
            for key in header_row:
                if key not in M:
                    logger.info("[%s] is not support" % key)
                    return
                keys.append(key)
            logger.info("Prase csv header: %s" % str(keys))

            line = 2
            for values in render:
                if len(values) != len(M):
                    logger.error("[%d] line format error" % line)
                    continue
                keys.append("line")
                values.append(str(line))
                task = zip(keys, values)  # list -> dict
                tasks.append(dict(task))
                line += 1
            return tasks
    except Exception as e:
        logger.exception(e)
        exit(-1)


def get_vod_v2_status(transcode_status, record_status):
    """转换v2点播状态码, vod_info表status值"""
    try:
        transcode_status = int(transcode_status)
        record_status = int(record_status)
        vod_v2_status = VodV2Status.PUBLISHED
        if transcode_status > 0:
            if transcode_status == 1:
                vod_v2_status = VodV2Status.PUBLISHED
            elif transcode_status == 2:
                vod_v2_status = VodV2Status.TRANSCODE_FAILED
            elif transcode_status == 3:
                vod_v2_status = VodV2Status.TRANSCODING
        elif record_status > 0:
            if record_status == 1:
                vod_v2_status = VodV2Status.RECORD_SUCCEED
            elif record_status == 2:
                vod_v2_status = VodV2Status.RECORD_FAILED

        return vod_v2_status
    except Exception as e:
        logger.exception(e)
        return


def insert_migration_record(record_id, vod_id, app_id):
    """初始化迁移记录信息，status:0失败，1成功"""
    migration_record = {
        "app_id": app_id,
        "record_id": record_id,
        "vod_id": vod_id,
        "status": 1,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    # logger.info("migration_record is %s" % migration_record)
    re = insert_copy_info(db_name="vod_saas_migration_records", dict_data=migration_record)
    if re:
        return re
    else:
        return


def migration_record_count(record_id, status):
    """查询record_id是否已经迁移成功，已经迁移成功不再处理"""
    re = DbVodManage.get_migration_record_count(record_id, status)[0]["c"]
    if not re:
        return True
    else:
        return False


def producer(q1, input_file):
    """
    解析csv文件，格式化为json格式，push队列中；
    判断队列最大长度，循环等待；
    完成push结束标识，线程退出。
    :return:
    """
    tasks = csv_to_json(input_file)
    if tasks:
        exist_num = 0
        record_num = 0
        upload_num = 0
        for task in tasks:
            re = migration_record_count(task["record_id"], status=1)
            if not re:
                logger.info("[%s] line already exists" % task["line"])
                continue

            if task["paas_record_id"] and task["paas_record_id"] != "0":
                task["type"] = types.exist  # 有paas_record_id 的数据，需要新导入数据库，创建新vod_id;
                exist_num += 1
            elif task["source"] == "0" or task["source"] == "1":
                if (not task["video_path"] or task["video_path"] == "0") or (
                        not task["msg_path"] or task["msg_path"] == "0"):
                    logger.error("[%s] line video_path and msg_path error" % task["line"])
                    continue
                if not task["quality"] or task["quality"] == "0":  # 针对回放类型，"quality"为空，默认为same类型；
                    task["quality"] = "same"
                task["type"] = types.record  # HLS不需要上传文件类型;
                record_num += 1
            elif task["source"] == "2":
                task["type"] = types.upload  # MP4需要上传类型，检查quality;
                upload_num += 1
            else:
                pass

            re = get_vod_v2_status(task["transcode_status"], task["record_status"])
            if not re:
                logger.error("[%s] line, transcode_status or record_status type error" % task["line"])
                continue
            task["vod_v2_status"] = re

            # saas和paas，'source'字段映射关系
            if task["source"] == "0":
                task["record_mode"] = "0"
            elif task["source"] == "1":
                task["source"] = "0"
                task["record_mode"] = "1"
            elif task["source"] == "2":
                task["source"] = "1"
                task["record_mode"] = "0"
                """针对没有原始上传到oss的MP4文件，上传类型视频， 拼接规则demand/d9eb06c7f5314508eee2058ee1024639.mp4，保存数据库，不需要上传原画MP4;"""
                if (not task["source_path"] or task["source_path"] == "0") and (not task["paas_record_id"] or
                                                                                task["paas_record_id"] == "0"):
                    task["source_path"] = os.path.join("demand", os.path.split(task["video_path"])[1])
            else:
                logger.error("[%s] line, source[%s] type error" % (task["line"], task["source"]))
                continue

            q1.push_task_to_queue(task)  # 生产队列
        logger.info("Type total statistics. exist: %d, record: %d, upload: %d" % (exist_num, record_num, upload_num))

    q1.push_task_to_queue("")  # 结束标识


def create_vod_id():
    """创建点播id"""
    rand_str = random.randint(111111, 999999)
    timestamp = int(time.time())
    crc_str = binascii.crc32("%s%s%s%s" % ("vod", "create", timestamp, rand_str)) & 0xffffffff
    vod_id = hex(crc_str)[2:]
    return vod_id


def insert_copy_info(db_name="", dict_data=None):
    """插入数据，返回id"""
    try:
        rs = DbVodManage.insert_data(db_name, dict_data)
        if rs and rs[0] > 0:
            re = rs[1]
            # DbVodManage.close()
            return re
        else:
            # DbVodManage.close()
            return None
    except Exception as e:
        logger.exception(e)
        # DbVodManage.close()


def get_each_quality_path(quality, video_path):
    """
    上传文件拼接各个清晰度路径；
    例如：49e7d98daf8959f77f9108c36461821c_480p.mp4
    """
    video_path_list = list()
    if quality and quality != "0":
        quality = list(quality.split(","))
        for q in quality:
            if not video_path or video_path == "0":
                return
            if q == "raw":
                video_path_list.append((video_path, "same"))
                continue

            sp = os.path.splitext(video_path)
            path = "{pre}_{q}{suf}".format(pre=sp[0], q=q, suf=sp[1])
            video_path_list.append((path, q))

        # video_path_list.append((video_path, "same"))
        return video_path_list


def insert_new_vod_info(task, app_id, task_type=0):
    """上传文件创建新vod_info，返回新创建vod_id"""
    for i in range(20):
        new_vod_id = create_vod_id()
        rs = DbVodManage.get_exist_of_vod(new_vod_id)
        if rs and len(rs) > 0:
            continue
        if task_type == types.exist:
            old_vod_id = task["paas_record_id"]
            vod_info = DbVodManage.get_vod_info(old_vod_id)
            if not vod_info:
                logger.error("[%s] line 'paas_record_id:%s' does not exist" % (task["line"], task["paas_record_id"]))
                return

            for r in vod_info:
                copy_vod_info = {
                    "app_id": app_id,
                    "vod_id": new_vod_id,
                    "name": r["name"],
                    "room_id": r["room_id"],
                    "source": r["source"],
                    "status": r["status"],
                    "record_status": r["record_status"],
                    "duration": r["duration"],
                    "storage": r["storage"],
                    "msg_path": r["msg_path"],
                    "snapshot_path": r["snapshot_path"],
                    "audit_status": r["audit_status"],
                    "video_points": r["video_points"],
                    "transcode_status": r["transcode_status"],
                    "transcode_code": r["transcode_code"],
                    "is_edited": r["is_edited"],
                    "record_mode": r["record_mode"],
                    "is_deleted": r["is_deleted"],
                    "cate_id": r["cate_id"],
                    "has_subtitle": r["has_subtitle"],
                    "is_safe": r["is_safe"],
                    "doc_title_status": r["doc_title_status"],
                    "created_at": str(r["created_at"]),
                    "updated_at": str(r["updated_at"]),
                    "deleted_at": str(r["deleted_at"]),
                }
                re = insert_copy_info(db_name="vod_info", dict_data=copy_vod_info)
                if re:
                    return new_vod_id
                else:
                    return
        else:
            copy_vod_info = {
                "app_id": app_id,
                "vod_id": new_vod_id,
                "name": task["name"],
                "room_id": task["webinar_id"],
                "source": task["source"],
                "status": task["vod_v2_status"],
                "record_status": task["record_status"],
                "duration": task["duration"],
                "storage": task["storage"],
                "msg_path": task["msg_path"] if task_type == types.record else "",
                # "snapshot_path": task["snapshot_path"],
                # "audit_status": task["audit_status"], # 待确定 审核状态:0 上线  1待审核 2下线
                # "video_points": task["video_points"],
                "transcode_status": task["transcode_status"],
                # "transcode_code": task["transcode_code"],
                # "is_edited": task["is_edited"], # 待确定 是否是剪辑点播，0不是，1为是
                "record_mode": task["record_mode"],
                # "is_deleted": task["is_deleted"],
                # "cate_id": task["cate_id"],
                # "has_subtitle": task["has_subtitle"],
                # "is_safe": task["is_safe"], # 待确定
                # "doc_title_status": task["doc_title_status"],
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                # "updated_at": str(task["updated_at"]),
                # "deleted_at": str(task["deleted_at"]),
            }
            re = insert_copy_info(db_name="vod_info", dict_data=copy_vod_info)
            if re:
                return new_vod_id
            else:
                return


def insert_new_video_info(task, video_path=None, task_type=0):
    """插入video_info，返回video_id"""
    if task_type == types.exist:
        new_video_info = {
            "source": task["source"],
            "is_original": task["is_original"],
            "video_type": task["video_type"],
            "file_format": task["file_format"],
            "media_type": task["media_type"],
            "md5": task["md5"],
            "video_path": task["video_path"],
            "quality": task["quality"],
            "definition": task["definition"],
            "v_width": task["v_width"],
            "v_height": task["v_height"],
            "storage_kbyte": task["storage_kbyte"],
            "duration_s": task["duration_s"],
            "bitrate_kbps": task["bitrate_kbps"],
            "is_exist": task["is_exist"],
            "record_status": task["record_status"],
            "transcode_status": task["transcode_status"],
            "oss_endpoint": task["oss_endpoint"],
            "oss_bucket": task["oss_bucket"],
            "visit_num": task["visit_num"],
            "is_deleted": task["is_deleted"],
            "created_at": str(task["created_at"]),
            "updated_at": str(task["updated_at"]),
            "deleted_at": str(task["deleted_at"]),
            "safe": task["safe"],
        }
    else:
        file_suffix = os.path.splitext(task["video_path"])[1]
        if file_suffix == ".mp4":
            task["video_type"] = "mp4"
            task["file_format"] = "mp4"
        elif file_suffix == ".m3u8":
            task["video_type"] = "hls"
            task["file_format"] = "m3u8"
        new_video_info = {
            "is_original": task["is_original"],
            "source": task["source"],
            "is_exist": 0,
            "video_path": video_path[0] if task_type == types.upload else video_path,  # [("*/.mp4", "720p")]
            "video_type": task["video_type"],
            "file_format": task["file_format"],
            "duration_s": task["duration"],
            "transcode_status": task["transcode_status"],
            "quality": video_path[1] if task_type == types.upload else task["quality"],
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }
    logger.info("video_path: %s" % new_video_info["video_path"])
    re = insert_copy_info(db_name="video_info", dict_data=new_video_info)
    if re:
        return re
    else:
        return


def oss_batch_copy(src_path_list, saas_record_id, new_vod_id, task_type):
    """
    oss内部批量拷贝文件，返回拷贝后的路径
    src_path_list例子：[("/*/.mp4", "720p"), ("/*/.mp4", "")]
    """
    # todo 测试关闭
    new_path_list = list()
    for src_path in src_path_list:
        copy_file_path = os.path.join(config["paas"]["path_prefix"], src_path[0][1:])
        copy_file_path = copy_file_path.replace(saas_record_id, new_vod_id)
        re = dest_file_vio.all.vbackup(src_path[0], str(config["global"]["oss_mp4_bucket"]),
                                       target_dir_path=os.path.split(copy_file_path)[0])
        logger.info("Copy path [%s] to [%s]" % (src_path[0], copy_file_path))
        if re:
            if task_type == types.upload:
                new_path_list.append((copy_file_path, src_path[1]))
            else:
                new_path_list.append(copy_file_path)
        else:
            continue
    return new_path_list
    # todo 测试使用
    # new_path_list = list()
    # for i in src_path_list:
    #     new_path_list.append(i)
    #
    # return new_path_list
    # new_path_list = list()
    # for src_path in src_path_list:
    #     # 下载
    #     local_file_path = os.path.join(config["paas"]["path_prefix"], src_path[0][1:])
    #     re = dest_file_vio.all.advanced_download(src_path[0], local_file_path)
    #     if not re:
    #         logger.error("Download file [%s] failed" % src_path[0])
    #         continue
    #
    #     logger.info("Download file [%s] to local [%s]" % (src_path[0], local_file_path))
    #     # 上传
    #     re = dest_file_vio.all.advanced_oss_upload(src=local_file_path, dest=local_file_path)
    #     if not re:
    #         logger.error("Upload file [%s] failed" % local_file_path)
    #         continue
    #
    #     new_path_list.append(local_file_path)
    #
    # return new_path_list


def insert_new_vod_ext_video(new_vod_id, new_video_id_list):
    """批量插入vod_ext_video，vod_id与video_id关联数据"""
    new_ext_id_list = list()
    if not new_vod_id or not new_video_id_list:
        return

    for new_video_id in new_video_id_list:
        copy_vod_ext_video = {
            "vod_id": new_vod_id,
            "video_id": new_video_id,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }
        re = insert_copy_info(db_name="vod_ext_video", dict_data=copy_vod_ext_video)
        if re:
            new_ext_id_list.append(re)
        else:
            continue
    return new_ext_id_list


def batch_insert_video_info(task, new_video_path_list, task_type):
    """上传和回放点播批量插入video_info"""
    video_id_list = list()
    if task["type"] == types.record or task["type"] == types.upload:
        task["is_original"] = 1
        task["transcode_status"] = 1
        for new_video_path in new_video_path_list:
            re = insert_new_video_info(task, video_path=new_video_path, task_type=task_type)
            if not re:
                continue
            video_id_list.append(re)
    else:
        return "skip"
    return video_id_list


# def update_migration_record(migration_id, vod_id, status=0):
#     """更新迁移表，迁移失败或成功状态"""
#     update_data = {
#         "vod_id": vod_id,
#         "status": status,
#         "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     }
#     where_data = {
#         "id": migration_id
#     }
#     return DbVodManage.update_data(db_name="saas_migrate_to_paas_records", update_dict=update_data,
#                                    where_dict=where_data)


def document_format_conversion(input_js, old_msg_file, user_id, input_mapping):
    """
    cuepoint.msg文档格式转换;
    可使用nodejs执行的新旧文档转换程序;
    node flash_covert_h5.js -i flash_old.msg  -u 17815972  -r doc_mapping.js  -o paas_new.msg
    -u user_id
    """
    # todo 测试关闭
    try:
        new_msg_file = os.path.join(config["paas"]["path_prefix"], old_msg_file[1:])
        output_path, _ = os.path.split(new_msg_file)
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        command = "node {js} -i {old} -u {user_id} -r {map_file} -o {output}".format(js=input_js,
                                                                                     old=old_msg_file, user_id=user_id,
                                                                                     map_file=input_mapping,
                                                                                     output=new_msg_file)
        logger.info("Command: %s" % command)
        process = subprocess.Popen(command, stdin=None,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, shell=True)
        process.communicate()
        # logger.info("Doc converted successfully: %s" % new_msg_file)
        return new_msg_file
    except Exception as e:
        logger.exception("")
        return ""
    # return old_msg_file


def download_cuepoint_msg(msg_path):
    """下载saas的cuepoint.msg文件到本地"""
    local_msg = msg_path
    logger.info("msg_path: %s, local_msg: %s" % (msg_path, local_msg))
    re = dest_file_vio.all.advanced_download(msg_path, local_msg)
    # re = dest_file_vio.all.download(msg_path, local_msg)
    if not re:
        logger.error(re)
        return
    else:
        return local_msg
    # return msg_path


def upload_new_msg_file(new_msg_file):
    """上传oss"""
    # todo 测试关闭
    return dest_file_vio.all.advanced_oss_upload(src=new_msg_file, dest=new_msg_file)
    # return True


def remove_file(files):
    """删除文件"""
    for f in files:
        if os.path.exists(f):
            os.remove(f)


def insert_vod_saas_migration_data(task):
    """插表vod_saas_migration，保存saas数据，添加paas点播ID"""

    def insert_data(db_name, data_dict):
        try:
            data_values = "(" + "%s," * (len(data_dict)) + ")"
            data_values = data_values.replace(',)', ')')

            db_field = data_dict.keys()
            data_tuple = tuple(data_dict.values())
            db_field = str(tuple(db_field)).replace("'", '')
            sql = """ insert into %s %s values %s """ % (db_name, db_field, data_values)
            params = data_tuple
            rs = db_obj_mig.sql_execute(sql, params)
            return rs
        except Exception as e:
            logger.exception(e)
            return None

    saas_migration_info = {
        "record_id": task["record_id"],
        "webinar_id": task["webinar_id"],
        "user_id": task["user_id"],
        "room_id": task["room_id"],
        "name": task["name"],
        "video_path": task["video_path"],
        "msg_path": task["msg_path"],
        "quality": task["quality"],
        "duration": task["duration"],
        "storage": task["storage"],
        "source": task["source"],
        "source_path": task["source_path"],
        "record_status": task["record_status"],
        "transcode_status": task["transcode_status"],
        # "created_at": task["created_at"],
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "paas_record_id": task["paas_record_id"],
        "paas_app_id": task["app_id"],
    }
    rs = insert_data("vod_saas_migration", saas_migration_info)
    if rs and rs[0] > 0:
        re = rs[1]
        return re
    else:
        return None


def start_migration(q1, coroutine_id, input_js, input_mapping, app_id):
    """开始点播数据迁移"""
    migration_total = 0
    migration_success = 0
    while True:
        try:
            task = q1.get_task_from_queue()
            if not task:
                q1.push_task_to_queue("")  # 结束标识
                break

            migration_total += 1
            # logger.info("[%s] line start migration" % task["line"])
            new_vod_id = None
            new_video_id_list = list()
            new_video_path_list = list()
            if task["type"] == types.exist:  # 有paas_record_id 的数据，需要新导入数据库，创建新vod_id;
                # logger.info("[%s] line, exist type start migration" % task["line"])
                re = insert_new_vod_info(task, app_id, task_type=types.exist)
                if not re:
                    # logger.error("[%s] line insert vod_info failed" % task["line"])
                    logger.error("[%s] line migrate failed" % task["line"])
                    continue

                new_vod_id = re
                video_info = DbVodManage.get_video_info(task["paas_record_id"])
                for r in video_info:
                    new_video_path = r["video_path"].replace(task["paas_record_id"], new_vod_id)
                    if new_video_path == r["video_path"]:  # 由于video_path不能重复插入demand/*.mp4，在此处做判断过滤；
                        continue
                    re = dest_file_vio.all.vbackup(r["video_path"], str(config["global"]["oss_mp4_bucket"]),
                                                   target_dir_path=os.path.split(new_video_path)[0])
                    if not re:
                        logger.error("[%s] line backup video failed" % task["line"])
                    r["video_path"] = new_video_path
                    re = insert_new_video_info(r, task_type=types.exist)
                    if not re:
                        logger.error("[%s] line insert video_info failed" % task["line"])
                        continue
                    new_video_id_list.append(re)
                if not new_video_id_list:
                    logger.error("[%s] line migrate failed" % task["line"])
                    continue
            elif task["type"] == types.record:  # HLS不需要上传文件类型;
                # logger.info("[%s] line, record type start migration" % task["line"])
                re = download_cuepoint_msg(task["msg_path"])
                if not re:
                    logger.error("[%s] line download cuepoint.msg failed" % task["line"])
                    continue

                download_cuepoint_file = re
                re = document_format_conversion(input_js, download_cuepoint_file, task["user_id"], input_mapping)
                if not os.path.exists(re):
                    logger.error("[%s] line doc format conversion failed" % task["line"])
                    task["msg_path"] = ""
                else:
                    upload_cuepoint_file = re
                    re = upload_new_msg_file(upload_cuepoint_file)
                    if not re:
                        logger.error("[%s] line upload new cuepoint.msg failed" % task["line"])
                        continue

                    task["msg_path"] = upload_cuepoint_file
                    remove_file((download_cuepoint_file, upload_cuepoint_file))
                re = insert_new_vod_info(task, app_id, task_type=types.record)
                if not re:
                    logger.error("[%s] line insert vod_info failed" % task["line"])
                    continue
                new_vod_id = re
                # logger.info("New vod_id is %s" % new_vod_id)
                re = oss_batch_copy([(task["video_path"], "")], task["record_id"], new_vod_id, task["type"])
                if not re:
                    logger.error("[%s] line [%s] copy MP4 file error" % (task["line"], task["video_path"]))
                    continue
                new_video_path_list = re
                logger.info("New video_path is %s" % new_video_path_list)
            elif task["type"] == types.upload:  # MP4需要上传类型，检查quality;
                # logger.info("[%s] line, upload type start migration" % task["line"])
                re = get_each_quality_path(task["quality"], task["video_path"])
                if not re:
                    logger.error("[%s] line, video_path[%s] error" % (task["line"], task["video_path"]))
                    continue

                old_video_path_list = re
                re = insert_new_vod_info(task, app_id, task_type=types.upload)
                if not re:
                    logger.error("[%s] line insert vod_info failed" % task["line"])
                    continue

                new_vod_id = re
                # logger.info("New vod_id is %s" % new_vod_id)
                task["is_original"] = 0
                task["transcode_status"] = 0
                re = insert_new_video_info(task, video_path=(task["source_path"], ""),
                                           task_type=types.upload)  # 插入一条原始视频文件路径数据：demand/*.mp4
                if not re:
                    logger.info("[%s] line duplicate entry video_path" % task["line"])
                else:
                    new_video_id_list.append(re)

                re = oss_batch_copy(old_video_path_list, task["record_id"], new_vod_id, task["type"])
                if not re:
                    logger.error("[%s] line [%s] copy MP4 file error." % (task["line"], task["video_path"]))
                    continue
                new_video_path_list = re
                # logger.info("New video_path is %s" % new_video_path_list)
            else:
                pass

            re = batch_insert_video_info(task, new_video_path_list, task["type"])
            if not re:
                logger.error("[%s] line batch insert video_info failed" % task["line"])
            elif re == "skip":
                pass
            else:
                new_video_id_list += re

            # logger.info("New video_id is %s" % new_video_id_list)
            re = insert_new_vod_ext_video(new_vod_id, new_video_id_list)
            if not re:
                logger.error("[%s] line migrate failed" % task["line"])
                continue
            new_ext_id = re

            task["paas_record_id"] = new_vod_id
            re = insert_migration_record(task["record_id"], new_vod_id, app_id)
            if not re:
                logger.error("[%s] line insert migration_record failed" % task["line"])
                continue
            else:
                task["migration_id"] = re

            logger.info("[%s] line : new vod_id:[%s], new video_id:[%s], new ext_id:[%s], new migration_id:[%s]" % (
                task["line"], new_vod_id, new_video_id_list, new_ext_id, task["migration_id"]))
            migration_success += 1
            task["app_id"] = app_id
            re = insert_vod_saas_migration_data(task)
            if not re:
                logger.error("[%s] line insert vod_saas_migration failed" % task["line"])
            gevent.sleep(0.01)
        except Exception as e:
            logger.exception(e)

    logger.info("cor_id[%d] The total number of migrated data is [%d], [%d] successfully and [%d] failed" % (
        coroutine_id, migration_total, migration_success, (migration_total - migration_success)))


def get_migrate_records(app_id):
    """查看迁移数据是否已经迁移成功"""
    where_dict = {
        "app_id": app_id,
        "status": 1
    }
    return DbVodManage.select_data("vod_saas_migration_records", ["record_id", "vod_id"], where_dict)


def output_mapping_file(mapping_file, app_id):
    """查表saas_migrate_to_paas_records获取所有数据，输出点播数据迁移完成后映射结"""
    try:
        re = get_migrate_records(app_id)
        if not re:
            logger.info("Migrate failed")
            return

        headers = ["record_id", "vod_id"]
        rows = list()
        for mapping_data in re:
            rows.append(mapping_data)

        with open(mapping_file, "wb") as w_fd:
            f_csv = csv.DictWriter(w_fd, fieldnames=headers)
            f_csv.writeheader()
            f_csv.writerows(rows)
        logger.info("Migrate successed")
    except Exception as e:
        logger.exception(e)


def consumer(q1, num_coroutines, output_csv, input_js, input_mapping, app_id):
    """
    启动*个协程pop任务，下载视频，并上传oss；
    将点播数据写入数据库，获取vod_id；
    使用nodejs工具格式化文档文件输出paas_new.msg；
    上传oss，根据vod_id更新到数据库与视频地址关联；
    将vod_id和record_id写入数据库；
    从数据库获取全量映射数据写入文件，关闭文件，线程退出；
    :return:
    """
    jobs = list()
    for i in range(10 if not num_coroutines else int(num_coroutines)):
        jobs.append(gevent.spawn(start_migration, q1, i, input_js, input_mapping, app_id))

    gevent.joinall(jobs)
    # start_migration(q1)
    output_mapping_file(output_csv, app_id)


def parser_command_line(args):
    parser = optparse.OptionParser("Usage: %prog [options]")
    # 检查文件类型
    parser.add_option("-c", "--check", dest="check_type", default="", help="check migration data type")
    # 队列限制大小
    parser.add_option("-m", "--max_size", dest="max_size", default="", help="set max size of queue")
    # 启动协程数量
    parser.add_option("-n", "--coroutines", dest="num_coroutines", default="", help="number of coroutines")
    # 待导入的文件
    parser.add_option("-i", "--input", dest="input_csv", default="", help="input csv file")
    parser.add_option("-f", "--flash_covert_h5_js", dest="input_js", default="", help="input js file")
    parser.add_option("-d", "--doc_mapping", dest="input_mapping", default="", help="input mapping file")
    # saas平台的app_id
    parser.add_option("-a", "--app_id", dest="app_id", default="", help="saas app_id")
    # 输出文件路径
    parser.add_option("-y", "--output", dest="output_csv", default="", help="output mapping file")
    # 配置文件信息
    parser.add_option("--config", dest="config", type=str, help="config file")

    options, _ = parser.parse_args(args)

    return options


if __name__ == '__main__':
    options = parser_command_line(sys.argv[1:])
    q1 = TaskQueue(max_size=3000 if not options.max_size else options.max_size)
    t1 = threading.Thread(target=producer, args=(q1, options.input_csv,))
    if not options.check_type:
        t2 = threading.Thread(target=consumer, args=(
            q1, options.num_coroutines, options.output_csv, options.input_js, options.input_mapping, options.app_id,))

        t1.start()
        t2.start()
        t1.join()
        t2.join()
    else:
        logger.info("66666666666666666")
        t1.start()
        t1.join()

    exit(0)
