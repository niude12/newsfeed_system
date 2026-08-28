#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: create_logger.py
项目: HotnewsFeed

本文件干什么：
    日志工厂。提供一个 setup_logger() 函数，创建一个「既能打印到控制台、又能写入日志文件」的
    日志对象（仿照同级目录 LlmProject 的 create_logger.py）。项目里所有模块都通过它拿统一的 logger：
        from create_logger import logger
        logger.info("...")   # 控制台 + 日志文件都会出现这条信息
        logger.error("...")  # 出错时用 error 级别

    启动：本文件不单独启动，是被其他模块 import 使用的。
"""
# ======================= 引入需要用到的模块 =======================
# logging：Python 自带的日志库。负责分级输出（DEBUG/INFO/ERROR...）、
#          格式化和分发到不同「处理器」（控制台/文件）。
import logging
# os：操作系统接口库。os.makedirs 用来创建日志文件所在的文件夹。
import os

# Config：读配置的类。这里用 Config().log_file 拿日志文件路径（config.ini [log] log_file）。
from config import Config


# ===== 创建日志器的函数 =====
def setup_logger(name, log_file=None):
    """
    用法：创建一个"双通道"日志器（控制台 + 文件）。

    示例：
        from create_logger import setup_logger
        logger = setup_logger('MyApp', 'logs/my.log')
        logger.info("开始处理...")     # 控制台和 my.log 里都有

    参数:
        name:     日志器名字。logging 会按名字缓存日志器，同名多次调用返回同一个。
        log_file: 日志文件路径，默认用 Config().log_file（即 项目根/logs/app.log）。
    返回:
        一个 logging.Logger 对象。用它 logger.info(...) / logger.warning(...) / logger.error(...) 打日志。
    """
    # ===== 1. 创建日志文件夹 =====
    # os.makedirs(路径, exist_ok=True)：递归创建目录，已存在也不报错。
    # 这样日志文件所在的目录一定存在，FileHandler 才不会因为目录缺失而启动失败。
    log_file = log_file or Config().log_file
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # ===== 2. 获取日志记录器 =====
    # logging.getLogger(name)：按名字获取日志器对象。
    # logging 内部用字典缓存 Logger，同名调用返回同一个对象（这就是为什么下面要防重复）。
    # 如果之前没有这个名字，就新建一个。
    logger = logging.getLogger(name)
    # setLevel(logging.DEBUG)：设置日志器的总开关级别。
    # 只有「级别 >= 这个值」的日志才可能被处理。DEBUG 最低，表示所有日志都放行，
    # 具体要不要显示，再看下面的每个处理器(Handler)自己的级别。
    logger.setLevel(logging.DEBUG)
    # ===== 防止重复输出的关键！=====
    # logger.propagate = False：关闭「向父日志器传递」。
    # 日志器有父子链（默认有一个 root 根日志器），如果不关掉，本条日志除了被这里
    # 的处理器打印，还会传给根日志器再打一遍 → 控制台会出现两遍相同的日志。
    # 设成 False 就只走我们自己的处理器。
    logger.propagate = False

    # ===== 3. 定义日志格式 =====
    # logging.Formatter(格式字符串)：
    #   %(name)s      日志器名字（如 HotnewsFeed）
    #   %(asctime)s   时间（如 2026-08-26 16:50:00,123）
    #   %(levelname)s 级别（如 INFO / ERROR）
    #   %(message)s   你自己传入的日志内容
    # 之后两个处理器都用这个格式。
    formatter = logging.Formatter('%(name)s - %(asctime)s - %(levelname)s - %(message)s')

    # ===== 4. 创建控制台处理器 =====
    # logging.StreamHandler()：把日志写到控制台（标准输出）的处理器。
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)   # 用上面定义的格式
    # setLevel(logging.INFO)：这个处理器只处理 INFO 及以上日志。
    console_handler.setLevel(logging.INFO)

    # ===== 5. 创建文件处理器 =====
    # logging.FileHandler(文件名, encoding=..., mode=...)：
    #   把日志写到文件的处理器。encoding='utf-8' 避免中文乱码，mode='a' 追加写。
    file_handler = logging.FileHandler(filename=log_file, encoding="utf-8", mode="a")
    file_handler.setFormatter(formatter)   # 用上面定义的格式
    # setLevel(logging.DEBUG)：文件里记录更全（连 DEBUG 也写进去，方便排查细节）
    file_handler.setLevel(logging.DEBUG)

    # ===== 6. 将处理器添加到日志记录器中 =====
    # logger.addHandler(处理器)：把处理器挂到日志器上。
    # if not logger.handlers 先判断：因为 logging 按名字缓存，setup_logger 可能被多次调用，
    # 如果重复 addHandler，同一个处理器会被挂两次 → 日志打两遍。
    # 所以只在没有处理器时才添加。
    if not logger.handlers:  # 先进行判断，再进行添加。避免重复添加处理器
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger  # 把配好的日志器返回给调用方


# ===== 全局唯一的日志器 =====
# import 本模块时立刻创建好一个全局 logger：
#   name 传的是 'HotnewsFeed'（项目名）
#   log_file 用 Config().log_file（即 项目根/logs/app.log）
# 之后所有模块 import 这个 logger 直接用即可。
logger = setup_logger('HotnewsFeed')
