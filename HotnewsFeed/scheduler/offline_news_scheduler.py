#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每天 06:00 更新离线新闻库；支持 --init 和 --run-once。

离线新闻定时任务的命令行入口：
- 无参数     ：启动 APScheduler 阻塞调度器，每天 schedule_hour:schedule_minute 执行采集入库；
- --init      ：仅初始化 MySQL 表与 Milvus 集合后退出；
- --run-once  ：立即执行一次采集入库后退出；
- --briefing-only：不重新抓取，仅用今日 MySQL 数据生成各板块简报。

模块依赖:
- ``OfflineNewsService``       : offline_news/service.py，采集入库与查询门面。
- ``generate_daily_briefings`` : offline_news/briefing.py，每日简报生成。
- ``BlockingScheduler``        : apscheduler.schedulers.blocking，cron 触发器每日定时。
- ``Config``                   : offline_news 属性提供 schedule_hour / schedule_minute。
"""

import argparse
import asyncio

from config import Config
from create_logger import logger
from offline_news import OfflineNewsService


def run_ingest() -> None:
    """同步包装：完整执行一轮「采集入库 + 简报」任务。

    说明:
        asyncio.run() 在同步上下文里驱动 OfflineNewsService().ingest() 协程；
        该函数被 APScheduler 作为无参 job 直接调用，也由 --run-once 路径调用。
    """
    result = asyncio.run(OfflineNewsService().ingest())  # 同步上下文驱动采集入库协程。
    logger.info(f"[scheduler] 离线新闻任务完成: {result}")


def run_briefings() -> None:
    """不重新抓取，只使用今日 MySQL 数据生成简报。

    说明:
        延迟 import generate_daily_briefings，避免未启用简报时引入其依赖；
        asyncio.run 驱动协程，结果记入日志。
    """
    # 延迟 import：仅生成简报时才引入 briefing 模块。
    from offline_news.briefing import generate_daily_briefings
    result = asyncio.run(generate_daily_briefings())  # 同步上下文驱动简报生成协程。
    logger.info(f"[scheduler] 离线每日简报完成: {result}")


def main():
    """解析命令行参数并分派到对应执行路径。

    说明:
        - argparse 定义三个可选开关：--init / --run-once / --briefing-only；
        - --briefing-only 会先 initialize() 确保表结构存在再生成简报；
        - 默认（无参数）路径：BlockingScheduler(timezone="Asia/Shanghai") 创建阻塞调度器，
          add_job 用 cron 触发器把 run_ingest 绑定到配置的时刻；
          replace_existing=True 防止重复注册、max_instances=1 防止任务重叠、
          coalesce=True 让错过的时间点合并只执行一次；
        - scheduler.start() 阻塞主线程直到 KeyboardInterrupt / SystemExit。
    """
    parser = argparse.ArgumentParser(description="HotnewsFeed 离线新闻定时任务")
    parser.add_argument("--init", action="store_true", help="仅初始化 MySQL 表和 Milvus 集合")
    parser.add_argument("--run-once", action="store_true", help="立即执行一次采集入库后退出")
    parser.add_argument("--briefing-only", action="store_true", help="仅使用今日数据生成各板块简报")
    args = parser.parse_args()  # 解析命令行参数。
    service = OfflineNewsService()  # 创建服务实例（供 --init / --briefing-only 路径使用）。
    if args.init:  # 仅初始化模式：建表/建集合后直接退出。
        service.initialize()
        return
    if args.run_once:  # 立即执行一次采集入库后退出。
        run_ingest()
        return
    if args.briefing_only:  # 仅生成简报模式：不重新抓取。
        service.initialize()  # 确保表结构存在（简报要从 MySQL 读今日数据）。
        run_briefings()
        return

    # 延迟 import：只有默认调度路径才需要 APScheduler。
    from apscheduler.schedulers.blocking import BlockingScheduler
    cfg = Config().offline_news  # [offline_news] 配置段，取每日调度时刻。
    # 阻塞式调度器：start() 后主线程阻塞，到点执行注册的 cron job。
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_ingest, trigger="cron", hour=cfg["schedule_hour"],  # 每日小时。
        minute=cfg["schedule_minute"], id="offline_news_daily",  # 每日分钟 + 任务唯一 ID。
        replace_existing=True, max_instances=1, coalesce=True,  # 防重复注册 / 防重叠 / 错过补跑合并。
    )
    logger.info(
        f"[scheduler] 已启动：每天 {cfg['schedule_hour']:02d}:{cfg['schedule_minute']:02d} "
        "采集离线新闻并按板块生成简报"
    )
    try:
        scheduler.start()  # 阻塞运行调度器，直到被中断。
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 已停止")  # 收到 Ctrl+C / 退出信号时记录日志。


if __name__ == "__main__":
    main()
