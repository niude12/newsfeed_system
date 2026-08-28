#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每天 06:00 更新离线新闻库；支持 --init 和 --run-once。"""

import argparse
import asyncio

from config import Config
from create_logger import logger
from offline_news import OfflineNewsService


def run_ingest() -> None:
    result = asyncio.run(OfflineNewsService().ingest())
    logger.info(f"[scheduler] 离线新闻任务完成: {result}")


def run_briefings() -> None:
    """不重新抓取，只使用今日 MySQL 数据生成简报。"""
    from offline_news.briefing import generate_daily_briefings
    result = asyncio.run(generate_daily_briefings())
    logger.info(f"[scheduler] 离线每日简报完成: {result}")


def main():
    parser = argparse.ArgumentParser(description="HotnewsFeed 离线新闻定时任务")
    parser.add_argument("--init", action="store_true", help="仅初始化 MySQL 表和 Milvus 集合")
    parser.add_argument("--run-once", action="store_true", help="立即执行一次采集入库后退出")
    parser.add_argument("--briefing-only", action="store_true", help="仅使用今日数据生成各板块简报")
    args = parser.parse_args()
    service = OfflineNewsService()
    if args.init:
        service.initialize()
        return
    if args.run_once:
        run_ingest()
        return
    if args.briefing_only:
        service.initialize()
        run_briefings()
        return

    from apscheduler.schedulers.blocking import BlockingScheduler
    cfg = Config().offline_news
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_ingest, trigger="cron", hour=cfg["schedule_hour"],
        minute=cfg["schedule_minute"], id="offline_news_daily",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    logger.info(
        f"[scheduler] 已启动：每天 {cfg['schedule_hour']:02d}:{cfg['schedule_minute']:02d} "
        "采集离线新闻并按板块生成简报"
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 已停止")


if __name__ == "__main__":
    main()
