#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""账户发布定时监控：默认每 30 分钟检查一次。"""

import argparse
import asyncio

from account_monitor import AccountMonitorService
from config import Config
from create_logger import logger


def run_check() -> None:
    result = asyncio.run(AccountMonitorService().check_all())
    logger.info(f"[account-monitor] 本轮完成: {result}")


def main():
    parser = argparse.ArgumentParser(description="HotnewsFeed 账户发布监控")
    parser.add_argument("--init", action="store_true", help="仅初始化监控表和配置账户")
    parser.add_argument("--run-once", action="store_true", help="立即检查一次后退出")
    args = parser.parse_args()
    service = AccountMonitorService()
    if args.init:
        service.initialize()
        logger.info("[account-monitor] MySQL 监控表初始化完成")
        return
    if args.run_once:
        run_check()
        return

    from apscheduler.schedulers.blocking import BlockingScheduler
    minutes = Config().account_monitor["interval_minutes"]
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        run_check, "interval", minutes=minutes, id="account_monitor",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    logger.info(f"[account-monitor] 已启动：每 {minutes} 分钟检查账户新发布")
    scheduler.start()


if __name__ == "__main__":
    main()
