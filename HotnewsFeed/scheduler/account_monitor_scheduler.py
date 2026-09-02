#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""账户发布定时监控：默认每 30 分钟检查一次。

独立于 A2A Agent 的定时调度入口。用 APScheduler 的 BlockingScheduler 每
interval_minutes（config.ini [account_monitor]）分钟触发一次 check_all()，
全量检查已启用账户的新发布。支持命令行三态：--init 只建表并注册配置账户、
--run-once 立即检查一次后退出、无参数则启动常驻定时调度。

模块依赖:
- ``apscheduler.schedulers.blocking.BlockingScheduler`` : 阻塞式定时调度器，本进程内
  按固定间隔触发任务。
- ``account_monitor.AccountMonitorService`` : 账户监控业务（check_all / initialize）。
- ``Config``          : 读取 [account_monitor] interval_minutes 调度间隔。
- ``create_logger``   : 全局日志器。

对外暴露的接口：
- run_check : 执行一轮全量检查（供调度器 / --run-once 调用）。
- main      : 命令行入口。
- __main__  : 直接运行即启动常驻调度。
"""

import argparse
import asyncio

from account_monitor import AccountMonitorService
from config import Config
from create_logger import logger


def run_check() -> None:
    """立即执行一轮账户全量检查。

    说明:
        - ``asyncio.run(AccountMonitorService().check_all())`` 在新建事件循环里跑异步
          check_all（定时回调是同步函数，不能直接 await）。
        - 结果 dict 打日志，便于从日志确认本轮 fetched/new 数量。
    """
    # asyncio.run 在新建事件循环里跑异步 check_all（定时回调是同步函数）。
    result = asyncio.run(AccountMonitorService().check_all())
    logger.info(f"[account-monitor] 本轮完成: {result}")  # 记录本轮各账户检查结果。


def main():
    """命令行入口：--init 建表注册账户 / --run-once 检查一次 / 默认启动常驻调度。

    说明:
        - argparse 解析 --init / --run-once 两个开关。
        - --init：只调 ``service.initialize()``（建表 + 注册 config.ini 账户）后退出。
        - --run-once：调 ``run_check()`` 执行一轮后退出。
        - 默认：BlockingScheduler（Asia/Shanghai）按 interval_minutes 分钟固定间隔调度
          run_check；replace_existing 覆盖同名任务、max_instances=1 防止重叠执行、
          coalesce=True 错过多次只补执行一次。
    """
    # 命令行参数解析：--init 仅初始化，--run-once 立即检查一次。
    parser = argparse.ArgumentParser(description="HotnewsFeed 账户发布监控")
    parser.add_argument("--init", action="store_true", help="仅初始化监控表和配置账户")
    parser.add_argument("--run-once", action="store_true", help="立即检查一次后退出")
    args = parser.parse_args()  # 解析命令行参数。
    service = AccountMonitorService()  # 业务对象（内部含 MySQL 存储层）。
    if args.init:
        service.initialize()  # 建表 + 注册 config.ini 账户。
        logger.info("[account-monitor] MySQL 监控表初始化完成")
        return  # 初始化完成即退出。
    if args.run_once:
        run_check()  # 立即执行一轮检查后退出。
        return

    # 延迟导入：只有启动常驻调度（默认路径）才需要 APScheduler。
    from apscheduler.schedulers.blocking import BlockingScheduler
    # 调度间隔（分钟）来自 config.ini [account_monitor] interval_minutes，默认 30。
    minutes = Config().account_monitor["interval_minutes"]
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")  # 阻塞式调度器，东八区。
    scheduler.add_job(
        run_check, "interval", minutes=minutes, id="account_monitor",
        replace_existing=True, max_instances=1, coalesce=True,  # 覆盖同名任务、防重叠、错过补执行。
    )
    logger.info(f"[account-monitor] 已启动：每 {minutes} 分钟检查账户新发布")
    scheduler.start()  # 阻塞直到进程被终止。


if __name__ == "__main__":
    main()
