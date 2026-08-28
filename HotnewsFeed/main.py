#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""对外服务主入口（main.py）—— 测试用对话循环

在线：用户输入自然语言 → 协调调度 Agent（LLM 意图识别）
  → 经 A2A 路由到功能 Agent → MCP 网关调用（不可达自动降级进程内直调）
  → 结果格式化展示 → A2A 反问是否生成简报并推送（handoff_briefing）。
离线：`query_offline_news()` / `/offline` / “离线查询……”
  → Redis 精确缓存 → Milvus 向量检索 ID → MySQL 取原文（最多 3 条）。

运行前提：把 HotnewsFeed 加入 PYTHONPATH（PyCharm Sources Root，或
PYTHONPATH=D:\\agent_立项\\HotnewsFeed），conda 环境 news_feed。

用法：
    python main.py                 # 进入对话循环，输入 exit 退出
    # 建议先手动启动四台 MCP 服务器（见 项目流程.md）：
    #   python -m mcp_servers.mcp_collect_server   :8004 采集
    #   python -m mcp_servers.mcp_process_server   :8005 加工
    #   python -m mcp_servers.mcp_publish_server   :8006 输出
    #   python -m mcp_servers.mcp_video_server     :8007 视频内容
    # 未启动时 main 会提示，并自动降级进程内直调 tools.*（链路不断）。
"""

import json
import re
import sys
from typing import Dict, Optional

from create_logger import logger

# ===== 结果展示 =====
_TASK_TITLES = {
    "hotspot_query": "热点新闻",
    "latest_news": "最新新闻",
    "account_follow": "账户发布",
    "account_monitor": "账户监控",
}


def format_result(result: Optional[object]) -> str:
    """把 PipelineResult 格式化成控制台文本（按 task_type 分支）。

    条目格式参考 tools/publish.py::_format_markdown：
    - hotspot_query   → HotEvent：标题（热度 X · 可信度 Y · N 来源）＋ summary
    - latest_news     → NewsItem：标题（来源 · 时间）＋ url
    - account_follow  → AccountPost：标题/内容（账户@平台 · 时间）＋ url
    """
    if result is None:
        return "[空结果]"
    # follow_up：不是错误，是 LLM 意图识别的追问/直接回复话术（放在 error 字段）
    if getattr(result, "task_type", "") == "follow_up":
        return f"[需要追问/直接回复]\n{getattr(result, 'error', None) or '(无追问内容)'}"
    if getattr(result, "error", None):
        return f"[错误] {result.error}"
    task_type = getattr(result, "task_type", "")
    items = getattr(result, "items", None) or []
    title = _TASK_TITLES.get(task_type, task_type)
    lines = [f"== {title}（{len(items)} 条） =="]
    if task_type == "account_monitor":
        lines.extend(
            f"{i}. {json.dumps(item, ensure_ascii=False, default=str, indent=2)}"
            for i, item in enumerate(items, 1)
        )
        return "\n".join(lines)
    for i, item in enumerate(items, 1):
        lines.append(_format_item(i, task_type, item))
    return "\n".join(lines)


def _format_item(idx: int, task_type: str, item) -> str:
    """格式化单条 item（按 task_type 取不同字段）"""
    if task_type == "hotspot_query":
        heat = getattr(item, "heat_score", 0)
        cred = getattr(item, "credibility", "")
        sources = getattr(item, "sources", None) or []
        n = getattr(item, "article_count", None) or len(sources)
        head = f"{idx}. {getattr(item, 'title', '')}（热度 {heat} · 可信度 {cred} · {n} 来源）"
        summary = getattr(item, "summary", "") or ""
        return head + (f"\n   {summary}" if summary else "")
    if task_type == "latest_news":
        source = getattr(item, "source", "")
        when = getattr(item, "published_at", "")
        url = getattr(item, "url", "")
        head = f"{idx}. {getattr(item, 'title', '')}（{source} · {when}）"
        return head + (f"\n   {url}" if url else "")
    # account_follow
    text = getattr(item, "title", "") or getattr(item, "content", "")
    acc = getattr(item, "account", "")
    plat = getattr(item, "platform", "")
    when = getattr(item, "published_at", "")
    url = getattr(item, "url", "")
    head = f"{idx}. {text}（{acc}@{plat} · {when}）"
    return head + (f"\n   {url}" if url else "")


# ===== MCP 服务器探测 =====
def check_mcp_servers() -> Dict[str, dict]:
    """探测 MCP 服务器连通性（不托管进程）。

    Returns:
        {server: {"ok": bool, "tools": List[str], "url": str}}，
        连不上时 ok=False 并带上 error 原因。
    """
    from mcp_servers.mcp_access import MCP_URLS, mcp_list_tools, sync_call
    status: Dict[str, dict] = {}
    for name, url in MCP_URLS.items():
        try:
            tools = sync_call(mcp_list_tools(name, timeout=5.0))
            status[name] = {"ok": True, "tools": tools, "url": url}
        except Exception as exc:
            logger.warning(f"[main] MCP 服务器 {name}({url}) 不可达: {exc}")
            status[name] = {"ok": False, "tools": [], "url": url, "error": str(exc)}
    return status


def _print_mcp_status(status: Dict[str, dict]) -> None:
    print("MCP 服务器状态：")
    for name, info in status.items():
        if info["ok"]:
            print(f"  [OK]  {name:9s} {info['url']}  工具: {', '.join(info['tools']) or '(空)'}")
        else:
            print(f"  [X]   {name:9s} {info['url']}  不可达（将走进程内降级直调 tools.*）")


# ===== 任务执行 =====
def run_task_result(agent, result) -> None:
    """展示流水线结果；任务成功且有内容时 A2A 反问是否生成简报"""
    print(format_result(result))
    if (result and not getattr(result, "error", None)
            and getattr(result, "items", None)
            and getattr(result, "task_type", "") in
            ("hotspot_query", "latest_news", "account_follow")):
        print()
        agent.handoff_briefing(result)


# ===== 离线新闻对外接口 =====
def query_offline_news(query: str, limit: int = 3) -> dict:
    """查询离线新闻库，供控制台、未来 HTTP API 或其他 Python 代码复用。"""
    query = query.strip()
    if not query:
        raise ValueError("离线查询内容不能为空")
    from offline_news import OfflineNewsService
    return OfflineNewsService().query(query, limit=min(3, max(1, limit)))


def run_offline_query(query: str, limit: int = 3) -> None:
    """执行并打印离线查询结果。"""
    from offline_main import format_rows
    print(format_rows(query_offline_news(query, limit=limit)))


def _extract_offline_query(text: str) -> Optional[str]:
    """识别明确的离线查询话术；普通新闻问题仍走在线 Agent。"""
    patterns = (
        r"^(?:请)?(?:帮我)?从离线(?:新闻)?库(?:中)?(?:查询|查找|搜索)?[\s：:]*",
        r"^(?:请)?(?:帮我)?离线查询[\s：:]*",
    )
    for pattern in patterns:
        if re.match(pattern, text, flags=re.IGNORECASE):
            return re.sub(pattern, "", text, count=1, flags=re.IGNORECASE).strip()
    return None


# ===== 快捷命令（覆盖前端功能直选路径）=====
_HELP = """用法：
  直接输入自然语言查询（推荐）：
    帮我查一下科技的热点新闻        今天有关足球的新闻
    财经模块最新新闻                查询 @新京报 最近发布了什么
    持续监控B站账户 https://space.bilibili.com/312249633/video
    查看账户监控状态                立即执行账户监控
  离线数据库查询：
    /offline 今日俄乌战争新闻
    离线查询今日俄乌战争新闻
  命令：
  exit / quit / q            退出对话
  /help                      显示本帮助
  /status                    查看四台 MCP 服务器连通状态
  /hotspot <模块> [topN]     直选：查询模块热点（默认 科技 10）
  /latest  <模块> [count]    直选：查询模块最新（默认 科技 10）
  /account <账户> <平台> [limit]   直选：关注账户新发布（默认 @新京报 weibo 10）
  /monitor add <主页URL> [账户名]  注册持续监控并立即做一次基线检查
  /monitor run                    立即检查全部已注册账户
  /monitor status                 查看监控状态和已发现数量
  /monitor stop <账户名> [平台]   停止监控（保留历史数据）
  提示：持续监控需先启动 python -m agents.account_monitor_agent（A2A :8009）
  /offline <查询内容>        离线库：Redis → Milvus → MySQL（最多3条）
"""


def handle_command(agent, cmd: str) -> bool:
    """处理快捷命令；识别并执行成功返回 True，否则 False（交给自然语言）"""
    parts = cmd.split()
    name = parts[0].lower().lstrip("/")
    args = parts[1:]

    if name in ("help", "h"):
        print(_HELP)
        return True
    if name == "status":
        _print_mcp_status(check_mcp_servers())
        return True
    if name == "hotspot":
        module = args[0] if args else "科技"
        top_n = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        run_task_result(agent, agent.run_hotspot(module, top_n))
        return True
    if name == "latest":
        module = args[0] if args else "科技"
        count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        run_task_result(agent, agent.run_latest(module, count))
        return True
    if name == "account":
        account = args[0] if args else "@新京报"
        platform = args[1] if len(args) > 1 else "weibo"
        limit = int(args[2]) if len(args) > 2 and args[2].isdigit() else 10
        run_task_result(agent, agent.run_account_follow(account, platform, limit=limit))
        return True
    if name == "monitor":
        action = args[0].lower() if args else "status"
        if action == "run":
            run_task_result(agent, agent.run_account_monitor_from_text("立即执行账户监控"))
        elif action == "status":
            run_task_result(agent, agent.run_account_monitor_from_text("查看账户监控状态"))
        elif action == "add":
            if len(args) < 2:
                print("用法：/monitor add <账户主页URL> [账户名]")
                return True
            url = args[1]
            account = args[2] if len(args) > 2 else ""
            account_text = f"@{account.lstrip('@')}" if account else ""
            run_task_result(
                agent,
                agent.run_account_monitor_from_text(
                    f"持续监控B站账户 {account_text} {url}".strip()
                ),
            )
        elif action == "stop":
            if len(args) < 2:
                print("用法：/monitor stop <账户名> [平台]")
                return True
            platform = args[2] if len(args) > 2 else "bilibili"
            run_task_result(
                agent,
                agent.run_account_monitor_from_text(
                    f"停止监控 @{args[1].lstrip('@')} {platform}"
                ),
            )
        else:
            print("用法：/monitor add|run|status|stop（输入 /help 查看完整格式）")
        return True
    if name == "offline":
        query = " ".join(args).strip()
        if not query:
            print("用法：/offline <查询内容>，例如 /offline 最近的足球新闻")
            return True
        try:
            run_offline_query(query, limit=3)
        except Exception as exc:
            print(f"[离线查询失败] {exc}")
        return True
    return False


# ===== 对话循环 =====
def main() -> None:
    """对外服务主入口：横幅 → 探测 MCP → REPL 对话循环"""
    # Windows 控制台编码兜底（否则 GBK 下打印中文可能 UnicodeEncodeError）
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stdin.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print("=" * 62)
    print("  实时热点资讯多智能体系统 · 对外服务（测试用）")
    print("  直接输入自然语言查询，如：")
    print("    帮我查一下科技的热点新闻")
    print("    今天有关足球的新闻")
    print("    财经模块最新新闻")
    print("    持续监控B站账户 https://space.bilibili.com/312249633/video")
    print("    查看账户监控状态 / 立即执行账户监控")
    print("  账户持续监控依赖 AccountMonitorAgent（python -m agents.account_monitor_agent）")
    print("  查询离线数据库（最多 3 条），如：")
    print("    /offline 今日俄乌战争新闻")
    print("    离线查询今日俄乌战争新闻")
    print("  输入 exit 退出；输入 /help 查看全部命令")
    print("=" * 62)

    # 启动时探测 MCP 服务器（不托管进程，未起则走优雅降级）
    status = check_mcp_servers()
    print()
    _print_mcp_status(status)
    up = sum(1 for s in status.values() if s["ok"])
    total = len(status)
    if up < total:
        print(f"（已启动 {up}/{total} 台。未启动的服务器将降级为进程内直调 tools.*，链路仍可跑通。）")

    # 创建协调调度 Agent（真实 LLM 意图识别）
    from agents.coordinator_agent import create_coordinator_agent
    agent = create_coordinator_agent()
    print("\n已就绪。请输入（exit 退出）：\n")

    while True:
        try:
            raw = input("你> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[再见]")
            break
        if not raw:
            continue
        if raw.lower() in ("exit", "quit", "q"):
            print("[再见]")
            break
        # 快捷命令（/命令 或裸 help/status 等）；否则走自然语言 → LLM 意图识别
        first = re.split(r"\s+", raw, maxsplit=1)[0].lower().lstrip("/")
        if first in ("help", "h", "status", "hotspot", "latest", "account", "monitor", "offline"):
            if not handle_command(agent, raw):
                print(f"未知命令: {raw}（/help 查看）")
            continue
        offline_query = _extract_offline_query(raw)
        if offline_query is not None:
            if not offline_query:
                print("请在“离线查询”后输入新闻内容，例如：离线查询今日俄乌战争新闻")
                continue
            try:
                logger.info(f"[main] 收到离线查询: {offline_query}")
                run_offline_query(offline_query, limit=3)
            except Exception as exc:
                logger.error(f"[main] 离线查询失败: {exc}", exc_info=True)
                print(f"[离线查询失败] {exc}")
            continue
        try:
            logger.info(f"[main] 收到对话: {raw}")
            result = agent.route(raw)
            run_task_result(agent, result)
        except Exception as exc:
            logger.error(f"[main] 处理对话失败: {exc}", exc_info=True)
            print(f"[处理失败] {exc}")


if __name__ == "__main__":
    main()
