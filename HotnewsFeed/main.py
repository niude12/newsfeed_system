#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""对外服务主入口（main.py）—— 测试用对话循环

在线自然语言：用户输入 → Coordinator Agent Loop → A2A 专业 Agent → MCP 工具。
明确命令：斜杠命令 → operations.py 确定性操作层，不进入 Agent 推理。
离线：`query_offline_news()` / `/offline` / “离线查询……”
  → Redis 精确缓存 → Milvus 向量检索 ID → MySQL 取原文（最多 3 条）。

模块职责:
    本模块是系统的命令行 / 交互式对话入口。它既提供一组可被其他模块（如
    app.py 的 Web 界面）复用的纯函数：结果格式化（format_result）、MCP 服务器
    探测（check_mcp_servers）、离线新闻查询（query_offline_news）；也承载一个
    REPL 对话循环（main()），把用户的自然语言 / 快捷命令转成对协调调度 Agent
    的业务调用（热点查询、最新新闻、账户发布、账户持续监控）。

模块依赖:
- ``CoordinatorAgent`` : 仅处理自然语言自主循环与 A2A dispatch。
- ``operations``       : 快捷命令使用的确定性热点、最新、账户和监控操作。
- ``mcp_access``        : mcp_servers/mcp_access.py。MCP_URLS 是四台 MCP 服务器
  的 streamable-http 端点；mcp_list_tools / sync_call 用于探测服务器连通性。
- ``OfflineNewsService``: offline_news/service.py。query() 编排
  Redis 精确缓存 → Milvus 向量召回 ID → MySQL 取原文（最多 3 条）。
- ``offline_main.format_rows`` : 把离线查询结果 dict 格式化成控制台文本。
- ``create_logger.logger``     : 全局日志器（控制台 + 文件双通道），见 create_logger.py。

典型调用链::

    main() 对话循环
      ├─ 快捷命令 handle_command()
      │     └─ operations.run_*() → format_result()
      ├─ 离线话术 _extract_offline_query()
      │     └─ run_offline_query() → query_offline_news()
      │          └─ OfflineNewsService().query()   # Redis → Milvus → MySQL
      └─ 自然语言 agent.route(raw) → Agent Loop → A2A → MCP

对外暴露的接口:
- format_result        : 把 PipelineResult 格式化成控制台文本（app.py 复用）。
- query_offline_news   : 查询离线新闻库，返回 dict，供控制台 / HTTP API 复用。
- run_offline_query    : 执行并打印离线查询结果。
- check_mcp_servers    : 探测四台 MCP 服务器连通性（不托管进程）。
- handle_command       : 处理快捷命令，返回是否已识别（True 已处理 / False 交给自然语言）。
- main                 : 对话循环主入口（python main.py）。

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
# task_type（PipelineResult.task_type）→ 中文标题 的映射，供 format_result 展示用。
_TASK_TITLES = {
    "hotspot_query": "热点新闻",     # 热点查询结果（HotEvent 列表）
    "latest_news": "最新新闻",       # 最新新闻结果（NewsItem 列表）
    "account_follow": "账户发布",    # 一次性账户发布（AccountPost 列表）
    "account_monitor": "账户监控",   # 账户持续监控状态（dict 列表）
}


def format_result(result: Optional[object]) -> str:
    """把 PipelineResult 格式化成控制台文本（按 task_type 分支）。

    条目格式参考 tools/publish.py::_format_markdown：
    - hotspot_query   → HotEvent：标题（热度 X · 可信度 Y · N 来源）＋ summary
    - latest_news     → NewsItem：标题（来源 · 时间）＋ url
    - account_follow  → AccountPost：标题/内容（账户@平台 · 时间）＋ url

    参数:
        result: PipelineResult 实例（dataclass，见 task_pipelines/schemas.py）；
                也兼容任意带 task_type / items / error 属性的对象。

    返回:
        str：可直接打印到控制台的多行文本；result 为空时返回占位文本。

    说明:
        getattr(result, 'task_type', '') 用 getattr 取属性并给默认值，避免传入
        的对象不是 PipelineResult 时抛 AttributeError。
        _TASK_TITLES 把 task_type 映射成中文标题，映射不到时回退用原值。
        account_monitor 的结果条目本身是 dict（AccountMonitorAgent 经 A2A 回传
        的状态对象），这里用 json.dumps 整条序列化展示，便于查看监控完整字段。
    """
    if result is None:
        # 空结果：直接返回占位文本，不继续往下取值。
        return "[空结果]"
    # follow_up：不是错误，是 LLM 意图识别的追问/直接回复话术（放在 error 字段）
    if getattr(result, "task_type", "") == "follow_up":
        # 把追问/回复内容单独展示，提示用户需要补充信息或这是直接回复。
        return f"[需要追问/直接回复]\n{getattr(result, 'error', None) or '(无追问内容)'}"
    # 带 error 的结果：把错误信息拼进文本，提示用户本次查询失败。
    if getattr(result, "error", None):
        return f"[错误] {result.error}"
    # 用 getattr 安全取 task_type，非 PipelineResult 对象也能容错（缺省空串）。
    task_type = getattr(result, "task_type", "")
    # items 为空时退化成空列表，避免下面 len()/遍历 处理 None。
    items = getattr(result, "items", None) or []
    # 中文标题：task_type 能映射到 _TASK_TITLES 就用映射，否则回退原始 task_type。
    title = _TASK_TITLES.get(task_type, task_type)
    # 组装结果块的第一行标题行（含条数统计）。
    lines = [f"== {title}（{len(items)} 条） =="]
    if task_type == "account_monitor":
        # 账户监控：每条 item 本身就是 dict，整条 JSON 展开（缩进 2 空格，中文不转义）。
        lines.extend(
            f"{i}. {json.dumps(item, ensure_ascii=False, default=str, indent=2)}"
            for i, item in enumerate(items, 1)
        )
        return "\n".join(lines)
    # 其余任务类型：逐条交给 _format_item 按 task_type 取不同字段。
    for i, item in enumerate(items, 1):
        lines.append(_format_item(i, task_type, item))
    # 用换行符拼接所有行，作为多行文本整体返回。
    return "\n".join(lines)


def _format_item(idx: int, task_type: str, item) -> str:
    """格式化单条 item（按 task_type 取不同字段）。

    参数:
        idx:      条目标题前的序号（从 1 开始）。
        task_type: 当前任务类型（hotspot_query / latest_news / account_follow）。
        item:     对应 DTO 实例（HotEvent / NewsItem / AccountPost，见
                  task_pipelines/schemas.py），字段用 getattr 取并给默认值。

    返回:
        str：形如 "序号. 标题（附属信息）＋ 可选的摘要/链接" 的单条文本。

    说明:
        - hotspot_query 的 item 是 HotEvent：展示热度 heat_score、可信度
          credibility、来源数 sources/article_count 和摘要 summary。
        - latest_news 的 item 是 NewsItem：展示来源 source、发布时间
          published_at 和原文链接 url。
        - account_follow 的 item 是 AccountPost：标题取不到时回退用 content，
          展示账户 account、平台 platform、时间 published_at 和链接 url。
    """
    if task_type == "hotspot_query":
        # 热点事件：取热度评分 heat_score，缺省 0。
        heat = getattr(item, "heat_score", 0)
        # 可信度结论（可信/存疑/证据不足），缺省空串。
        cred = getattr(item, "credibility", "")
        # 相关来源列表，取不到时退化成空列表。
        sources = getattr(item, "sources", None) or []
        # 关联资讯数优先用 article_count，没有则退化为来源列表长度。
        n = getattr(item, "article_count", None) or len(sources)
        # 拼第一条信息行：序号 + 标题 +（热度/可信度/来源数）。
        head = f"{idx}. {getattr(item, 'title', '')}（热度 {heat} · 可信度 {cred} · {n} 来源）"
        # 事件摘要（可选），没有则不追加到行尾。
        summary = getattr(item, "summary", "") or ""
        return head + (f"\n   {summary}" if summary else "")
    if task_type == "latest_news":
        # 最新新闻：来源名，缺省空串。
        source = getattr(item, "source", "")
        # 发布时间（ISO 8601），缺省空串。
        when = getattr(item, "published_at", "")
        # 原文链接，缺省空串。
        url = getattr(item, "url", "")
        head = f"{idx}. {getattr(item, 'title', '')}（{source} · {when}）"
        return head + (f"\n   {url}" if url else "")
    # account_follow（AccountPost：标题/内容二选一，配合账户@平台·时间）
    # 展示文本：优先标题，标题为空则退回内容摘要。
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

    只负责“探测”不负责“启动”：连得上就列出工具清单，连不上就标记 ok=False
    并记录 error，由调用方（main 启动横幅 / /status 命令）决定如何展示。

    返回:
        Dict[str, dict]：键是 MCP 服务器名（collect / process / publish / video），
        值是 {"ok": bool, "tools": List[str], "url": str}；
        连不上时 ok=False 并额外带上 error 原因。

    说明:
        - mcp_servers.mcp_access.MCP_URLS：四台 MCP 服务器的 streamable-http 端点
          （如 http://127.0.0.1:8004/mcp）。
        - mcp_list_tools(name, timeout)：经官方 mcp 客户端连远端服务器、initialize
          握手后列出注册工具名（异步函数）。
        - sync_call(coro)：把异步协程桥进同步上下文执行（见 mcp_access.sync_call），
          这里以 5 秒超时探测，避免某台服务器无响应拖住整个循环。
    """
    # 延迟 import：真要探测 MCP 才引入访问层，避免启动时加载 mcp 客户端依赖。
    from mcp_servers.mcp_access import MCP_URLS, mcp_list_tools, sync_call
    # 结果字典：服务器名 → {"ok", "tools", "url"[, "error"]}。
    status: Dict[str, dict] = {}
    # 遍历四台 MCP 服务器（collect/process/publish/video）逐个探测。
    for name, url in MCP_URLS.items():
        try:
            # sync_call 跑 mcp_list_tools：成功说明服务器在线，拿到工具名列表。
            tools = sync_call(mcp_list_tools(name, timeout=5.0))
            # 记录成功状态与可用工具清单。
            status[name] = {"ok": True, "tools": tools, "url": url}
        except Exception as exc:
            # 任何异常（连不上 / 握手失败 / 超时）都按“不可达”处理，记 warning 日志。
            logger.warning(f"[main] MCP 服务器 {name}({url}) 不可达: {exc}")
            # 记录失败状态与错误原因（tools 置空，供横幅/状态面板展示）。
            status[name] = {"ok": False, "tools": [], "url": url, "error": str(exc)}
    return status


def _print_mcp_status(status: Dict[str, dict]) -> None:
    """把 check_mcp_servers 的探测结果打印成控制台状态表格。

    参数:
        status: check_mcp_servers() 的返回字典（含 ok / url / tools / error 键）。

    说明:
        - name:9s 是 Python 格式化的“左对齐补宽 9 格”，让四台服务器的名字对齐。
        - 工具名用逗号拼接；服务器在线但没有注册任何工具时显示 "(空)"。
        - 不可达时提示“将走进程内降级直调 tools.*”，说明链路仍可跑通。
    """
    # 打印状态表的标题行。
    print("MCP 服务器状态：")
    # 遍历每台服务器的探测结果。
    for name, info in status.items():
        if info["ok"]:
            # 在线：打印 [OK] + 工具名列表（无工具时显示 "(空)"）。
            print(f"  [OK]  {name:9s} {info['url']}  工具: {', '.join(info['tools']) or '(空)'}")
        else:
            # 不可达：打印 [X] 并提示将降级为进程内直调。
            print(f"  [X]   {name:9s} {info['url']}  不可达（将走进程内降级直调 tools.*）")


# ===== 任务执行 =====
def run_task_result(result) -> None:
    """展示流水线结果与自主 Agent Trace。

    参数:
        result: PipelineResult 实例（热点 / 最新 / 账户发布 / 账户监控等任务结果）。
    """
    # 先把流水线结果格式化打印到控制台。
    print(format_result(result))
    trace = getattr(result, "trace", None) or []
    if trace:
        print("\n== Agent Trace ==")
        for step in trace:
            print(f"{step.get('step_id')}. {step.get('agent')}.{step.get('skill')} "
                  f"→ {step.get('status')} ({step.get('duration_ms', 0)}ms)"
                  + (f" · {step.get('error')}" if step.get('error') else ""))


# ===== 离线新闻对外接口 =====
def query_offline_news(query: str, limit: int = 3) -> dict:
    """查询离线新闻库，供控制台、未来 HTTP API 或其他 Python 代码复用。

    参数:
        query:  查询内容（自然语言，如“今日俄乌战争新闻”）；会先去掉首尾空白，
                为空时抛 ValueError。
        limit:  最多返回的条数；内部会夹在 [1, 3] 区间（离线库设计最多 3 条）。

    返回:
        dict：形如 {"source": "redis" 或 "milvus+mysql", "items": [...]}，
        items 里每条含 module / title / source / published_at / url / summary 等键。

    抛出:
        ValueError: query 去空白后为空字符串时抛出。

    说明:
        offline_news.OfflineNewsService 是离线新闻服务类（offline_news/service.py），
        其 query() 编排三层检索：Redis 精确缓存 → Milvus 向量召回 ID → MySQL 取原文；
        min(3, max(1, limit)) 保证 limit 永远落在 [1,3]（离线库最多返回 3 条）。
    """
    # 去掉首尾空白，避免“只输入空格”这类脏输入。
    query = query.strip()
    if not query:
        # 空查询没有意义，直接抛异常提示调用方。
        raise ValueError("离线查询内容不能为空")
    # 延迟 import：只有真用到离线查询才引入服务类，避免影响在线路径的启动速度。
    from offline_news import OfflineNewsService
    # 实例化离线新闻服务并查询；limit 用 min(3, max(1, limit)) 夹到 [1,3]。
    return OfflineNewsService().query(query, limit=min(3, max(1, limit)))


def run_offline_query(query: str, limit: int = 3) -> None:
    """执行并打印离线查询结果。

    参数:
        query:  查询内容（透传给 query_offline_news）。
        limit:  最多返回条数（默认 3，内部被 query_offline_news 夹到 [1,3]）。

    说明:
        offline_main.format_rows 负责把离线查询返回的 dict 排版成多行文本
        （标题、来源、时间、链接、摘要），这里直接打印到控制台。
    """
    # 延迟 import 排版函数，避免顶层加载 offline_main 及其依赖。
    from offline_main import format_rows
    # 查询后立即用 format_rows 排版成多行文本并打印到控制台。
    print(format_rows(query_offline_news(query, limit=limit)))


def _extract_offline_query(text: str) -> Optional[str]:
    """识别明确的离线查询话术；普通新闻问题仍走在线 Agent。

    只匹配“话术开头就是离线查询指令”的情况，例如：
        “从离线新闻库查询今日俄乌战争新闻” / “帮我从离线库查找足球新闻” /
        “离线查询最近的科技新闻”
    命中后把“指令前缀”剥掉，只返回真正的查询内容；普通新闻问题（如
    “帮我查一下科技热点”）不匹配，返回 None 继续走在线 Agent。

    参数:
        text: 用户输入的原始话术。

    返回:
        Optional[str]：识别为离线查询时返回剥离前缀后的查询内容（去空白）；
        未识别时返回 None。

    说明:
        - 两条正则都加了 ^ 锚定开头，并允许“请/帮我”等客气词可选；
          IGNORECASE 忽略大小写（兼容英文前缀场景）。
        - re.match 从头匹配；re.sub 用 count=1 只替换掉前缀一处。
    """
    # 两条“离线查询”话术前缀正则：^ 锚定开头，允许“请/帮我”等客气词可选。
    patterns = (
        # 形态一：“从离线(新闻)库中查询/查找/搜索 <内容>”。
        r"^(?:请)?(?:帮我)?从离线(?:新闻)?库(?:中)?(?:查询|查找|搜索)?[\s：:]*",
        # 形态二：“离线查询 <内容>”。
        r"^(?:请)?(?:帮我)?离线查询[\s：:]*",
    )
    for pattern in patterns:
        # re.match 从头匹配；IGNORECASE 忽略大小写（兼容英文前缀场景）。
        if re.match(pattern, text, flags=re.IGNORECASE):
            # 命中即剥离前缀，剩余部分去空白后作为查询内容返回。
            return re.sub(pattern, "", text, count=1, flags=re.IGNORECASE).strip()
    # 没有任何离线话术前缀命中 → 返回 None，由调用方改走在线 Agent。
    return None


# ===== 快捷命令（覆盖前端功能直选路径）=====
# /help 显示的帮助文本：列出自然语言示例、退出命令和所有快捷命令的格式。
# 只做展示用，实际解析在 handle_command() 里。
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
  /monitor add <主页URL> [账户名]  注册持续监控（检查由调度器或 /monitor run 完成）
  /monitor run                    立即检查全部已注册账户
  /monitor status                 查看监控状态和已发现数量
  /monitor stop <账户名> [平台]   停止监控（保留历史数据）
  提示：持续监控需先启动 python -m agents.account_monitor_agent（A2A :8009）
  /offline <查询内容>        离线库：Redis → Milvus → MySQL（最多3条）
"""


def handle_command(cmd: str) -> bool:
    """处理快捷命令；识别并执行成功返回 True，否则 False（交给自然语言）。

    支持的快捷命令（首词，前面可带 /）：
      help/h、status、hotspot、latest、account、monitor（add|run|status|stop）、offline。

    参数:
        cmd:   用户输入的命令行（如 "/hotspot 科技 10"）。

    返回:
        bool：命令被识别并执行返回 True；不认识返回 False，调用方改走自然语言。

    说明:
        - 参数解析策略：args[0] 有则用、没有则给内置默认值（模块默认“科技”、
          账户默认“@新京报”）；数字参数用 args[1].isdigit() 先校验再 int()。
        - 账户监控使用结构化参数经 operations.run_monitor 分发给 AccountMonitorAgent。
    """
    # 按空白切分命令行：首词是命令名，其余是参数。
    parts = cmd.split()
    name = parts[0].lower().lstrip("/")   # 首词去掉 "/" 并转小写，作为命令名。
    args = parts[1:]                      # 其余参数列表。
    from operations import run_account_follow, run_hotspot, run_latest, run_monitor

    if name in ("help", "h"):
        # help/h：直接打印帮助文本。
        print(_HELP)
        return True
    if name == "status":
        # status：探测四台 MCP 服务器并打印连通状态。
        _print_mcp_status(check_mcp_servers())
        return True
    if name == "hotspot":
        # hotspot：直选热点查询。模块缺省“科技”，topN 缺省 10。
        module = args[0] if args else "科技"
        # 第二个参数是纯数字才转 int，否则用默认 10（防脏输入）。
        top_n = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        # 调用 Coordinator 热点查询并展示结果。
        run_task_result(run_hotspot(module, top_n))
        return True
    if name == "latest":
        # latest：直选最新新闻。模块缺省“科技”，数量缺省 10。
        module = args[0] if args else "科技"
        count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        run_task_result(run_latest(module, count))
        return True
    if name == "account":
        # account：直选一次性账户发布。账户缺省 @新京报，平台缺省 weibo。
        account = args[0] if args else "@新京报"
        platform = args[1] if len(args) > 1 else "weibo"
        limit = int(args[2]) if len(args) > 2 and args[2].isdigit() else 10
        run_task_result(run_account_follow(account, platform, limit=limit))
        return True
    if name == "monitor":
        # monitor：账户持续监控。子动作缺省 status（查看状态）。
        action = args[0].lower() if args else "status"
        if action == "run":
            # 立即执行全部已注册账户的监控检查。
            run_task_result(run_monitor("run"))
        elif action == "status":
            # 查看已注册监控的状态和已发现数量。
            run_task_result(run_monitor("status"))
        elif action == "add":
            # 注册监控必须带账户主页 URL；账户名可选，缺省由 B 站 UID 生成。
            if len(args) < 2:
                # 参数不够时打印用法并结束。
                print("用法：/monitor add <账户主页URL> [账户名]")
                return True
            url = args[1]
            account = args[2] if len(args) > 2 else ""
            # 有账户名时补上 @ 前缀（兼容用户已带 @ 的情况）。
            run_task_result(run_monitor("add", account=account, platform="bilibili", url=url))
        elif action == "stop":
            # 停止监控必须提供账户名；平台可选，默认 bilibili。
            if len(args) < 2:
                print("用法：/monitor stop <账户名> [平台]")
                return True
            platform = args[2] if len(args) > 2 else "bilibili"
            run_task_result(run_monitor("stop", account=args[1], platform=platform))
        else:
            # 未知的 monitor 子动作：打印完整用法。
            print("用法：/monitor add|run|status|stop（输入 /help 查看完整格式）")
        return True
    if name == "offline":
        # offline：/offline 后面的所有词拼成查询内容（最多 3 条）。
        query = " ".join(args).strip()
        if not query:
            print("用法：/offline <查询内容>，例如 /offline 最近的足球新闻")
            return True
        try:
            # 执行离线查询并打印结果。
            run_offline_query(query, limit=3)
        except Exception as exc:
            # 离线查询失败时打印错误，不让 REPL 崩溃。
            print(f"[离线查询失败] {exc}")
        return True
    # 不是已知命令 → 返回 False，调用方改走自然语言意图识别。
    return False


# ===== 对话循环 =====
def main() -> None:
    """对外服务主入口：横幅 → 探测 MCP → REPL 对话循环。

    步骤:
        1. 重配标准输入输出编码为 utf-8（Windows 控制台默认 GBK，避免中文
           UnicodeEncodeError）。
        2. 打印系统横幅，提示支持的输入方式。
        3. 探测四台 MCP 服务器连通性，未启动的提示“降级进程内直调”。
        4. 创建 CoordinatorAgent（自主规划循环）。
        5. 进入 while 循环的 REPL：读一行输入，依次判断
           退出指令 → 快捷命令（handle_command）→ 离线话术（_extract_offline_query）
           → 自然语言（agent.route）。

    说明:
        - agent.route(raw) 只进入自主 Agent Loop；明确命令进入 operations.py。
        - 所有业务调用都用 try/except 包裹，异常记 logger.error 并打印给用户，
          避免 REPL 循环因单次异常崩溃退出。
    """
    # Windows 控制台编码兜底（否则 GBK 下打印中文可能 UnicodeEncodeError）
    try:
        # 把标准输出/输入都重配为 utf-8，保证中文打印不乱码。
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stdin.reconfigure(encoding="utf-8")
    except Exception:
        # 某些环境不支持 reconfigure（如被重定向的流），失败则忽略。
        pass

    # ===== 打印系统横幅 =====
    # 用 62 个 "=" 组成分隔线，标出系统名称、支持的输入示例与退出方式。
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
    # 统计在线服务器数量：ok=True 的个数。
    up = sum(1 for s in status.values() if s["ok"])
    total = len(status)
    if up < total:
        # 有服务器未启动时提示会降级为进程内直调 tools.*。
        print(f"（已启动 {up}/{total} 台。未启动的服务器将降级为进程内直调 tools.*，链路仍可跑通。）")

    # 创建协调调度 Agent（真实 LLM 意图识别）
    from agents.coordinator_agent import create_coordinator_agent
    agent = create_coordinator_agent()
    print("\n已就绪。请输入（exit 退出）：\n")

    # ===== REPL 对话循环 =====
    while True:
        try:
            # 读取一行用户输入并去掉首尾空白。
            raw = input("你> ").strip()
        except (EOFError, KeyboardInterrupt):
            # 用户按 Ctrl+D / Ctrl+C 直接退出，不打印 traceback。
            print("\n[再见]")
            break
        if not raw:
            # 空输入直接跳过，重新等待下一行。
            continue
        if raw.lower() in ("exit", "quit", "q"):
            # 退出指令（不区分大小写）。
            print("[再见]")
            break
        # 快捷命令（/命令 或裸 help/status 等）；否则走自然语言 → LLM 意图识别
        # 取首词并去 "/"、转小写，判断是否命中快捷命令集合。
        first = re.split(r"\s+", raw, maxsplit=1)[0].lower().lstrip("/")
        if first in ("help", "h", "status", "hotspot", "latest", "account", "monitor", "offline"):
            # 首词命中快捷命令集合就交给 handle_command；识别失败给“未知命令”提示。
            if not handle_command(raw):
                print(f"未知命令: {raw}（/help 查看）")
            continue
        # 离线查询话术（“从离线库查询…/离线查询…”）：命中则直接走离线链路。
        offline_query = _extract_offline_query(raw)
        if offline_query is not None:
            if not offline_query:
                # 命中前缀但后面没有内容，提示用户补上查询内容。
                print("请在“离线查询”后输入新闻内容，例如：离线查询今日俄乌战争新闻")
                continue
            try:
                logger.info(f"[main] 收到离线查询: {offline_query}")
                run_offline_query(offline_query, limit=3)
            except Exception as exc:
                logger.error(f"[main] 离线查询失败: {exc}", exc_info=True)
                print(f"[离线查询失败] {exc}")
            continue
        # 其余全部按自然语言交给协调调度 Agent：LLM 意图识别 → 路由 → 结果展示。
        try:
            logger.info(f"[main] 收到对话: {raw}")
            result = agent.route(raw)
            run_task_result(result)
        except Exception as exc:
            logger.error(f"[main] 处理对话失败: {exc}", exc_info=True)
            print(f"[处理失败] {exc}")


if __name__ == "__main__":
    # 作为脚本直接运行时，进入对话循环主入口。
    main()
