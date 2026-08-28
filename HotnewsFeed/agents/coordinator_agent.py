#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: coordinator_agent.py
项目: HotnewsFeed

本文件干什么：
    协调调度 Agent —— 对话入口的核心。
    职责：意图识别 → 抽取参数 → 真实 HTTP A2A 分发任务给采集/加工子 Agent 服务器 → 汇总结果；
    任务完成后 A2A 反问用户是否生成简报。
    四类上游任务：
      ① 查询某模块当前热点新闻  → run_hotspot
      ② 查询某模块最新新闻      → run_latest
      ③ 关注某账户新闻/作品发布 → run_account_follow
      ④ 建立/检查账户持续监控   → run_account_monitor（A2A 交给 AccountMonitorAgent）

    真实实现：LLM 意图识别（intent_agent）→ 真实 HTTP A2A 分发任务给
    采集(collector)/加工(processor) 子 Agent 服务器（AgentNetwork + send_task_async），
    子 Agent 内部经 mcp_servers.mcp_access 网关调用 MCP 注册工具（streamable-http 连
    远端服务器，不可达时降级进程内直调 tools.*）。
    task_pipelines 保留给前端「功能直选」直接调用，coordinator 不再走它。

    A2A 协议：直接继承官方包 python_a2a 的 A2AServer，挂一张标准 AgentCard（含 skill）；
    协议入口是 handle_message(message) —— 收到 Message 后按 metadata 里的 task_type 路由。

    用法：
        python -m agents.coordinator_agent    # 跑一段模拟演示
"""

import json
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

from python_a2a import A2AServer, AgentCard, AgentSkill

from a2a.protocol import (ask_user_confirm, delegate, encode_result,
                          parse_task, reply_text)
from agents.intent_agent import intent_agent
from config import Config
from create_logger import logger
from task_pipelines.schemas import (AccountPost, HotEvent, NewsItem,
                                    PipelineResult, dto_from_dict)

# Config() 创建配置对象（模拟版主要用于触发配置读取，LLM 配置见 conf.llm）
conf = Config()

# ===== 槽位抽取（轻量：从用户话术里猜模块/账户/平台，补意图识别的参数缺位）=====
_DEFAULT_MODULES = ["科技", "财经", "体育", "娱乐", "国际"]
_ACCOUNT_RE = re.compile(r"@([\w一-龥\-]+)")
_PLATFORM_ALIASES = {
    "weibo": ["微博", "weibo", "wb"],
    "wechat": ["微信", "公众号", "wechat"],
    "xiaohongshu": ["小红书", "xiaohongshu"],
    "bilibili": ["哔哩哔哩", "bilibili", "b站", "b23.tv"],
}
_URL_RE = re.compile(r"https?://[^\s，。；;]+", re.IGNORECASE)
# 模块主题词 → 模块名 映射：处理「今天有关足球的新闻」这类不含模块名、但含话题词的话术
_MODULE_ALIASES = {
    "科技": ["数码", "手机", "芯片", "人工智能", "互联网", "软件", "硬件", "机器人",
             "自动驾驶", "云计算", "量子", "算法"],
    "财经": ["股票", "股市", "基金", "金融", "经济", "理财", "投资", "银行", "楼市", "人民币", "外贸"],
    "体育": ["足球", "篮球", "英超", "nba", "cba", "中超", "国足", "世界杯", "奥运会",
             "运动", "比赛", "球队", "球员", "冠军", "田径", "网球"],
    "娱乐": ["电影", "影视", "明星", "综艺", "音乐", "娱乐圈", "演员", "电视剧", "票房"],
    "国际": ["世界", "全球", "国外", "海外", "美国", "欧洲", "日本", "俄乌", "地缘"],
}


def _contains_word(text: str, word: str) -> bool:
    """匹配话题词：中文直接子串；英文/数字用单词边界，避免 'nba' 误中 'basketball' 之类"""
    if re.search(r"^[一-龥]", word):
        return word in text
    return re.search(r"(?<![a-z0-9])" + re.escape(word.lower()) + r"(?![a-z0-9])",
                     text.lower()) is not None

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
agent_card = AgentCard(
    name="coordinator",
    description="协调调度 Agent：识别用户意图，经真实 HTTP A2A 分发任务给采集/加工子 Agent 服务器，完成后反问是否生成简报（可交接给 publisher）",
    url="http://localhost:8000",          # A2A 网络端点（真实部署时改实际地址）
    version="1.0.0",
    capabilities={"streaming": True, "memory": True},
    skills=[
        AgentSkill(
            id="hotspot_query",
            name="hotspot_query",
            description="协调采集与加工 Agent，返回指定模块或主题的热点事件列表",
            tags=["news", "hotspot", "orchestration"],
            examples=[
                "查询科技模块近24小时的热点新闻",
                "帮我查找今天的俄乌战争热点",
                "查看足球领域热度最高的5条新闻",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="latest_news",
            name="latest_news",
            description="协调采集 Agent，返回指定模块或主题按时间排序的最新新闻列表",
            tags=["news", "latest", "orchestration"],
            examples=[
                "查询财经模块最新的10条新闻",
                "帮我查找今天的俄乌战争新闻",
                "查看今天最新的足球新闻",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="account_follow",
            name="account_follow",
            description="协调采集 Agent，检查指定平台账户在给定时间后的新发布内容",
            tags=["account", "posts", "orchestration"],
            examples=[
                "检查微博账户 @新京报 的最新发布",
                "查询微信公众号人民日报最近发布的内容",
                "获取指定账户在2026-08-26之后发布的作品",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="account_monitor",
            name="account_monitor",
            description="把账户持续监控任务经 A2A 分发给 AccountMonitorAgent，完成注册、立即检查或状态查询",
            tags=["account", "monitor", "a2a", "orchestration"],
            examples=[
                '{"task_type":"account_monitor","items":[{"account":"bilibili_312249633",'
                '"platform":"bilibili","registered":true}]}',
                '{"task_type":"account_monitor","items":[{"action":"status","monitors":[]}]}',
            ],
            input_modes=["text/plain"], output_modes=["application/json"],
        ),
    ],
)


# coordinator 作为 A2A 客户端，经 AgentNetwork 投递任务到子 agent 服务器（真实 HTTP A2A），
# 不再在进程内持有子 agent 对象（网络懒加载见 a2a.protocol）。
class CoordinatorAgent(A2AServer):
    """协调调度 Agent"""

    name = "coordinator"
    role = "意图识别 · 任务路由 · A2A 简报交接"

    def __init__(self):
        super().__init__(agent_card=agent_card)

    # ===== 意图路由 =====
    def route(self, intent: str) -> PipelineResult:
        """LLM 意图识别（intent_agent）→ 真实 HTTP A2A 分发任务给采集/加工子 Agent 服务器"""
        logger.info(f"[coordinator] 收到意图: {intent}")
        # 真实 LLM 意图识别：识别新闻查询、一次性账户查询、持续账户监控与 out_of_scope。
        # 返回 (intents, user_queries, follow_up_message)，提示词见 prompt/main_prompt.py。
        # 参数抽取（module / account / platform 等槽位）后续再接，当前按【模拟】默认值。
        intents, user_queries, follow_up_message = intent_agent(intent)
        logger.info(f"[coordinator] 识别到意图: {intents}，改写查询: {user_queries}，追问: {follow_up_message!r}")
        # 有追问（意图有歧义 / 超出范围 out_of_scope）：直接把追问返回给用户（走结果 error 字段展示）
        if follow_up_message:
            return PipelineResult(task_type="follow_up", items=[], error=follow_up_message)
        # 按识别到的意图路由（优先级 hotspot_query > latest_news > account_follow）
        kw = self._guess_keyword(intent)
        for it in intents:
            if it == "hotspot_query":
                return self.run_hotspot(module=self._guess_module(intent), top_n=5,
                                        keywords=[kw] if kw else None)
            if it == "latest_news":
                return self.run_latest(module=self._guess_module(intent), count=5,
                                       keywords=[kw] if kw else None)
            if it == "account_follow":
                account, platform = self._guess_account(intent)
                return self.run_account_follow(account=account, platform=platform, limit=5)
            if it == "account_monitor":
                return self.run_account_monitor_from_text(intent)
        logger.warning(f"[coordinator] 意图未识别，按默认热点查询处理: {intent}")
        return self.run_hotspot(module=self._guess_module(intent))

    # ===== 槽位抽取（轻量：LLM 只识别意图，模块/账户/关键词等参数从话术里直接扫）=====
    def _guess_module(self, intent: str) -> str:
        """从用户话术里猜模块名（默认「科技」）：
        config 自定义模块名 > 内置模块名 > 主题词映射（足球→体育）"""
        for m in conf.rss_sources:
            if m in intent:
                return m
        for m in _DEFAULT_MODULES:
            if m in intent:
                return m
        for module, aliases in _MODULE_ALIASES.items():
            if any(_contains_word(intent, a) for a in aliases):
                return module
        return "科技"

    def _guess_keyword(self, intent: str) -> Optional[str]:
        """从话术里提取最具体的话题词作为搜索关键词（如「足球」「芯片」），无则 None。

        跳过模块名本身与过短的词（太宽泛）；命中后由 collect_news 按关键词过滤标题/摘要。
        """
        for aliases in _MODULE_ALIASES.values():
            for a in aliases:
                if a in _DEFAULT_MODULES or len(a) < 2:
                    continue
                if _contains_word(intent, a):
                    return a
        return None

    def _guess_account(self, intent: str) -> tuple:
        """从用户话术里猜 账户(@名) 和 平台（默认 @新京报 / weibo）"""
        m = _ACCOUNT_RE.search(intent)
        account = "@" + m.group(1) if m else "@新京报"
        low = intent.lower()
        platform = "weibo"
        for p, aliases in _PLATFORM_ALIASES.items():
            if any(a in low for a in aliases):
                platform = p
                break
        return account, platform

    def _monitor_params(self, text: str) -> tuple:
        """抽取监控动作、账户、平台和主页地址；只做槽位抽取，不执行业务。"""
        low = text.lower()
        if any(word in text for word in ("状态", "列表", "监控了哪些", "监控情况")):
            action = "status"
        elif any(word in text for word in ("立即检查", "检查全部", "执行监控", "运行监控")):
            action = "check"
        elif any(word in text for word in ("停止监控", "取消监控", "不再监控")):
            action = "stop"
        else:
            action = "register"
        match = _URL_RE.search(text)
        url = match.group(0).rstrip(")]}）】") if match else ""
        account, platform = self._guess_account(text)
        if platform == "weibo" and ("b站" in low or "bilibili" in low or "b23.tv" in low):
            platform = "bilibili"
        uid = re.search(r"space\.bilibili\.com/(\d+)", url)
        if platform == "bilibili" and account == "@新京报" and uid:
            account = f"bilibili_{uid.group(1)}"
        return action, account, platform, url

    # ===== A2A 委派辅助（真实 HTTP）=====
    def _delegate(self, agent_name: str, task_type: str, params: dict, dto_cls=None):
        """真实 HTTP A2A 委派子 Agent 服务器并还原回传 DTO；ok=False 时抛异常

        agent_name 是子 agent 名字（collector / processor / publisher），经
        a2a.protocol.delegate 的 AgentNetwork HTTP 路径投递；回传文本经 encode_result
        序列化成 dict，这里再用 dto_from_dict 还原成 DTO 类型。
        """
        text = delegate(agent_name, task_type, params, from_agent=self.name)
        data = json.loads(text)
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or f"{task_type} 委派失败")
        result = data.get("result")
        if dto_cls and isinstance(result, list):
            return [dto_from_dict(dto_cls, r) for r in result if isinstance(r, dict)]
        return result

    # ===== 上游查询任务（A2A 分发子 Agent）=====
    def run_hotspot(self, module: str, top_n: int = 10,
                    time_window_hours: int = 24,
                    keywords: Optional[List[str]] = None) -> PipelineResult:
        """① 模块热点查询 → 真实 HTTP A2A 委派链：采集(collector) → 聚类 → 热度 → 核验(processor)"""
        from tools.process import EventCluster
        logger.info(f"[coordinator] 查询「{module}」热点 top{top_n}（近{time_window_hours}h）"
                    f"{' 关键词=' + str(keywords) if keywords else ''}")
        started = time.time()
        news = self._delegate("collector", "collect_news",
                              {"module": module, "keywords": keywords,
                               "limit": max(top_n * 5, 30)}, dto_cls=NewsItem)
        clusters = self._delegate("processor", "cluster_events",
                                  {"news_items": news, "threshold": 0.8},
                                  dto_cls=EventCluster)
        events = self._delegate("processor", "score_heat",
                                {"clusters": clusters, "time_window_hours": time_window_hours},
                                dto_cls=HotEvent)
        verified = self._delegate("processor", "verify_events",
                                  {"events": events}, dto_cls=HotEvent)
        result = PipelineResult(
            task_type="hotspot_query",
            items=verified[:top_n],
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            elapsed_ms=int((time.time() - started) * 1000),
        )
        logger.info(f"[coordinator] 热点查询完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        return result

    def run_latest(self, module: str, count: int = 20,
                   keywords: Optional[List[str]] = None) -> PipelineResult:
        """② 模块最新新闻 → A2A 委派采集子 Agent（去重/时间倒序已内置于 collect 工具）"""
        logger.info(f"[coordinator] 查询「{module}」最新新闻 top{count}"
                    f"{' 关键词=' + str(keywords) if keywords else ''}")
        started = time.time()
        news = self._delegate("collector", "collect_news",
                              {"module": module, "keywords": keywords, "limit": count},
                              dto_cls=NewsItem)
        result = PipelineResult(
            task_type="latest_news",
            items=news[:count],
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            elapsed_ms=int((time.time() - started) * 1000),
        )
        logger.info(f"[coordinator] 最新新闻查询完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        return result

    def run_account_follow(self, account: str, platform: str,
                           since: Optional[str] = None, limit: int = 20) -> PipelineResult:
        """③ 账户发布关注 → A2A 委派采集子 Agent（since 过滤/时间倒序已内置于工具）"""
        logger.info(f"[coordinator] 关注账户 {account}@{platform} 新发布（limit={limit}）")
        started = time.time()
        posts = self._delegate("collector", "fetch_account_posts",
                               {"account": account, "platform": platform,
                                "since": since, "limit": limit},
                               dto_cls=AccountPost)
        result = PipelineResult(
            task_type="account_follow",
            items=posts[:limit],
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            elapsed_ms=int((time.time() - started) * 1000),
        )
        logger.info(f"[coordinator] 账户发布拉取完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        return result

    def run_account_monitor_from_text(self, text: str) -> PipelineResult:
        """账户监控大任务只做参数抽取和 A2A 分发，不在 Coordinator 内实现监控业务。"""
        action, account, platform, url = self._monitor_params(text)
        task_map = {
            "register": "register_monitor",
            "check": "check_monitors",
            "status": "monitor_status",
            "stop": "stop_monitor",
        }
        if action == "register" and not url:
            return PipelineResult(
                task_type="follow_up", items=[],
                error="请提供要监控的账户主页地址，例如：https://space.bilibili.com/312249633/video",
            )
        params = {"account": account, "platform": platform, "url": url, "check_now": True}
        logger.info(f"[coordinator] A2A 分发账户监控任务 action={action} account={account}@{platform}")
        result = self._delegate("account_monitor", task_map[action], params)
        return PipelineResult(
            task_type="account_monitor", items=result if isinstance(result, list) else [result],
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )

    # ===== A2A 简报交接 =====
    def handoff_briefing(self, result: PipelineResult) -> bool:
        """A2A 反问：“是否生成简报？” 确认后经真实 HTTP A2A 委派 PublisherAgent，返回是否生成"""
        logger.info("[coordinator] 任务完成，准备 A2A 简报交接")
        if not ask_user_confirm("任务已完成，是否生成简报并推送？"):
            logger.info("[coordinator] 用户选择不生成简报，流程结束")
            return False
        # 委派简报任务给输出 Agent（publisher）→ 真实 HTTP A2A（:8003）
        try:
            text = delegate("publisher", task_type="briefing",
                            params={"result": result}, from_agent=self.name)
            logger.info(f"[coordinator] 简报交接回传: {text}")
            data = json.loads(text)
            return bool(data.get("ok"))
        except Exception:
            return False

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """收到 A2A 消息：解析 task_type / params → 路由执行 → 回传"""
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[coordinator] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "hotspot_query":
                result = self.run_hotspot(params.get("module", "科技"), params.get("top_n", 10))
                ok, error = True, None
            elif task_type == "latest_news":
                result = self.run_latest(params.get("module", "科技"), params.get("count", 20))
                ok, error = True, None
            elif task_type == "account_follow":
                result = self.run_account_follow(
                    params.get("account", "未知账户"), params.get("platform", "weibo"),
                    params.get("since"), params.get("limit", 20))
                ok, error = True, None
            elif task_type == "account_monitor":
                result = self.run_account_monitor_from_text(params.get("text", ""))
                ok, error = True, None
            else:
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            logger.error(f"[coordinator] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）
        text = encode_result(ok, result, error)
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建协调调度 Agent =====
def create_coordinator_agent():
    """创建协调调度 Agent（仿照 create_order_mcp_server 的工厂模式）"""
    logger.info("=== 协调调度Agent信息 ===")
    logger.info(f"名称: {CoordinatorAgent.name}")
    logger.info(f"职责: {CoordinatorAgent.role}")
    return CoordinatorAgent()


if __name__ == "__main__":
    # 演示：真实 LLM 意图识别（intent_agent）→ 路由 → 结果展示 → A2A 简报交接（交互式输入 y 确认生成简报）
    agent = create_coordinator_agent()
    result = agent.route("帮我查一下科技模块的热点新闻")
    if result.task_type == "follow_up":
        print(f"\n需要向用户追问/直接回复: {result.error}")
    else:
        print(f"\n路由结果: task_type={result.task_type} items={len(result.items)}")
        for item in result.items:
            print(f"  - {item.title}（热度 {item.heat_score}）")
        agent.handoff_briefing(result)

    # 歧义追问演示：没说明模块 → intent_agent 返回 follow_up_message
    print("\n=== 歧义追问演示 ===")
    result2 = agent.route("有什么热点新闻")
    print(f"task_type={result2.task_type}，追问: {result2.error}")

    # A2A 入口演示：构造 Message（task_type/params 放 metadata）→ handle_message → 回传
    print("\n=== A2A 入口演示 ===")
    text = delegate(agent, task_type="hotspot_query",
                    params={"module": "财经", "top_n": 3}, from_agent="main")
    print("回传:", text)
