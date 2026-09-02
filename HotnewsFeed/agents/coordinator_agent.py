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

模块依赖:
- ``python_a2a``             : 官方 A2A 协议包。A2AServer 是 A2A 服务器基类（协议入口
                               handle_message），AgentCard / AgentSkill 声明能力供发现与路由。
- ``a2a.protocol``           : 项目 A2A 适配层。delegate 做真实 HTTP A2A 委派（AgentNetwork +
                               send_task_async），parse_task / encode_result / reply_text 处理协议消息，
                               ask_user_confirm 反问用户。
- ``agents.intent_agent``    : LLM 意图识别。返回 (intents, user_queries, follow_up_message)，
                               是本模块路由的依据。
- ``Config``                 : 全局配置单例（config.ini）。本模块用到 conf.llm 与 conf.rss_sources
                               （自定义模块名列表）。
- ``task_pipelines.schemas`` : 统一 DTO 数据模型。NewsItem / HotEvent / AccountPost / PipelineResult，
                               dto_from_dict 把 A2A 回传的 JSON 还原成 DTO 对象。

典型调用链::

    用户话术
      → CoordinatorAgent.route(intent)
      → intent_agent(intent)                    # LLM 意图识别（intents / user_queries / follow_up）
      → 槽位抽取 _guess_module / _guess_keyword / _guess_account
      → run_hotspot / run_latest / run_account_follow / run_account_monitor_from_text
      → self._delegate(子 Agent 名, task_type, params)   # a2a.protocol.delegate 真实 HTTP A2A
      → 子 Agent 服务器 handle_message → MCP 网关 → tools.*
      → handoff_briefing(result)               # A2A 反问是否生成简报 → 委派 publisher

对外暴露的接口：
- CoordinatorAgent          : 协调调度 Agent 类（继承 A2AServer）。对外：route（意图路由）、
                              run_hotspot / run_latest / run_account_follow（上游查询）、
                              run_account_monitor_from_text（账户监控分发）、
                              handoff_briefing（简报交接）、handle_message（A2A 协议入口）。
- create_coordinator_agent  : 工厂函数，返回 CoordinatorAgent 实例。

用法：
    python -m agents.coordinator_agent    # 跑一段模拟演示
"""

import json
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

from python_a2a import A2AServer, AgentCard, AgentSkill

# 项目 A2A 适配层：delegate 做真实 HTTP A2A 委派、parse_task 解析路由信息、
# encode_result / reply_text 构造回传消息、ask_user_confirm 反问用户（简报交接确认）。
from a2a.protocol import (ask_user_confirm, delegate, encode_result,
                          parse_task, reply_text)
# LLM 意图识别：返回 (intents, user_queries, follow_up_message)，供 route 做路由依据。
from agents.intent_agent import intent_agent
# 全局配置单例：conf.llm（LLM 参数）、conf.rss_sources（自定义模块名列表）。
from config import Config
# 项目统一日志器（控制台 + 文件双通道）。
from create_logger import logger
# 统一 DTO 数据模型：NewsItem / HotEvent / AccountPost / PipelineResult / dto_from_dict。
from task_pipelines.schemas import (AccountPost, HotEvent, NewsItem,
                                    PipelineResult, dto_from_dict)

# Config() 创建配置对象（模拟版主要用于触发配置读取，LLM 配置见 conf.llm）
conf = Config()

# ===== 槽位抽取（轻量：从用户话术里猜模块/账户/平台，补意图识别的参数缺位）=====
# 内置默认模块名（与 config.ini [rss_sources] 自定义模块名互补，作为猜模块的二级来源）。
_DEFAULT_MODULES = ["科技", "财经", "体育", "娱乐", "国际"]
# 账户正则：@ 后跟 中文/字母数字/连字符，用于从话术里抓 @账户名。
_ACCOUNT_RE = re.compile(r"@([\w一-龥\-]+)")
# 平台别名表：把话术里的中文平台词映射成内部平台标识（rss/weibo/wechat/xiaohongshu/bilibili）。
# rss 排在首位：监控面板下拉允许选 RSS，话术形如「持续监控rss账户 <url>」，必须命中 rss
# 而非落到默认 weibo；「订阅源」避开与微信公众号「订阅号」的歧义。
_PLATFORM_ALIASES = {
    "rss": ["rss", "feed", "订阅源"],
    "weibo": ["微博", "weibo", "wb"],
    "wechat": ["微信", "公众号", "wechat"],
    "xiaohongshu": ["小红书", "xiaohongshu"],
    "bilibili": ["哔哩哔哩", "bilibili", "b站", "b23.tv"],
}
# URL 正则：抓话术里第一个 http(s) 链接（账户监控需要主页地址），忽略中英文标点边界。
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
    """判断话术 text 里是否包含话题词 word。

    中文话题词（首字符是汉字）直接做子串匹配即可；
    英文/数字话题词要带单词边界（(?<!...) / (?!...) 前后断言），
    避免 'nba' 误中 'basketball' 这类部分拼写的情况。

    参数:
        text: 用户话术。
        word: 待匹配的话题词（如 "足球" / "nba"）。

    返回:
        bool：命中返回 True。
    """
    # 中文词直接子串判断（无需处理分词边界）。
    if re.search(r"^[一-龥]", word):
        return word in text
    # 英文/数字词：先转小写，再用左右边界断言精确匹配整词，避免部分拼写误命中。
    return re.search(r"(?<![a-z0-9])" + re.escape(word.lower()) + r"(?![a-z0-9])",
                     text.lower()) is not None

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
# AgentCard 声明本 Agent 的名称 / 描述 / 网络端点 / 能力 / 技能列表；
# 四个 AgentSkill 分别对应四类上游任务，外部调用方可据此发现能力并构造 A2A 请求。
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
        # 技能②：最新新闻查询（task_type=latest_news）。
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
        # 技能③：一次性账户发布查询（task_type=account_follow）。
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
        # 技能④：账户持续监控（task_type=account_monitor，经 A2A 交给 AccountMonitorAgent）。
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
    """协调调度 Agent：意图识别 · 任务路由 · A2A 简报交接。

    直接继承官方包 python_a2a 的 A2AServer，通过模块级 agent_card（含四个 skill）对外声明能力。
    核心职责：
      - route(intent)：LLM 意图识别 + 轻量槽位抽取 + 按意图分发到对应 run_* 任务；
      - run_hotspot / run_latest / run_account_follow / run_account_monitor_from_text：
        四类上游任务的 A2A 委派实现（真实 HTTP 委派子 Agent 服务器）；
      - handle_message(message)：A2A 协议入口，按 metadata 里的 task_type 路由执行并回传；
      - handoff_briefing(result)：任务完成后反问用户是否生成简报，确认后 A2A 委派 publisher。
    """

    name = "coordinator"
    role = "意图识别 · 任务路由 · A2A 简报交接"

    def __init__(self):
        """初始化协调调度 Agent：把模块级 agent_card 传给 A2AServer 基类注册能力。"""
        super().__init__(agent_card=agent_card)

    # ===== 意图路由 =====
    def route(self, intent: str) -> PipelineResult:
        """LLM 意图识别（intent_agent）→ 真实 HTTP A2A 分发任务给采集/加工子 Agent 服务器。

        路由流程：
          1) 调 intent_agent(intent) 做真实 LLM 意图识别，返回 (intents, user_queries, follow_up_message)；
          2) 若 follow_up_message 非空（意图有歧义 / 超出范围 out_of_scope），直接以
             PipelineResult(task_type="follow_up", error=...) 把追问返回给用户，不再分发任务；
          3) 否则做轻量槽位抽取（_guess_keyword 等），再按意图列表依次路由，优先级为
             hotspot_query > latest_news > account_follow > account_monitor；
          4) 意图列表为空或全未命中时，兜底按默认热点查询处理，保证用户不空手而归。

        参数:
            intent: 用户原始话术字符串（如 "帮我查一下科技模块的热点新闻"）。

        返回:
            PipelineResult：可能为 hotspot_query / latest_news / account_follow /
            account_monitor / follow_up 之一（task_type 字段标识）。
        """
        # 打印收到的原始用户话术，作为路由链路的第一步日志。
        logger.info(f"[coordinator] 收到意图: {intent}")
        # 真实 LLM 意图识别：识别新闻查询、一次性账户查询、持续账户监控与 out_of_scope。
        # 返回 (intents, user_queries, follow_up_message)，提示词见 prompt/main_prompt.py。
        # 参数抽取（module / account / platform 等槽位）后续再接，当前按【模拟】默认值。
        intents, user_queries, follow_up_message = intent_agent(intent)
        # 打印意图识别结果，便于核对 LLM 是否理解用户意图。
        logger.info(f"[coordinator] 识别到意图: {intents}，改写查询: {user_queries}，追问: {follow_up_message!r}")
        # 有追问（意图有歧义 / 超出范围 out_of_scope）：直接把追问返回给用户（走结果 error 字段展示）。
        if follow_up_message:
            # 追问走 follow_up 任务类型：items 留空，追问文本放进 error 字段供前端展示。
            return PipelineResult(task_type="follow_up", items=[], error=follow_up_message)
        # 先抽关键词（最具体的主题词，如「足球」「芯片」），作为下游采集的关键词过滤条件。
        kw = self._guess_keyword(intent)
        # 按识别到的意图依次路由（优先级 hotspot_query > latest_news > account_follow > account_monitor）。
        # intents 是意图列表，可能同时命中多个意图，这里只取第一个能分发执行的。
        for it in intents:
            # 意图①热点查询：猜模块 + 关键词过滤（有则传，无则 None），top_n 取 5。
            if it == "hotspot_query":
                return self.run_hotspot(module=self._guess_module(intent), top_n=5,
                                        keywords=[kw] if kw else None)
            # 意图②最新新闻：同样猜模块 + 关键词，count 取 5 条。
            if it == "latest_news":
                return self.run_latest(module=self._guess_module(intent), count=5,
                                       keywords=[kw] if kw else None)
            # 意图③账户发布查询：猜账户与平台，limit 取 5 条。
            if it == "account_follow":
                account, platform = self._guess_account(intent)
                return self.run_account_follow(account=account, platform=platform, limit=5)
            # 意图④账户监控：整段话术透传，由 _monitor_params 抽槽位并 A2A 分发。
            if it == "account_monitor":
                # 账户监控只需把话术透传给 _monitor_params 抽槽位，再 A2A 分发给 AccountMonitorAgent。
                return self.run_account_monitor_from_text(intent)
        # 兜底：意图列表为空 / 全是未识别值 → 按默认热点查询处理，避免用户拿不到任何结果。
        # 兜底分支：intents 为空或全是无法分发的值，退化为默认热点查询，保证有结果返回。
        logger.warning(f"[coordinator] 意图未识别，按默认热点查询处理: {intent}")
        # 默认热点查询：只猜模块（默认科技），条数用 run_hotspot 的默认 top_n=10。
        return self.run_hotspot(module=self._guess_module(intent))

    # ===== 槽位抽取（轻量：LLM 只识别意图，模块/账户/关键词等参数从话术里直接扫）=====
    def _guess_module(self, intent: str) -> str:
        """从用户话术里猜模块名（默认「科技」）。

        三级匹配，命中即返回：
          1. config.ini [rss_sources] 自定义的模块名（conf.rss_sources 的键，精确子串出现）；
          2. 内置默认模块名（_DEFAULT_MODULES，精确子串出现）；
          3. 主题词映射（_MODULE_ALIASES，如话术含「足球」→ 体育），
             用 _contains_word 做中英文边界匹配。
        全部未命中则回退到「科技」模块。

        参数:
            intent: 用户原始话术字符串。

        返回:
            猜测出的模块名（str）。
        """
        # 一级：config.ini 自定义模块名（用户可能在配置里加了新模块）。
        for m in conf.rss_sources:
            # 自定义模块名精确出现在话术里即命中。
            if m in intent:
                return m
        # 二级：内置默认模块名精确子串匹配。
        for m in _DEFAULT_MODULES:
            # 内置模块名出现在话术中即命中。
            if m in intent:
                return m
        # 三级：主题词别名映射（足球→体育、芯片→科技），用边界匹配避免英文词误中。
        for module, aliases in _MODULE_ALIASES.items():
            # 任一别名命中（中英文边界匹配）即返回对应模块。
            if any(_contains_word(intent, a) for a in aliases):
                return module
        # 兜底：什么都没命中时默认科技模块。
        return "科技"

    def _guess_keyword(self, intent: str) -> Optional[str]:
        """从话术里提取最具体的话题词作为搜索关键词（如「足球」「芯片」），无则 None。

        遍历 _MODULE_ALIASES 的所有主题词别名，跳过模块名本身（太宽泛）与过短的词
        （len < 2，如「ai」）；命中第一个匹配即返回。返回的关键词会作为 keywords
        传给 collect_news，由采集工具按关键词过滤标题/摘要。

        参数:
            intent: 用户原始话术字符串。

        返回:
            命中的话题词（str）；找不到则返回 None。
        """
        # 遍历所有模块的主题词别名，找最具体的话题词作为搜索关键词。
        for aliases in _MODULE_ALIASES.values():
            for a in aliases:
                # 跳过模块名（如「体育」太宽泛）与过短的词（如「ai」），避免关键词失去区分度。
                if a in _DEFAULT_MODULES or len(a) < 2:
                    continue
                # 命中即把该主题词作为关键词返回（越具体越能过滤噪音）。
                if _contains_word(intent, a):
                    return a
        # 没有任何别名命中 → 返回 None（下游不传 keywords，不做关键词过滤）。
        return None

    def _guess_account(self, intent: str) -> tuple:
        """从用户话术里猜 账户(@名) 和 平台（默认 @新京报 / weibo）。

        用 _ACCOUNT_RE（@ 后跟中文/数字/连字符）抓第一个 @账户名，抓不到就用默认 @新京报；
        平台按 _PLATFORM_ALIASES 的别名做小写子串匹配（weibo / wechat / xiaohongshu / bilibili），
        未命中默认 weibo。

        参数:
            intent: 用户原始话术字符串。

        返回:
            (account, platform) 元组，如 ("@新京报", "weibo")。
        """
        # 抓 @账户名：_ACCOUNT_RE 的捕获组 1 是不带 @ 的名字，再补回 @ 前缀。
        m = _ACCOUNT_RE.search(intent)
        # 有 @账户则用它，否则回退默认 @新京报（演示账户）。
        account = "@" + m.group(1) if m else "@新京报"
        # 转小写便于做平台别名的子串匹配（如 "B站" → "b站"）。
        low = intent.lower()
        # 平台默认 weibo（微博），后续循环未命中任何别名时保持该默认值。
        platform = "weibo"
        # 遍历平台别名表，命中最先匹配到的平台（weibo 排在第一个，天然是默认值）。
        for p, aliases in _PLATFORM_ALIASES.items():
            # 任一别名出现在话术里即切换到该平台，并停止继续匹配。
            if any(a in low for a in aliases):
                platform = p
                break
        # 返回 (账户, 平台)，供 run_account_follow 委派采集子 agent。
        return account, platform

    def _monitor_params(self, text: str) -> tuple:
        """抽取监控动作、账户、平台和主页地址；只做槽位抽取，不执行业务。

        动作判定按关键词优先级：
          - 话术含「状态 / 列表 / 监控了哪些 / 监控情况」→ status（查询监控状态）；
          - 话术含「立即检查 / 检查全部 / 执行监控 / 运行监控」→ check（立即执行监控）；
          - 话术含「停止监控 / 取消监控 / 不再监控」→ stop（停止监控）；
          - 其它 → register（默认注册新的持续监控）。
        主页地址用 _URL_RE 从话术中抓第一个 URL，并去掉可能粘连的右括号/引号；
        账户与平台复用 _guess_account，并对「微博话术里提到 b 站」与「URL 含
        bilibili UID」两种情况做平台/账户修正。

        参数:
            text: 用户监控话术（A2A account_monitor 任务的 params.text）。

        返回:
            (action, account, platform, url) 元组。
        """
        # 转小写文本，供后续 b 站相关的平台修正做子串匹配。
        low = text.lower()
        # 动作识别：按 状态 > 检查 > 停止 > 注册 的优先级判定（第一个命中即生效）。
        if any(word in text for word in ("状态", "列表", "监控了哪些", "监控情况")):
            action = "status"   # 查询当前监控状态
        elif any(word in text for word in ("立即检查", "检查全部", "立即执行", "执行监控", "运行监控")):
            action = "check"    # 立即执行一次监控检查
        elif any(word in text for word in ("停止监控", "取消监控", "不再监控")):
            action = "stop"     # 停止既有监控
        else:
            action = "register" # 兜底：注册新的持续监控
        # 主页地址：抓第一个 URL，rstrip 掉可能粘连的右括号/花括号/全角括号，得到干净的地址。
        match = _URL_RE.search(text)
        # 有 URL 则去尾部粘连符号，无则留空（register 缺地址会在上层触发追问）。
        url = match.group(0).rstrip(")]}）】") if match else ""
        # 账户与平台：复用 _guess_account 的默认逻辑。
        account, platform = self._guess_account(text)
        # 平台修正：话术里虽然没出现平台别名，但提到了 b 站相关词 → 判定为 bilibili。
        if platform == "weibo" and ("b站" in low or "bilibili" in low or "b23.tv" in low):
            platform = "bilibili"
        # 账户修正：bilibili 平台且没有显式 @账户时，用 URL 里的 UID 拼出账户标识。
        uid = re.search(r"space\.bilibili\.com/(\d+)", url)
        # 正则捕获组 1 是数字 UID；命中才替换默认账户。
        if platform == "bilibili" and account == "@新京报" and uid:
            account = f"bilibili_{uid.group(1)}"
        # 返回抽取到的四个槽位，供上层映射任务类型并构造委派参数。
        return action, account, platform, url

    # ===== A2A 委派辅助（真实 HTTP）=====
    def _delegate(self, agent_name: str, task_type: str, params: dict, dto_cls=None):
        """真实 HTTP A2A 委派子 Agent 服务器并还原回传 DTO；ok=False 时抛异常。

        agent_name 是子 agent 名字（collector / processor / publisher），经
        a2a.protocol.delegate 的 AgentNetwork HTTP 路径投递（懒加载网络，见 a2a/protocol.py）；
        子 agent 的 handle_message 回传文本是 encode_result 编码的 {ok, result, error} JSON，
        这里 json.loads 解析；若 ok 为 False 抛 RuntimeError。
        result 若为 list 且给了 dto_cls，则用 dto_from_dict 逐条还原成 DTO 对象
        （如 NewsItem / EventCluster / HotEvent）。

        参数:
            agent_name: 子 agent 名字（对应 a2a.protocol._AGENT_ENDPOINTS 里的 key）。
            task_type:  子 agent 侧 handle_message 要路由的任务类型（如 "collect_news"）。
            params:     任务参数字典（会放进 A2A 消息 metadata.custom_fields）。
            dto_cls:    可选。回传 result 是 list 时，用它逐条还原 DTO。

        返回:
            子 agent 回传的 result（还原后的 DTO 列表，或原样 dict / 标量）。

        抛出:
            RuntimeError: 委派失败（子 agent 回传 ok=False）时抛出，错误信息透出。
        """
        # 真实 HTTP A2A 委派：目标为 agent 名 → a2a.protocol.delegate 走 AgentNetwork 投递。
        text = delegate(agent_name, task_type, params, from_agent=self.name)
        # 回传文本是 encode_result 编码的 JSON，统一按 {ok, result, error} 契约解析。
        data = json.loads(text)
        # ok=False：子 agent 明确返回失败（或协议异常），把 error 抛给上层调用方。
        if not data.get("ok"):
            # 用子 agent 回传的 error 文本（缺省时用任务类型拼一句兜底信息）。
            raise RuntimeError(data.get("error") or f"{task_type} 委派失败")
        # 取出业务结果（可能是 DTO 列表、dict 或标量）。
        result = data.get("result")
        # 需要还原 DTO 且回传是 list 时，逐条用 dto_from_dict 重建（过滤非 dict 元素）。
        if dto_cls and isinstance(result, list):
            return [dto_from_dict(dto_cls, r) for r in result if isinstance(r, dict)]
        # 不需要还原 DTO / 非列表：原样返回。
        return result

    # ===== 上游查询任务（A2A 分发子 Agent）=====
    def run_hotspot(self, module: str, top_n: int = 10,
                    time_window_hours: int = 24,
                    keywords: Optional[List[str]] = None) -> PipelineResult:
        """① 模块热点查询 → 真实 HTTP A2A 委派链：采集(collector) → 聚类 → 热度 → 核验(processor)。

        共四条 A2A 委派，形成「采集 → 加工」的流水线：
          1. collector.collect_news      采集原始资讯（limit 取 max(top_n*5, 30)，给聚类留足样本）；
          2. processor.cluster_events    按内容相似度（threshold=0.8）聚成事件簇（EventCluster）；
          3. processor.score_heat        计算热度得分并排序（HotEvent，回溯窗口 time_window_hours）；
          4. processor.verify_events     多源交叉核验，回填可信度（可信/存疑/证据不足）。
        最终把核验后的事件列表截断到 top_n，包装成 PipelineResult（带 queried_at / elapsed_ms）。

        参数:
            module:             模块名（科技 / 财经 / 体育 / 娱乐 / 国际）。
            top_n:              返回热点条数上限（默认 10）。
            time_window_hours:  热度回溯窗口（小时，默认 24）。
            keywords:           可选关键词过滤列表（如 ["足球", "芯片"]）。

        返回:
            PipelineResult：task_type="hotspot_query"，items 为 List[HotEvent]。
        """
        # 延迟 import EventCluster：只有热点任务才需要该 DTO 类型，避免顶层循环依赖。
        from tools.process import EventCluster
        # 打印本次热点查询的参数（模块/条数/回溯窗口/关键词），便于链路追踪。
        logger.info(f"[coordinator] 查询「{module}」热点 top{top_n}（近{time_window_hours}h）"
                    f"{' 关键词=' + str(keywords) if keywords else ''}")
        # 记录开始时间，用于统计整条链路耗时（elapsed_ms）。
        started = time.time()
        # 1) 委派采集子 agent 拿原始资讯；limit 放大 5 倍给下游聚类留足样本。
        news = self._delegate("collector", "collect_news",
                              # 采集参数：模块 + 可选关键词 + 采样上限（至少 30 条）。
                              {"module": module, "keywords": keywords,
                               "limit": max(top_n * 5, 30)}, dto_cls=NewsItem)
        # 2) 委派加工子 agent 做相似度聚类，产出事件簇。
        clusters = self._delegate("processor", "cluster_events",
                                  # 聚类参数：原始资讯列表 + 相似度阈值 0.8。
                                  {"news_items": news, "threshold": 0.8},
                                  dto_cls=EventCluster)
        # 3) 委派加工子 agent 计算热度并排序。
        events = self._delegate("processor", "score_heat",
                                # 热度参数：事件簇 + 回溯窗口（默认 24 小时）。
                                {"clusters": clusters, "time_window_hours": time_window_hours},
                                dto_cls=HotEvent)
        # 4) 委派加工子 agent 做多源交叉核验，回填每条事件的可信度。
        verified = self._delegate("processor", "verify_events",
                                  # 核验参数：待核验的热点事件列表。
                                  {"events": events}, dto_cls=HotEvent)
        # 组装统一返回结构：截断 top_n，记录查询时刻与耗时（毫秒）。
        result = PipelineResult(
            task_type="hotspot_query",          # 任务类型：热点查询。
            items=verified[:top_n],             # 只保留热度最高的 top_n 条。
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),  # 查询时刻（UTC ISO8601）。
            elapsed_ms=int((time.time() - started) * 1000),  # 整条链路耗时（毫秒）。
        )
        # 打印完成日志：命中条数与耗时。
        logger.info(f"[coordinator] 热点查询完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        # 返回统一的 PipelineResult（items 为 List[HotEvent]）。
        return result

    def run_latest(self, module: str, count: int = 20,
                   keywords: Optional[List[str]] = None) -> PipelineResult:
        """② 模块最新新闻 → A2A 委派采集子 Agent（去重/时间倒序已内置于 collect 工具）。

        单条 A2A 委派 collector.collect_news 拉取原始资讯（limit=count），
        采集工具内部已做关键词过滤、去重与按时间倒序，这里只需按 count 截断并
        包装成 PipelineResult（task_type="latest_news"，items 为 List[NewsItem]）。

        参数:
            module:   模块名。
            count:    返回条数上限（默认 20）。
            keywords: 可选关键词过滤列表。

        返回:
            PipelineResult：task_type="latest_news"，items 为 List[NewsItem]。
        """
        # 打印本次最新新闻查询的参数（模块/条数/关键词）。
        logger.info(f"[coordinator] 查询「{module}」最新新闻 top{count}"
                    f"{' 关键词=' + str(keywords) if keywords else ''}")
        # 记录开始时间，用于计算耗时（elapsed_ms）。
        started = time.time()
        # 委派采集子 agent 拿最新资讯（去重/时间倒序在 tools/collect.collect_news 内部完成）。
        news = self._delegate("collector", "collect_news",
                              # 采集参数：模块 + 可选关键词 + 条数上限。
                              {"module": module, "keywords": keywords, "limit": count},
                              dto_cls=NewsItem)
        # 组装统一返回结构：截断 count，记录查询时刻与耗时。
        result = PipelineResult(
            task_type="latest_news",            # 任务类型：最新新闻。
            items=news[:count],                 # 只保留前 count 条（工具已按时间倒序）。
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),  # 查询时刻（UTC ISO8601）。
            elapsed_ms=int((time.time() - started) * 1000),  # 整条链路耗时（毫秒）。
        )
        # 打印完成日志：命中条数与耗时。
        logger.info(f"[coordinator] 最新新闻查询完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        # 返回统一的 PipelineResult（items 为 List[NewsItem]）。
        return result

    def run_account_follow(self, account: str, platform: str,
                           since: Optional[str] = None, limit: int = 20) -> PipelineResult:
        """③ 账户发布关注 → A2A 委派采集子 Agent（since 过滤/时间倒序已内置于工具）。

        委派 collector.fetch_account_posts 拉取指定账户在给定时间后的新发布
        （account + platform 定位账户，since 做时间过滤，工具内部按时间倒序返回），
        按 limit 截断后包装成 PipelineResult（task_type="account_follow"，items 为 List[AccountPost]）。

        参数:
            account:  账户标识（如 "@新京报" 或 "bilibili_312249633"）。
            platform: 平台（weibo / wechat / xiaohongshu / bilibili）。
            since:    可选 ISO 8601 时间字符串，只返回该时刻之后的发布。
            limit:    返回条数上限（默认 20）。

        返回:
            PipelineResult：task_type="account_follow"，items 为 List[AccountPost]。
        """
        # 打印本次账户查询参数（账户/平台/条数上限）。
        logger.info(f"[coordinator] 关注账户 {account}@{platform} 新发布（limit={limit}）")
        # 记录开始时间，用于计算耗时（elapsed_ms）。
        started = time.time()
        # 委派采集子 agent 拉取账户新发布（since 过滤/时间倒序在 tools/collect 内部完成）。
        posts = self._delegate("collector", "fetch_account_posts",
                               # 采集参数：账户 + 平台 + 可选时间过滤 + 条数上限。
                               {"account": account, "platform": platform,
                                "since": since, "limit": limit},
                               dto_cls=AccountPost)
        # 组装统一返回结构：截断 limit，记录查询时刻与耗时。
        result = PipelineResult(
            task_type="account_follow",         # 任务类型：一次性账户发布查询。
            items=posts[:limit],                # 只保留前 limit 条发布。
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),  # 查询时刻（UTC ISO8601）。
            elapsed_ms=int((time.time() - started) * 1000),  # 整条链路耗时（毫秒）。
        )
        # 打印完成日志：拉取条数与耗时。
        logger.info(f"[coordinator] 账户发布拉取完成，共 {len(result.items)} 条（耗时 {result.elapsed_ms}ms）")
        # 返回统一的 PipelineResult（items 为 List[AccountPost]）。
        return result

    def run_account_monitor_from_text(self, text: str) -> PipelineResult:
        """④ 账户监控大任务：只做参数抽取和 A2A 分发，不在 Coordinator 内实现监控业务。

        _monitor_params 从话术里抽 (action, account, platform, url)；
        action 通过 task_map 映射到 account_monitor 子 agent 侧的任务类型：
          register → register_monitor / check → check_monitors / status → monitor_status /
          stop → stop_monitor。
        注册（register）但话术里没给主页 URL 时，直接返回 follow_up 追问（error 字段带提示），
        避免退化成一次性 account_follow（与 intent 提示词的要求一致）。
        其余情况经 _delegate 真实 HTTP A2A 委派给 account_monitor 子 agent（:8009）。

        参数:
            text: 用户监控话术（A2A account_monitor 任务的 params.text）。

        返回:
            PipelineResult：task_type="account_monitor"，items 为监控结果列表。
        """
        # 槽位抽取：从话术里解析出 动作/账户/平台/主页地址，不执行任何监控业务。
        action, account, platform, url = self._monitor_params(text)
        # 动作 → 子 agent 任务类型 的映射表。
        task_map = {
            "register": "register_monitor",  # 注册新监控
            "check": "check_monitors",       # 立即执行检查
            "status": "monitor_status",      # 查询监控状态
            "stop": "stop_monitor",          # 停止监控
        }
        # 注册监控但缺主页地址：直接追问用户，不委派（避免拿不到抓取源）。
        if action == "register" and not url:
            # follow_up 任务类型：items 留空，把引导提示放进 error 字段返回给用户。
            return PipelineResult(
                task_type="follow_up", items=[],
                error="请提供要监控的账户主页地址，例如：https://space.bilibili.com/312249633/video",
            )
        # 构造参数：check_now=False 表示注册后不再同步等待采集/视频处理（避免超过
        # A2A 客户端 30 秒超时）；实际检查由定时调度器或「立即检查全部」后台任务完成。
        params = {"account": account, "platform": platform, "url": url, "check_now": False}
        # 打印分发的动作与账户信息，便于追踪监控链路。
        logger.info(f"[coordinator] A2A 分发账户监控任务 action={action} account={account}@{platform}")
        # 真实 HTTP A2A 委派给 account_monitor 子 agent 服务器。
        result = self._delegate("account_monitor", task_map[action], params)
        # 回传可能是单条 dict 或多条 list，统一包装成 list 放进 PipelineResult。
        return PipelineResult(
            task_type="account_monitor",  # 任务类型：账户持续监控。
            items=result if isinstance(result, list) else [result],  # 单条时包成单元素列表。
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),  # 查询时刻（UTC ISO8601）。
        )

    # ===== A2A 简报交接 =====
    def handoff_briefing(self, result: PipelineResult) -> bool:
        """A2A 反问“是否生成简报？” 确认后经真实 HTTP A2A 委派 PublisherAgent，返回是否生成。

        ask_user_confirm 在终端交互等待用户输入（y/yes/是 表示确认，见 a2a.protocol）；
        确认后 delegate("publisher", task_type="briefing", params={"result": result})
        真实 HTTP A2A 委派输出 Agent 生成简报并推送（publisher 端点 :8003）。
        回传文本是 {ok, result, error} JSON，解析后返回 ok 的布尔值；
        任何异常（子 agent 未启动 / 解析失败）都记为 False，不中断主流程。

        参数:
            result: 上游任务完成的 PipelineResult（作为简报的数据源）。

        返回:
            bool：简报是否生成成功（用户取消或委派失败也返回 False）。
        """
        # 打印进入简报交接阶段的日志。
        logger.info("[coordinator] 任务完成，准备 A2A 简报交接")
        # 反问用户：终端交互输入 y/yes/是 确认生成，否则整个简报流程结束。
        if not ask_user_confirm("任务已完成，是否生成简报并推送？"):
            # 用户未确认：记录日志并直接返回 False。
            logger.info("[coordinator] 用户选择不生成简报，流程结束")
            return False
        # 委派简报任务给输出 Agent（publisher）→ 真实 HTTP A2A（:8003）。
        try:
            # 构造 briefing 任务：把整份 PipelineResult 放进 params 传给 publisher。
            text = delegate("publisher", task_type="briefing",
                            params={"result": result}, from_agent=self.name)
            # 打印 publisher 回传的原始文本，便于排查交接问题。
            logger.info(f"[coordinator] 简报交接回传: {text}")
            # 回传是 {ok, result, error} JSON，解析后取 ok 字段。
            data = json.loads(text)
            # 只有 ok=True 才认为简报生成成功。
            return bool(data.get("ok"))
        except Exception:
            # 委派异常（服务器未起 / JSON 解析失败）按失败处理，不让异常穿透到上游。
            return False

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """A2A 协议入口：收到 Message → 解析 task_type / params → 路由执行 → 回传。

        python_a2a 的 A2AServer 默认 handle_task 会桥接本方法；路由信息放在
        message.metadata.custom_fields（见 a2a.protocol.send_task）。按 task_type 分发：
          hotspot_query / latest_news / account_follow / account_monitor；
        未知任务类型或执行异常都记 ok=False + error。最后统一 encode_result 编码成
        {ok, result, error} JSON，经 reply_text 构造 AGENT 角色回传消息
        （挂父消息 / 会话 ID，保证 A2A 会话连续性）。

        参数:
            message: python_a2a 的 Message 对象（含 content / metadata / message_id / conversation_id）。

        返回:
            python_a2a 的 Message 对象（回传给请求方）。
        """
        # 解析路由信息：task_type / params 在 metadata.custom_fields 里。
        task_type, params, from_agent = parse_task(message)
        # 打印收到的 A2A 任务类型与来源，便于链路追踪。
        logger.info(f"[coordinator] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            # 按任务类型分发到对应的 run_* 业务方法；成功时 ok=True、error=None。
            if task_type == "hotspot_query":
                # 热点查询：module 默认科技，top_n 默认 10。
                result = self.run_hotspot(params.get("module", "科技"), params.get("top_n", 10))
                ok, error = True, None
            elif task_type == "latest_news":
                # 最新新闻：module 默认科技，count 默认 20。
                result = self.run_latest(params.get("module", "科技"), params.get("count", 20))
                ok, error = True, None
            elif task_type == "account_follow":
                # 账户发布查询：account 默认未知账户，platform 默认 weibo。
                result = self.run_account_follow(
                    params.get("account", "未知账户"), params.get("platform", "weibo"),
                    params.get("since"), params.get("limit", 20))
                ok, error = True, None
            elif task_type == "account_monitor":
                # 账户监控任务传的是话术文本，交给 run_account_monitor_from_text 抽槽位并分发。
                result = self.run_account_monitor_from_text(params.get("text", ""))
                ok, error = True, None
            else:
                # 未知任务类型：不抛异常，而是以 ok=False + error 回传。
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            # 业务执行异常：记录日志并把异常信息透出到回传 error 字段。
            logger.error(f"[coordinator] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）。
        text = encode_result(ok, result, error)
        # 用 reply_text 构造 AGENT 角色回传消息，挂上父消息与会话 ID 保持会话连续性。
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建协调调度 Agent =====
def create_coordinator_agent():
    """创建协调调度 Agent 实例（仿照 create_order_mcp_server 的工厂模式）。

    打印 Agent 名称与职责日志后返回 CoordinatorAgent()（内部已挂上模块级 agent_card）。

    返回:
        CoordinatorAgent 实例。
    """
    # 打印 Agent 启动信息：名称与职责。
    logger.info("=== 协调调度Agent信息 ===")
    logger.info(f"名称: {CoordinatorAgent.name}")
    logger.info(f"职责: {CoordinatorAgent.role}")
    # 返回实例（内部已挂上模块级 agent_card）。
    return CoordinatorAgent()


if __name__ == "__main__":
    # 演示：真实 LLM 意图识别（intent_agent）→ 路由 → 结果展示 → A2A 简报交接（交互式输入 y 确认生成简报）
    # 创建协调 Agent 实例（挂载模块级 agent_card）。
    agent = create_coordinator_agent()
    # 演示一次真实意图识别 → 路由：识别到 hotspot_query 会走采集+加工链路。
    result = agent.route("帮我查一下科技模块的热点新闻")
    # follow_up 表示意图有歧义 / out_of_scope，直接展示追问文本。
    if result.task_type == "follow_up":
        print(f"\n需要向用户追问/直接回复: {result.error}")
    else:
        # 正常路由结果：展示任务类型、条数与每条标题/热度。
        print(f"\n路由结果: task_type={result.task_type} items={len(result.items)}")
        # 逐条打印热点事件的标题与热度分。
        for item in result.items:
            print(f"  - {item.title}（热度 {item.heat_score}）")
        # 任务完成后反问用户是否生成简报（交互式输入 y 确认）。
        agent.handoff_briefing(result)

    # 歧义追问演示：没说明模块 → intent_agent 返回 follow_up_message
    print("\n=== 歧义追问演示 ===")
    # 不带模块名 → LLM 应返回 follow_up_message（追问）。
    result2 = agent.route("有什么热点新闻")
    print(f"task_type={result2.task_type}，追问: {result2.error}")

    # A2A 入口演示：构造 Message（task_type/params 放 metadata）→ handle_message → 回传
    print("\n=== A2A 入口演示 ===")
    # 通过 delegate 直接走 handle_message 入口（绕过 route 的意图识别）。
    text = delegate(agent, task_type="hotspot_query",
                    params={"module": "财经", "top_n": 3}, from_agent="main")
    print("回传:", text)
