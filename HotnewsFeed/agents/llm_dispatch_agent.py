#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""文件名: llm_dispatch_agent.py  项目: HotnewsFeed

本文件干什么：
    一个「由 LLM 决定把任务派给谁」的新调度 Agent（实验性 / 演示，纯新增文件，
    不改动其它任何代码）。

    与现有 CoordinatorAgent 的本质区别：
      CoordinatorAgent 的 route() 收到话术后，先让 LLM 把话术归到 4 个写死的意图，
      再靠硬编码的 if/elif + 写死的委派链决定派给哪个子 Agent、调哪个 skill、按什么顺序
      （例如 run_hotspot 内部写死 collect_news → cluster_events → score_heat → verify_events）。

      本模块不预置任何「意图 → 任务链」的代码分支。它做的三件事：
        1) 读取各子 Agent 模块顶层的 AgentCard（name / description / skills），拼成一份
           「能力目录」；
        2) 把能力目录 + 用户话术一起喂给 LLM，让 LLM 自己输出一个 JSON 计划：
           steps = [{agent, skill, params}, ...]，params 里可用 {"$use": n} 引用前序
           某一步的完整返回（数据流自动拼接）；
        3) 逐条执行计划：经 a2a.protocol.delegate 委派给对应子 Agent（HTTP 优先，
           连不上则降级进程内直调其 handle_message）。执行器是通用的，不按 skill 名
           分派任何业务——"派给谁、调哪个能力、先后顺序"全部来自 LLM 对能力目录的判断。

    本文件体现了「基于服务名 / 服务描述实现功能」的 agent 范式：
      新增一个能力时，只要某子 Agent 的 AgentCard 多声明一个 skill，并在此文件的
      _SKILL_SPECS（声明式参数说明表）补一条签名，LLM 调度器就能自动学会调用它，
      而 Coordinator 那种写死链路的代码一行都不用加。

    说明：为使演示安全、可离线跑，默认能力目录只放 collector / processor 两个子 Agent
    （采集+加工即覆盖热点/最新/账户发布的主链路，且其 MCP 网关支持进程内降级）。
    publisher / account_monitor 可用 build_catalog(...) 的开关加入目录。

模块依赖:
- ``python_a2a``             : 官方 A2A 包（A2AServer / AgentCard / AgentSkill）。
- ``langchain_openai``       : ChatOpenAI（OpenAI 兼容接口调 qwen，见 config.ini [llm]）。
- ``langchain_core.prompts`` : ChatPromptTemplate。
- ``a2a.protocol``           : delegate——真 HTTP A2A 委派；传 A2AServer 实例则同进程直调。
- ``agents.*_agent``         : 各子 Agent 模块。只读它们的模块级 agent_card 与工厂函数，
                              不改写任何东西。
- ``config.Config``          : conf.llm（LLM 参数）。
- ``create_logger.logger``   : 全局日志器。

典型调用链::

    用户话术
      → LLMDispatchAgent.route(query)
      → _plan(query)                       # LLM 读能力目录 → 输出 JSON 步骤计划
      → _run_plan(plan)                    # 逐条执行
          → _call(agent, skill, params)    # delegate：HTTP A2A → 降级进程内 handle_message
          → 若 params 含 {"$use": n}，用第 n 步返回替换后拼接成下一条的入参
      → 返回 (最终结果, 计划, 每步执行记录)

对外暴露的接口：
- LLMDispatchAgent   : 调度 Agent 类（继承 A2AServer）。route / _plan / _run_plan / handle_message。
- create_llm_dispatch_agent : 工厂函数。
- build_catalog     : 从各子 Agent 的 AgentCard 构造能力目录（dict 列表）。

用法：
    python -m agents.llm_dispatch_agent          # 演示：读目录 → LLM 规划 → 执行
    python -m agents.llm_dispatch_agent --serve  # 以 A2A 服务器形式跑在 :8010
"""

import json
import os
import re
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

# 官方 A2A：A2AServer 服务器基类、AgentCard / AgentSkill 声明本调度器的能力。
from python_a2a import A2AServer, AgentCard, AgentSkill

# 项目 A2A 适配层：encode_result / parse_task / reply_text 处理协议消息；delegate 做委派。
from a2a.protocol import (delegate, encode_result, parse_task, reply_text)
from config import Config
from create_logger import logger

conf = Config()
# 规划用 LLM：温度压低，保证输出的 JSON 计划尽量稳定（qwen-plus，见 config.ini [llm]）。
_planner_llm = ChatOpenAI(
    model=conf.llm["model_name"],
    base_url=conf.llm["base_url"],
    api_key=conf.llm["api_key"],
    temperature=0.2,
)

# 每个计划步骤允许的最大条数（防止 LLM 抽风生成超长链）。
_MAX_STEPS = 8
# 允许的默认模块名（与 coordinator 的 _DEFAULT_MODULES 对齐，仅供 LLM 选 module 参数参考）。
_ALLOWED_MODULES = ["科技", "财经", "体育", "娱乐", "国际"]

# ===== 能力签名表（声明式数据，不是逻辑分支）=====
# 现有 AgentSkill 只带自然语言 description，不携带机器可读的参数契约。为了让 LLM 能正确
# 填写 params、执行器能安全拼接数据流，这里给每个 skill 补一份「参数示例 + 返回语义」。
# 这是把 Coordinator 过去写死在代码里的任务参数知识，下沉成一份声明式元数据。
# 注意 dispatch 的键 = skill 的 id（子 Agent handle_message 就是按 skill.id 路由的）。
_SKILL_SPECS: Dict[str, Dict[str, Any]] = {
    "collect_news": dict(
        agent="collector",
        params='{"module": "科技", "keywords": null, "sources": null, "since": null, "limit": 40}',
        returns="List[资讯对象]：多源采集(RSS/热榜/HN)，已按时间倒序；每项含 title/url/summary/source/published_at。若后面还要聚类，limit 建议 30~60；若只是给用户看最新 N 条，limit 取 N。",
    ),
    "fetch_account_posts": dict(
        agent="collector",
        params='{"account": "@新京报", "platform": "weibo", "since": null, "limit": 10}',
        returns="List[账户发布对象]：指定平台账户的新发布，按时间倒序；每项含 title/url/account/platform/published_at。platform ∈ weibo/wechat/xiaohongshu/bilibili/rss。",
    ),
    "cluster_events": dict(
        agent="processor",
        params='{"news_items": {"$use": 0}, "threshold": 0.8}',
        returns="List[事件簇对象]：把原始资讯按内容相似度聚成簇；每簇含 title/related_news 等。news_items 必须引用前序 collect_news 的整段返回。",
    ),
    "score_heat": dict(
        agent="processor",
        params='{"clusters": {"$use": 1}, "time_window_hours": 24}',
        returns="List[热点事件对象]：按来源数/时效衰减算热度，已按 heat_score 降序。clusters 必须引用前序 cluster_events 的整段返回。",
    ),
    "verify_events": dict(
        agent="processor",
        params='{"events": {"$use": 2}}',
        returns="List[热点事件对象]：多源交叉核验后回填 credibility（可信/存疑/证据不足）。events 必须引用前序 score_heat 的整段返回。",
    ),
    "optimize": dict(
        agent="publisher",
        params='{"result": {"task_type": "hotspot_query", "items": {"$use": 3}}}',
        returns="PipelineResult 对象：把上游结果整理成统一展示结构（当前原样返回）。",
    ),
    "briefing": dict(
        agent="publisher",
        params='{"result": {"task_type": "hotspot_query", "items": {"$use": 3}}, "channels": ["web_ui"]}',
        returns="PublishResult 对象：{briefing_id, channels:{通道: 是否成功}}。注意会真实推送，只有用户明确要求生成/推送简报时才用。",
    ),
    "register_monitor": dict(
        agent="account_monitor",
        params='{"account": "bilibili_312249633", "platform": "bilibili", "url": "https://space.bilibili.com/312249633/video"}',
        returns="dict：{account, platform, url, registered}，把持续监控任务写入 MySQL。",
    ),
    "check_monitors": dict(
        agent="account_monitor",
        params='{}',
        returns="dict：{accepted, started, running, message}，后台启动全量检查并立即返回。",
    ),
    "monitor_status": dict(
        agent="account_monitor",
        params='{}',
        returns="list：当前各监控账户的 {account, post_count, last_check_at, last_error}。",
    ),
    "stop_monitor": dict(
        agent="account_monitor",
        params='{"account": "bilibili_312249633", "platform": "bilibili"}',
        returns="dict：停止监控的结果。",
    ),
}


# ===== 读取 AgentCard（通用字段读取，兼容 pydantic / dataclass）=====
def _f(obj: Any, name: str, default: Any = None) -> Any:
    """从任意对象按属性名取值，取不到 / 为 None 时用 default。"""
    v = getattr(obj, name, None)
    return default if v is None else v


def _skill_to_dict(skill) -> Dict[str, Any]:
    """把一个 AgentSkill 对象转成普通 dict（取 id/name/description/tags/示例前两条）。"""
    return {
        "id": _f(skill, "id", ""),            # 委派键 = skill.id（handle_message 的 task_type）。
        "name": _f(skill, "name", ""),
        "description": _f(skill, "description", ""),
        "tags": list(_f(skill, "tags", []) or []),
        "examples": list(_f(skill, "examples", []) or [])[:2],
    }


def build_catalog(include_publisher: bool = False,
                  include_account_monitor: bool = False) -> List[Dict[str, Any]]:
    """从各子 Agent 模块顶层的 AgentCard 读取真实能力，拼成能力目录。

    参数:
        include_publisher:        是否把 publisher 能力列入目录（其 briefing 会真实推送，
                                  默认关，避免演示误触发外发）。
        include_account_monitor:  是否把 account_monitor 能力列入目录（依赖 MySQL / 其
                                  A2A 服务器在 :8009，默认关）。

    返回:
        list[dict]：能力目录，每个元素 {agent, description, url, skills:[...]}。
        skills 里的每条是 AgentSkill 的 id/name/description/tags/examples，
        并合入了 _SKILL_SPECS 的 params/returns 声明。
    """
    catalog = []

    def _append(agent_name: str, spec_name: str, include: bool):
        if not include:
            return
        # 延迟 import 对应 Agent 模块：只读其模块级 agent_card，绝不实例化业务。
        mod = __import__(f"agents.{agent_name}_agent", fromlist=["agent_card", "create_collector_agent"])
        card = mod.agent_card
        skills = []
        for skill in _f(card, "skills", []):
            sk = _skill_to_dict(skill)
            spec = _SKILL_SPECS.get(sk["id"], {})
            # 只收录本模块声明过的 skill（spec 里 agent 与当前 agent 一致才给参数契约）。
            if spec.get("agent") == agent_name:
                sk["params"] = spec.get("params")
                sk["returns"] = spec.get("returns")
            else:
                # 没有参数契约的 skill 也留在目录里（description 仍可驱动 LLM 判断用途）。
                sk["params"] = "（本目录未提供参数契约，按描述自行推断）"
                sk["returns"] = "（未提供返回语义）"
            skills.append(sk)
        catalog.append({
            "agent": agent_name,
            "description": _f(card, "description", ""),
            "url": _f(card, "url", ""),
            "skills": skills,
        })

    # 默认收录：采集 + 加工（两条最常用的主链路，且 MCP 网关支持进程内降级，可离线演示）。
    _append("collector", None, include=True)
    _append("processor", None, include=True)
    # 可选收录（安全 / 依赖原因默认关闭）。
    _append("publisher", None, include_publisher)
    _append("account_monitor", None, include_account_monitor)
    return catalog


# ===== 规划提示词（纯文本规则；决定权交给 LLM）=====
_SYSTEM_PROMPT = """你是一个自主任务调度器。下面是当前可用的 Agent 能力目录，每个 Agent 有名字、描述，以及它对外提供的技能（skill.id 就是调用它时的键）。

你要做的是：把用户的请求拆成一个有序的执行计划 steps。规则：

1. 每个 step 只能调用目录里【某个 Agent 的某个 skill.id】。agent、skill 都必须是目录里出现过的。
2. steps 会严格按顺序执行。如果某一步需要用到前面某一步返回的整段数据，就把该参数写成 {"$use": 上一步在 steps 里的下标}（从 0 开始）。执行器会自动把前一步的完整返回填进去。
3. 判断数据流（用于挑选链路的顺序）：
   - 查某模块/某话题的【热点/热门事件】：collect_news(limit 建议 40) → cluster_events → score_heat → verify_events，最后一步的返回就是答案。
   - 查某模块【最新新闻】：一次 collect_news(limit=用户要的条数) 即可，不用聚类。
   - 查某账户【最新发布/作品】：一次 fetch_account_posts 即可。
   - 用户明确要求【生成/推送简报】才考虑 publisher；【注册/查询账户持续监控】才用 account_monitor。
4. 用不到的 step 不要加。用户请求信息不足时，用合理默认值补齐参数（module 默认“科技”，platform 默认“weibo”），不要反问。
5. 只输出一个 JSON 对象，不要 Markdown 代码块，不要多余文字。结构：
   {"reasoning": "简短说明你为什么这样安排", "steps": [{"agent": "...", "skill": "...", "params": {...}}]}
   用户请求不需要任何 Agent 能力（闲聊、感谢等）时返回 {"reasoning": "无需调度", "steps": []}。
6. module 参数只能取这些默认模块名之一：科技 / 财经 / 体育 / 娱乐 / 国际。
"""


# ===== 解析工具 =====
def _parse_json_plan(text: str) -> Dict[str, Any]:
    """把 LLM 返回的文本解析成 JSON 对象（容忍 ```json 围栏与前后杂讯）。"""
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t).strip()
    try:
        return json.loads(t)
    except Exception:
        # 兜底：抓第一对 { ... }（从首个 { 到最后 }）。
        start, end = t.find("{"), t.rfind("}")
        if start != -1 and end > start:
            return json.loads(t[start:end + 1])
        raise


def _resolve_refs(value: Any, results: List[Any], current_index: int) -> Any:
    """递归替换 params 里的 {"$use": n}：用第 n 步（n 必须 < 当前下标）的完整返回替换。"""
    if isinstance(value, dict):
        # 识别引用标记 {"$use": 下标}。
        if set(value.keys()) == {"$use"} and isinstance(value.get("$use"), int):
            ref = value["$use"]
            if not (0 <= ref < current_index):
                raise RuntimeError(f"步骤 {current_index} 引用了不存在的上一步下标 {ref}（引用只能指向更早的步骤）")
            return results[ref]
        return {k: _resolve_refs(v, results, current_index) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_refs(v, results, current_index) for v in value]
    return value


# ===== LLM 自主调度 Agent =====
class LLMDispatchAgent(A2AServer):
    """LLM 自主调度 Agent：读能力目录 → LLM 决定派给谁 → 通用执行器按序委派。

    与 CoordinatorAgent 最大的不同：本类不含任何「意图 → 写死任务链」的分支。
    它会话术连同各子 Agent 的 AgentCard 能力目录喂给 LLM，让 LLM 自己挑 agent / skill /
    顺序 / 参数。执行器 _run_plan 是通用的——不知道任何 skill 的业务含义。
    """

    name = "llm_dispatcher"
    role = "能力目录驱动 · LLM 决定派给谁 · 通用按序执行"

    def __init__(self):
        # 本调度器自己也声明一张 AgentCard（含 skill dispatch），供外部 A2A 调用。
        card = AgentCard(
            name=self.name,
            description="自主调度 Agent：读取各子 Agent 的能力目录（AgentCard），由 LLM 决定把用户请求派给哪个 Agent 的哪个 skill，并自动拼接数据流执行",
            url="http://localhost:8010",
            version="1.0.0",
            capabilities={"streaming": True, "memory": False},
            skills=[
                AgentSkill(
                    id="dispatch",
                    name="dispatch",
                    description="输入一段用户请求，LLM 根据能力目录规划并执行多 Agent 任务链",
                    tags=["orchestration", "llm", "discovery"],
                    examples=["查一下科技模块近24小时的热点新闻", "看看@新京报微博今天发了什么"],
                    input_modes=["text/plain"],
                    output_modes=["application/json"],
                ),
            ],
        )
        super().__init__(agent_card=card)
        self._instances: Dict[str, A2AServer] = {}  # 进程内降级用的子 Agent 实例缓存。
        self.last_plan: Optional[List[Dict]] = None  # 最近一次 LLM 出的计划（便于复盘）。
        self.last_trace: List[Dict] = []            # 最近一次执行的每步记录。

    # ===== 对外主入口 =====
    def route(self, query: str,
              include_publisher: bool = False,
              include_account_monitor: bool = False) -> Dict[str, Any]:
        """LLM 读能力目录自主规划并执行，返回最终结果。

        参数:
            query:                  用户话术。
            include_publisher:       是否把 publisher 列入能力目录（默认否，防误推送）。
            include_account_monitor: 是否把 account_monitor 列入能力目录（默认否，依赖 MySQL）。

        返回:
            dict：最终一步的返回（JSON 安全的 dict/list）；steps 为空时返回
            {"task_type": "follow_up", "error": "无需调度"}。
        """
        logger.info(f"[llm_dispatch] 收到请求: {query}")
        catalog = build_catalog(include_publisher, include_account_monitor)
        # 规划：LLM 决定 steps。
        plan = self._plan(query, catalog)
        self.last_plan = plan
        if not plan:
            logger.info("[llm_dispatch] LLM 判定无需调度")
            return {"task_type": "follow_up", "error": "这个问题不需要调用任何 Agent 能力。"}
        # 执行。
        results = self._run_plan(plan)
        logger.info("[llm_dispatch] 调度完成，共执行 %d 步", len(results))
        return results[-1]

    # ===== 规划：LLM 决定派给谁 =====
    def _plan(self, query: str, catalog: List[Dict]) -> List[Dict]:
        """把能力目录 + 用户话术喂给 LLM，要它输出步骤计划（steps 列表）。

        注意：直接用 llm.invoke([(role, text), ...]) 拼消息，而不用 ChatPromptTemplate。
        因为模板会把正文里字面的 { ... }（如示例 JSON 的 {"$use": 0} / {"reasoning": ...}）
        当成占位符去校验，抛 KeyError（见 prompt/main_prompt.py 头注的双花括号坑）。
        """
        human = (
            "能力目录：\n"
            f"{json.dumps(catalog, ensure_ascii=False, indent=1)}\n\n"
            f"用户请求：\n{query}\n\n"
            "请按规则只输出一个 JSON 对象。"
        )
        raw = _planner_llm.invoke([
            ("system", _SYSTEM_PROMPT),
            ("human", human),
        ]).content
        logger.debug(f"[llm_dispatch] 规划原始输出: {raw}")
        parsed = _parse_json_plan(str(raw))
        steps = parsed.get("steps") or []
        if not isinstance(steps, list):
            raise RuntimeError("LLM 输出异常：steps 不是列表")
        steps = steps[:_MAX_STEPS]
        # 做一次目录校验：agent / skill 必须真实存在，杜绝 LLM 幻觉出不存在的能力。
        valid = {(a["agent"], s["id"]) for a in catalog for s in a["skills"]}
        for i, st in enumerate(steps):
            agent, skill = st.get("agent"), st.get("skill")
            if (agent, skill) not in valid:
                raise RuntimeError(
                    f"LLM 计划第 {i} 步引用了目录里不存在的能力 agent={agent!r} skill={skill!r}；"
                    f"可用能力：{[f'{a}.{s}' for a in catalog for s in [x['id'] for x in a['skills']]]}"
                )
            st.setdefault("params", {})
        logger.info("[llm_dispatch] LLM 规划: %s",
                    json.dumps(steps, ensure_ascii=False))
        return steps

    # ===== 执行：通用按序委派 =====
    def _run_plan(self, plan: List[Dict]) -> List[Any]:
        """按序执行计划；把每步结果缓存下来，供后续步骤用 {"$use": n} 引用。"""
        results: List[Any] = []
        self.last_trace = []
        for i, st in enumerate(plan):
            agent, skill = st["agent"], st["skill"]
            params = _resolve_refs(st.get("params") or {}, results, i)
            logger.info("[llm_dispatch] step %d -> %s.%s", i, agent, skill)
            result = self._call(agent, skill, params)
            results.append(result)
            self.last_trace.append({
                "index": i, "agent": agent, "skill": skill,
                "params_keys": list((st.get("params") or {}).keys()),
                "return_type": type(result).__name__,
                "return_len": len(result) if isinstance(result, (list, dict)) else None,
            })
        return results

    # ===== 委派：HTTP A2A 优先，连不上降级进程内直调 =====
    def _call(self, agent: str, skill: str, params: Dict) -> Any:
        """委派 agent.skill 并返回解析后的 result（{ok,result,error} 契约）。

        - 先走真实 HTTP A2A（子 Agent 服务器在线时，与 coordinator 的委派路径一致）；
        - 服务器没起（连接被拒等异常）时，若本模块有该 Agent 的进程内工厂则降级直调其
          handle_message；没有则报错提示先启动服务器。
        """
        # 1) 真 HTTP A2A。
        data = None
        try:
            text = delegate(agent, skill, params, from_agent=self.name)
            data = json.loads(text)  # 子 Agent 回传的 {ok, result, error} JSON。
        except Exception as exc:
            logger.warning("[llm_dispatch] HTTP 委派 %s.%s 失败(%s)，尝试进程内直调", agent, skill, exc)
        if data is not None:
            if not data.get("ok"):
                # 业务失败（ok=False）：是真实业务错误，不降级（降级会重复干活）。
                raise RuntimeError(data.get("error") or f"{agent}.{skill} 执行失败")
            return data.get("result")
        # 2) 进程内降级：实例化该 Agent 直调其 handle_message（同进程走 send_task 消息）。
        inst = self._instance(agent)
        text = delegate(inst, skill, params, from_agent=self.name)
        data = json.loads(text)
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or f"{agent}.{skill} 进程内执行失败")
        return data.get("result")

    def _instance(self, agent: str) -> A2AServer:
        """取（必要时构造并缓存）某子 Agent 的进程内实例，用于降级直调。

        只对构造无副作用 / 无外部依赖的 Agent 提供进程内工厂；
        account_monitor（依赖 MySQL）不在此列，会提示启动其 A2A 服务器。
        """
        if agent in self._instances:
            return self._instances[agent]
        factories = {
            "collector": ("agents.collector_agent", "create_collector_agent"),
            "processor": ("agents.processor_agent", "create_processor_agent"),
            "publisher": ("agents.publisher_agent", "create_publisher_agent"),
        }
        mod_name, fn_name = factories.get(agent, (None, None))
        if mod_name is None:
            raise RuntimeError(
                f"Agent「{agent}」无法进程内直调（有外部依赖）。请先启动它的 A2A 服务器，"
                f"例如 python -m agents.{agent}_agent"
            )
        mod = __import__(mod_name, fromlist=[fn_name])
        inst = getattr(mod, fn_name)()
        self._instances[agent] = inst
        return inst

    # ===== A2A 入口 =====
    def handle_message(self, message) -> object:
        """协议入口：task_type="dispatch" → 把 query 交给 route 处理并回传。"""
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[llm_dispatch] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "dispatch":
                result = self.route(params.get("query", ""),
                                    params.get("include_publisher", False),
                                    params.get("include_account_monitor", False))
                ok, error = True, None
            else:
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            logger.error(f"[llm_dispatch] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        text = encode_result(ok, result, error)
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 工厂 =====
def create_llm_dispatch_agent():
    """创建 LLM 自主调度 Agent 实例（打印名称/职责后返回）。"""
    logger.info("=== LLM自主调度Agent信息 ===")
    logger.info(f"名称: {LLMDispatchAgent.name}")
    logger.info(f"职责: {LLMDispatchAgent.role}")
    return LLMDispatchAgent()


def _pretty_final(result: Any) -> None:
    """终端展示最终结果：list 摘要 / dict 关键字段。"""
    if isinstance(result, list):
        print(f"\n最终返回共 {len(result)} 条：")
        for item in result[:10]:
            if isinstance(item, dict):
                title = item.get("title") or item.get("name") or item.get("account")
                heat = item.get("heat_score")
                cred = item.get("credibility")
                print(f"  - {title}{f'（热度 {heat}' if heat is not None else ''}{f'，可信度 {cred}' if cred else ''}{'）' if heat is not None else ''}")
            else:
                print(f"  - {item}")
        if len(result) > 10:
            print(f"  …共 {len(result)} 条，仅展示前 10 条")
    elif isinstance(result, dict):
        for k, v in result.items():
            print(f"  {k}: {v if not isinstance(v, (list, dict)) else json.dumps(v, ensure_ascii=False)[:200]}")
    else:
        print(f"\n最终返回: {result}")


if __name__ == "__main__":
    import sys

    agent = create_llm_dispatch_agent()

    # 先打印能力目录，证明"派给谁"的依据来自各子 Agent 的 AgentCard 描述。
    print("\n========== 能力目录（来自各子 Agent 的 AgentCard）==========")
    for a in build_catalog():
        print(f"\n[Agent: {a['agent']}] {a['description']}")
        for s in a["skills"]:
            print(f"  - skill.id={s['id']}  {s['description']}")

    # --catalog-only：只看目录，不调 LLM。
    if "--catalog-only" in sys.argv:
        sys.exit(0)

    # --serve：以 A2A 服务器形式跑在 :8010。
    if "--serve" in sys.argv:
        from python_a2a import run_server
        run_server(agent, host="127.0.0.1", port=8010)
        sys.exit(0)

    # 演示：两条真实 LLM 规划 + 执行（collector/processor 走 MCP 网关，不可达自动降级 tools.*）。
    demos = [
        "查一下科技模块近24小时的热点新闻",       # 期望 LLM 自组 collect→cluster→score→verify 四步链
        "财经模块最新5条新闻",                    # 期望 LLM 只派一步 collect_news
    ]
    for q in demos:
        print(f"\n========== 用户请求: {q} ==========")
        try:
            final = agent.route(q)
            print("\nLLM 规划（agent.skill -> params 引用）:")
            for i, st in enumerate(agent.last_plan):
                print(f"  step{i} {st['agent']}.{st['skill']} {json.dumps(st['params'], ensure_ascii=False)}")
            _pretty_final(final)
        except Exception as exc:
            print(f"\n调度失败: {exc}")
