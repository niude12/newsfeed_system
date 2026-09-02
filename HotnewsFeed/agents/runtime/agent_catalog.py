"""从真实 AgentCard 构建机器可读能力目录。"""

from typing import Any, Dict, List


SKILL_CONTRACTS: Dict[str, Dict[str, Any]] = {
    "collect_news": {"input": {"module": "科技", "keywords": None, "sources": None, "since": None, "limit": 40}, "output": "NewsItem[]", "side_effect": "read"},
    "fetch_account_posts": {"input": {"account": "账户标识或主页URL", "platform": "bilibili|rss|weibo|wechat|xiaohongshu", "since": None, "limit": 10}, "output": "AccountPost[]", "side_effect": "read"},
    "cluster_events": {"input": {"news_items": {"$observation": 0}, "threshold": 0.8}, "output": "EventCluster[]", "side_effect": "compute"},
    "score_heat": {"input": {"clusters": {"$observation": 1}, "time_window_hours": 24}, "output": "HotEvent[]", "side_effect": "compute"},
    "verify_events": {"input": {"events": {"$observation": 2}}, "output": "HotEvent[]", "side_effect": "compute"},
    "optimize": {"input": {"result": {"task_type": "hotspot_query", "items": {"$observation": 0}}}, "output": "PipelineResult", "side_effect": "compute"},
    "briefing": {"input": {"result": {}, "channels": ["web_ui"]}, "output": "Publish", "side_effect": "external_write", "requires_confirmation": True},
    "register_monitor": {"input": {"account": "", "platform": "bilibili", "url": ""}, "output": "registration", "side_effect": "database_write"},
    "check_monitors": {"input": {}, "output": "job receipt", "side_effect": "long_running"},
    "monitor_status": {"input": {}, "output": "monitor[]", "side_effect": "read"},
    "stop_monitor": {"input": {"account": "", "platform": "bilibili"}, "output": "stop result", "side_effect": "database_write", "requires_confirmation": True},
}


def _value(obj: Any, key: str, default: Any = None) -> Any:
    value = getattr(obj, key, None)
    return default if value is None else value


def build_agent_catalog(include_publisher: bool = True, include_account_monitor: bool = True) -> List[Dict[str, Any]]:
    names = ["collector", "processor"]
    if include_publisher:
        names.append("publisher")
    if include_account_monitor:
        names.append("account_monitor")
    catalog: List[Dict[str, Any]] = []
    for name in names:
        module = __import__(f"agents.{name}_agent", fromlist=["agent_card"])
        card = module.agent_card
        tools = []
        for skill in _value(card, "skills", []) or []:
            skill_id = _value(skill, "id", "")
            if skill_id == "execute":
                continue
            tools.append({
                "id": skill_id,
                "description": _value(skill, "description", ""),
                "tags": list(_value(skill, "tags", []) or []),
                "examples": list(_value(skill, "examples", []) or [])[:2],
                "contract": SKILL_CONTRACTS.get(skill_id, {}),
            })
        # Coordinator 只把目标交给 Agent.execute；具体工具由专业 Agent 的 LLM 决定。
        catalog.append({
            "agent": name,
            "description": _value(card, "description", ""),
            "skills": [{
                "id": "execute",
                "description": "根据 objective/context 自主选择下列一个工具并执行",
                "contract": {"input": {"objective": "本步目标", "context": {}}, "output": "tool result"},
            }],
            "tools": tools,
        })
    return catalog


def allowed_skill_map(catalog: List[Dict[str, Any]]) -> Dict[str, set]:
    return {item["agent"]: {skill["id"] for skill in item["skills"]} for item in catalog}
