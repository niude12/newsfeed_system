# -*- coding: utf-8 -*-
"""agents 包：核心 Agent 集合（数据流 · 账户监控）。

Agent 用类表示，统一直接继承官方包 python_a2a 的 A2AServer，
每张标准 AgentCard（含 skill）声明在各自模块顶层：
  - CoordinatorAgent  协调调度（意图识别 · 路由 · A2A 简报交接）
  - CollectorAgent    采集（多源采集 · 账户监控）
  - ProcessorAgent    加工（聚类 · 热度 · 核验）
  - PublisherAgent    输出（优化呈现 · 简报推送）
  - AccountMonitorAgent 账户持续监控（注册 · 增量检查 · 入库通知）

惰性加载：用模块级 __getattr__ 按需导入，避免 import agents 时把所有子模块
一次性拉进来（否则直接运行 python -m agents.xxx 会触发重复导入告警）。
"""

_LAZY = {
    "CoordinatorAgent": "agents.coordinator_agent",
    "CollectorAgent": "agents.collector_agent",
    "ProcessorAgent": "agents.processor_agent",
    "PublisherAgent": "agents.publisher_agent",
    "AccountMonitorAgent": "agents.account_monitor_agent",
}

__all__ = list(_LAZY)


def __getattr__(name):
    """按需导入子模块并返回对应类（from agents import CoordinatorAgent 也能工作）"""
    if name in _LAZY:
        import importlib
        module = importlib.import_module(_LAZY[name])
        return getattr(module, name)
    raise AttributeError(f"module 'agents' has no attribute {name!r}")
