# -*- coding: utf-8 -*-
"""task_pipelines 共用的入参 / 返回值数据模型（schemas.py）。

所有流水线（task_pipelines/*_pipeline.py）的 run() 统一返回 PipelineResult，
便于前端 / 协调器 / A2A 协议统一消费；HotEvent / NewsItem / AccountPost 分别是
任务①②③ 的条目类型。

模块依赖:
- ``dataclasses.dataclass`` : 标准库数据类装饰器。四个模型都是 @dataclass，
                             构造时按关键字传参，缺省字段用默认值补齐。
- 依赖本模块的调用方        : task_pipelines/*_pipeline.py（run 的返回值类型）、
                             agents/coordinator_agent.py（A2A 回传还原）、
                             app.py / account_monitor/service.py（DTO 转换）。

典型调用链::

    task_pipelines/hotspot_query_pipeline.py  ->  PipelineResult(items=[HotEvent, ...])
    agents/coordinator_agent.py               ->  dto_from_dict(HotEvent, {...})  # A2A/MCP 回传还原
    account_monitor/service.py                ->  PipelineResult(task_type="account_monitor", items=[...])

对外暴露：
- ``HotEvent``       : 热点事件（任务①热点查询输出条目）。
- ``NewsItem``       : 最新新闻（任务②最新新闻输出条目）。
- ``AccountPost``    : 账户发布（任务③账户关注 / 持续监控输出条目）。
- ``PipelineResult`` : 流水线统一返回结构（含 task_type / items / queried_at / elapsed_ms / error）。
- ``dto_from_dict``  : 把 JSON dict 还原成对应 DTO 实例的工具函数。
- ``no_real_items``  : 判断结果是否全是【模拟】兜底样例的回退判断工具。
"""

from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class HotEvent:
    """热点事件（任务①热点查询的输出条目）。

    由任务①流水线（hotspot_query_pipeline.run）产出：采集 → 聚类 → 热度排序 → 核验，
    最终以 List[HotEvent] 形式放进 PipelineResult.items。
    字段与 tools/process.py 的 EventCluster / score_heat / verify_events 结果对应：
    event_id 聚类生成、heat_score 由「来源数 × 时效衰减 × 讨论量 × 权重」算出、
    credibility 是 LLM 多源交叉核验的结论（可信 / 存疑 / 证据不足）。
    """
    event_id: str            # 事件 ID（聚类生成）
    module: str              # 所属模块
    title: str               # 事件标题
    heat_score: float        # 热度评分（来源数 × 时效衰减 × 讨论量 × 权重）
    credibility: str         # 核验结论：可信 / 存疑 / 证据不足
    sources: List[str]       # 相关来源列表
    article_count: int = 0   # 关联资讯数
    first_seen_at: str = ""  # 首次出现时间（ISO 8601）
    latest_at: str = ""      # 最近更新时间（ISO 8601）
    summary: str = ""        # 事件摘要


@dataclass
class NewsItem:
    """最新新闻条目（任务②最新新闻查询的输出条目）。

    由任务②流水线（latest_news_pipeline.run）或 tools/collect.py 的 collect_news 产出，
    是「按时间倒序」的最新新闻列表元素。title / source / published_at / url 直接来自
    RSS 源解析；summary 为可选摘要（空串表示未生成）。
    """
    news_id: str             # 资讯 ID
    module: str              # 所属模块
    title: str               # 标题
    source: str              # 来源
    published_at: str        # 发布时间（ISO 8601）
    url: str                 # 原文链接
    summary: str = ""        # 摘要


@dataclass
class AccountPost:
    """账户发布内容（任务③账户关注 / 持续监控的输出条目）。

    由任务③流水线（account_follow_pipeline.run）、tools/bilibili.py 的
    fetch_bilibili_space_videos、account_monitor/service.py 共同产出。
    account 是账户标识（如 bilibili_312249633 / @新京报），platform 是平台名，
    post_id 是发布 ID（如 B 站 BV 号）。
    """
    post_id: str             # 发布 ID
    account: str             # 账户标识
    platform: str            # 平台（weibo / wechat / xiaohongshu …）
    title: str = ""          # 标题
    content: str = ""        # 内容摘要
    published_at: str = ""   # 发布时间（ISO 8601）
    url: str = ""            # 原文链接


@dataclass
class PipelineResult:
    """流水线统一返回结构（所有 run() 的出口类型）。

    items 的静态类型是 HotEvent / NewsItem / AccountPost 三选一的联合类型，
    实际元素类型由 task_type 决定：hotspot_query → HotEvent、latest_news → NewsItem、
    account_follow → AccountPost。queried_at 是本次执行时间（UTC ISO 8601），
    elapsed_ms 是执行耗时（毫秒），error 非空表示本次执行失败（成功为 None）。
    """
    task_type: str                        # hotspot_query / latest_news / account_follow
    items: Union[List[HotEvent], List[NewsItem], List[AccountPost]]  # 结果条目
    queried_at: str = ""                  # 本次执行时间（ISO 8601）
    elapsed_ms: int = 0                   # 执行耗时
    error: Optional[str] = None           # 失败时的错误信息（成功为 None）
    trace: Optional[List[dict]] = None    # 自主 Agent 每步决策/执行轨迹；固定流程为 None


def dto_from_dict(cls, d):
    """把 JSON dict 还原成 DTO 实例（MCP 回传 / 前端入参）；传入的本来就是 DTO 则原样返回。

    兼容写法：不存在的键自动忽略、缺的键用 dataclass 默认值补上。
    例：dto_from_dict(NewsItem, {"title": "..."})  # 只有 title 也能建出 NewsItem

    参数:
        cls: 目标 DTO 类（HotEvent / NewsItem / AccountPost / PipelineResult 等 dataclass）。
        d:   待还原的数据；可以是 dict（JSON 反序列化结果）或已是 DTO 实例。

    返回:
        cls 的一个实例。d 本来就是 cls 类型时原样返回（不做二次构造）。

    说明:
        cls.__dataclass_fields__ 是 dataclass 自动生成的字段名字典，
        用「键在该字典里才保留」的方式过滤掉 dict 里多余的键（如 MCP / A2A 回传
        多带的 metadata），缺的字段则走 dataclass 构造时的默认值，因此非常宽容。
    """
    if isinstance(d, cls):
        return d  # 本身就是目标 DTO，直接返回避免二次构造
    # 过滤掉 dict 里不在字段表里的多余键后按关键字构造；缺的字段走 dataclass 默认值
    return cls(**{k: v for k, v in (d or {}).items() if k in cls.__dataclass_fields__})


def no_real_items(items) -> bool:
    """判断结果里是否没有任何「真实」条目（空列表，或全是【模拟】兜底样例）。

    用于关键词过滤后的回退判断：关键词过滤可能把真实源全滤掉只剩 mock，
    此时应放宽条件重试（否则用户拿到一堆【模拟】假数据）。

    参数:
        items: 条目列表（HotEvent / NewsItem / AccountPost 等，元素有 title 属性即可）。

    返回:
        bool。True 表示「没有真实条目」：空列表，或每条 title 都以【模拟】开头。

    说明:
        getattr(i, "title", "") 用 getattr 取 title 并给空串兜底，避免对缺失 title
        的对象抛 AttributeError；all() 对空列表天然返回 True，因此与前面的 not items
        合并成「空或全模拟」的统一判断。
    """
    # 空列表直接判 True；否则要求「每条 title 都以【模拟】开头」才算没有真实数据
    return not items or all(
        getattr(i, "title", "").startswith("【模拟】") for i in items  # getattr 兜底空 title，避免缺字段抛异常
    )
