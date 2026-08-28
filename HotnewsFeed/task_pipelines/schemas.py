# -*- coding: utf-8 -*-
"""task_pipelines 共用的入参 / 返回值模型

所有流水线的 run() 统一返回 PipelineResult，便于前端 / 协调器统一消费。
"""

from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class HotEvent:
    """热点事件（任务①输出）"""
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
    """最新新闻条目（任务②输出）"""
    news_id: str             # 资讯 ID
    module: str              # 所属模块
    title: str               # 标题
    source: str              # 来源
    published_at: str        # 发布时间（ISO 8601）
    url: str                 # 原文链接
    summary: str = ""        # 摘要


@dataclass
class AccountPost:
    """账户发布内容（任务③输出）"""
    post_id: str             # 发布 ID
    account: str             # 账户标识
    platform: str            # 平台（weibo / wechat / xiaohongshu …）
    title: str = ""          # 标题
    content: str = ""        # 内容摘要
    published_at: str = ""   # 发布时间（ISO 8601）
    url: str = ""            # 原文链接


@dataclass
class PipelineResult:
    """流水线统一返回结构"""
    task_type: str                        # hotspot_query / latest_news / account_follow
    items: Union[List[HotEvent], List[NewsItem], List[AccountPost]]  # 结果条目
    queried_at: str = ""                  # 本次执行时间（ISO 8601）
    elapsed_ms: int = 0                   # 执行耗时
    error: Optional[str] = None           # 失败时的错误信息（成功为 None）


def dto_from_dict(cls, d):
    """把 JSON dict 还原成 DTO 实例（MCP 回传 / 前端入参）；传入的本来就是 DTO 则原样返回

    兼容写法：不存在的键自动忽略、缺的键用 dataclass 默认值补上。
    例：dto_from_dict(NewsItem, {"title": "..."})  # 只有 title 也能建出 NewsItem
    """
    if isinstance(d, cls):
        return d
    return cls(**{k: v for k, v in (d or {}).items() if k in cls.__dataclass_fields__})


def no_real_items(items) -> bool:
    """判断结果里是否没有任何「真实」条目（空列表，或全是【模拟】兜底样例）

    用于关键词过滤后的回退判断：关键词过滤可能把真实源全滤掉只剩 mock，
    此时应放宽条件重试（否则用户拿到一堆【模拟】假数据）。
    """
    return not items or all(
        getattr(i, "title", "").startswith("【模拟】") for i in items
    )
