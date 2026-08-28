# -*- coding: utf-8 -*-
"""数据加工类工具（tools/process.py）

事件聚类 · 热度评分 · 交叉验证
对应 MCP Server：mcp_process_server（:8005，挂载 PROCESS_TOOLS）

实现原则：
  1. 真实为主：
     - 聚类：调 dashscope 兼容接口的 /embeddings（模型见 config.ini [embedding]，默认
       text-embedding-v3，复用 [llm] 的 base_url/api_key）把标题向量化，余弦相似度贪婪聚类；
     - 核验：LLM（qwen-plus）按 prompt/main_prompt.py verify_prompt 判断每条事件可信度。
  2. 优雅降级：
     - embedding 接口失败 → 降级为「TF 词频向量 + 余弦」继续聚类；
     - LLM 核验失败 → 降级为启发式规则（来源数 / 关联资讯数阈值）。
"""

import asyncio
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from langchain_openai import ChatOpenAI

from config import Config
from create_logger import logger
from prompt.main_prompt import HotnewsFeedPrompts
from task_pipelines.schemas import HotEvent, NewsItem

# ===== 全局配置与 LLM =====
conf = Config()
llm = ChatOpenAI(
    model=conf.llm["model_name"],
    base_url=conf.llm["base_url"],
    api_key=conf.llm["api_key"],
    temperature=conf.temperature,
)


@dataclass
class EventCluster:
    """事件簇（事件聚类结果，写入事件聚类库 / pgvector）"""
    cluster_id: str                # 事件簇 ID
    title: str                     # 事件簇代表标题
    member_ids: List[str]          # 关联资讯 ID 列表
    centroid: List[float]          # 质心向量（Embedding / 降级时 TF 向量）
    created_at: str = ""           # 创建时间（ISO 8601）
    module: str = ""               # 所属模块（聚类时从成员带入）
    sources: List[str] = field(default_factory=list)  # 关联来源（去重）
    first_seen_at: str = ""        # 首次出现时间（ISO 8601）
    latest_at: str = ""            # 最近更新时间（ISO 8601）


# ===== Embedding（dashscope 兼容接口）=====
def _embed_texts(texts: List[str]) -> Optional[List[List[float]]]:
    """批量向量化（调 /embeddings）；失败返回 None，由上层降级为 TF 词频向量"""
    try:
        from openai import OpenAI
        client = OpenAI(base_url=conf.llm["base_url"], api_key=conf.llm["api_key"])
        resp = client.embeddings.create(model=conf.embedding_model, input=texts)
        data = sorted(resp.data, key=lambda d: d.index)   # 按输入顺序排好
        return [d.embedding for d in data]
    except Exception as exc:
        logger.warning(f"[process] embedding 调用失败，降级为 TF 词频向量: {exc}")
        return None


def _normalize(text: str) -> str:
    """压缩空白，避免分词时被换行/空格干扰"""
    return re.sub(r"\s+", " ", (text or "").strip())


def _tokenize(text: str) -> set:
    """中文分词降级用：英文单词 + 中文字符 + 中文二元组（对新闻标题效果足够）"""
    tokens = set(re.findall(r"[a-zA-Z0-9]+", text.lower()))
    zh = re.findall(r"[一-龥]", text)
    tokens.update(zh)
    tokens.update("".join(pair) for pair in zip(zh, zh[1:]))   # 二元组
    return tokens


def _tf_vectors(texts: List[str]) -> List[List[float]]:
    """TF 词频向量（embedding 降级用）：全量词表 + 归一化，维度 = 词表大小"""
    token_sets = [_tokenize(t) for t in texts]
    vocab: Dict[str, int] = {}
    for tokens in token_sets:
        for t in tokens:
            if t not in vocab:
                vocab[t] = len(vocab)
    vecs = []
    for tokens in token_sets:
        v = [0.0] * len(vocab)
        for t in tokens:
            v[vocab[t]] += 1.0
        norm = sum(x * x for x in v) ** 0.5
        vecs.append([x / norm if norm else 0.0 for x in v])
    return vecs


def _cosine(a: List[float], b: List[float]) -> float:
    """余弦相似度；维度不一致或零向量返回 0"""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


def _merge_uniq(lst: List[str], values: List[str]) -> List[str]:
    """把 values 里非空且不重复的元素并入 lst"""
    out = list(lst)
    for v in values:
        if v and v not in out:
            out.append(v)
    return out


# ===== 事件聚类 =====
async def cluster_events(
    news_items: List[NewsItem],
    threshold: float = 0.8,
) -> List[EventCluster]:
    """去重与事件聚类（规范化 → Embedding → 余弦相似度贪婪聚类）

    实现：单遍扫描，每条资讯与已有簇质心求余弦，最高相似度 ≥ threshold 就并入该簇
    （更新质心/来源/时间范围），否则新开一簇。

    Args:
        news_items: 原始资讯列表。
        threshold: 相似度阈值，默认 0.8，高于阈值合并为一个事件簇。

    Returns:
        List[EventCluster]: 事件簇列表。
    """
    if not news_items:
        return []

    texts = [_normalize(it.title + " " + it.summary) for it in news_items]
    # 真实优先：dashscope embedding；失败返回 None → 降级 TF 向量
    vectors = await asyncio.to_thread(_embed_texts, texts)
    if vectors is None:
        vectors = _tf_vectors(texts)

    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    clusters: List[EventCluster] = []
    for it, vec in zip(news_items, vectors):
        best_idx, best_sim = -1, -1.0
        for ci, c in enumerate(clusters):
            sim = _cosine(vec, c.centroid)
            if sim > best_sim:
                best_sim, best_idx = sim, ci
        if best_idx >= 0 and best_sim >= threshold:
            c = clusters[best_idx]
            c.member_ids.append(it.news_id)
            c.sources = _merge_uniq(c.sources, [it.source])
            # 质心 = 成员向量的均值（n-1 个旧均值 + 新向量）
            n = len(c.member_ids)
            c.centroid = [(c.centroid[k] * (n - 1) + vec[k]) / n for k in range(len(vec))]
            if len(it.title) > len(c.title):        # 代表标题取最长的（更完整）
                c.title = it.title
            if it.published_at and (not c.latest_at or it.published_at > c.latest_at):
                c.latest_at = it.published_at
            if it.published_at and (not c.first_seen_at or it.published_at < c.first_seen_at):
                c.first_seen_at = it.published_at
        else:
            clusters.append(EventCluster(
                cluster_id=f"evt-{uuid.uuid4().hex[:8]}",
                title=it.title, member_ids=[it.news_id], centroid=vec,
                created_at=now, module=it.module,
                sources=[it.source] if it.source else [],
                first_seen_at=it.published_at, latest_at=it.published_at,
            ))

    logger.info(f"[process] 聚类完成: {len(news_items)} 条 → {len(clusters)} 簇（阈值 {threshold}）")
    return clusters


# ===== 热度评分 =====
def _heat_score(cluster: EventCluster, time_window_hours: int) -> float:
    """单簇热度 = 基础分 + 讨论量 + 来源多样性 + 时效奖励（封顶 100）"""
    n = len(cluster.member_ids)
    n_sources = len(cluster.sources)
    score = 30.0
    score += min(n, 30) * 1.5           # 讨论量（关联资讯数）
    score += min(n_sources, 8) * 4.0    # 来源多样性（最多记 8 个源）
    if cluster.latest_at:
        try:
            age_h = (datetime.now() - datetime.fromisoformat(cluster.latest_at)).total_seconds() / 3600.0
            if age_h <= 1:
                score += 20
            elif age_h <= 6:
                score += 12
            elif age_h <= 24:
                score += 6
            elif age_h <= 72:
                score += 2
        except Exception:
            pass  # 时间格式异常就当作无时效信息，不加分
    return min(round(score, 1), 100.0)


async def score_heat(
    clusters: List[EventCluster],
    time_window_hours: int = 24,
) -> List[HotEvent]:
    """热度评分（来源数 × 时效衰减 × 讨论量 × 权重）并排序

    Args:
        clusters: 事件簇列表。
        time_window_hours: 热度回溯窗口（小时），默认 24；超出窗口的簇不再算热点。

    Returns:
        List[HotEvent]: 带 heat_score 的热点事件列表（按热度降序）。
    """
    now = datetime.now()
    hot_events: List[HotEvent] = []
    for c in clusters:
        # 超窗过滤：最近更新时间早于窗口的簇淘汰（时间未知的保留，避免误删）
        if c.latest_at:
            try:
                latest = datetime.fromisoformat(c.latest_at)
                if (now - latest).total_seconds() > time_window_hours * 3600:
                    logger.info(f"[process] 簇 {c.cluster_id} 超出 {time_window_hours}h 窗口，跳过")
                    continue
            except Exception:
                pass
        hot_events.append(HotEvent(
            event_id=c.cluster_id, module=c.module, title=c.title,
            heat_score=_heat_score(c, time_window_hours),
            credibility="待核验", sources=list(c.sources),
            article_count=len(c.member_ids),
            first_seen_at=c.first_seen_at, latest_at=c.latest_at,
        ))
    hot_events.sort(key=lambda e: e.heat_score, reverse=True)
    logger.info(f"[process] 热度评分完成: {len(hot_events)} 个热点")
    return hot_events


# ===== 多源交叉验证 =====
def _heuristic_credibility(event: HotEvent) -> str:
    """启发式核验（LLM 降级用）：来源数 × 关联资讯数阈值"""
    if event.article_count >= 3 and len(event.sources) >= 3:
        return "可信"
    if event.article_count >= 2:
        return "存疑"
    return "证据不足"


async def verify_events(
    events: List[HotEvent],
) -> List[HotEvent]:
    """多源交叉验证：LLM 判断可信度（verify_prompt），失败降级为启发式规则

    Args:
        events: 待核验的热点事件列表。

    Returns:
        List[HotEvent]: 回填 credibility 字段（可信 / 存疑 / 证据不足）。
    """
    if not events:
        return events

    verdict_map: Dict[str, str] = {}
    try:
        payload = json.dumps(
            [{"event_id": e.event_id, "title": e.title,
              "sources": e.sources, "article_count": e.article_count}
             for e in events],
            ensure_ascii=False)
        chain = HotnewsFeedPrompts.verify_prompt() | llm
        resp = await asyncio.to_thread(
            lambda: chain.invoke({"events": payload}).content.strip())
        resp = re.sub(r"^```json\s*|\s*```$", "", resp).strip()   # 去掉 markdown 代码块
        verdict_map = {v.get("event_id"): v.get("credibility")
                       for v in json.loads(resp) if isinstance(v, dict)}
        logger.info(f"[process] LLM 核验完成: {verdict_map}")
    except Exception as exc:
        logger.warning(f"[process] LLM 核验失败，降级为启发式规则: {exc}")

    for e in events:
        e.credibility = verdict_map.get(e.event_id) or _heuristic_credibility(e)
        logger.info(f"[process] 事件 {e.event_id} 可信度: {e.credibility}")
    return events
