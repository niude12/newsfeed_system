# -*- coding: utf-8 -*-
"""数据加工类工具（tools/process.py）

事件聚类 · 热度评分 · 交叉验证
对应 MCP Server：mcp_process_server（:8005，挂载 PROCESS_TOOLS）

该模块消费 tools/collect.py 采集到的原始资讯（NewsItem 列表），依次完成
「去重聚类 → 热度评分 → 可信度核验」三件事，产出热点事件（HotEvent）列表。

模块依赖:
- ``ChatOpenAI``                     : langchain_openai，配置来自 Config.llm（qwen-plus · dashscope 兼容）。
- ``Config.embedding_model``         : config.ini [embedding] 的模型名（默认 text-embedding-v3），
                                      复用 [llm] 的 base_url/api_key 调 dashscope 兼容 /embeddings。
- ``ToolPrompts.verify_prompt()`` : 加工工具使用的事件核验模板。
- ``NewsItem`` / ``HotEvent``        : task_pipelines/schemas.py 的入参 / 输出模型。
- ``EventCluster``                   : 本模块定义的事件簇 dataclass（聚类中间产物，写入事件聚类库 / pgvector）。

实现原则：
  1. 真实为主：
     - 聚类：调 dashscope 兼容接口的 /embeddings（模型见 config.ini [embedding]，默认
       text-embedding-v3，复用 [llm] 的 base_url/api_key）把标题向量化，余弦相似度贪婪聚类；
     - 核验：LLM（qwen-plus）按 tool_prompts.py 判断每条事件可信度。
  2. 优雅降级：
     - embedding 接口失败 → 降级为「TF 词频向量 + 余弦」继续聚类；
     - LLM 核验失败 → 降级为启发式规则（来源数 / 关联资讯数阈值）。

典型调用链::

    registry.PROCESS_TOOLS
        ->  cluster_events(news_items, threshold)   # 聚类 → List[EventCluster]
        ->  score_heat(clusters, time_window_hours) # 热度评分 → List[HotEvent]
        ->  verify_events(events)                   # 可信度核验 → 回填 credibility

对外暴露的接口：
- ``cluster_events`` : 去重与事件聚类，返回事件簇列表。
- ``score_heat``     : 计算事件簇热度并排序，返回热点事件列表。
- ``verify_events``  : 对热点事件做多源交叉验证，回填可信度结论。
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
from prompt.tool_prompts import ToolPrompts
from task_pipelines.schemas import HotEvent, NewsItem

# ===== 全局配置与 LLM =====
conf = Config()
# ChatOpenAI：langchain_openai 的 OpenAI 兼容客户端，配置复用 [llm] 段，
# 供 verify_events 里的事件核验（verify_prompt | llm）使用。
llm = ChatOpenAI(
    model=conf.llm["model_name"],
    base_url=conf.llm["base_url"],
    api_key=conf.llm["api_key"],
    temperature=conf.temperature,
)


@dataclass
class EventCluster:
    """事件簇（事件聚类结果，写入事件聚类库 / pgvector）。

    一个事件簇代表「同一事件的多条相关资讯的聚合」：member_ids 记录关联资讯，
    centroid 是簇质心向量（Embedding 或降级时的 TF 向量），用于后续相似度比较。

    参数:
        cluster_id:    事件簇 ID（格式 "evt-<uuid8>"）。
        title:         事件簇代表标题（取成员中最长的标题，信息更完整）。
        member_ids:    关联资讯 ID 列表。
        centroid:      质心向量（Embedding / 降级时 TF 向量），维度与成员向量一致。
        created_at:    创建时间（ISO 8601）。
        module:        所属模块（聚类时从成员带入）。
        sources:       关联来源（去重后的列表）。
        first_seen_at: 首次出现时间（ISO 8601，取成员最早发布时间）。
        latest_at:     最近更新时间（ISO 8601，取成员最晚发布时间）。
    """
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
    """批量向量化（调 /embeddings）；失败返回 None，由上层降级为 TF 词频向量。

    参数:
        texts: 要向量化的文本列表（这里是每条资讯的“标题 + 摘要”）。

    返回:
        Optional[List[List[float]]]：每个输入文本一个向量；调用失败返回 None，
        由调用方（cluster_events）决定降级为 TF 词频向量。

    说明:
        - 延迟 import ``openai``：只有真正向量化时才引入，避免模块顶层硬依赖 openai 包。
        - ``OpenAI(base_url=..., api_key=...)`` 指向 dashscope 兼容接口（复用 [llm] 配置）。
        - ``client.embeddings.create(model=conf.embedding_model, input=texts)`` 批量向量化；
          ``sorted(resp.data, key=lambda d: d.index)`` 按输入顺序排序（返回顺序不保证与输入一致）。
    """
    # 用 try/except 包裹：embedding 接口任何失败都不抛给上层，而是降级处理。
    try:
        # 延迟 import openai：只有真正向量化时才引入，避免模块顶层硬依赖 openai 包。
        from openai import OpenAI
        # 用 [llm] 的 base_url/api_key 连 dashscope 兼容的 /embeddings 接口。
        client = OpenAI(base_url=conf.llm["base_url"], api_key=conf.llm["api_key"])
        # 批量向量化：model 取 [embedding] 段配置的模型名。
        resp = client.embeddings.create(model=conf.embedding_model, input=texts)
        data = sorted(resp.data, key=lambda d: d.index)   # 按输入顺序排好
        return [d.embedding for d in data]
    except Exception as exc:
        logger.warning(f"[process] embedding 调用失败，降级为 TF 词频向量: {exc}")
        return None


def _normalize(text: str) -> str:
    """压缩空白，避免分词时被换行/空格干扰。

    参数:
        text: 原始文本（标题 + 摘要）。

    返回:
        str：去除首尾空白、把连续空白压缩成单个空格后的文本。

    说明:
        ``re.sub(r"\\s+", " ", ...)`` 把换行 / Tab / 多个空格统一替换为单个空格，
        保证后续 `_tokenize` 的英文单词边界判断稳定。
    """
    # \s+ 匹配任意连续空白（换行/Tab/多空格），统一替换成单个空格，避免干扰后续分词。
    return re.sub(r"\s+", " ", (text or "").strip())


def _tokenize(text: str) -> set:
    """中文分词降级用：英文单词 + 中文字符 + 中文二元组（对新闻标题效果足够）。

    参数:
        text: 要分词的文本。

    返回:
        set：去重后的 token 集合（英文单词 / 数字 + 单个中文字符 + 相邻中文二元组）。

    说明:
        - ``re.findall(r"[a-zA-Z0-9]+", text.lower())`` 切出英文单词与数字（小写统一）；
        - ``re.findall(r"[一-龥]", text)`` 逐字提取中文字符（Unicode CJK 基本区范围）；
        - ``zip(zh, zh[1:])`` 生成相邻字符对，即中文二元组，增强短标题的区分度。
    """
    # 第一步：用正则切出英文单词/数字（已小写统一），放入集合天然去重。
    tokens = set(re.findall(r"[a-zA-Z0-9]+", text.lower()))
    # 第二步：逐字提取中文字符（Unicode CJK 基本区 一～龥）。
    zh = re.findall(r"[一-龥]", text)
    # 把单个中文字符也作为 token 加入。
    tokens.update(zh)
    # 第三步：zip(zh, zh[1:]) 生成相邻字符对，即中文二元组，增强短标题的区分度。
    tokens.update("".join(pair) for pair in zip(zh, zh[1:]))   # 二元组
    return tokens


def _tf_vectors(texts: List[str]) -> List[List[float]]:
    """TF 词频向量（embedding 降级用）：全量词表 + 归一化，维度 = 词表大小。

    参数:
        texts: 文本列表（与 _embed_texts 的入参一致）。

    返回:
        List[List[float]]：每个文本一个 TF 向量，已按 L2 范数归一化。

    说明:
        先扫全部文本建词表（vocab: token → 索引），再对每个文本统计词频生成向量；
        ``norm = sum(x*x for x in v) ** 0.5`` 是 L2 范数，除之即归一化，方便后续余弦相似度。
    """
    # 先对每条文本分词，得到 token 集合列表（后面建词表和统计都复用它）。
    token_sets = [_tokenize(t) for t in texts]
    # 词表：token → 索引，维度 = 全量 token 种类数。
    vocab: Dict[str, int] = {}
    # 第一遍扫描全部 token 集合，给每个 token 分配唯一索引。
    for tokens in token_sets:
        for t in tokens:
            if t not in vocab:
                vocab[t] = len(vocab)
    # 结果向量列表。
    vecs = []
    # 第二遍：为每条文本统计词频生成向量。
    for tokens in token_sets:
        v = [0.0] * len(vocab)
        for t in tokens:
            v[vocab[t]] += 1.0
        norm = sum(x * x for x in v) ** 0.5
        vecs.append([x / norm if norm else 0.0 for x in v])
    return vecs


def _cosine(a: List[float], b: List[float]) -> float:
    """余弦相似度；维度不一致或零向量返回 0。

    参数:
        a: 向量 a。
        b: 向量 b。

    返回:
        float：两向量夹角余弦值（-1~1）；维度不同或任一为零向量时返回 0.0。

    说明:
        ``sum(x*y for x, y in zip(a, b))`` 是点积，``sum(x*x for x in a) ** 0.5`` 是 L2 模长，
        点积除以两模长即余弦相似度；分子分母任一为 0 说明存在零向量，直接返回 0。
    """
    # 任一为空或维度不一致，无法计算，直接返回 0（不抛异常）。
    if not a or not b or len(a) != len(b):
        return 0.0
    # 点积：对应位置相乘再求和。
    dot = sum(x * y for x, y in zip(a, b))
    # a 的 L2 模长。
    na = sum(x * x for x in a) ** 0.5
    # b 的 L2 模长。
    nb = sum(x * x for x in b) ** 0.5
    # 余弦 = 点积 / (模长之积)；任一模长为 0 说明是零向量，返回 0。
    return dot / (na * nb) if na and nb else 0.0


def _merge_uniq(lst: List[str], values: List[str]) -> List[str]:
    """把 values 里非空且不重复的元素并入 lst。

    参数:
        lst:    原列表（会被复制，不原地修改）。
        values: 待并入的元素列表。

    返回:
        List[str]：新列表，包含原 lst 全部元素，并按顺序追加 values 中「非空且不在其中」的元素。
    """
    # 复制原列表，避免原地修改调用方持有的列表。
    out = list(lst)
    # 逐个检查待并入的元素。
    for v in values:
        # 空串跳过；已在 out 里的跳过（去重）。
        if v and v not in out:
            out.append(v)
    return out


# ===== 事件聚类 =====
async def cluster_events(
    news_items: List[NewsItem],
    threshold: float = 0.8,
) -> List[EventCluster]:
    """去重与事件聚类（规范化 → Embedding → 余弦相似度贪婪聚类）。

    参数:
        news_items: 原始资讯列表（:class:`~task_pipelines.schemas.NewsItem`，来自 collect_news）。
        threshold:  相似度阈值，默认 0.8，与已有簇质心最高相似度 ≥ 阈值则并入该簇。

    返回:
        List[EventCluster]：事件簇列表，每簇含代表标题 / 关联资讯 ID / 质心 / 来源 / 时间范围。

    抛出:
        （无显式抛出；内部 embedding 失败自动降级为 TF 向量。）

    说明:
        - 单遍贪婪聚类：对每条资讯，与已有每个簇的质心求余弦，取最高相似度；
          高于阈值则并入（更新质心 / 来源 / 时间范围），否则新开一簇。
        - 质心更新为成员向量的均值：``(旧均值 * (n-1) + 新向量) / n``，n 是并入后的成员数。
        - 代表标题取成员中最长的（通常信息更完整）；时间范围取成员的 min/max 发布时间。
    """
    # 空输入直接返回空列表，避免下面空遍历。
    if not news_items:
        return []

    # 每条资讯的文本 = 标题 + 摘要，先压缩空白再向量化。
    texts = [_normalize(it.title + " " + it.summary) for it in news_items]
    # 真实优先：dashscope embedding；失败返回 None → 降级 TF 向量
    vectors = await asyncio.to_thread(_embed_texts, texts)
    if vectors is None:
        vectors = _tf_vectors(texts)

    # 统一的创建时间戳（所有新簇共用同一时刻）。
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    # 聚类结果列表。
    clusters: List[EventCluster] = []
    # 单遍贪婪聚类：逐个资讯与已有簇比较。
    for it, vec in zip(news_items, vectors):
        # 找与这条资讯相似度最高的已有簇。
        best_idx, best_sim = -1, -1.0
        for ci, c in enumerate(clusters):
            sim = _cosine(vec, c.centroid)
            if sim > best_sim:
                best_sim, best_idx = sim, ci
        if best_idx >= 0 and best_sim >= threshold:
            # 命中已有簇：并入成员 / 来源，更新质心、代表标题、时间范围。
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
            # 相似度不够 → 以这条资讯为种子新开一簇。
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
    """单簇热度 = 基础分 + 讨论量 + 来源多样性 + 时效奖励（封顶 100）。

    参数:
        cluster:          事件簇（含关联资讯数 / 来源 / 最近更新时间）。
        time_window_hours: 时间回溯窗口（小时），用于时效奖励的档位判定。

    返回:
        float：热度分（0~100，四舍五入保留 1 位小数）。

    说明:
        计分规则：
        - 基础分 30；
        - 讨论量：关联资讯数最多加 30 分（每条 1.5 分）；
        - 来源多样性：来源数最多加 32 分（每个源 4 分，最多记 8 个源）；
        - 时效奖励：最近更新距今 ≤1h 加 20，≤6h 加 12，≤24h 加 6，≤72h 加 2，
          时间格式异常（datetime.fromisoformat 抛错）按无时效信息处理，不加分。
    """
    # 关联资讯数（讨论量指标）。
    n = len(cluster.member_ids)
    # 去重后的来源数（多样性指标）。
    n_sources = len(cluster.sources)
    # 基础分。
    score = 30.0
    score += min(n, 30) * 1.5           # 讨论量（关联资讯数）
    score += min(n_sources, 8) * 4.0    # 来源多样性（最多记 8 个源）
    # 有最近更新时间才计算时效奖励。
    if cluster.latest_at:
        try:
            # 距今小时数 = (现在 - 最近更新时间) 的秒数 / 3600。
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
    # 封顶 100，防止分数过度膨胀。
    return min(round(score, 1), 100.0)


async def score_heat(
    clusters: List[EventCluster],
    time_window_hours: int = 24,
) -> List[HotEvent]:
    """热度评分（来源数 × 时效衰减 × 讨论量 × 权重）并排序。

    参数:
        clusters:          事件簇列表（cluster_events 的输出）。
        time_window_hours: 热度回溯窗口（小时），默认 24；最近更新时间超出窗口的簇不再算热点。

    返回:
        List[HotEvent]：带 heat_score 的热点事件列表，按热度降序排列。

    说明:
        - 超窗过滤：簇的 latest_at 距今超过 time_window_hours 则淘汰（时间未知的保留，避免误删）。
        - 把事件簇映射为 :class:`~task_pipelines.schemas.HotEvent`，credibility 先置「待核验」，
          由 verify_events 后续回填。
    """
    # 当前时刻，用于计算「距今」并判断是否超窗。
    now = datetime.now()
    # 输出的热点事件列表。
    hot_events: List[HotEvent] = []
    # 逐个事件簇处理。
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
        # 簇 → HotEvent：credibility 先置「待核验」，由 verify_events 后续回填。
        hot_events.append(HotEvent(
            event_id=c.cluster_id, module=c.module, title=c.title,
            heat_score=_heat_score(c, time_window_hours),
            credibility="待核验", sources=list(c.sources),
            article_count=len(c.member_ids),
            first_seen_at=c.first_seen_at, latest_at=c.latest_at,
        ))
    # 按热度降序排序，热点排前面。
    hot_events.sort(key=lambda e: e.heat_score, reverse=True)
    logger.info(f"[process] 热度评分完成: {len(hot_events)} 个热点")
    return hot_events


# ===== 多源交叉验证 =====
def _heuristic_credibility(event: HotEvent) -> str:
    """启发式核验（LLM 降级用）：来源数 × 关联资讯数阈值。

    参数:
        event: 待判断的热点事件。

    返回:
        str：核验结论，取值为「可信 / 存疑 / 证据不足」。

    说明:
        规则：关联资讯 ≥3 且来源 ≥3 → 「可信」；关联资讯 ≥2 → 「存疑」；
        其余 → 「证据不足」。仅当 LLM 核验失败时作为降级路径被调用。
    """
    # 关联资讯 ≥3 且来源 ≥3：多源佐证充分，判为「可信」。
    if event.article_count >= 3 and len(event.sources) >= 3:
        return "可信"
    # 有 ≥2 条资讯但来源不足：有一定佐证但不够充分，判「存疑」。
    if event.article_count >= 2:
        return "存疑"
    # 其余情况证据过少，判「证据不足」。
    return "证据不足"


async def verify_events(
    events: List[HotEvent],
) -> List[HotEvent]:
    """多源交叉验证：LLM 判断可信度（verify_prompt），失败降级为启发式规则。

    参数:
        events: 待核验的热点事件列表（score_heat 的输出）。

    返回:
        List[HotEvent]：回填 credibility 字段（可信 / 存疑 / 证据不足）后的同一列表。

    说明:
        - ``HotnewsFeedPrompts.verify_prompt()`` 返回事件核验模板，与全局 ``llm`` 拼成 LangChain 链，
          把事件列表 JSON 串喂进去，让 LLM 对每条事件输出可信度。
        - LLM 输出可能是带 ```json``` 围栏的 Markdown，用 ``re.sub`` 剥掉围栏再 ``json.loads``。
        - 任一环节失败（网络 / 解析 / 非 JSON）→ 整批降级：逐条调用 ``_heuristic_credibility``。
        - 逐条回填时优先用 LLM 结论，缺失的（LLM 没给该 event_id）用启发式兜底。
    """
    # 空列表直接返回，避免发起空 LLM 调用。
    if not events:
        return events

    # event_id → credibility 的映射（LLM 给出的核验结论）。
    verdict_map: Dict[str, str] = {}
    try:
        # 构造事件摘要列表并序列化为 JSON，作为 LLM 核验模板的输入。
        payload = json.dumps(
            [{"event_id": e.event_id, "title": e.title,
              "sources": e.sources, "article_count": e.article_count}
             for e in events],
            ensure_ascii=False)
        # verify_prompt 模板 | llm 拼成 LangChain 链。
        chain = ToolPrompts.verify_prompt() | llm
        # asyncio.to_thread：LLM 调用是阻塞式的，丢到线程池避免阻塞事件循环。
        resp = await asyncio.to_thread(
            lambda: chain.invoke({"events": payload}).content.strip())
        resp = re.sub(r"^```json\s*|\s*```$", "", resp).strip()   # 去掉 markdown 代码块
        # 解析 LLM 返回的 JSON 数组，构造成 event_id → credibility 的映射。
        verdict_map = {v.get("event_id"): v.get("credibility")
                       for v in json.loads(resp) if isinstance(v, dict)}
        logger.info(f"[process] LLM 核验完成: {verdict_map}")
    except Exception as exc:
        logger.warning(f"[process] LLM 核验失败，降级为启发式规则: {exc}")

    # 逐条回填可信度：LLM 没给结论的事件用启发式规则兜底。
    for e in events:
        e.credibility = verdict_map.get(e.event_id) or _heuristic_credibility(e)
        logger.info(f"[process] 事件 {e.event_id} 可信度: {e.credibility}")
    return events
