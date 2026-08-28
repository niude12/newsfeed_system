# -*- coding: utf-8 -*-
"""数据采集类工具（tools/collect.py）

多源资讯采集 · 账户发布监控
对应 MCP Server：mcp_collect_server（:8004，挂载 COLLECT_TOOLS）

实现原则：
  1. 真实为主：RSS / Hacker News / 关键词搜索源（Google News · Bing News）都直接联网抓真实数据，
     模块→RSS 源可用 config.ini [rss_sources] 覆盖，缺省用内置 DEFAULT_RSS_FEEDS。
  2. 结果相关性优先：
     - 指定关键词时严格匹配，不用模块全量新闻冒充查询结果；
     - 综合新闻源按模块主题词过滤，模块专用 RSS 可直接保留；
     - 中文主题自动扩展常见英文同义词（足球 → football / soccer）。
  3. 优雅降级：
     - 单个源抓取失败 → 跳过该源并记 warning（不中断其它源）；
     - 账户监控未配置抓取地址（缺配置）→ 返回【模拟】样例数据并打日志；
     - 全部源都无数据 → 返回【模拟】样例数据，保证下游流水线可演示。
  4. 不新增第三方依赖：HTTP 用 urllib（标准库），RSS/Atom 用 xml.etree 手写解析，
     缺任何包都不会 ImportError。
"""

import asyncio
import email.utils
import hashlib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional
from xml.etree import ElementTree

from config import Config
from create_logger import logger
from task_pipelines.schemas import AccountPost, NewsItem

conf = Config()

# ===== 请求头与超时（部分站点会拦截默认 UA，超时防止拖死整条流水线）=====
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
_HTTP_TIMEOUT = 10

# ===== 内置 RSS 默认源（config.ini [rss_sources] 按模块覆盖）=====
# 以下源均实测可用（2026-08-27 批量联网验证）；通用/英文/失效源已移除。
# 注意：search:xxx 是「关键词搜索源」占位 → 走 _fetch_news_search（Google News / Bing News RSS，
# 免 key、中英文皆可、按关键词实时返回）——国内财经/体育/娱乐分类 RSS 源批量实测全部失效（404/拒连）。
DEFAULT_RSS_FEEDS: Dict[str, List[str]] = {
    "科技": [
        "https://www.ithome.com/rss/",            # IT之家
        "https://www.36kr.com/feed",              # 36氪
        "https://www.solidot.org/index.rss",      # Solidot
        "https://www.qbitai.com/feed",            # 量子位
        "https://sspai.com/feed",                 # 少数派
    ],
    "财经": [
        "search:财经",
    ],
    "体育": [
        "search:足球",
        "search:体育",
    ],
    "娱乐": [
        "search:娱乐",
    ],
    "国际": [
        "https://www.rfi.fr/cn/rss",              # RFI 中文（国际新闻，实测可用）
        "search:国际",
    ],
}

# Hacker News 官方 Firebase API（免 key）
HN_TOP_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

# 默认采集源组合（sources=None 时用）。vvhan 热榜接口已失效（SSL EOF），已从代码中移除。
DEFAULT_SOURCES = ["rss", "hn"]

# 综合来源本身没有模块分类，必须通过 MODULE_TERMS 做相关性过滤。
# 中新网滚动是通用新闻源：即便模块配置里不再默认使用，也保留在通用源集合里防绕过模块过滤。
# （config.ini 若配置了其它通用 feed，也把它加进 GENERAL_FEEDS。）
GENERAL_FEEDS = {"https://www.chinanews.com.cn/rss/scroll-news.xml"}
GENERAL_SOURCE_NAMES = {"Hacker News", "Google News", "Bing News"}

# 模块主题词用于过滤综合 RSS / HN / 热榜；模块专用 RSS 不需要标题必须包含模块名。
MODULE_TERMS: Dict[str, List[str]] = {
    "科技": ["科技", "人工智能", "ai", "芯片", "互联网", "软件", "硬件", "机器人",
             "手机", "数码", "算法", "云计算", "自动驾驶", "量子"],
    "财经": ["财经", "经济", "金融", "股票", "股市", "基金", "银行", "投资", "证券",
             "人民币", "汇率", "楼市", "企业", "贸易", "关税", "gdp"],
    "体育": ["体育", "足球", "篮球", "英超", "欧冠", "中超", "国足", "世界杯", "奥运",
             "比赛", "赛事", "球队", "球员", "冠军", "田径", "网球", "football", "soccer",
             "basketball", "fifa", "premier league", "champions league", "nba", "cba"],
    "娱乐": ["娱乐", "电影", "影视", "明星", "综艺", "音乐", "演员", "电视剧", "票房",
             "演唱会", "导演", "歌手"],
    "国际": ["国际", "全球", "世界", "美国", "欧洲", "日本", "俄罗斯", "乌克兰", "俄乌",
             "外交", "战争", "冲突", "international", "world", "russia", "ukraine"],
}

# 用户关键词的跨语言/同义表达。未配置的关键词保持原样。
# 金融/影视等常见话题补同义表达，提高搜索源结果过关键词过滤时的召回（如搜「股票」也能命中「A股」「证券」标题）。
KEYWORD_ALIASES: Dict[str, List[str]] = {
    "足球": ["足球", "football", "soccer", "fifa", "英超", "欧冠", "世界杯", "中超", "国足",
             "premier league", "champions league"],
    "篮球": ["篮球", "basketball", "nba", "cba"],
    "俄乌": ["俄乌", "俄罗斯", "乌克兰", "russia", "russian", "ukraine", "ukrainian"],
    "人工智能": ["人工智能", "ai", "artificial intelligence"],
    "股票": ["股票", "股市", "a股", "证券", "沪指"],
    "基金": ["基金", "etf", "公募", "私募"],
    "电影": ["电影", "影片", "影视"],
    "芯片": ["芯片", "半导体"],
}


# ===== 基础工具函数 =====
def _now_iso() -> str:
    """当前时间（本地，ISO 8601，与项目其它 mock 时间格式保持一致）"""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _clean_text(value) -> str:
    """清洗外部源文本，保证是干净的 UTF-8 str。

    部分资讯源（尤其国内热榜接口）偶尔返回错标编码 / 混入非法字节，
    直接进 NewsItem 会让 MCP 回传序列化 utf-8 失败、整单报错（优雅降级原则）。
    这里把孤立代理项 / 无法解码的字节统一替换成 �（U+FFFD）：
    宁可标题带一个替换符，也不让坏字节炸掉整条 MCP 调用。
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    # 某些 RSS 标题夹带 BOM（U+FEFF），Windows GBK 控制台无法编码它。
    value = value.replace("\ufeff", "")
    return value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def _fetch(url: str, timeout: int = _HTTP_TIMEOUT) -> bytes:
    """同步抓取 URL 内容（urllib，带 UA + 超时）；失败会抛异常由调用方处理"""
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _parse_date(value: Optional[str]) -> str:
    """把 RSS 的各种时间格式统一成项目内 ISO 字符串（本地时区）；解析失败返回空串"""
    if not value:
        return ""
    try:
        # RFC 822 格式（pubDate，如 'Tue, 26 Aug 2026 12:00:00 GMT'）
        dt = email.utils.parsedate_to_datetime(value)
        if dt is not None:
            return dt.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass
    try:
        # ISO 8601 格式（Atom 的 updated，取前 19 位 'YYYY-MM-DDTHH:MM:SS'）
        return datetime.fromisoformat(value.strip()[:19]).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""


def _local_name(tag: str) -> str:
    """去掉 XML 标签的命名空间前缀（<{ns}title> → title），方便同时处理 RSS/Atom"""
    return tag.rsplit("}", 1)[-1]


def _first_field(fields: dict, *names: str):
    """返回第一个存在的 XML 字段；不使用 Element 的真假值，避免空元素被误判。"""
    for name in names:
        element = fields.get(name)
        if element is not None:
            return element
    return None


def _expand_terms(terms: Optional[List[str]]) -> List[str]:
    """展开同义词并去重，统一转成小写。"""
    expanded: List[str] = []
    for term in terms or []:
        for alias in KEYWORD_ALIASES.get(term.lower(), [term]):
            alias = alias.strip().lower()
            if alias and alias not in expanded:
                expanded.append(alias)
    return expanded


def _matches(item: NewsItem, terms: List[str]) -> bool:
    """判断新闻标题或摘要是否包含任一主题词。"""
    text = f"{item.title} {item.summary}".lower()
    return any(term in text for term in terms)


def _is_general_source(item: NewsItem) -> bool:
    """综合来源没有可靠模块分类，需要额外做模块相关性过滤。"""
    return item.source in GENERAL_FEEDS or item.source in GENERAL_SOURCE_NAMES


def _parse_feed(xml_bytes: bytes, module: str, source: str) -> List[NewsItem]:
    """解析 RSS 2.0 / Atom 源，返回 NewsItem 列表（纯标准库，免 feedparser）

    - RSS 2.0：<rss><channel><item>...</item>；item 里 title/link/pubDate/description
    - Atom  ：<feed><entry>...</entry>；entry 里 title/link(href)/updated/summary
    """
    items: List[NewsItem] = []
    root = ElementTree.fromstring(xml_bytes)  # XML 不合法会抛异常，由调用方跳过该源
    entries: List[ElementTree.Element] = []
    for child in root:
        if _local_name(child.tag) == "channel":
            entries = [c for c in child if _local_name(c.tag) == "item"]
            break
        if _local_name(child.tag) == "feed":
            entries = [c for c in child if _local_name(c.tag) == "entry"]
            break

    for entry in entries:
        fields = {_local_name(c.tag): c for c in entry}
        # 标题（RSS/Atom 都有 <title>）
        title_el = fields.get("title")
        title = _clean_text(title_el.text if title_el is not None else "").strip()
        if not title:
            continue
        # 链接：RSS 是 <link>文本</link>；Atom 是 <link href="..."/>
        link_el = fields.get("link")
        url = ""
        if link_el is not None:
            url = _clean_text((link_el.get("href") or "").strip() or (link_el.text or "")).strip()
        # 摘要：RSS description / Atom summary / content
        desc_el = _first_field(fields, "description", "summary", "content")
        summary = _clean_text(re.sub(r"\s+", " ", (desc_el.text or "").strip())[:200]) if desc_el is not None else ""
        # 时间：RSS pubDate / Atom updated / dc:date
        time_el = _first_field(fields, "pubDate", "published", "updated", "date")
        published = _parse_date(time_el.text if time_el is not None else None)

        news_id = f"rss-{hashlib.md5((url or title).encode('utf-8')).hexdigest()[:8]}"
        items.append(NewsItem(
            news_id=news_id, module=module, title=title, source=source,
            # 时间未知就保留空值，不能伪装成“刚刚发布”并获得虚假时效分。
            published_at=published, url=url, summary=summary,
        ))
    return items


# ===== 各采集源实现（真实抓取）=====
async def _fetch_rss(module: str, since: Optional[str], limit: int) -> List[NewsItem]:
    """按模块抓 RSS 源：config.ini [rss_sources] 优先，缺省用内置 DEFAULT_RSS_FEEDS。
    URL 以 search: 开头 → 走关键词搜索源（_fetch_news_search，国内分类 RSS 失效的替代）。
    （异步：网络抓取放线程池，保证与其它源在 asyncio.gather 里并发）"""
    feeds = conf.rss_sources.get(module) or DEFAULT_RSS_FEEDS.get(module, [])
    items: List[NewsItem] = []
    for url in feeds:
        try:
            if url.startswith("search:"):
                query = url[len("search:"):].strip()
                got = await _fetch_news_search([query], limit, module)
                logger.info(f"[collect] RSS 搜索源「{query}」抓到 {len(got)} 条")
            else:
                got = _parse_feed(await asyncio.to_thread(_fetch, url), module, url)
                logger.info(f"[collect] RSS 源 {url} 抓到 {len(got)} 条")
            items.extend(got)
        except Exception as exc:
            logger.warning(f"[collect] RSS 源 {url} 抓取失败，跳过: {exc}")
    return items


async def _fetch_hn(module: str, since: Optional[str], limit: int) -> List[NewsItem]:
    """Hacker News Top 榜单（官方 Firebase API，免 key，条目并发抓取）"""
    try:
        ids = json.loads(await asyncio.to_thread(_fetch, HN_TOP_URL))[:limit]
    except Exception as exc:
        logger.warning(f"[collect] HN 榜单抓取失败，跳过: {exc}")
        return []
    results = await asyncio.gather(
        *[asyncio.to_thread(_fetch, HN_ITEM_URL.format(sid)) for sid in ids],
        return_exceptions=True,
    )
    items: List[NewsItem] = []
    for sid, raw in zip(ids, results):
        if isinstance(raw, Exception):
            logger.warning(f"[collect] HN 条目 {sid} 抓取失败: {raw}")
            continue
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
            if not data or data.get("type") != "story" or not data.get("title"):
                continue
            published = ""
            if data.get("time"):
                published = datetime.fromtimestamp(data["time"]).strftime("%Y-%m-%dT%H:%M:%S")
            items.append(NewsItem(
                news_id=f"hn-{sid}", module=module, title=_clean_text(data["title"]),
                source="Hacker News",
                published_at=published or _now_iso(),
                url=_clean_text(data.get("url") or f"https://news.ycombinator.com/item?id={sid}"),
                summary=_clean_text(f"HN 得分 {data.get('score', 0)} · 作者 {data.get('by', '')}"),
            ))
        except Exception as exc:
            logger.warning(f"[collect] HN 条目 {sid} 抓取失败: {exc}")
    return items


# ===== 关键词搜索源（Google News / Bing News RSS，免 key，按关键词实时返回相关新闻）=====
NEWS_SEARCH_APIS = [
    "https://news.google.com/rss/search?q={q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
    "https://www.bing.com/news/search?q={q}&format=rss&setlang=zh-cn",
]


async def _fetch_news_search(keywords: List[str], limit: int, module: str) -> List[NewsItem]:
    """关键词搜索源：Google News / Bing News 的 RSS 搜索接口。

    国内分类 RSS 源大多失效，关键词查询用搜索源保证结果与查询相关（中英文皆可）。
    Google News 标题尾部带「 - 来源」后缀，解析后剥掉；失败自动换下一个 API。
    """
    query = " ".join(keywords)
    for api in NEWS_SEARCH_APIS:
        source = "Google News" if "google" in api else "Bing News"
        try:
            got = _parse_feed(await asyncio.to_thread(_fetch, api.format(q=urllib.parse.quote(query))),
                              module, source)
            if not got:
                continue
            if source == "Google News":
                for it in got:
                    it.title = re.sub(r"\s*-\s*[^\s-]+(?:\s+[^\s-]+)?$", "", it.title).strip() or it.title
            logger.info(f"[collect] 搜索源 {source} 关键词「{query}」返回 {len(got)} 条")
            return got[:limit]
        except Exception as exc:
            logger.warning(f"[collect] 搜索源 {source} 抓取失败，尝试下一个: {exc}")
    return []


# 源 → 处理函数注册表（统一签名：module, since, limit）
SOURCE_HANDLERS = {
    "rss": _fetch_rss,
    "hn": _fetch_hn,
}


def _mock_news(module: str, count: int = 3) -> List[NewsItem]:
    """【模拟】兜底样例：所有真实源都不可用时的降级产物，标题带标记方便识别"""
    return [
        NewsItem(news_id=f"mock-{i}", module=module,
                 title=f"【模拟】{module}资讯样例{i}（真实源均不可用）",
                 source="模拟源", published_at=_now_iso(), url="",
                 summary="模拟数据：所有真实资讯源抓取失败时兜底，保证链路可演示。")
        for i in range(count)
    ]


# ===== 对外工具函数 =====
async def _fetch_sources(module: str, since: Optional[str],
                         limit: int, sources: List[str]) -> List[NewsItem]:
    """并发抓取来源；单个来源失败只跳过该来源，不丢弃其他成功结果。"""
    coros = []
    for src in sources:
        handler = SOURCE_HANDLERS.get(src)
        if handler is None:
            logger.warning(f"[collect] 未知采集源 {src}，跳过")
            continue
        coros.append(handler(module, since, limit))
    if not coros:
        return []
    results = await asyncio.gather(*coros, return_exceptions=True)
    items: List[NewsItem] = []
    for result in results:
        if isinstance(result, Exception):
            logger.warning(f"[collect] 单个来源失败，跳过: {result}")
            continue
        items.extend(result)
    return items


async def collect_news(
    module: str,
    keywords: Optional[List[str]] = None,
    sources: Optional[List[str]] = None,
    since: Optional[str] = None,
    limit: int = 50,
) -> List[NewsItem]:
    """多源资讯采集（RSS · Hacker News · 关键词搜索源）

    Args:
        module: 新闻模块，如 "科技" / "财经" / "体育"。
        keywords: 附加关键词过滤，None 表示不限。
        sources: 指定采集源，None 使用默认源列表（rss/hn）。
        since: 只采集该时间点之后的资讯（ISO 8601）。
        limit: 单次采集上限，默认 50。

    Returns:
        List[NewsItem]: 原始资讯列表（写入原始资讯库）。
    """
    sources = list(sources or DEFAULT_SOURCES)

    # 各源并发抓取（顶层兜底：任何意外异常都不得中断整条链路 → 降级 mock）
    items = await _fetch_sources(module, since, limit, sources)

    # 关键词查询增强：模块源（尤其 search:模块名 这类宽词源，如财经源搜「财经」）可能不含
    # 窄话题（如「股票」「基金」），若只搜模块名再过滤关键词，过滤后容易空手而归。
    # 这里直接用关键词再搜一次搜索源（Google News / Bing News，按关键词实时返回相关新闻），
    # 保证窄话题也能命中；结果继续走下方关键词过滤 + 去重，不会引入无关数据。
    if keywords:
        try:
            kw_items = await _fetch_news_search(list(keywords), limit, module)
            if kw_items:
                logger.info(f"[collect] 关键词「{keywords}」直接搜索源补采 {len(kw_items)} 条")
                items.extend(kw_items)
        except Exception as exc:
            logger.warning(f"[collect] 关键词「{keywords}」搜索源补采失败，忽略: {exc}")
    raw_count = len(items)

    # since 时间过滤
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            items = [it for it in items if it.published_at and datetime.fromisoformat(it.published_at) >= since_dt]
        except Exception as exc:
            logger.warning(f"[collect] since 过滤失败，忽略该条件: {exc}")

    # 精确主题查询：中英文同义词扩展后严格过滤；无命中就返回空，不返回无关模块新闻。
    if keywords:
        terms = _expand_terms(keywords)
        items = [item for item in items if _matches(item, terms)]
        if not items:
            logger.info(f"[collect] 关键词 {keywords}（扩展为 {terms}）无匹配结果")
    else:
        # 模块查询：模块专用 RSS 直接保留；综合 RSS/HN/热榜必须命中模块主题词。
        module_terms = [term.lower() for term in MODULE_TERMS.get(module, [])]
        if module_terms:
            items = [item for item in items
                     if not _is_general_source(item) or _matches(item, module_terms)]

    # 去重（标题指纹） + 按时间倒序 + 截断
    seen: set = set()
    uniq: List[NewsItem] = []
    for it in sorted(items, key=lambda x: x.published_at, reverse=True):
        fp = re.sub(r"\W+", "", it.title).lower()[:40]
        if fp in seen:
            continue
        seen.add(fp)
        uniq.append(it)
        if len(uniq) >= limit:
            break

    # 只有“所有真实来源都不可用”才返回模拟数据；过滤无命中必须如实返回空列表。
    if not uniq:
        if raw_count == 0:
            logger.warning("[collect] 所有真实来源均不可用，降级返回【模拟】样例数据")
            uniq = _mock_news(module)
        else:
            logger.info(f"[collect] 已抓取 {raw_count} 条，但没有符合模块/关键词的新闻")

    logger.info(f"[collect] 采集完成: 源={sources} 原始{raw_count} 条 → 筛选去重后 {len(uniq)} 条")
    return uniq


# ===== 账户发布监控 =====
def _resolve_account_feed(account: str, platform: str) -> Optional[str]:
    """解析账户的抓取地址：config.ini [accounts] 按账户名映射 > account 本身是 URL"""
    name = account.strip("@")
    url = conf.accounts.get(name) or conf.accounts.get(account)
    if url:
        return url
    if account.lower().startswith("http"):
        return account
    return None


def _mock_posts(account: str, platform: str, count: int = 3) -> List[AccountPost]:
    """【模拟】兜底样例：账户未配置抓取地址时降级（缺配置 → 样例数据）"""
    return [
        AccountPost(
            post_id=f"mock-{i}", account=account, platform=platform,
            title=f"【模拟】{account} 的新发布{i}",
            content="模拟内容（未配置抓取地址，缺配置降级样例）",
            published_at=_now_iso(), url="",
        )
        for i in range(count)
    ]


async def fetch_account_posts(
    account: str,
    platform: str,
    since: Optional[str] = None,
    limit: int = 50,
) -> List[AccountPost]:
    """账户 / 作品发布监控采集（真实为主：配置了抓取地址就走 RSS 真实抓取；缺配置降级样例）

    Args:
        account: 账户标识（weibo uid / 公众号名 …）。
        platform: 平台，如 "weibo" / "wechat" / "xiaohongshu"。
        since: 只返回该时间点之后的新发布（ISO 8601）。
        limit: 单次拉取上限，默认 50。

    Returns:
        List[AccountPost]: 账户发布内容列表。
    """
    feed_url = _resolve_account_feed(account, platform)
    if not feed_url:
        logger.warning(f"[collect] 账户 {account}@{platform} 未配置抓取地址，降级为【模拟】样例数据")
        return _mock_posts(account, platform)
    if platform.lower() in ("bilibili", "bili", "b站"):
        from tools.bilibili import fetch_bilibili_space_videos
        return await fetch_bilibili_space_videos(
            account=account, space_url=feed_url, since=since, limit=limit,
        )
    try:
        news = _parse_feed(await asyncio.to_thread(_fetch, feed_url), module=platform, source=feed_url)
        posts = [
            AccountPost(
                post_id=f"post-{hashlib.md5((it.title or it.url).encode('utf-8')).hexdigest()[:8]}",
                account=account, platform=platform,
                title=it.title, content=it.summary,
                published_at=it.published_at, url=it.url,
            )
            for it in news[:limit]
        ]
        logger.info(f"[collect] 账户 {account}@{platform} 抓到 {len(posts)} 条")
        if not posts:
            logger.warning(f"[collect] 账户 {account} 抓取地址 {feed_url} 无内容")
        return posts
    except Exception as exc:
        logger.warning(f"[collect] 账户 {account} 抓取失败: {exc}，降级为【模拟】样例数据")
        return _mock_posts(account, platform)
