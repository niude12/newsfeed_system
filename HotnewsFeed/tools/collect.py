# -*- coding: utf-8 -*-
"""数据采集类工具（tools/collect.py）

多源资讯采集 · 账户发布监控
对应 MCP Server：mcp_collect_server（:8004，挂载 COLLECT_TOOLS）

该模块是流水线的最上游：负责从多个渠道抓取「真实」资讯与账户发布内容，
并统一包装成 :class:`~task_pipelines.schemas.NewsItem`（资讯）和
:class:`~task_pipelines.schemas.AccountPost`（账户发布）供下游加工 / 输出消费。

模块依赖:
- ``Config``                    : 全局配置单例。用到 ``rss_sources``（[rss_sources] 段模块→RSS 源映射）、
                                 ``accounts``（[accounts] 段账户→抓取地址映射）属性。
- ``NewsItem`` / ``AccountPost``: task_pipelines/schemas.py 定义的数据模型，本模块的输出类型。
- ``tools.bilibili``            : B 站账户发布采集委托给 fetch_bilibili_space_videos。
- ``create_logger.logger``      : 全局日志器，用于各源抓取成败的 warning / info 记录。

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

典型调用链::

    registry.COLLECT_TOOLS
        ->  collect_news(module, keywords, sources, since, limit)   # 多源资讯采集
        ->    _fetch_sources(module, since, limit, sources)         # 并发抓取各源
        ->      _fetch_rss / _fetch_hn / _fetch_news_search
        ->  fetch_account_posts(account, platform, since, limit)    # 账户发布监控
        ->    _resolve_account_feed -> tools.bilibili.fetch_bilibili_space_videos / _parse_feed

对外暴露的接口：
- ``collect_news``         : 按模块 / 关键词多源采集资讯，返回 NewsItem 列表。
- ``fetch_account_posts``  : 采集指定账户的发布内容，返回 AccountPost 列表。
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
    """当前时间（本地，ISO 8601，与项目其它 mock 时间格式保持一致）。

    返回:
        str：形如 "YYYY-MM-DDTHH:MM:SS" 的本地时间字符串，用作模拟样例的发布时间。
    """
    # datetime.now() 取本地当前时间，strftime 格式化为项目统一的 ISO 8601 字符串。
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _clean_text(value) -> str:
    """清洗外部源文本，保证是干净的 UTF-8 str。

    部分资讯源（尤其国内热榜接口）偶尔返回错标编码 / 混入非法字节，
    直接进 NewsItem 会让 MCP 回传序列化 utf-8 失败、整单报错（优雅降级原则）。
    这里把孤立代理项 / 无法解码的字节统一替换成 �（U+FFFD）：
    宁可标题带一个替换符，也不让坏字节炸掉整条 MCP 调用。

    参数:
        value: 任意来源的原始文本（可能为 None / 非 str）。

    返回:
        str：清洗后的干净 UTF-8 字符串（None 返回空串）。

    说明:
        - ``str.encode("utf-8", errors="replace")`` 把非法字节替换成 U+FFFD，
          再 ``decode("utf-8", errors="replace")"`` 解码回 str，双重保证安全。
        - 先去掉 BOM（U+FEFF），否则 Windows GBK 控制台 / 部分下游无法编码它。
    """
    # None 直接返回空串，避免下游拼接时炸出 NoneType 错误。
    if value is None:
        return ""
    # 非 str 类型（数字 / bytes 等）先统一转成 str，保证后续替换 / 编码可用。
    if not isinstance(value, str):
        value = str(value)
    # 某些 RSS 标题夹带 BOM（U+FEFF），Windows GBK 控制台无法编码它。
    value = value.replace("\ufeff", "")
    # encode 时用 errors="replace" 把非法字节替换成 U+FFFD，再 decode 回 str，双重保证干净。
    return value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def _fetch(url: str, timeout: int = _HTTP_TIMEOUT) -> bytes:
    """同步抓取 URL 内容（urllib，带 UA + 超时）；失败会抛异常由调用方处理。

    参数:
        url:     要抓取的完整 URL。
        timeout: 超时秒数，默认 _HTTP_TIMEOUT（10 秒）。

    返回:
        bytes：HTTP 响应体原始字节（RSS/JSON 等，由调用方解析）。

    抛出:
        URLError / HTTPError / socket.timeout：网络失败、非 2xx、超时时抛出，
        由上层各源的 try/except 捕获并跳过该源。

    说明:
        urllib.request 是标准库 HTTP 客户端；Request 里带浏览器 UA 头规避部分站点默认拦截。
    """
    # 构造 HTTP 请求对象：显式带浏览器 UA 头，规避站点对默认 UA 的拦截。
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    # urlopen 返回文件对象，with 保证响应连接被正确关闭，读回原始字节。
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _parse_date(value: Optional[str]) -> str:
    """把 RSS 的各种时间格式统一成项目内 ISO 字符串（本地时区）；解析失败返回空串。

    参数:
        value: 原文时间字符串（如 RSS 的 pubDate 或 Atom 的 updated）。

    返回:
        str：形如 "YYYY-MM-DDTHH:MM:SS" 的本地时间；解析失败返回空串。

    说明:
        - 优先 ``email.utils.parsedate_to_datetime`` 解析 RFC 822 格式（pubDate，
          如 'Tue, 26 Aug 2026 12:00:00 GMT'），再 astimezone() 转本地时区；
        - 失败则用 ``datetime.fromisoformat(value.strip()[:19])`` 解析 ISO 8601
          （Atom 的 updated，取前 19 位 'YYYY-MM-DDTHH:MM:SS'）；
        - 两种都失败说明格式不认识，返回空串（宁可留空也不伪造时间）。
    """
    # 空串 / None 直接返回空，表示无时间信息。
    if not value:
        return ""
    try:
        # RFC 822 格式（pubDate，如 'Tue, 26 Aug 2026 12:00:00 GMT'）
        dt = email.utils.parsedate_to_datetime(value)
        # 解析成功则转本地时区并格式化成 ISO 字符串（astimezone() 默认转当前本地时区）。
        if dt is not None:
            return dt.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        # 非 RFC 822 格式（如 ISO 8601 或其它）会抛异常，跳过走下一套解析。
        pass
    try:
        # ISO 8601 格式（Atom 的 updated，取前 19 位 'YYYY-MM-DDTHH:MM:SS'）
        return datetime.fromisoformat(value.strip()[:19]).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        # 两种格式都失败说明不认识该时间串，返回空串（不伪造时间）。
        return ""


def _local_name(tag: str) -> str:
    """去掉 XML 标签的命名空间前缀（<{ns}title> → title），方便同时处理 RSS/Atom。

    参数:
        tag: ElementTree 元素标签（带命名空间时为 "{http://...}title" 形式）。

    返回:
        str：去掉命名空间后的本地标签名；无命名空间时原样返回。

    说明:
        ``str.rsplit("}", 1)[-1]`` 从最后一个 "}" 处切分并取右段，正好去掉 "{ns}" 前缀。
    """
    # 从最后一个 "}" 处切分取右段："{http://ns}title" → "title"；无 "}" 时原样返回。
    return tag.rsplit("}", 1)[-1]


def _first_field(fields: dict, *names: str):
    """返回第一个存在的 XML 字段；不使用 Element 的真假值，避免空元素被误判。

    参数:
        fields: 以「本地标签名 → Element」为键值对的字典。
        names:  按优先级排列的字段名，逐个查找。

    返回:
        Element 或 None：第一个存在于 fields 中的字段对应元素；都不存在返回 None。

    说明:
        用 ``is not None`` 判断存在，而不是 if element —— 空元素（无文本）也是合法存在，
        用真假值判断会把「空元素」误判为「不存在」。
    """
    # 按优先级依次尝试各字段名。
    for name in names:
        element = fields.get(name)
        # 用 is not None 判断，避免把「空元素」误判为不存在。
        if element is not None:
            return element
    # 全都不存在返回 None，由调用方做缺省处理。
    return None


def _expand_terms(terms: Optional[List[str]]) -> List[str]:
    """展开同义词并去重，统一转成小写。

    参数:
        terms: 用户输入的关键词列表（可含中文，如 ["足球"]）。

    返回:
        List[str]：展开同义词 + 去掉首尾空白 + 小写 + 去重后的词表。

    说明:
        对每个关键词，取 KEYWORD_ALIASES 里配置的同义词列表；未配置的关键词保持原样。
        展开结果统一小写，便于与标题文本做大小写不敏感的子串匹配。
    """
    # 结果列表，用于收集展开后的全部同义词。
    expanded: List[str] = []
    # terms 为 None 时按空列表处理，避免 for 报错。
    for term in terms or []:
        # 取该关键词配置的同义词表；未配置则保持原词（[term] 兜底）。
        for alias in KEYWORD_ALIASES.get(term.lower(), [term]):
            # 去掉首尾空白并统一小写，保证后续匹配大小写不敏感。
            alias = alias.strip().lower()
            # 空串直接丢弃，重复词也丢弃，保证结果无重复。
            if alias and alias not in expanded:
                expanded.append(alias)
    return expanded


def _matches(item: NewsItem, terms: List[str]) -> bool:
    """判断新闻标题或摘要是否包含任一主题词。

    参数:
        item:  :class:`~task_pipelines.schemas.NewsItem` 资讯对象。
        terms: 已展开的主题词列表（小写）。

    返回:
        bool：标题或摘要的小写文本里命中任一主题词则 True。

    说明:
        把「标题 + 摘要」拼起来统一转小写，再对每个主题词做子串匹配（``in``）。
    """
    # 标题 + 摘要拼成一段文本并统一小写，做大小写不敏感的子串匹配。
    text = f"{item.title} {item.summary}".lower()
    # any()：任一主题词命中即返回 True。
    return any(term in text for term in terms)


def _is_general_source(item: NewsItem) -> bool:
    """综合来源没有可靠模块分类，需要额外做模块相关性过滤。

    参数:
        item: :class:`~task_pipelines.schemas.NewsItem` 资讯对象。

    返回:
        bool：来源是通用综合源（GENERAL_FEEDS 里的 RSS 或 GENERAL_SOURCE_NAMES 里的知名源）则 True。

    说明:
        模块专用 RSS（如 IT之家 / 36氪）标题天然贴合所属模块，可直接保留；
        而综合源（中新网滚动 / HN / Google News / Bing News）什么话题都有，必须再过 MODULE_TERMS 过滤。
    """
    # 命中通用 RSS 集合或知名综合源名字集合之一，即视为「需要额外过滤」的综合来源。
    return item.source in GENERAL_FEEDS or item.source in GENERAL_SOURCE_NAMES


def _parse_feed(xml_bytes: bytes, module: str, source: str) -> List[NewsItem]:
    """解析 RSS 2.0 / Atom 源，返回 NewsItem 列表（纯标准库，免 feedparser）。

    参数:
        xml_bytes: 抓回的 RSS/Atom 原始字节。
        module:    资讯所属模块（写进每条 NewsItem.module）。
        source:    来源标识（写进每条 NewsItem.source，通常为 feed URL 或 "Google News" 等）。

    返回:
        List[NewsItem]：解析出的资讯列表；标题为空或 XML 非法的条目会被跳过 / 抛异常。

    抛出:
        xml.etree.ElementTree.ParseError：XML 格式非法时抛出，由调用方捕获并跳过该源。

    说明:
        - RSS 2.0：``<rss><channel><item>...</item>``；item 里 title/link/pubDate/description。
        - Atom  ：``<feed><entry>...</entry>``；entry 里 title/link(href)/updated/summary。
        - news_id 由 md5(url 或 title) 前 8 位生成，带 "rss-" 前缀，用于下游去重。
    """
    # 解析结果列表。
    items: List[NewsItem] = []
    # ElementTree.fromstring 解析 XML；不合法会抛 ParseError，由调用方跳过该源。
    root = ElementTree.fromstring(xml_bytes)  # XML 不合法会抛异常，由调用方跳过该源
    # 存放解析出的条目元素（RSS 的 <item> 或 Atom 的 <entry>）。
    entries: List[ElementTree.Element] = []
    # 识别根节点类型：channel → RSS，feed → Atom，收集对应的条目元素列表。
    for child in root:
        if _local_name(child.tag) == "channel":
            entries = [c for c in child if _local_name(c.tag) == "item"]
            break
        if _local_name(child.tag) == "feed":
            entries = [c for c in child if _local_name(c.tag) == "entry"]
            break

    # 逐个条目解析成 NewsItem。
    for entry in entries:
        # 把条目所有子元素按「本地标签名 → Element」建字典，方便按字段名取。
        fields = {_local_name(c.tag): c for c in entry}
        # 标题（RSS/Atom 都有 <title>）
        title_el = fields.get("title")
        title = _clean_text(title_el.text if title_el is not None else "").strip()
        # 标题为空无法构成资讯，跳过该条。
        if not title:
            continue
        # 链接：RSS 是 <link>文本</link>；Atom 是 <link href="..."/>
        link_el = fields.get("link")
        url = ""
        # 优先取 href 属性（Atom），回退到元素文本（RSS）。
        if link_el is not None:
            url = _clean_text((link_el.get("href") or "").strip() or (link_el.text or "")).strip()
        # 摘要：RSS description / Atom summary / content
        desc_el = _first_field(fields, "description", "summary", "content")
        # 压缩空白并截断到 200 字，避免超长摘要污染下游。
        summary = _clean_text(re.sub(r"\s+", " ", (desc_el.text or "").strip())[:200]) if desc_el is not None else ""
        # 时间：RSS pubDate / Atom updated / dc:date
        time_el = _first_field(fields, "pubDate", "published", "updated", "date")
        published = _parse_date(time_el.text if time_el is not None else None)

        # 资讯 ID：md5(url 或 title) 前 8 位 + "rss-" 前缀，稳定且跨源可去重。
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
    （异步：网络抓取放线程池，保证与其它源在 asyncio.gather 里并发）

    参数:
        module: 资讯模块（"科技" / "财经" 等）。
        since:  可选时间过滤（透传给搜索源，不深入使用）。
        limit:  单源最多返回条数。

    返回:
        List[NewsItem]：该模块所有 RSS 源抓到的资讯；单个源失败跳过并记 warning。

    说明:
        ``conf.rss_sources`` 是 Config.rss_sources 属性返回的「模块 → RSS 源 URL 列表」映射，
        配置了就用配置的，没配置就回退到本文件内置 DEFAULT_RSS_FEEDS。
    """
    # 取该模块的 RSS 源列表：配置优先，缺省回退内置 DEFAULT_RSS_FEEDS。
    feeds = conf.rss_sources.get(module) or DEFAULT_RSS_FEEDS.get(module, [])
    # 汇总该模块所有 RSS 源抓到的资讯。
    items: List[NewsItem] = []
    # 逐个源抓取，单个失败只跳过自己，不影响其它源。
    for url in feeds:
        try:
            # search: 前缀是「关键词搜索源」占位 → 转交 _fetch_news_search 实时搜索。
            if url.startswith("search:"):
                query = url[len("search:"):].strip()
                got = await _fetch_news_search([query], limit, module)
                logger.info(f"[collect] RSS 搜索源「{query}」抓到 {len(got)} 条")
            else:
                # 普通 RSS：_fetch 抓字节（放线程池），_parse_feed 解析成 NewsItem。
                got = _parse_feed(await asyncio.to_thread(_fetch, url), module, url)
                logger.info(f"[collect] RSS 源 {url} 抓到 {len(got)} 条")
            # 把本源的抓取结果并入总列表。
            items.extend(got)
        except Exception as exc:
            # 单个源失败不中断整个模块，记 warning 后跳过。
            logger.warning(f"[collect] RSS 源 {url} 抓取失败，跳过: {exc}")
    return items


async def _fetch_hn(module: str, since: Optional[str], limit: int) -> List[NewsItem]:
    """Hacker News Top 榜单（官方 Firebase API，免 key，条目并发抓取）。

    参数:
        module: 资讯模块（写进每条 NewsItem.module）。
        since:  时间过滤（本实现未使用，保留签名统一）。
        limit:  最多返回条目数（截取榜单前 limit 条）。

    返回:
        List[NewsItem]：HN 头条转成的资讯列表；榜单抓取失败返回空列表。

    说明:
        - ``HN_TOP_URL`` 返回最新榜单的条目 ID 数组，取前 limit 个。
        - ``HN_ITEM_URL.format(sid)`` 逐个取条目详情；用 ``asyncio.gather`` 并发，
          return_exceptions=True 让单条失败不拖垮整体。
        - 只保留 type=story 且有 title 的条目；时间用 ``datetime.fromtimestamp`` 转 ISO。
    """
    try:
        # 拉取 topstories ID 列表，取前 limit 个（榜单接口返回的是 JSON 数组）。
        ids = json.loads(await asyncio.to_thread(_fetch, HN_TOP_URL))[:limit]
    except Exception as exc:
        logger.warning(f"[collect] HN 榜单抓取失败，跳过: {exc}")
        return []
    # 并发抓取每条新闻详情，单条失败以异常形式返回（不中断整体）。
    results = await asyncio.gather(
        *[asyncio.to_thread(_fetch, HN_ITEM_URL.format(sid)) for sid in ids],
        return_exceptions=True,
    )
    # 汇总解析出的 HN 资讯。
    items: List[NewsItem] = []
    # zip 把 ID 与抓取结果一一配对处理。
    for sid, raw in zip(ids, results):
        # 单条抓取失败：raw 是异常对象，记 warning 后跳过。
        if isinstance(raw, Exception):
            logger.warning(f"[collect] HN 条目 {sid} 抓取失败: {raw}")
            continue
        try:
            # 解析条目 JSON；errors="replace" 兜底非法字节。
            data = json.loads(raw.decode("utf-8", errors="replace"))
            # 只要 story 类型且带标题的条目，其它类型（comment/job 等）丢弃。
            if not data or data.get("type") != "story" or not data.get("title"):
                continue
            # 发布时间：unix 时间戳转 ISO 字符串。
            published = ""
            if data.get("time"):
                published = datetime.fromtimestamp(data["time"]).strftime("%Y-%m-%dT%H:%M:%S")
            items.append(NewsItem(
                news_id=f"hn-{sid}", module=module, title=_clean_text(data["title"]),
                source="Hacker News",
                # 时间未知回退为当前时间（_now_iso），保证排序可用。
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

    参数:
        keywords: 关键词列表（会拼成一个空格分隔的查询串）。
        limit:    最多返回条数。
        module:   资讯模块（写进每条 NewsItem.module）。

    返回:
        List[NewsItem]：搜索源返回的相关资讯；全部 API 都失败返回空列表。

    说明:
        - 遍历 NEWS_SEARCH_APIS，逐个尝试；当前 API 无结果或抛异常就换下一个。
        - ``urllib.parse.quote`` 对查询串做 URL 编码，保证中文关键词可用。
        - Google News 标题尾部会带「 - 来源」后缀，用正则 ``\\s*-\\s*[^\\s-]+(?:\\s+[^\\s-]+)?$``
          把最后一个「 - xxx」剥掉，只保留真正的新闻标题。
    """
    # 把多个关键词拼成一个空格分隔的查询串。
    query = " ".join(keywords)
    # 依次尝试各搜索 API，失败自动换下一个。
    for api in NEWS_SEARCH_APIS:
        # 根据 URL 里是否含 "google" 判断当前源名字（用于日志与来源标识）。
        source = "Google News" if "google" in api else "Bing News"
        try:
            # quote 对查询串做 URL 编码后填入 {q}，再抓取 RSS 字节并解析。
            got = _parse_feed(await asyncio.to_thread(_fetch, api.format(q=urllib.parse.quote(query))),
                              module, source)
            # 无结果则尝试下一个 API。
            if not got:
                continue
            # Google News 标题尾部带「 - 来源」后缀，需要剥掉只留新闻标题。
            if source == "Google News":
                for it in got:
                    # 剥掉标题尾部的「 - 来源」后缀，如 "xxx - 华尔街见闻" → "xxx"。
                    it.title = re.sub(r"\s*-\s*[^\s-]+(?:\s+[^\s-]+)?$", "", it.title).strip() or it.title
            logger.info(f"[collect] 搜索源 {source} 关键词「{query}」返回 {len(got)} 条")
            # 第一个成功的 API 直接返回（截断到 limit）。
            return got[:limit]
        except Exception as exc:
            logger.warning(f"[collect] 搜索源 {source} 抓取失败，尝试下一个: {exc}")
    # 全部 API 都失败，返回空列表。
    return []


# 源 → 处理函数注册表（统一签名：module, since, limit）
SOURCE_HANDLERS = {
    "rss": _fetch_rss,
    "hn": _fetch_hn,
}


def _mock_news(module: str, count: int = 3) -> List[NewsItem]:
    """【模拟】兜底样例：所有真实源都不可用时的降级产物，标题带标记方便识别。

    参数:
        module: 资讯模块。
        count:  生成的样例条数（默认 3）。

    返回:
        List[NewsItem]：标题以「【模拟】」开头的样例数据。

    说明:
        标题带「【模拟】」前缀是约定标记，下游（如 schemas.no_real_items）可据此识别
        这不是真实数据，避免把假新闻当真。
    """
    # 用列表推导一次性生成 count 条模拟样例（标题带【模拟】标记）。
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
    """并发抓取来源；单个来源失败只跳过该来源，不丢弃其他成功结果。

    参数:
        module:  资讯模块。
        since:   可选时间过滤（透传给各源处理函数）。
        limit:   每个源最多返回条数。
        sources: 要抓取的源名列表（"rss" / "hn" 等）。

    返回:
        List[NewsItem]：所有成功来源抓到的资讯合并结果。

    说明:
        - 通过 SOURCE_HANDLERS 注册表把源名映射到对应处理函数，未注册的源名记 warning 跳过。
        - 用 ``asyncio.gather(..., return_exceptions=True)`` 并发执行各源，
          某个源抛异常只会在结果里出现异常对象，不影响其它源。
    """
    # 待并发执行的协程列表。
    coros = []
    # 把源名通过注册表映射成处理协程。
    for src in sources:
        handler = SOURCE_HANDLERS.get(src)
        # 未注册的源名直接跳过并告警，不当作硬错误。
        if handler is None:
            logger.warning(f"[collect] 未知采集源 {src}，跳过")
            continue
        coros.append(handler(module, since, limit))
    # 没有可用源则直接返回空（避免对空列表 gather）。
    if not coros:
        return []
    # 并发执行所有源；return_exceptions=True 让单个源的异常以返回值形式出现。
    results = await asyncio.gather(*coros, return_exceptions=True)
    items: List[NewsItem] = []
    # 逐个检查结果：异常则跳过该源，否则把抓到的资讯并入总列表。
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
    """多源资讯采集（RSS · Hacker News · 关键词搜索源）。

    参数:
        module:   新闻模块，如 "科技" / "财经" / "体育"。
        keywords: 附加关键词过滤，None 表示不限（精确主题查询）。
        sources:  指定采集源，None 使用默认源列表（rss/hn）。
        since:    只采集该时间点之后的资讯（ISO 8601）。
        limit:    单次采集上限，默认 50。

    返回:
        List[NewsItem]：采集 + 过滤 + 去重后的资讯列表（写入原始资讯库）；
        所有真实源都不可用且无命中时才降级返回【模拟】样例数据。

    说明:
        - 抓取阶段：各源并发抓取（_fetch_sources），顶层兜底保证任何意外都不中断链路。
        - 关键词补采：若指定了 keywords，直接用关键词再搜一次搜索源，提高窄话题召回。
        - since 过滤：按 published_at 与 since_dt 比较，丢弃更早的资讯。
        - 精确主题查询：关键词场景下做「同义词扩展 + 严格子串匹配」，无命中返回空列表；
          模块查询场景下，综合源必须命中 MODULE_TERMS，模块专用 RSS 直接保留。
        - 去重：按标题指纹（去非字母数字后取前 40 位小写）判重，再按时间倒序、截断到 limit。
        - 降级判定：只有「真实来源全部不可用（raw_count==0）」才降级 mock；
          若抓到了但过滤后为空，必须如实返回空列表。
    """
    # 规范化采集源列表（None → 默认 ["rss", "hn"]），并复制一份避免改到调用方入参。
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
    # 记录「过滤前」的真实抓取条数，用于后面判断是否要降级 mock。
    raw_count = len(items)

    # since 时间过滤
    if since:
        try:
            # 把 since 字符串解析成 datetime 作为时间边界。
            since_dt = datetime.fromisoformat(since)
            items = [it for it in items if it.published_at and datetime.fromisoformat(it.published_at) >= since_dt]
        except Exception as exc:
            logger.warning(f"[collect] since 过滤失败，忽略该条件: {exc}")

    # 精确主题查询：中英文同义词扩展后严格过滤；无命中就返回空，不返回无关模块新闻。
    if keywords:
        # 关键词场景：先做同义词扩展（含英文），再逐条严格子串匹配。
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
    # 按发布时间倒序遍历，保证保留的是最新版本的同题新闻。
    for it in sorted(items, key=lambda x: x.published_at, reverse=True):
        # 标题指纹：去掉所有非字母数字字符（\W+）后小写，取前 40 位作为去重键。
        fp = re.sub(r"\W+", "", it.title).lower()[:40]
        # 指纹已出现过 → 视为重复，跳过。
        if fp in seen:
            continue
        seen.add(fp)
        uniq.append(it)
        # 达到 limit 提前截断，避免无谓遍历。
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
    """解析账户的抓取地址：config.ini [accounts] 按账户名映射 > account 本身是 URL。

    参数:
        account:  账户标识（weibo uid / 公众号名 / B 站空间地址等）。
        platform: 平台名（weibo / wechat / bilibili …），当前仅用于日志上下文。

    返回:
        Optional[str]：抓取地址（RSS URL 或 B 站空间 URL）；找不到返回 None。

    说明:
        - 先去掉 account 开头的 "@"（用户常写 @新京报），再查 ``conf.accounts`` 配置映射；
        - 配置里没有，且 account 本身以 http 开头 → 直接把 account 当作 URL；
        - 两者都不满足 → 返回 None，由调用方降级为【模拟】样例数据。
    """
    # 去掉 account 开头的 "@"（用户常写 @新京报），优先用去 @ 后的名字查配置。
    name = account.strip("@")
    # 配置映射里查：先按去 @ 名，再按原始 account（两把键都试）。
    url = conf.accounts.get(name) or conf.accounts.get(account)
    if url:
        return url
    # 配置里没有，但 account 本身是 http 开头的 URL → 直接把 account 当抓取地址。
    if account.lower().startswith("http"):
        return account
    # 两者都不满足 → 返回 None，由调用方降级为【模拟】样例数据。
    return None


def _mock_posts(account: str, platform: str, count: int = 3) -> List[AccountPost]:
    """【模拟】兜底样例：账户未配置抓取地址时降级（缺配置 → 样例数据）。

    参数:
        account:  账户标识。
        platform: 平台名。
        count:    生成的样例条数（默认 3）。

    返回:
        List[AccountPost]：标题以「【模拟】」开头的账户发布样例。

    说明:
        与 _mock_news 一样，标题带「【模拟】」标记，下游可据此识别非真实数据。
    """
    # 列表推导生成 count 条模拟发布（标题带【模拟】标记，便于下游识别）。
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
    """账户 / 作品发布监控采集（真实为主：配置了抓取地址就走 RSS 真实抓取；缺配置降级样例）。

    参数:
        account:  账户标识（weibo uid / 公众号名 / B 站空间地址等）。
        platform: 平台，如 "weibo" / "wechat" / "bilibili" / "xiaohongshu"。
        since:    只返回该时间点之后的新发布（ISO 8601）。
        limit:    单次拉取上限，默认 50。

    返回:
        List[AccountPost]：账户发布内容列表；未配置抓取地址或抓取失败时降级为【模拟】样例。

    说明:
        - B 站平台（bilibili / bili / b站）委托 ``tools.bilibili.fetch_bilibili_space_videos``
          用 yt-dlp 抓 UP 主空间，space_url 即 _resolve_account_feed 解析出的地址；
        - 其它平台按 RSS 处理：_parse_feed 解析抓回内容，再映射成 AccountPost
          （post_id 由 md5(标题或链接) 前 8 位生成，带 "post-" 前缀）；
        - 未配置地址 / 抓取失败 → 降级 _mock_posts，保证下游演示链路可用。
    """
    # 第一步：解析账户的抓取地址（配置映射 或 账户本身是 URL）。
    feed_url = _resolve_account_feed(account, platform)
    # 没有抓取地址 → 缺配置降级为【模拟】样例。
    if not feed_url:
        logger.warning(f"[collect] 账户 {account}@{platform} 未配置抓取地址，降级为【模拟】样例数据")
        return _mock_posts(account, platform)
    # B 站平台走专用采集器。
    if platform.lower() in ("bilibili", "bili", "b站"):
        # B 站专用：委托 tools/bilibili.py 的 yt-dlp 采集器（延迟 import 避免顶层依赖）。
        from tools.bilibili import fetch_bilibili_space_videos
        return await fetch_bilibili_space_videos(
            account=account, space_url=feed_url, since=since, limit=limit,
        )
    try:
        # 其它平台按 RSS 解析抓回内容（weibo/公众号多数提供 RSS 聚合地址）。
        news = _parse_feed(await asyncio.to_thread(_fetch, feed_url), module=platform, source=feed_url)
        # 把 RSS 资讯映射成 AccountPost（post_id 取 md5 前 8 位，带 "post-" 前缀）。
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
        # 抓到了但内容为空，单独告警方便排查。
        if not posts:
            logger.warning(f"[collect] 账户 {account} 抓取地址 {feed_url} 无内容")
        return posts
    except Exception as exc:
        # 抓取/解析异常 → 记 warning 并降级为【模拟】样例。
        logger.warning(f"[collect] 账户 {account} 抓取失败: {exc}，降级为【模拟】样例数据")
        return _mock_posts(account, platform)
