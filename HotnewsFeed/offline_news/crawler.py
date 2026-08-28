# -*- coding: utf-8 -*-
"""离线新闻爬虫：复用在线采集器，并按需抓取文章正文。"""

import asyncio
import hashlib
import re
import urllib.request
from dataclasses import dataclass, asdict
from html.parser import HTMLParser
from datetime import datetime, timedelta
from typing import Dict, List

from config import Config
from create_logger import logger
from tools.collect import collect_news

conf = Config()
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) HotnewsFeed/1.0"


@dataclass
class CrawledNews:
    news_uid: str
    module: str
    title: str
    source: str
    url: str
    summary: str
    content: str
    published_at: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


class _ArticleParser(HTMLParser):
    """轻量正文解析器：优先提取 article/main 中的段落。"""

    def __init__(self):
        super().__init__()
        self.depth = 0
        self.parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("article", "main"):
            self.depth += 1
        elif self.depth and tag == "p":
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("article", "main") and self.depth:
            self.depth -= 1

    def handle_data(self, data):
        if self.depth:
            text = re.sub(r"\s+", " ", data).strip()
            if text:
                self.parts.append(text)

    def text(self) -> str:
        return re.sub(r"\n\s*\n+", "\n", " ".join(self.parts)).strip()


def _fetch_article(url: str) -> str:
    """抓取单篇正文；失败返回空串，由摘要兜底。"""
    if not url:
        return ""
    request = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html = response.read().decode(charset, errors="replace")
        parser = _ArticleParser()
        parser.feed(html)
        return parser.text()[:20000]
    except Exception as exc:
        logger.debug(f"[offline-crawler] 正文抓取失败 {url}: {exc}")
        return ""


async def crawl_module(module: str, limit: int) -> List[CrawledNews]:
    """采集一个板块，并并发补充文章正文。"""
    cutoff = datetime.now() - timedelta(days=conf.offline_news["retention_days"])
    news = await collect_news(module=module, sources=["rss"],
                              since=cutoff.isoformat(timespec="seconds"), limit=limit)
    news = [item for item in news if not item.title.startswith("【模拟】")]
    semaphore = asyncio.Semaphore(conf.offline_news["article_concurrency"])

    async def convert(item):
        content = ""
        if conf.offline_news["fetch_article_content"]:
            async with semaphore:
                content = await asyncio.to_thread(_fetch_article, item.url)
        uid_seed = item.url or f"{module}|{item.title}|{item.source}"
        return CrawledNews(
            news_uid=hashlib.sha256(uid_seed.encode("utf-8")).hexdigest(),
            module=module,
            title=item.title,
            source=item.source,
            url=item.url,
            summary=item.summary,
            content=content or item.summary or item.title,
            published_at=item.published_at,
        )

    return await asyncio.gather(*(convert(item) for item in news))


async def crawl_all_modules() -> List[CrawledNews]:
    """按配置并发采集全部板块，并按 news_uid 去重。"""
    cfg = conf.offline_news
    groups = await asyncio.gather(
        *(crawl_module(module, cfg["per_module_limit"]) for module in cfg["modules"]),
        return_exceptions=True,
    )
    unique: Dict[str, CrawledNews] = {}
    for module, group in zip(cfg["modules"], groups):
        if isinstance(group, Exception):
            logger.warning(f"[offline-crawler] 板块 {module} 采集失败: {group}")
            continue
        for item in group:
            unique[item.news_uid] = item
    logger.info(f"[offline-crawler] 本轮共采集 {len(unique)} 条有效新闻")
    return list(unique.values())
