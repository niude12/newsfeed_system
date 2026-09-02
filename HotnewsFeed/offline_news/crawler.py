# -*- coding: utf-8 -*-
"""离线新闻爬虫：复用在线采集器，并按需抓取文章正文。

该模块负责离线资讯库的“采集”环节：复用 tools/collect.py 的在线多源采集器
collect_news 拉取各板块新闻，并按配置决定是否额外抓取文章正文（HTML 解析），
最终把结果统一包装成 :class:`CrawledNews` 数据结构，供 offline_news/stores.py 入库向量化使用。

模块依赖:
- ``collect_news`` : tools/collect.py 的在线多源采集器（RSS / Hacker News / 关键词搜索源），
                     本模块直接复用，只透传 module / sources / since / limit 四个参数。
- ``Config``       : 全局配置单例。通过 ``offline_news`` 属性读取 [offline_news] 段下的
                     retention_days（保留天数）、fetch_article_content（是否抓正文）、
                     article_concurrency（正文抓取并发数）、per_module_limit、modules 等键。
- ``HTMLParser``   : 标准库 html.parser，用于轻量抽取文章正文（优先 article/main 里的 <p>）。

典型调用链::

    service.ingest()  ->  crawl_all_modules()
                      ->  crawl_module(module, limit)     # 每个板块一个协程
                      ->  collect_news(...)               # 复用在线采集器拿 NewsItem
                      ->  _fetch_article(url)             # 并发抓取正文（可关闭）
                      ->  CrawledNews(...)                # 包装为统一结构

对外暴露两个接口：
- crawl_module      : 采集单个板块（含正文补充），返回 List[CrawledNews]。
- crawl_all_modules : 按配置并发采集全部板块并按 news_uid 去重。
"""

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

# 全局配置对象（读取 config.ini）。本模块只用到其 offline_news 属性。
conf = Config()
# 统一的 User-Agent：模拟真实浏览器，降低被目标站拦截的概率。
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) HotnewsFeed/1.0"


@dataclass
class CrawledNews:
    """采集结果的统一数据结构（离线入库前的标准形态）。

    相比在线链路的 NewsItem，额外多出 content（正文全文）与 news_uid（内容指纹）两个字段：
    news_uid 作为 MySQL 唯一键（去重/更新依据），content 作为向量化与简报的正文来源。

    字段:
        news_uid:     内容指纹，sha256(url) 的十六进制串；url 为空时退化为「板块|标题|来源」。
        module:       所属板块（科技/财经/体育/娱乐/国际…）。
        title:        新闻标题。
        source:       来源名（RSS 域名 / Hacker News / Google News 等）。
        url:          原文链接。
        summary:      摘要文本（在线采集器已生成）。
        content:      正文全文；未抓正文或抓取失败时回退为 summary 或 title。
        published_at: 发布时间（ISO 8601 字符串，可为空）。

    说明:
        to_dict() 用 dataclasses.asdict 把 dataclass 递归转成 dict，
        供 stores.MySQLNewsStore.upsert 直接消费。
    """

    news_uid: str
    module: str
    title: str
    source: str
    url: str
    summary: str
    content: str
    published_at: str = ""

    def to_dict(self) -> Dict:
        """把 CrawledNews 序列化为普通 dict。

        返回:
            Dict：字段名 → 字段值，供 MySQL 批量 upsert 作为行参数。

        说明:
            dataclasses.asdict() 是标准库函数，把 dataclass 实例深度转成普通 dict
            （嵌套 dataclass 也会一并展开）。
        """
        return asdict(self)


class _ArticleParser(HTMLParser):
    """轻量正文解析器：优先提取 article/main 中的段落。

    继承标准库 html.parser.HTMLParser，用“深度计数”跟踪当前是否处于
    <article> / <main> 容器内：
    - 遇到容器起始标签 depth+1、结束标签 depth-1；
    - 容器内遇到 <p> 起始标签时向 parts 追加一个换行，作为段落分隔；
    - 容器内的文本统一做空白归一化后收集到 parts 列表。

    说明:
        HTMLParser.feed(html) 是流式解析入口，解析过程中会回调
        handle_starttag / handle_endtag / handle_data 三个方法，本类即通过重写它们实现正文抽取。
    """

    def __init__(self):
        """初始化解析器状态：深度计数归零，正文片段列表置空。

        说明:
            调用 super().__init__() 先完成 HTMLParser 的初始化；
            depth 记录 <article>/<main> 容器嵌套深度（0 = 容器外），
            parts 收集正文片段，供 text() 最终拼接。
        """
        super().__init__()
        self.depth = 0  # 当前位于 <article>/<main> 容器的嵌套深度（0 = 容器外）。
        self.parts: List[str] = []  # 收集到的正文片段列表，最后在 text() 里拼接。

    def handle_starttag(self, tag, attrs):
        """处理起始标签：维护容器深度，并在容器内的 <p> 处插入换行。

        参数:
            tag:   标签名（小写，如 "article" / "main" / "p"）。
            attrs: (属性名, 属性值) 元组列表；本解析器不关心属性，仅按标签名处理。

        说明:
            只有位于容器内（depth > 0）的 <p> 才追加换行，用作段落分隔符；
            容器外的 <p> 不处理，避免把页头页脚等噪声段落混入正文。
        """
        # 容器起始标签：进入 <article>/<main>，深度加一，标记“正在正文容器内”。
        if tag in ("article", "main"):
            self.depth += 1
        # 容器内遇到段落起始标签 <p>，追加换行作段落分隔（容器外的 <p> 忽略）。
        elif self.depth and tag == "p":
            # 在容器内遇到段落开始标签，追加换行作为段落分隔符。
            self.parts.append("\n")

    def handle_endtag(self, tag):
        """处理结束标签：离开 <article>/<main> 容器时深度减一。

        参数:
            tag: 标签名（小写）。

        说明:
            与 handle_starttag 对称维护 depth；若 depth 已为 0（异常 HTML），
            不再自减，防止深度变成负数。
        """
        # 容器结束标签：离开 <article>/<main>，深度减一（深度为 0 时不减，防异常 HTML）。
        if tag in ("article", "main") and self.depth:
            self.depth -= 1

    def handle_data(self, data):
        """容器内文本数据处理：空白归一化后追加到 parts。

        参数:
            data: HTMLParser 解析出的文本内容（可能是标签间的大段文本）。

        说明:
            只有位于 <article>/<main> 容器内（self.depth > 0）的文本才会被收集；
            re.sub(r"\\s+", " ", data) 把任意连续空白（含换行）压成单个空格，
            strip() 去掉首尾空白后追加到 parts。
        """
        # 只收集容器内的文本；容器外文本（导航/页脚等）一律丢弃。
        if self.depth:
            # \s+ 匹配任意连续空白（含换行），替换为单个空格，把多行文本压成一行。
            text = re.sub(r"\s+", " ", data).strip()
            # 空白清理后仍有内容才收集，避免纯空片段污染正文。
            if text:
                self.parts.append(text)

    def text(self) -> str:
        """输出规整后的正文全文。

        返回:
            str：把 parts 用空格拼接后，再压缩连续空行为单个换行，并去掉首尾空白。

        说明:
            re.sub(r"\\n\\s*\\n+", "\\n", " ".join(self.parts))：把拼接产生的
            连续空行压缩成单个换行，得到段落分明的正文文本。
        """
        # 空格拼接各片段后，把连续空行（\n\s*\n+）压缩为单个换行，并去首尾空白。
        return re.sub(r"\n\s*\n+", "\n", " ".join(self.parts)).strip()


def _fetch_article(url: str) -> str:
    """抓取单篇正文；失败返回空串，由摘要兜底。

    同步阻塞式函数：用 urllib 带 UA 请求原文页，读取后交给 _ArticleParser 抽取
    <article>/<main> 内的段落文本，并截断到 20000 字符，防止正文过大撑爆数据库。

    参数:
        url: 文章原文链接；为空时直接返回空串。

    返回:
        抽取出的正文字符串；网络 / 解析 / 解码失败时返回空串（由调用方用摘要兜底）。

    说明:
        - urllib.request.Request 构造请求对象，headers 里的 User-Agent 用模块级 _UA 伪装浏览器。
        - urllib.request.urlopen(request, timeout=12) 发起请求，12 秒超时防止请求挂死。
        - response.headers.get_content_charset() 从响应头取字符集，取不到时默认 utf-8；
          decode(errors="replace") 把无法解码的坏字节替换为 U+FFFD，不让编码错误中断链路。
        - _ArticleParser().feed(html) 流式解析 HTML，text() 输出规整后的正文。
        - 任何异常只记 debug 日志并返回空串，符合“正文抓取失败不阻断采集”的降级原则。
    """
    # 空链接直接返回空串，由调用方用摘要兜底。
    if not url:
        return ""
    # 构造带浏览器 UA 的请求对象，降低被目标站拦截的概率。
    request = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        # urlopen 发起请求并打开响应流；12 秒超时防止请求挂死。
        with urllib.request.urlopen(request, timeout=12) as response:
            # 优先用响应头声明的字符集解码，防止中文站点按 GBK 输出时乱码。
            charset = response.headers.get_content_charset() or "utf-8"
            # 读取正文并解码；errors="replace" 让坏字节变成 U+FFFD，不中断链路。
            html = response.read().decode(charset, errors="replace")
        # 用轻量 HTMLParser 抽取 <article>/<main> 内的段落文本。
        parser = _ArticleParser()
        # feed() 是流式解析入口，会回调 handle_* 系列方法完成正文收集。
        parser.feed(html)
        return parser.text()[:20000]  # 截断超长正文，避免 MEDIUMTEXT 超限/向量化开销过大。
    except Exception as exc:
        # 任何网络/解析/解码异常都只记 debug 日志，返回空串由摘要兜底。
        logger.debug(f"[offline-crawler] 正文抓取失败 {url}: {exc}")
        return ""


async def crawl_module(module: str, limit: int) -> List[CrawledNews]:
    """采集一个板块，并并发补充文章正文。

    以保留天数（retention_days）为截止时间，复用在线采集器 collect_news 抓取该板块 RSS 源；
    过滤掉【模拟】样例数据后，按配置的并发数并发抓取每篇文章正文，最终包装成 CrawledNews 列表。

    参数:
        module: 板块名（如 "科技"），对应 config [offline_news] modules 之一。
        limit:  单板块采集上限，透传给 collect_news 的 limit 参数。

    返回:
        List[CrawledNews]：该板块采集到的新闻列表（content 已按「正文 → 摘要 → 标题」兜底填充）。

    说明:
        - cutoff = now - retention_days：collect_news 的 since 参数会用该时刻过滤更早的旧闻。
        - collect_news 返回 List[NewsItem]；标题以【模拟】开头的降级样例被过滤，离线库只存真实新闻。
        - asyncio.Semaphore(article_concurrency) 限制正文抓取并发数，防止瞬时大量请求打爆目标站。
        - asyncio.to_thread(_fetch_article, item.url) 把阻塞的 urllib 抓取丢线程池执行，
          不阻塞事件循环（这是本函数声明为 async 的原因）。
        - news_uid = sha256(url) 十六进制串：同一 URL 稳定生成同一指纹，作为 MySQL 唯一键；
          url 为空时用「模块|标题|来源」拼接串兜底。
        - content 为空时依次回退到 summary、title，保证入库记录的正文字段非空。
    """
    # 截止时间 = 当前时间 - 保留天数；collect_news 用 since 过滤更早的旧闻。
    cutoff = datetime.now() - timedelta(days=conf.offline_news["retention_days"])
    # 复用在线采集器抓取该板块 RSS 源（sources=["rss"]，不走 HN/关键词源）。
    news = await collect_news(module=module, sources=["rss"],
                              since=cutoff.isoformat(timespec="seconds"), limit=limit)
    # 过滤掉在线采集器降级生成的【模拟】样例数据，保证离线库只入库真实新闻。
    news = [item for item in news if not item.title.startswith("【模拟】")]
    # 信号量：控制正文抓取的并发上限（取值自 [offline_news] article_concurrency）。
    semaphore = asyncio.Semaphore(conf.offline_news["article_concurrency"])

    async def convert(item):
        """把单个 NewsItem 转换为 CrawledNews，并可选抓取正文。

        参数:
            item: collect_news 返回的 NewsItem（含 title/source/url/summary/published_at）。

        返回:
            CrawledNews：含 news_uid 指纹与 content 正文（正文为空时回退摘要/标题）。

        说明:
            asyncio.Semaphore 上下文管理器限制并发；asyncio.to_thread 把同步抓取
            放到线程池执行，不阻塞事件循环。
        """
        # 默认不带正文；只有配置 fetch_article_content=true 时才抓正文。
        content = ""
        # 判断是否启用正文抓取（[offline_news] fetch_article_content 配置开关，默认 true）。
        if conf.offline_news["fetch_article_content"]:
            # 用信号量限制并发，防止瞬时大量请求打爆目标站。
            async with semaphore:
                # to_thread 把同步的 urllib 抓取放到线程池，并发抓正文且不阻塞事件循环。
                content = await asyncio.to_thread(_fetch_article, item.url)
        # 指纹种子：优先 url；url 缺失时用「板块|标题|来源」组合，保证不同内容不撞指纹。
        uid_seed = item.url or f"{module}|{item.title}|{item.source}"
        return CrawledNews(
            # sha256 摘要转十六进制串，作为 MySQL 唯一键 news_uid。
            news_uid=hashlib.sha256(uid_seed.encode("utf-8")).hexdigest(),
            module=module,  # 板块名原样透传。
            title=item.title,  # 标题原样透传。
            source=item.source,  # 来源名原样透传。
            url=item.url,  # 原文链接原样透传。
            summary=item.summary,  # 摘要原样透传。
            # 正文优先，失败/关闭时依次回退摘要、标题。
            content=content or item.summary or item.title,
            published_at=item.published_at,  # 发布时间（ISO 8601）原样透传。
        )

    # asyncio.gather 并发执行全部转换协程，结果顺序与 news 保持一致。
    return await asyncio.gather(*(convert(item) for item in news))


async def crawl_all_modules() -> List[CrawledNews]:
    """按配置并发采集全部板块，并按 news_uid 去重。

    遍历 config [offline_news] modules 里配置的每个板块，并发调用 crawl_module；
    单个板块失败只记 warning 并跳过（不影响其它板块），最终以 news_uid 为键去重。

    参数:
        无。

    返回:
        List[CrawledNews]：全部板块去重后的有效新闻列表（跨板块间保持采集顺序）。

    说明:
        - asyncio.gather(..., return_exceptions=True)：并发执行所有板块协程，
          单个板块的异常被收集进结果列表，而不是中断整体采集。
        - zip(cfg["modules"], groups) 把板块名与采集结果一一配对；
          isinstance(group, Exception) 判断该板块是否失败。
        - 用 dict 以 news_uid 为键去重：同一文章被多个板块 / RSS 源重复采集时只保留一份。
        - logger.info 记录本轮有效新闻总数，便于调度任务核对采集量。
    """
    # 取 [offline_news] 配置段字典（modules / per_module_limit / retention_days 等键）。
    cfg = conf.offline_news
    # 并发采集全部板块；return_exceptions=True 让单个板块失败不中断整体。
    groups = await asyncio.gather(
        *(crawl_module(module, cfg["per_module_limit"]) for module in cfg["modules"]),
        return_exceptions=True,
    )
    # 以 news_uid 为键去重：同一文章被多板块/多源重复采集时只保留一份。
    unique: Dict[str, CrawledNews] = {}
    # zip 把板块名与采集结果一一配对，便于识别哪个板块失败。
    for module, group in zip(cfg["modules"], groups):
        # gather 把异常对象放进结果列表，isinstance 判断该板块是否失败。
        if isinstance(group, Exception):
            # 板块失败只记 warning 并跳过，不影响其它板块。
            logger.warning(f"[offline-crawler] 板块 {module} 采集失败: {group}")
            continue
        # 遍历该板块成功采集到的新闻，逐条写入去重字典。
        for item in group:
            unique[item.news_uid] = item  # 同一指纹只保留最后一条，天然去重。
    # 记录本轮有效新闻总数，便于调度任务核对采集量。
    logger.info(f"[offline-crawler] 本轮共采集 {len(unique)} 条有效新闻")
    return list(unique.values())
