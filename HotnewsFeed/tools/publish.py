# -*- coding: utf-8 -*-
"""数据输出类工具（tools/publish.py）

简报生成与推送
对应 MCP Server：mcp_publish_server（:8006，挂载 PUBLISH_TOOLS）

该模块把任务流水线的结果（热点 / 最新新闻 / 账户发布）整理成一份简报正文，
并推送到飞书 webhook、通用 webhook、邮件 SMTP、本地 web_ui 等通道。

模块依赖:
- ``HotnewsFeedPrompts.briefing_prompt()`` : prompt/main_prompt.py 里的简报模板，拼成 LangChain 链。
- ``ChatOpenAI``                           : langchain_openai，配置来自 Config.llm（qwen-plus · dashscope 兼容）。
- ``Config.publish``                       : config.ini [publish] 段的推送配置字典。
- ``PipelineResult``                       : task_pipelines/schemas.py 的流水线统一返回结构。

实现原则：
  1. 真实为主：
     - 简报正文：LLM（qwen-plus）按 prompt/main_prompt.py briefing_prompt 生成；
     - 推送：飞书 webhook / 通用 webhook / 邮件 SMTP 有配置就走真实推送（标准库）。
  2. 优雅降级：
     - LLM 生成失败 → 降级为手写 Markdown 简报；
     - 通道未配置或推送失败 → 该通道标记 False 并记录错误；
     - web_ui 通道始终写本地 output/briefings/ 落盘，保证至少有一条可交付简报产物。

典型调用链::

    registry.PUBLISH_TOOLS
        ->  publish_briefing(task_result, channels, template)
        ->  _generate_briefing(task_result)          # LLM 生成，失败降级 _format_markdown
        ->  _push_feishu / _push_webhook / _push_email / _write_local
        ->  PublishResult(briefing_id, channels, error)

对外暴露的接口：
- ``publish_briefing`` : 生成简报并推送，返回 PublishResult（各通道成败状态）。
"""

import asyncio
import json
import re
import smtplib
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from email.header import Header
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

from langchain_openai import ChatOpenAI

from config import Config
from create_logger import logger
from prompt.main_prompt import HotnewsFeedPrompts
from task_pipelines.schemas import AccountPost, HotEvent, NewsItem, PipelineResult

# ===== 全局配置与 LLM =====
conf = Config()
# ChatOpenAI：langchain_openai 提供的 OpenAI 兼容客户端，配置复用 [llm] 段
# （base_url / api_key / model_name / temperature），由 prompt 模板 + llm 拼成链后 invoke。
llm = ChatOpenAI(
    model=conf.llm["model_name"],
    base_url=conf.llm["base_url"],
    api_key=conf.llm["api_key"],
    temperature=conf.temperature,
)

# 默认推送通道（缺配置的通道会在 publish_briefing 里被优雅降级）
DEFAULT_CHANNELS = ["feishu", "email", "webhook", "web_ui"]


@dataclass
class PublishResult:
    """简报推送结果（publish_briefing 的返回值）。

    参数:
        briefing_id: 简报 ID（生成时的时间戳，格式 YYYYMMDD-HHMMSS-ffffff）。
        channels:    通道名 → 是否推送成功（{feishu: True, email: False, ...}）。
        error:       失败时的错误信息（多通道错误用 "; " 拼接）；全成功则为空串。
    """
    briefing_id: str                # 简报 ID
    channels: Dict[str, bool]       # 通道名 → 是否推送成功
    error: str = ""                 # 失败时的错误信息


# ===== 简报正文生成 =====
def _format_markdown(task_result: PipelineResult) -> str:
    """手写 Markdown 简报（LLM 降级用）：标题 + 任务信息 + 条目列表。

    当 LLM 生成简报失败或返回空文本时，用这个纯手写的 Markdown 兜底，
    保证 web_ui 落盘 / 各通道推送总有内容可用。

    参数:
        task_result: 上游任务流水线结果（含 task_type 与 items 列表）。

    返回:
        str：Markdown 格式的简报正文。

    说明:
        task_result.items 里的条目可能是 :class:`~task_pipelines.schemas.HotEvent` /
        :class:`~task_pipelines.schemas.NewsItem` / :class:`~task_pipelines.schemas.AccountPost`
        三种之一，分别按各自的字段渲染；其它未知对象直接用 str(it) 兜底。
    """
    # 先拼固定头部：标题 + 任务信息 + 生成时间。
    lines = [
        "# 实时热点资讯简报",
        "",
        f"- 任务类型：{task_result.task_type}",
        f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    # 取出条目列表（None 时按空列表处理）。
    items = task_result.items or []
    # 没有条目就写一句提示，避免正文空白。
    if not items:
        lines.append("本次无有效结果。")
    # 逐条渲染，行首带序号（从 1 开始）。
    for i, it in enumerate(items, 1):
        # 按条目类型分别渲染：热点事件带热度/可信度/来源数，新闻带来源/时间，账户发布带账户/平台。
        if isinstance(it, HotEvent):
            lines.append(f"{i}. **{it.title}**"
                         f"（热度 {it.heat_score} · 可信度 {it.credibility} · {len(it.sources)} 来源）")
        elif isinstance(it, NewsItem):
            lines.append(f"{i}. **{it.title}**（{it.source} · {it.published_at}）")
        elif isinstance(it, AccountPost):
            lines.append(f"{i}. **{it.title or it.content}**（{it.account}@{it.platform} · {it.published_at}）")
        else:
            # 未知类型对象用 str() 兜底渲染，保证不丢内容。
            lines.append(f"{i}. {it}")
    lines.append("")
    lines.append("> 本简报由 HotnewsFeed 多智能体系统生成。")
    # 用换行符把各行拼接成完整的 Markdown 文本。
    return "\n".join(lines)


def _generate_briefing(task_result: PipelineResult) -> str:
    """生成简报正文：优先 LLM（briefing_prompt），失败降级为手写 Markdown。

    参数:
        task_result: 上游任务流水线结果（作为 LLM 模板的 {task_result} 占位输入）。

    返回:
        str：简报正文（LLM 输出或降级的手写 Markdown）。

    说明:
        ``HotnewsFeedPrompts.briefing_prompt()`` 返回一个 ChatPromptTemplate 模板，
        用 ``|`` 与上面的全局 ``llm`` 拼成 LangChain 链，``chain.invoke(...)`` 填充占位符并调用。
        期间任何异常（网络 / 解析 / 空返回）都会落到 ``_format_markdown`` 兜底，
        绝不因为 LLM 不可用而中断整个发布流程。
    """
    # 优先走 LLM 生成，任何异常都落到手写 Markdown 兜底。
    try:
        # 用 json.dumps 把任务结果序列化为 JSON 字符串喂给模板（ensure_ascii=False 保留中文，
        # default=str 兜底处理 dataclass / datetime 等不可直接 JSON 化的对象）。
        payload = json.dumps(
            {"task_type": task_result.task_type,
             "items": [item.__dict__ for item in (task_result.items or [])]},
            ensure_ascii=False, default=str)
        # briefing_prompt 模板 | llm 拼成 LangChain 链。
        chain = HotnewsFeedPrompts.briefing_prompt() | llm
        # invoke 返回的 content 可能是 None，需要转成字符串并去掉首尾空白。
        text = (chain.invoke({"task_result": payload}).content or "").strip()
        # LLM 正常返回非空文本则直接用。
        if text:
            return text
    except Exception as exc:
        logger.warning(f"[publish] LLM 简报生成失败，降级为手写 Markdown: {exc}")
    # 走到这里说明 LLM 失败或返回空文本 → 手写 Markdown 兜底。
    return _format_markdown(task_result)


# ===== 各通道推送实现（同步，由 publish_briefing 用 to_thread 调起）=====
def _push_feishu(webhook: str, text: str) -> None:
    """飞书自定义机器人 webhook：POST 文本消息。

    参数:
        webhook: 飞书自定义机器人 webhook 地址（config.ini [publish] feishu_webhook）。
        text:    要推送的简报正文（纯文本）。

    返回:
        None（HTTP 2xx 即视为成功；失败抛异常由上层 _run 捕获记录）。

    说明:
        urllib.request 是标准库 HTTP 客户端；payload 是飞书「文本消息」约定的 JSON 结构
        {msg_type: text, content: {text: ...}}，必须带 Content-Type: application/json。
    """
    # 构造飞书文本消息的 JSON 并编码成 UTF-8 字节（ensure_ascii=False 保留中文）。
    payload = json.dumps({"msg_type": "text", "content": {"text": text}},
                         ensure_ascii=False).encode("utf-8")
    # 构造 POST 请求：data 即请求体，Content-Type 必须为 application/json。
    req = urllib.request.Request(webhook, data=payload,
                                 headers={"Content-Type": "application/json"})
    # 发起请求，with 保证连接被关闭；读一下响应以触发完整请求发送。
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()   # 读一下以触发完整请求


def _push_webhook(url: str, text: str) -> None:
    """通用 webhook：POST JSON（{text: 简报}）。

    参数:
        url:  通用 webhook 地址（config.ini [publish] webhook_url）。
        text: 要推送的简报正文。

    返回:
        None（HTTP 2xx 即视为成功；失败抛异常由上层 _run 捕获记录）。

    说明:
        与 _push_feishu 的区别只是 payload 结构不同：通用 webhook 约定外层是 {text: 简报}，
        适配企业微信 / Server酱 / 自建回调等「收文本」的网关。
    """
    # 通用 webhook 约定外层是 {text: 简报}，编码成 UTF-8 字节。
    payload = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    # 构造 POST 请求：data 即请求体，Content-Type 为 application/json。
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    # 发起请求，with 保证连接被关闭；读一下响应以触发完整请求发送。
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def _push_email(cfg: Dict[str, str], text: str) -> None:
    """邮件推送：smtplib 发纯文本邮件。

    参数:
        cfg:  config.ini [publish] 段的推送配置字典（需含 smtp_host / smtp_port /
              smtp_user / smtp_password / mail_to）。
        text: 简报正文（作为邮件纯文本正文）。

    返回:
        None（发送成功即返回；失败抛异常由上层 _run 捕获记录）。

    说明:
        - ``MIMEText`` 构造纯文本正文，``Header`` 解决邮件主题中文编码（RFC 2047）。
        - 端口 465 走 ``SMTP_SSL``（SSL 加密）；其它端口走普通 ``SMTP``（StartTLS 之外的直连）。
        - ``smtp.login`` 登录发件账户，``send_message`` 发送，``finally`` 保证连接被 quit 关闭。
    """
    # 构造纯文本邮件正文（UTF-8）。
    msg = MIMEText(text, "plain", "utf-8")
    # Header 处理中文主题（RFC 2047 编码），避免主题乱码。
    msg["Subject"] = Header(f"热点简报 {datetime.now().strftime('%Y-%m-%d')}", "utf-8")
    msg["From"] = cfg["smtp_user"]
    msg["To"] = cfg["mail_to"]
    # 端口 465 走 SMTP_SSL（SSL 加密），其它端口走普通 SMTP 直连。
    if cfg.get("smtp_port") == 465:
        smtp = smtplib.SMTP_SSL(cfg["smtp_host"], cfg["smtp_port"], timeout=10)
    else:
        smtp = smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=10)
    try:
        # 登录发件账户，然后发送邮件。
        smtp.login(cfg["smtp_user"], cfg["smtp_password"])
        smtp.send_message(msg)
    finally:
        # 无论成败都关闭连接，避免资源泄漏。
        smtp.quit()


def _write_local(output_dir: str, briefing_id: str, text: str) -> str:
    """web_ui 通道：简报落盘 output/briefings/，返回文件路径（始终可用）。

    参数:
        output_dir:  简报输出目录（config.ini [publish] output_dir，绝对路径）。
        briefing_id: 简报 ID（用于拼接文件名 briefing-<id>.md）。
        text:        简报正文（Markdown）。

    返回:
        str：写盘后的完整文件路径。

    说明:
        Path.mkdir(parents=True, exist_ok=True) 保证目录不存在时逐级创建；
        该通道不依赖任何外部服务，因此永远成功，是发布链路的最后兜底产物。
    """
    # 输出目录转成 Path 对象。
    out = Path(output_dir)
    # 目录不存在则逐级创建（已存在不报错）。
    out.mkdir(parents=True, exist_ok=True)
    # 拼接文件名：briefing-<简报ID>.md。
    path = out / f"briefing-{briefing_id}.md"
    # 用 UTF-8 写盘，保证中文简报不乱码。
    path.write_text(text, encoding="utf-8")
    return str(path)


# ===== 对外工具函数 =====
async def publish_briefing(
    task_result: PipelineResult,
    channels: Optional[List[str]] = None,
    template: str = "default",
) -> PublishResult:
    """生成热点简报并推送（飞书 · 邮件 · Webhook · Web UI）。

    参数:
        task_result: 上游任务流水线结果（热点 / 最新 / 账户发布），作为简报正文的数据来源。
        channels:    推送通道列表，None 时默认 ["feishu", "email", "webhook", "web_ui"]。
        template:    简报模板名，默认 "default"（其它模板名暂按 default 处理）。

    返回:
        PublishResult：包含 briefing_id 与各通道推送状态（channels: {通道: 是否成功}）
        及汇总错误信息 error。

    说明:
        - ``conf.publish`` 是 Config.publish 属性返回的 [publish] 段配置字典，
          缺省的通道键（feishu_webhook / webhook_url / smtp_host / mail_to）为空即视为未配置，
          该通道直接标记失败并记录「未配置」错误，不发起真实推送。
        - 各通道推送函数都是同步阻塞的，用 ``asyncio.to_thread`` 丢进线程池，
          避免阻塞事件循环；``_run`` 内联函数统一记录每个通道的成功 / 失败。
        - briefing_id 用 datetime 微秒保证同秒多板块并发生成时不重名。
    """
    # 模板名暂只支持 default / 空串，其它名字按 default 处理并告警。
    if template not in ("default", ""):
        logger.warning(f"[publish] 模板 {template} 暂不支持，按 default 处理")
    # 每日多板块可能在同一秒内同时生成，微秒避免文件名覆盖。
    briefing_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    # 通道列表缺省用 DEFAULT_CHANNELS。
    channels = channels or DEFAULT_CHANNELS

    # 简报正文（LLM，失败降级 Markdown），放线程池避免阻塞事件循环。
    text = await asyncio.to_thread(_generate_briefing, task_result)

    # 读一次 [publish] 段配置，后续各通道从它取键。
    p = conf.publish
    # 通道名 → 是否推送成功 的结果字典。
    results: Dict[str, bool] = {}
    # 各通道错误信息汇总列表。
    errors: List[str] = []

    async def _run(name: str, fn) -> None:
        """把同步推送函数放进线程池执行，统一记录成败"""
        try:
            # 同步推送函数丢进线程池执行，避免阻塞事件循环。
            await asyncio.to_thread(fn)
            results[name] = True
            logger.info(f"[publish] 通道 {name} 推送成功")
        except Exception as exc:
            results[name] = False
            errors.append(f"{name}: {exc}")
            logger.warning(f"[publish] 通道 {name} 推送失败: {exc}")

    # 已登记的推送任务列表（并发执行）。
    tasks = []
    # 按通道名分发：各自检查配置，已配置才登记真实推送任务。
    for ch in channels:
        if ch == "web_ui":
            # 本地落盘：始终可用
            tasks.append(_run("web_ui", lambda: _write_local(
                p["output_dir"] or "output/briefings", briefing_id, text)))
        elif ch == "feishu":
            # 飞书通道：未配置 feishu_webhook 时优雅降级（不发起真实请求）。
            if p.get("feishu_webhook"):
                tasks.append(_run("feishu", lambda: _push_feishu(p["feishu_webhook"], text)))
            else:
                results["feishu"] = False
                errors.append("feishu: 未配置 [publish] feishu_webhook")
        elif ch == "webhook":
            # 通用 webhook 通道：未配置 webhook_url 时优雅降级。
            if p.get("webhook_url"):
                tasks.append(_run("webhook", lambda: _push_webhook(p["webhook_url"], text)))
            else:
                results["webhook"] = False
                errors.append("webhook: 未配置 [publish] webhook_url")
        elif ch == "email":
            # 邮件通道：smtp_host 与 mail_to 是硬前提，缺一即降级。
            if p.get("smtp_host") and p.get("mail_to"):
                tasks.append(_run("email", lambda: _push_email(p, text)))
            else:
                results["email"] = False
                errors.append("email: 未配置 [publish] smtp_host/mail_to")
        else:
            logger.warning(f"[publish] 未知通道 {ch}，跳过")

    if tasks:
        # 并发执行所有已登记的推送任务（web_ui 落盘 + 各真实通道）。
        await asyncio.gather(*tasks)

    return PublishResult(briefing_id=briefing_id, channels=results,
                         error="; ".join(errors))
