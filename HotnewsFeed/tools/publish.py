# -*- coding: utf-8 -*-
"""数据输出类工具（tools/publish.py）

简报生成与推送
对应 MCP Server：mcp_publish_server（:8006，挂载 PUBLISH_TOOLS）

实现原则：
  1. 真实为主：
     - 简报正文：LLM（qwen-plus）按 prompt/main_prompt.py briefing_prompt 生成；
     - 推送：飞书 webhook / 通用 webhook / 邮件 SMTP 有配置就走真实推送（标准库）。
  2. 优雅降级：
     - LLM 生成失败 → 降级为手写 Markdown 简报；
     - 通道未配置或推送失败 → 该通道标记 False 并记录错误；
     - web_ui 通道始终写本地 output/briefings/ 落盘，保证至少有一条可交付简报产物。
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
    """简报推送结果"""
    briefing_id: str                # 简报 ID
    channels: Dict[str, bool]       # 通道名 → 是否推送成功
    error: str = ""                 # 失败时的错误信息


# ===== 简报正文生成 =====
def _format_markdown(task_result: PipelineResult) -> str:
    """手写 Markdown 简报（LLM 降级用）：标题 + 任务信息 + 条目列表"""
    lines = [
        "# 实时热点资讯简报",
        "",
        f"- 任务类型：{task_result.task_type}",
        f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    items = task_result.items or []
    if not items:
        lines.append("本次无有效结果。")
    for i, it in enumerate(items, 1):
        if isinstance(it, HotEvent):
            lines.append(f"{i}. **{it.title}**"
                         f"（热度 {it.heat_score} · 可信度 {it.credibility} · {len(it.sources)} 来源）")
        elif isinstance(it, NewsItem):
            lines.append(f"{i}. **{it.title}**（{it.source} · {it.published_at}）")
        elif isinstance(it, AccountPost):
            lines.append(f"{i}. **{it.title or it.content}**（{it.account}@{it.platform} · {it.published_at}）")
        else:
            lines.append(f"{i}. {it}")
    lines.append("")
    lines.append("> 本简报由 HotnewsFeed 多智能体系统生成。")
    return "\n".join(lines)


def _generate_briefing(task_result: PipelineResult) -> str:
    """生成简报正文：优先 LLM（briefing_prompt），失败降级为手写 Markdown"""
    try:
        payload = json.dumps(
            {"task_type": task_result.task_type,
             "items": [item.__dict__ for item in (task_result.items or [])]},
            ensure_ascii=False, default=str)
        chain = HotnewsFeedPrompts.briefing_prompt() | llm
        text = (chain.invoke({"task_result": payload}).content or "").strip()
        if text:
            return text
    except Exception as exc:
        logger.warning(f"[publish] LLM 简报生成失败，降级为手写 Markdown: {exc}")
    return _format_markdown(task_result)


# ===== 各通道推送实现（同步，由 publish_briefing 用 to_thread 调起）=====
def _push_feishu(webhook: str, text: str) -> None:
    """飞书自定义机器人 webhook：POST 文本消息"""
    payload = json.dumps({"msg_type": "text", "content": {"text": text}},
                         ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(webhook, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()   # 读一下以触发完整请求


def _push_webhook(url: str, text: str) -> None:
    """通用 webhook：POST JSON（{text: 简报}）"""
    payload = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def _push_email(cfg: Dict[str, str], text: str) -> None:
    """邮件推送：smtplib 发纯文本邮件"""
    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = Header(f"热点简报 {datetime.now().strftime('%Y-%m-%d')}", "utf-8")
    msg["From"] = cfg["smtp_user"]
    msg["To"] = cfg["mail_to"]
    if cfg.get("smtp_port") == 465:
        smtp = smtplib.SMTP_SSL(cfg["smtp_host"], cfg["smtp_port"], timeout=10)
    else:
        smtp = smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=10)
    try:
        smtp.login(cfg["smtp_user"], cfg["smtp_password"])
        smtp.send_message(msg)
    finally:
        smtp.quit()


def _write_local(output_dir: str, briefing_id: str, text: str) -> str:
    """web_ui 通道：简报落盘 output/briefings/，返回文件路径（始终可用）"""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"briefing-{briefing_id}.md"
    path.write_text(text, encoding="utf-8")
    return str(path)


# ===== 对外工具函数 =====
async def publish_briefing(
    task_result: PipelineResult,
    channels: Optional[List[str]] = None,
    template: str = "default",
) -> PublishResult:
    """生成热点简报并推送（飞书 · 邮件 · Webhook · Web UI）

    Args:
        task_result: 上游任务流水线结果（热点 / 最新 / 账户发布）。
        channels: 推送通道列表，None 时默认 ["feishu", "email", "webhook", "web_ui"]。
        template: 简报模板名，默认 "default"（其它模板名暂按 default 处理）。

    Returns:
        PublishResult: 各通道推送状态（channels: {通道: 是否成功}）。
    """
    if template not in ("default", ""):
        logger.warning(f"[publish] 模板 {template} 暂不支持，按 default 处理")
    # 每日多板块可能在同一秒内同时生成，微秒避免文件名覆盖。
    briefing_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    channels = channels or DEFAULT_CHANNELS

    # 简报正文（LLM，失败降级 Markdown）
    text = await asyncio.to_thread(_generate_briefing, task_result)

    p = conf.publish
    results: Dict[str, bool] = {}
    errors: List[str] = []

    async def _run(name: str, fn) -> None:
        """把同步推送函数放进线程池执行，统一记录成败"""
        try:
            await asyncio.to_thread(fn)
            results[name] = True
            logger.info(f"[publish] 通道 {name} 推送成功")
        except Exception as exc:
            results[name] = False
            errors.append(f"{name}: {exc}")
            logger.warning(f"[publish] 通道 {name} 推送失败: {exc}")

    tasks = []
    for ch in channels:
        if ch == "web_ui":
            # 本地落盘：始终可用
            tasks.append(_run("web_ui", lambda: _write_local(
                p["output_dir"] or "output/briefings", briefing_id, text)))
        elif ch == "feishu":
            if p.get("feishu_webhook"):
                tasks.append(_run("feishu", lambda: _push_feishu(p["feishu_webhook"], text)))
            else:
                results["feishu"] = False
                errors.append("feishu: 未配置 [publish] feishu_webhook")
        elif ch == "webhook":
            if p.get("webhook_url"):
                tasks.append(_run("webhook", lambda: _push_webhook(p["webhook_url"], text)))
            else:
                results["webhook"] = False
                errors.append("webhook: 未配置 [publish] webhook_url")
        elif ch == "email":
            if p.get("smtp_host") and p.get("mail_to"):
                tasks.append(_run("email", lambda: _push_email(p, text)))
            else:
                results["email"] = False
                errors.append("email: 未配置 [publish] smtp_host/mail_to")
        else:
            logger.warning(f"[publish] 未知通道 {ch}，跳过")

    if tasks:
        await asyncio.gather(*tasks)

    return PublishResult(briefing_id=briefing_id, channels=results,
                         error="; ".join(errors))
