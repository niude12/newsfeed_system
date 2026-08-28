#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: config.py
项目: HotnewsFeed

本文件干什么：
    提供 Config 类，读取项目根目录的 config.ini（仿照 mcp_order_server 依赖的 SmartVoyage.config）。
    其它模块用它拿 LLM / MySQL / 温度 等全局配置，不再各自硬编码。

    用法：
        from config import Config
        conf = Config()
        conf.llm            # {'base_url': ..., 'api_key': ..., 'model_name': ...}
        conf.mysql          # {'host': ..., 'user': ..., 'password': ..., 'database': ...}
        conf.temperature    # 0.1
"""

import configparser
from pathlib import Path

# 项目根目录 = 本文件所在目录（HotnewsFeed/），日志等相对路径都基于它
PROJECT_ROOT = Path(__file__).resolve().parent


def _clean(value):
    """去掉 config.ini 值两侧的引号（配置里写的是 'xxx'）"""
    if value is None:
        return ""
    return value.strip().strip("'\"")


class Config:
    """读取 config.ini 的配置类"""

    def __init__(self, config_path=None):
        # config.ini 默认在项目根目录；也可以手动传入指定路径
        self.config_path = config_path or str(PROJECT_ROOT / "config.ini")
        self._parser = configparser.ConfigParser()
        # encoding="utf-8"：配置里有中文也能正常读
        read_ok = self._parser.read(self.config_path, encoding="utf-8")
        if not read_ok:
            raise FileNotFoundError(f"config.ini not found: {self.config_path}")

    # ===== 通用读取 =====
    def get(self, section, key, default=None):
        """读取某节某键的值（自动去引号），不存在时返回 default"""
        try:
            return _clean(self._parser.get(section, key))
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    # ===== 便捷属性 =====
    @property
    def llm(self):
        """LLM 配置（qwen-plus · dashscope 兼容接口）"""
        return {
            "base_url": self.get("llm", "base_url"),
            "api_key": self.get("llm", "api_key"),
            "model_name": self.get("llm", "model_name"),
        }

    @property
    def mysql(self):
        """MySQL 配置（news_rag 库）"""
        return {
            "host": self.get("mysql", "host"),
            "port": int(self.get("mysql", "port", default="3306")),
            "user": self.get("mysql", "user"),
            "password": self.get("mysql", "password"),
            "database": self.get("mysql", "database"),
            "charset": self.get("mysql", "charset", default="utf8mb4"),
        }

    @property
    def redis(self):
        """离线新闻精确查询缓存配置。"""
        return {
            "host": self.get("redis", "host", default="localhost"),
            "port": int(self.get("redis", "port", default="6379")),
            "password": self.get("redis", "password") or None,
            "db": int(self.get("redis", "db", default="0")),
            "key_prefix": self.get("redis", "key_prefix", default="hotnews:offline"),
            "cache_ttl_seconds": int(self.get("redis", "cache_ttl_seconds", default="21600")),
        }

    @property
    def milvus(self):
        """离线新闻向量索引配置。"""
        return {
            "uri": self.get("milvus", "uri", default="http://localhost:19530"),
            "database_name": self.get("milvus", "database_name", default="news_rag"),
            "collection_name": self.get("milvus", "collection_name", default="hotnews_offline"),
            "dimension": int(self.get("milvus", "dimension", default="1024")),
        }

    @property
    def offline_news(self):
        """离线采集、保留期限和调度配置。"""
        modules = self.get("offline_news", "modules", default="科技,财经,体育,娱乐,国际")
        channels = self.get("offline_news", "daily_briefing_channels", default="web_ui")
        return {
            "modules": [m.strip() for m in modules.split(",") if m.strip()],
            "per_module_limit": int(self.get("offline_news", "per_module_limit", default="50")),
            "retention_days": int(self.get("offline_news", "retention_days", default="3")),
            "query_top_k": min(3, int(self.get("offline_news", "query_top_k", default="3"))),
            "schedule_hour": int(self.get("offline_news", "schedule_hour", default="6")),
            "schedule_minute": int(self.get("offline_news", "schedule_minute", default="0")),
            "fetch_article_content": self.get("offline_news", "fetch_article_content", default="true").lower()
            in ("1", "true", "yes", "on"),
            "article_concurrency": int(self.get("offline_news", "article_concurrency", default="5")),
            "daily_briefing_enabled": self.get(
                "offline_news", "daily_briefing_enabled", default="true"
            ).lower() in ("1", "true", "yes", "on"),
            "daily_briefing_top_n": int(self.get(
                "offline_news", "daily_briefing_top_n", default="10"
            )),
            "daily_briefing_channels": [
                value.strip() for value in channels.split(",") if value.strip()
            ] or ["web_ui"],
        }

    @property
    def temperature(self):
        """LLM 采样温度（默认 0.1，偏向确定性输出）"""
        return float(self.get("temperature", "temperature", default="0.1"))

    @property
    def log_file(self):
        """日志文件绝对路径（供 create_logger 使用）

        优先取 config.ini [log] log_file：
          - 值是普通路径（如 logs/app.log / D:/xx.log）→ 解析为绝对路径；
          - 值是表达式占位（如 os.path.join(...)）或缺失 → 回退 项目根/logs/app.log。
        """
        raw = (self.get("log", "log_file") or "").strip()
        if raw and not any(ch in raw for ch in "'\"()=, "):
            p = Path(raw)
            return str(p if p.is_absolute() else PROJECT_ROOT / p)
        return str(PROJECT_ROOT / "logs" / "app.log")

    # ===== 工具层用到的配置（tools/collect · process · publish）=====
    def _section_items(self, section):
        """读取某节全部键值（自动去引号）；节不存在返回空字典"""
        try:
            return {k: _clean(v) for k, v in self._parser.items(section)}
        except configparser.NoSectionError:
            return {}

    @property
    def embedding_model(self):
        """embedding 模型名（config.ini [embedding]，聚类向量化用；复用 [llm] 的 base_url/api_key）"""
        return self.get("embedding", "model_name", default="text-embedding-v3")

    @property
    def publish(self):
        """简报推送通道配置（config.ini [publish]；留空 = 该通道优雅降级）"""
        port = self.get("publish", "smtp_port", default="465") or "465"
        output_dir = Path(self.get("publish", "output_dir", default="output/briefings"))
        if not output_dir.is_absolute():
            output_dir = PROJECT_ROOT / output_dir
        return {
            "feishu_webhook": self.get("publish", "feishu_webhook"),
            "webhook_url": self.get("publish", "webhook_url"),
            "smtp_host": self.get("publish", "smtp_host"),
            "smtp_port": int(port),
            "smtp_user": self.get("publish", "smtp_user"),
            "smtp_password": self.get("publish", "smtp_password"),
            "mail_to": self.get("publish", "mail_to"),
            "output_dir": str(output_dir.resolve()),
        }

    @property
    def rss_sources(self):
        """模块 → RSS 源 URL 列表（config.ini [rss_sources]；模块缺省用 tools/collect.py 内置默认）"""
        return {name: [u.strip() for u in v.split(",") if u.strip()]
                for name, v in self._section_items("rss_sources").items()}

    @property
    def accounts(self):
        """账户 → 抓取地址（config.ini [accounts]；持续监控只接受真实数据）"""
        return self._section_items("accounts")

    @property
    def account_monitor(self):
        """账户轮询、B 站登录态、视频转写和通知配置。"""
        channels = self.get("account_monitor", "notify_channels", default="web_ui")
        temp_dir = Path(self.get("account_monitor", "temp_dir", default="storage/video_temp"))
        cookie_file = self.get("account_monitor", "cookie_file", default="") or ""
        if cookie_file and not Path(cookie_file).is_absolute():
            cookie_file = str((PROJECT_ROOT / cookie_file).resolve())
        if not temp_dir.is_absolute():
            temp_dir = PROJECT_ROOT / temp_dir
        return {
            "interval_minutes": int(self.get("account_monitor", "interval_minutes", default="30")),
            "latest_limit": int(self.get("account_monitor", "latest_limit", default="10")),
            "notify_on_first_run": self.get(
                "account_monitor", "notify_on_first_run", default="false"
            ).lower() in ("1", "true", "yes", "on"),
            "process_video_content": self.get(
                "account_monitor", "process_video_content", default="true"
            ).lower() in ("1", "true", "yes", "on"),
            "notify_channels": [c.strip() for c in channels.split(",") if c.strip()] or ["web_ui"],
            "bilibili_cookie": self.get("account_monitor", "bilibili_cookie", default="") or "",
            "cookie_file": cookie_file,
            "asr_model": self.get("account_monitor", "asr_model", default="paraformer-realtime-v2"),
            "temp_dir": str(temp_dir.resolve()),
        }


if __name__ == "__main__":
    # 自测：打印当前配置（不打印 api_key）
    conf = Config()
    print("config.ini:", conf.config_path)
    print("llm:", {k: v for k, v in conf.llm.items() if k != "api_key"})
    print("mysql:", conf.mysql)
    print("temperature:", conf.temperature)
