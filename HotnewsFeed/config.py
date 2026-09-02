#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全局配置模块（config.py）——读取 config.ini 的唯一入口。

文件名: config.py
项目: HotnewsFeed

本模块干什么：
    提供 Config 类，读取项目根目录的 config.ini（仿照 mcp_order_server 依赖的 SmartVoyage.config）。
    其它模块用它拿 LLM / MySQL / 温度 / Redis / Milvus / 推送通道 / 账户监控 等全局配置，
    不再各自硬编码。每个配置项都是一次性解析的 @property，返回 dict 或标量。

模块依赖:
- ``configparser``   : Python 标准库的 INI 解析器，把 config.ini 解析成 节(section) → 键 → 值。
- ``pathlib.Path``   : 路径工具。PROJECT_ROOT 定位项目根目录（本文件所在目录），
                       相对路径的日志 / 输出 / 临时目录都基于它解析成绝对路径。
- 依赖本模块的调用方 : tools/*、agents/*、task_pipelines/*、offline_news/*、scheduler/* 等，
                       几乎全项目都通过 ``conf = Config()`` 拿配置。

典型调用链::

    tools/bilibili.py             ->  conf.account_monitor          # B 站 Cookie 登录态
    tools/collect.py              ->  conf.rss_sources / conf.accounts  # RSS 源与账户地址
    tools/process.py              ->  conf.embedding_model          # 聚类向量化模型名
    tools/publish.py              ->  conf.publish                  # 简报推送通道
    create_logger.py              ->  conf.log_file                 # 日志文件路径
    scheduler/account_monitor     ->  conf.account_monitor["interval_minutes"]

对外暴露：
- ``Config`` : 全局配置类。实例化时自动读取 config.ini，可作为单例在模块顶层创建并复用。

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
    """去掉 config.ini 值两侧的引号与首尾空白，返回干净的字符串。

    配置文件里手写的值常带引号（如 base_url = 'https://...' 或 "..."），
    configparser 会原样返回含引号的字符串，这里统一剥掉一层再返回。

    参数:
        value: configparser 返回的原始值；None 时按空串处理（容错）。

    返回:
        清洗后的字符串：先 strip() 去掉首尾空白，再去掉首尾成对的引号字符。

    说明:
        str.strip(chars) 会去掉首尾所有属于 chars 的字符，因此 'xxx' 和 "xxx"
        都能一次剥干净；None 直接返回 ""，避免调用方再做 None 判断。
    """
    if value is None:
        return ""  # None 直接返回空串，上层无需再判空
    return value.strip().strip("'\"")  # 先去首尾空白，再剥掉成对的 ' 或 " 引号


class Config:
    """读取 config.ini 的全局配置类（项目唯一配置入口）。

    实例化时（__init__）立即用 configparser 读取 config.ini 并解析到 self._parser，
    之后所有 get() / @property 都实时从解析结果里取值。
    项目各模块通常在模块顶层建一个单例 ``conf = Config()`` 复用，避免每个函数重复读文件。
    """

    def __init__(self, config_path=None):
        """初始化配置对象：读取并解析 config.ini。

        参数:
            config_path: config.ini 的路径；为 None 时默认取项目根目录下的 config.ini。

        抛出:
            FileNotFoundError: 指定路径的 config.ini 不存在或不可读时抛出
                               （configparser.read 读取失败会返回空列表）。

        说明:
            configparser.ConfigParser() 是标准库的 INI 解析器，把配置文件解析成
            节(section) → 键 → 值的多层结构，供后续 get() / @property 查询。
            read(path, encoding="utf-8") 的返回值是被成功读取的文件列表，
            为空即表示文件不存在 / 读不了，据此抛 FileNotFoundError。
        """
        # config.ini 默认在项目根目录；也可以手动传入指定路径
        self.config_path = config_path or str(PROJECT_ROOT / "config.ini")
        self._parser = configparser.ConfigParser()  # 标准库 INI 解析器，后续 get()/@property 都查它
        # encoding="utf-8"：配置里有中文也能正常读
        read_ok = self._parser.read(self.config_path, encoding="utf-8")
        if not read_ok:  # read() 返回实际读取成功的文件列表；为空说明文件不存在/不可读
            raise FileNotFoundError(f"config.ini not found: {self.config_path}")  # 配置缺失属启动期致命错误，直接抛出

    # ===== 通用读取 =====
    def get(self, section, key, default=None):
        """读取 config.ini 某节某键的值（自动去引号），不存在时返回 default。

        参数:
            section: 节名，如 "llm" / "mysql"（对应 config.ini 里的 [llm] / [mysql]）。
            key:     键名，如 "base_url"。
            default: 节或键不存在时返回的兜底值（默认 None）。

        返回:
            _clean() 清洗后的字符串；节 / 键缺失时返回 default。

        抛出:
            不抛出。configparser.NoSectionError / NoOptionError 都被捕获并转为返回 default。

        说明:
            configparser.ConfigParser.get(section, key) 是标准库查询接口，
            当节不存在时抛 NoSectionError、键不存在时抛 NoOptionError，
            这里统一捕获，让调用方拿 default 优雅降级，避免到处 try/except。
        """
        try:
            return _clean(self._parser.get(section, key))  # 取原始值并经 _clean 剥引号
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default  # 节/键缺失时优雅降级为默认值，不向调用方抛异常

    # ===== 便捷属性 =====
    @property
    def llm(self):
        """LLM 对话配置（dashscope 兼容接口，如 qwen-plus）。

        返回:
            dict：{"base_url": 接口地址, "api_key": 密钥, "model_name": 模型名}。
            三个键都来自 config.ini 的 [llm] 段；被 agents/* 与 tools/* 拿去构造
            LangChain 的 OpenAI 兼容客户端（base_url + api_key + model_name）。
        """
        return {
            "base_url": self.get("llm", "base_url"),      # OpenAI 兼容接口地址（dashscope）
            "api_key": self.get("llm", "api_key"),        # 调用 LLM 用的密钥
            "model_name": self.get("llm", "model_name"),  # 模型名，如 qwen-plus
        }

    @property
    def mysql(self):
        """MySQL 数据库配置（news_rag 库，供离线新闻存取）。

        返回:
            dict：host / port / user / password / database / charset。
            port 与 charset 提供默认值（3306 / utf8mb4），其中 port 用 int() 转成整数；
            供 offline_news/stores.py 构造连接串读写离线新闻库。
        """
        return {
            "host": self.get("mysql", "host"),                          # 数据库主机地址
            "port": int(self.get("mysql", "port", default="3306")),     # 端口，默认 3306，字符串转 int
            "user": self.get("mysql", "user"),                          # 用户名
            "password": self.get("mysql", "password"),                  # 密码
            "database": self.get("mysql", "database"),                  # 库名（news_rag）
            "charset": self.get("mysql", "charset", default="utf8mb4"),  # 字符集，默认 utf8mb4
        }

    @property
    def redis(self):
        """Redis 缓存配置（离线新闻精确查询缓存）。

        返回:
            dict：host / port / password / db / key_prefix / cache_ttl_seconds。
            password 为空字符串时转成 None（表示无密码）；cache_ttl_seconds 单位秒，
            默认 21600（6 小时）。供 offline_news/stores.py 构造 redis 客户端做查询缓存。
        """
        return {
            "host": self.get("redis", "host", default="localhost"),                    # Redis 主机地址
            "port": int(self.get("redis", "port", default="6379")),                   # Redis 端口
            "password": self.get("redis", "password") or None,                        # 密码为空则转 None（表示无密码）
            "db": int(self.get("redis", "db", default="0")),                          # 逻辑库编号
            "key_prefix": self.get("redis", "key_prefix", default="hotnews:offline"),  # 缓存键前缀，隔离业务
            "cache_ttl_seconds": int(self.get("redis", "cache_ttl_seconds", default="21600")),  # 缓存有效期（秒），默认 6 小时
        }

    @property
    def milvus(self):
        """Milvus 向量库配置（离线新闻向量索引）。

        返回:
            dict：uri（连接地址）/ database_name / collection_name / dimension（向量维度）。
            供 offline_news/stores.py 连接 Milvus 做向量检索：先查 Redis 精确缓存，
            再向量检索拿 ID，最后回 MySQL 取原文（最多 3 条）。
        """
        return {
            "uri": self.get("milvus", "uri", default="http://localhost:19530"),           # Milvus 连接地址
            "database_name": self.get("milvus", "database_name", default="news_rag"),     # 数据库名
            "collection_name": self.get("milvus", "collection_name", default="hotnews_offline"),  # 向量集合名
            "dimension": int(self.get("milvus", "dimension", default="1024")),            # 向量维度
        }

    @property
    def offline_news(self):
        """离线新闻采集、保留期限、日报与调度配置。

        返回:
            dict，键包括：
            - modules               : 采集模块名列表（逗号分隔字符串拆成 list 并去空白）
            - per_module_limit      : 每模块采集条数上限
            - retention_days        : 新闻保留天数（到期清理）
            - query_top_k           : 离线查询返回条数上限（min(3, 配置值)，防止取太多）
            - schedule_hour/minute  : 每日定时采集的时分
            - fetch_article_content : 是否抓取正文
            - article_concurrency   : 抓正文的并发数
            - daily_briefing_*      : 日报开关 / 条数 / 推送通道

        说明:
            modules 与 daily_briefing_channels 都是「逗号分隔字符串」，这里先 split
            再 strip 过滤空串；布尔配置用 ``.lower() in ("1","true","yes","on")``
            统一做真值判断，兼容 1/true/yes/on 四种写法。
        """
        # 逗号分隔字符串先读出来，下面拆成列表；缺省为五大模块
        modules = self.get("offline_news", "modules", default="科技,财经,体育,娱乐,国际")
        # 日报推送通道同样以逗号分隔，缺省只发 web_ui
        channels = self.get("offline_news", "daily_briefing_channels", default="web_ui")
        return {
            "modules": [m.strip() for m in modules.split(",") if m.strip()],   # 逗号拆成模块名列表，去掉空白/空串
            "per_module_limit": int(self.get("offline_news", "per_module_limit", default="50")),  # 每模块采集条数上限
            "retention_days": int(self.get("offline_news", "retention_days", default="3")),       # 新闻保留天数（到期清理）
            "query_top_k": min(3, int(self.get("offline_news", "query_top_k", default="3"))),     # 离线查询返回条数上限（≤3）
            "schedule_hour": int(self.get("offline_news", "schedule_hour", default="6")),         # 每日定时采集小时
            "schedule_minute": int(self.get("offline_news", "schedule_minute", default="0")),     # 每日定时采集分钟
            # 布尔字符串统一按 1/true/yes/on 判定（兼容多种写法，先小写再比对）。
            "fetch_article_content": self.get("offline_news", "fetch_article_content", default="true").lower()
            in ("1", "true", "yes", "on"),                                                        # 是否抓取正文
            "article_concurrency": int(self.get("offline_news", "article_concurrency", default="5")),  # 抓正文的并发数
            "daily_briefing_enabled": self.get(  # 是否启用每日简报
                "offline_news", "daily_briefing_enabled", default="true"
            ).lower() in ("1", "true", "yes", "on"),
            "daily_briefing_top_n": int(self.get(  # 每日简报条数
                "offline_news", "daily_briefing_top_n", default="10"
            )),
            "daily_briefing_channels": [  # 日报推送通道列表
                value.strip() for value in channels.split(",") if value.strip()
            ] or ["web_ui"],
        }

    @property
    def temperature(self):
        """LLM 采样温度（默认 0.1，偏向确定性输出）。

        返回:
            float。数值越小输出越确定，越大越随机；
            供 agents/* 构造 LLM 时传给模型，控制回答的稳定性。
        """
        return float(self.get("temperature", "temperature", default="0.1"))  # 字符串转 float，默认 0.1（偏向确定性输出）

    @property
    def log_file(self):
        """日志文件绝对路径（供 create_logger 使用）

        优先取 config.ini [log] log_file：
          - 值是普通路径（如 logs/app.log / D:/xx.log）→ 解析为绝对路径；
          - 值是表达式占位（如 os.path.join(...)）或缺失 → 回退 项目根/logs/app.log。

        返回:
            绝对路径字符串（str），create_logger.setup_logger 直接用它在 os.makedirs
            之后创建 FileHandler。

        说明:
            - 若 raw 里含 ' " ( ) = , 空格 等字符，说明配置写的是表达式占位
              （如 os.path.join(...)）而不是普通路径，此时不解析、直接回退默认值。
            - Path(raw).is_absolute() 判断是否绝对路径；相对路径则基于 PROJECT_ROOT 拼绝对。
            - 最终兜底 项目根/logs/app.log，保证日志文件总有一个落点。
        """
        # 读 [log] log_file 并去首尾空白；没配或为空则走底部默认值
        raw = (self.get("log", "log_file") or "").strip()
        # 配置值含引号/括号/等号/逗号/空格时，视为表达式占位而非普通路径，直接回退默认值。
        if raw and not any(ch in raw for ch in "'\"()=, "):
            p = Path(raw)  # 包装成 Path 以复用 is_absolute() 与 "/" 拼接运算符
            return str(p if p.is_absolute() else PROJECT_ROOT / p)  # 绝对路径原样返回，相对路径基于项目根拼绝对
        return str(PROJECT_ROOT / "logs" / "app.log")  # 兜底日志路径，保证必有落点

    # ===== 工具层用到的配置（tools/collect · process · publish）=====
    def _section_items(self, section):
        """读取 config.ini 某节的全部键值对（自动去引号）；节不存在返回空字典。

        参数:
            section: 节名，如 "rss_sources" / "accounts"。

        返回:
            dict[str, str]：该节下所有 键 → 清洗后值；节不存在时返回 {} 而非抛错。

        说明:
            configparser.ConfigParser.items(section) 返回该节所有键值对的列表，
            这里用字典推导式逐项 _clean 清洗；NoSectionError 捕获后返回空字典，
            让调用方（rss_sources / accounts）拿到 {} 优雅降级。
        """
        try:
            return {k: _clean(v) for k, v in self._parser.items(section)}  # 整节转成 dict 并逐项清洗
        except configparser.NoSectionError:
            return {}  # 节不存在时返回空字典，调用方优雅降级

    @property
    def embedding_model(self):
        """embedding 模型名（聚类向量化用；复用 [llm] 的 base_url/api_key）。

        返回:
            str，来自 config.ini [embedding] 段的 model_name，默认 text-embedding-v3；
            供 tools/process.py 构造 embedding 客户端，对新闻标题 / 摘要做向量化聚类。
        """
        return self.get("embedding", "model_name", default="text-embedding-v3")  # 从 [embedding] 段取模型名，缺省 text-embedding-v3

    @property
    def publish(self):
        """简报推送通道配置（config.ini [publish]；留空 = 该通道优雅降级）。

        返回:
            dict：飞书 webhook / 通用 webhook / SMTP(host·port·user·password) / 收件人
            / 简报输出目录。smtp_port 默认 465 且转成 int；output_dir 为相对路径时
            基于 PROJECT_ROOT 解析并 resolve() 成绝对路径。
            供 tools/publish.py 推送简报（飞书 / 邮件 / Webhook / 落盘）。
        """
        port = self.get("publish", "smtp_port", default="465") or "465"  # SMTP 端口，缺省 465（SSL）
        output_dir = Path(self.get("publish", "output_dir", default="output/briefings"))  # 简报落盘目录
        if not output_dir.is_absolute():
            output_dir = PROJECT_ROOT / output_dir  # 相对路径基于项目根解析成绝对路径
        return {
            "feishu_webhook": self.get("publish", "feishu_webhook"),   # 飞书机器人 webhook 地址（留空=不启用）
            "webhook_url": self.get("publish", "webhook_url"),         # 通用 webhook 地址（留空=不启用）
            "smtp_host": self.get("publish", "smtp_host"),             # SMTP 服务器地址（留空=不启用邮件）
            "smtp_port": int(port),                                    # SMTP 端口（字符串转 int）
            "smtp_user": self.get("publish", "smtp_user"),             # SMTP 用户名
            "smtp_password": self.get("publish", "smtp_password"),     # SMTP 密码
            "mail_to": self.get("publish", "mail_to"),                 # 简报收件人
            "output_dir": str(output_dir.resolve()),                   # 简报输出目录（绝对路径）
        }

    @property
    def rss_sources(self):
        """模块 → RSS 源 URL 列表（config.ini [rss_sources]；模块缺省用 tools/collect.py 内置默认）。

        返回:
            dict[str, list[str]]：模块名 → 该模块的 RSS 源 URL 列表。
            逗号分隔的源地址被拆成 list 并去掉空白 / 空串；
            供 tools/collect.py 按模块拉取 RSS 源，也供 coordinator 识别自定义模块名。
        """
        # 每个模块的源地址以逗号分隔，拆成 list 并去空白/空串；整体为「模块→源列表」字典
        return {name: [u.strip() for u in v.split(",") if u.strip()]
                for name, v in self._section_items("rss_sources").items()}

    @property
    def accounts(self):
        """账户 → 抓取地址映射（config.ini [accounts]；持续监控只接受真实数据）。

        返回:
            dict[str, str]：账户标识（如 bilibili_312249633）→ 主页 / 抓取地址。
            直接复用 _section_items 的「整节读成字典」能力；
            供 account_monitor 注册监控时校验账户地址（无真实地址则拒绝注册）。
        """
        return self._section_items("accounts")  # 直接复用整节读取工具，返回「账户→地址」字典

    @property
    def account_monitor(self):
        """账户轮询、B 站登录态、视频转写和通知配置。

        返回:
            dict，键包括：
            - interval_minutes      : 轮询间隔（分钟）
            - latest_limit          : 每次检查最多返回的新发布条数
            - notify_on_first_run   : 首次注册是否也通知一次
            - process_video_content : 是否对视频做内容转写（asr）
            - notify_channels       : 通知通道列表（默认 ["web_ui"]）
            - bilibili_cookie       : B 站 Cookie 字符串（防 352/412 风控）
            - cookie_file           : B 站 Cookie 文件（绝对路径）
            - asr_model             : 语音转写模型名
            - temp_dir              : 视频临时目录（绝对路径）

        说明:
            cookie_file 与 temp_dir 若是相对路径，会基于 PROJECT_ROOT 解析成绝对路径；
            布尔配置用 ``.lower() in ("1","true","yes","on")`` 做统一真值判断。
            该字典被 tools/bilibili.py、account_monitor/service.py 等按下标直接访问。
        """
        channels = self.get("account_monitor", "notify_channels", default="web_ui")  # 通知通道（逗号分隔）
        temp_dir = Path(self.get("account_monitor", "temp_dir", default="storage/video_temp"))  # 视频临时目录
        # cookie_file 允许留空（未配置则走「无登录态」抓取，命中风控时再提示用户配置）。
        cookie_file = self.get("account_monitor", "cookie_file", default="") or ""
        # cookie 文件 / 临时目录若配的是相对路径，统一基于项目根解析成绝对路径，避免工作目录漂移。
        if cookie_file and not Path(cookie_file).is_absolute():
            cookie_file = str((PROJECT_ROOT / cookie_file).resolve())  # 相对 cookie 路径拼成绝对路径
        if not temp_dir.is_absolute():
            temp_dir = PROJECT_ROOT / temp_dir  # 相对临时目录拼成绝对路径
        return {
            "interval_minutes": int(self.get("account_monitor", "interval_minutes", default="30")),  # 轮询间隔（分钟）
            "latest_limit": int(self.get("account_monitor", "latest_limit", default="10")),          # 每次最多返回的新发布条数
            "notify_on_first_run": self.get(  # 首次注册时是否也发一次通知
                "account_monitor", "notify_on_first_run", default="false"
            ).lower() in ("1", "true", "yes", "on"),
            "process_video_content": self.get(  # 是否对视频做 ASR 内容转写
                "account_monitor", "process_video_content", default="true"
            ).lower() in ("1", "true", "yes", "on"),
            "notify_channels": [c.strip() for c in channels.split(",") if c.strip()] or ["web_ui"],  # 通知通道列表
            "bilibili_cookie": self.get("account_monitor", "bilibili_cookie", default="") or "",     # B 站 Cookie 字符串
            "cookie_file": cookie_file,                                                              # B 站 Cookie 文件路径
            "asr_model": self.get("account_monitor", "asr_model", default="paraformer-realtime-v2"),  # 语音转写模型名
            "temp_dir": str(temp_dir.resolve()),                                                     # 视频临时目录（绝对路径）
        }

    @property
    def agent_runtime(self):
        """自主 Agent 循环的开关、资源上限与兼容回退配置。"""
        enabled = self.get("agent_runtime", "enabled", default="true")
        fallback = self.get("agent_runtime", "fallback_to_workflow", default="true")
        show_trace = self.get("agent_runtime", "show_trace", default="true")
        return {
            "enabled": enabled.lower() in ("1", "true", "yes", "on"),
            "max_iterations": max(1, int(self.get("agent_runtime", "max_iterations", default="8"))),
            "max_agent_calls": max(1, int(self.get("agent_runtime", "max_agent_calls", default="6"))),
            "max_total_seconds": max(10, int(self.get("agent_runtime", "max_total_seconds", default="180"))),
            "fallback_to_workflow": fallback.lower() in ("1", "true", "yes", "on"),
            "show_trace": show_trace.lower() in ("1", "true", "yes", "on"),
        }


if __name__ == "__main__":
    # 自测：打印当前配置（不打印 api_key）
    conf = Config()  # 实例化即读取 config.ini
    print("config.ini:", conf.config_path)  # 打印配置文件路径
    print("llm:", {k: v for k, v in conf.llm.items() if k != "api_key"})  # 打印 LLM 配置，隐去 api_key
    print("mysql:", conf.mysql)  # 打印 MySQL 配置
    print("temperature:", conf.temperature)  # 打印采样温度
