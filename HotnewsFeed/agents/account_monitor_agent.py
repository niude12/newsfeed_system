#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Account Monitor Agent：管理持续账户监控任务并编排首次检查。

以 A2A 服务器形式暴露「账户监控」能力的子 Agent。注册/停止持续监控任务、立即检查
新发布、查询监控状态；B 站新视频经 mcp_access 网关提取字幕/ASR 并写入 MySQL。
业务逻辑全部委托给 account_monitor.service.AccountMonitorService。全量立即检查采用单实例后台
线程执行，A2A 请求只负责启动任务并立即回传，避免视频处理超过客户端超时时间。

模块依赖:
- ``python_a2a``             : 官方 A2A 包（A2AServer / AgentCard / AgentSkill / run_server）。
- ``a2a.protocol``           : 项目内 A2A 适配层（encode_result / parse_task / reply_text）。
- ``account_monitor``        : 包入口，导出 AccountMonitorService（账户监控业务编排）。
- ``mcp_servers.mcp_access`` : sync_call 把异步协程桥进同步上下文。
- ``create_logger.logger``   : 全局日志器。
- ``threading``              : 在后台线程执行耗时的全量检查，并用锁阻止重复启动。

典型调用链::

    coordinator / 其它 Agent
      -> delegate("account_monitor", "register_monitor" | "check_monitors" | ...)
      -> 本 Agent.handle_message(message)
        -> parse_task(message)                       # 解析任务类型与参数
        -> self.register_monitor / self.start_check_all / service.status / service.stop_account
        -> encode_result(ok, result, err)            # 编码回传 JSON
      -> reply_text(...)                             # 回传消息

对外暴露的接口：
- AccountMonitorAgent       : A2A 服务器子类，处理 register_monitor / check_monitors /
                              monitor_status / stop_monitor 四种任务；check_monitors 立即返回受理状态。
- create_account_monitor_agent : 工厂函数，返回实例并打印启动信息。
- __main__ 入口             : run_server 在 127.0.0.1:8009 启动 A2A 服务。
"""

import threading

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from account_monitor import AccountMonitorService
from create_logger import logger
from mcp_servers.mcp_access import sync_call


# AgentCard：描述本 Agent 的元信息，供 A2A 网络发现与能力协商使用。
agent_card = AgentCard(
    name="account_monitor",                # Agent 名，A2A 委派时按此定位。
    description=(
        "账户监控 Agent：注册或停止持续监控任务、立即检查新发布、查询监控状态；"
        "B站新视频经 Video MCP 提取字幕/ASR并写入MySQL"
    ),
    url="http://localhost:8009",           # A2A 服务端点，与 __main__ 端口一致。
    version="1.0.0",                       # 版本号。
    capabilities={"streaming": False, "memory": True},  # 同步应答；监控任务即“记忆”载体。
    skills=[
        AgentSkill(
            id="register_monitor",         # 技能 ID，也是 A2A 任务类型。
            name="register_monitor",
            description="注册账户持续监控（注册后不立即检查，由定时调度器或「立即检查全部」后台任务完成采集与视频处理）",
            tags=["account", "monitor", "bilibili", "register"],  # 技能标签便于发现。
            examples=[  # 示例输出，供对端了解返回结构。
                '{"account":"bilibili_312249633","platform":"bilibili",'
                '"registered":true,"checked":{"fetched":10,"new":10,"first_run":true}}'
            ],
            input_modes=["application/json"], output_modes=["application/json"],  # JSON 入、JSON 出。
        ),
        AgentSkill(
            id="check_monitors",           # 技能 ID，也是 A2A 任务类型。
            name="check_monitors",
            description="在后台启动全量检查并立即返回；发现新发布后进行去重、视频处理和通知",
            tags=["account", "monitor", "check"],  # 技能标签便于发现。
            examples=['{"accepted":true,"started":true,"running":true}'],  # 示例输出。
            input_modes=["application/json"], output_modes=["application/json"],  # JSON 入、JSON 出。
        ),
        AgentSkill(
            id="monitor_status",           # 技能 ID，也是 A2A 任务类型。
            name="monitor_status",
            description="返回当前监控账户、已发现发布数量、最后检查时间和错误",
            tags=["account", "monitor", "status"],  # 技能标签便于发现。
            examples=['[{"account":"bilibili_312249633","post_count":10,"last_error":null}]'],  # 示例输出。
            input_modes=["application/json"], output_modes=["application/json"],  # JSON 入、JSON 出。
        ),
    ],
)


class AccountMonitorAgent(A2AServer):
    """持续账户监控的下游业务 Agent；Coordinator 只负责把大任务交给它。

    继承 ``python_a2a.A2AServer``。构造时创建 AccountMonitorService 业务对象，
    handle_message 根据 task_type 分发到对应业务方法，最后统一编码回传。
    """

    name = "account_monitor"                              # Agent 名，与 AgentCard.name 一致。
    role = "账户监控注册 · 增量检查 · 视频处理 · 状态管理"  # 职责描述，打印用。

    def __init__(self):
        # 把模块级 agent_card 传给父类，注册本 Agent 的元信息与技能。
        super().__init__(agent_card=agent_card)
        # 业务编排对象：负责注册、检查、通知、状态查询等全部账户监控逻辑。
        self.service = AccountMonitorService()
        # 保护全量检查线程的读取与替换，防止两个 A2A 请求同时通过“未运行”判断。
        self._check_all_lock = threading.Lock()
        # 保存当前/最近一次全量检查线程；None 表示 Agent 启动后尚未触发过检查。
        self._check_all_thread = None

    def _run_check_all_background(self) -> None:
        """在后台线程中完成全量账户检查，并把最终情况写入日志。

        返回:
            None：检查结果由 AccountMonitorService 写入 MySQL，本方法不向原 A2A 请求回传。

        说明:
            - ``service.check_all`` 是异步方法；后台线程没有常驻事件循环，因此继续使用
              ``sync_call`` 建立并驱动事件循环。
            - ``check_all`` 内部会把每个账户的完成时间和错误写入 MySQL；前端随后点击
              “查看状态”即可读取最终结果。
            - 最外层异常仍需捕获，避免后台线程静默退出而没有排障日志。
        """
        logger.info("[account-monitor-agent] 后台全量检查开始")
        try:
            # 在独立后台线程内运行异步编排；耗时不再占用发起请求的 A2A HTTP 连接。
            results = sync_call(self.service.check_all())
            # 只记录账户数量，避免把可能很大的视频处理结果完整写入日志。
            logger.info(
                f"[account-monitor-agent] 后台全量检查完成，共处理 {len(results)} 个账户"
            )
        except Exception as exc:
            # 记录未被 service.check_all 内部消化的系统级异常，保留堆栈便于定位。
            logger.error(
                f"[account-monitor-agent] 后台全量检查失败: {exc}", exc_info=True
            )

    def start_check_all(self) -> dict:
        """启动单实例后台全量检查，并立即返回任务受理状态。

        返回:
            dict：包含 accepted / started / running / message：
            - 首次启动时 started=True；
            - 已有检查仍在运行时 started=False，不再创建重复线程；
            - accepted 始终为 True，表示“立即检查”请求已被系统正常接收。

        说明:
            ``threading.Thread`` 使用 daemon=True，使后台检查不会阻止 Agent 服务正常退出。
            本方法只负责启动，不等待 ``check_all`` 完成，因此能在 A2A 客户端的 30 秒
            请求超时之前立即返回。
        """
        # 锁住“检查线程是否存活 → 是否创建新线程”这一整个判断与更新过程。
        with self._check_all_lock:
            # 已有线程仍在处理时直接返回，避免重复采集、重复视频转写和临时文件竞争。
            if self._check_all_thread is not None and self._check_all_thread.is_alive():
                return {
                    "accepted": True,   # 请求合法且已被受理。
                    "started": False,   # 本次没有重复创建后台任务。
                    "running": True,    # 现有全量检查仍在运行。
                    "message": "已有账户全量检查正在后台运行，请稍后查看状态",
                }
            # 创建新的守护线程；明确命名，方便调试器和线程转储识别。
            self._check_all_thread = threading.Thread(
                target=self._run_check_all_background,
                name="account-monitor-check-all",
                daemon=True,
            )
            # 在线程引用保存完成后再启动，确保紧随其后的请求能识别正在运行的任务。
            self._check_all_thread.start()
            return {
                "accepted": True,   # 请求已被正常接收。
                "started": True,    # 本次成功创建了新的后台任务。
                "running": True,    # 返回时后台线程已经启动。
                "message": "账户全量检查已在后台启动，请稍后查看状态",
            }

    def register_monitor(
            self,
            account: str,
            platform: str,
            url: str,
            check_now: bool = False,
    ) -> dict:
        """注册账户持续监控任务；默认只入库立即返回，不做同步检查。

        参数:
            account:   账户标识（如 "bilibili_312249633"）。
            platform:  平台（"bilibili" / "rss" 等）。
            url:       账户主页 / 订阅地址。
            check_now: 注册后是否立即检查一次（True 则把检查结果挂到返回的 checked 键）。
                       默认 False：注册只持久化任务立即返回，采集/视频处理由定时调度器
                       （scheduler.account_monitor_scheduler）或「立即检查全部」后台任务完成，
                       避免同步等待视频转写超过 A2A 客户端 30 秒超时。

        返回:
            dict：{account, platform, url, registered}；check_now=True 时追加 checked 键，
            值为 check_account 的返回 dict（fetched / new / first_run / notified 等）。

        说明:
            - ``self.service.register_account`` 把监控任务持久化到 MySQL
              （account_monitor.store），定时器后续从 MySQL 读取该任务。
            - ``sync_call`` 把异步的 ``check_account`` 桥进同步上下文。
        """
        # 先持久化注册监控任务（写 MySQL）。
        result = self.service.register_account(account, platform, url)
        if check_now:
            # 立即检查一次：异步桥同步，结果挂到返回 dict 的 checked 键。
            result["checked"] = sync_call(
                self.service.check_account(account, platform, source_url=url)
            )
        return result

    def handle_message(self, message):
        """处理收到的 A2A 消息：按 task_type 分发到业务方法。

        参数:
            message: python_a2a.Message 实例（含 metadata.custom_fields 路由信息）。

        返回:
            reply_text 构造的回传 Message，内容为 {ok,result,error} JSON 文本。

        说明:
            - 支持四种任务：register_monitor（注册）、check_monitors（全量检查）、
              monitor_status（状态查询）、stop_monitor（停止监控）。
            - check_monitors 调 ``start_check_all()`` 启动单实例后台线程并立即返回，避免
              A2A 请求同步等待采集、视频处理和通知；其余为 Service 的同步方法直接调用。
        """
        # 从消息 metadata.custom_fields 解析出任务类型、参数与来源 Agent。
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[account-monitor-agent] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            # 按任务类型分发：注册 / 全量检查 / 状态查询 / 停止监控。
            if task_type == "register_monitor":
                result = self.register_monitor(
                    params.get("account", ""), params.get("platform", "bilibili"),
                    params.get("url", ""), params.get("check_now", False),
                )
            elif task_type == "check_monitors":
                # 只启动后台全量检查并立即返回，不让当前 A2A HTTP 请求等待耗时业务完成。
                result = self.start_check_all()
            elif task_type == "monitor_status":
                # 状态查询是同步方法，直接调用。
                result = self.service.status()
            elif task_type == "stop_monitor":
                result = self.service.stop_account(
                    params.get("account", ""), params.get("platform", "bilibili")
                )
            else:
                raise ValueError(f"未知任务类型: {task_type}")
            # 成功：把结果编码成 {ok,result,error} 回传 JSON。
            text = encode_result(True, result, None)
        except Exception as exc:
            logger.error(f"[account-monitor-agent] 处理失败: {exc}", exc_info=True)
            # 失败：错误信息编码进回传 JSON 的 error 字段。
            text = encode_result(False, None, str(exc))
        # 构造回传消息，挂上父消息 ID 与会话 ID。
        return reply_text(text, message.message_id, message.conversation_id)


def create_account_monitor_agent():
    """AccountMonitorAgent 工厂函数：打印启动信息并返回实例。

    返回:
        AccountMonitorAgent 实例（供 run_server 使用）。
    """
    # 打印启动信息，便于确认加载的是哪个 Agent。
    logger.info("=== 账户监控 Agent 信息 ===")
    logger.info(f"名称: {AccountMonitorAgent.name}")
    logger.info(f"职责: {AccountMonitorAgent.role}")
    return AccountMonitorAgent()  # 返回实例供 run_server 启动 HTTP 服务。


if __name__ == "__main__":
    # 启动 A2A HTTP 服务：监听 127.0.0.1:8009，与 agent_card.url 保持一致。
    run_server(create_account_monitor_agent(), host="127.0.0.1", port=8009)
