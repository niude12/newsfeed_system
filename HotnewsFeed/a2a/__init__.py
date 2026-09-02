# -*- coding: utf-8 -*-
"""A2A 适配层：基于官方包 python_a2a 的便捷封装

协议类型（Message / MessageRole / AgentCard / A2AServer ...）全部来自官方包
python_a2a，本包不再手写协议类型，只提供项目常用的小工具：

    from a2a import send_task, parse_task, delegate, reply_text
"""

from a2a.protocol import (
    send_task,
    parse_task,
    delegate,
    reply_text,
)

__all__ = ["send_task", "parse_task", "delegate", "reply_text"]
