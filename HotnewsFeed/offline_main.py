#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""离线新闻库测试入口：输入问题，exit 退出。"""

import sys

from offline_news import OfflineNewsService


def format_rows(result: dict) -> str:
    rows = result.get("items", [])
    lines = [f"== 离线新闻（{len(rows)} 条，来源链路：{result.get('source')}） =="]
    for index, row in enumerate(rows, 1):
        lines.append(f"{index}. [{row.get('module')}] {row.get('title')}")
        lines.append(f"   {row.get('source')} · {row.get('published_at') or '时间未知'}")
        if row.get("url"):
            lines.append(f"   {row['url']}")
        text = row.get("summary") or row.get("content") or ""
        if text:
            lines.append(f"   {text[:240]}")
    return "\n".join(lines)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stdin.reconfigure(encoding="utf-8")
    except Exception:
        pass
    service = OfflineNewsService()
    print("离线新闻查询：Redis精确缓存 → Milvus向量检索 → MySQL原文（最多3条）")
    print("输入 exit 退出")
    while True:
        query = input("离线查询> ").strip()
        if query.lower() in ("exit", "quit", "q"):
            break
        if query:
            try:
                print(format_rows(service.query(query)))
            except Exception as exc:
                print(f"[离线查询失败] {exc}")


if __name__ == "__main__":
    main()
