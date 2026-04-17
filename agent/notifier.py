"""
notifier.py —— 通知模块
=======================
支持:
  1. 飞书 Webhook (推荐,零配置)
  2. 邮件 (SMTP)
  3. 自定义 Webhook (通用 POST)

配置在 config.yaml 的 notify: 段,例:
  notify:
    enabled: true
    on_done: true
    on_failed: true
    on_agent_recover: true
    feishu:
      webhook: "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
    email:
      smtp_host: "smtp.163.com"
      smtp_port: 465
      smtp_user: "xxx@163.com"
      smtp_pass: "xxx"
      to: ["liyufeng@xxx.com"]
      use_ssl: true
    webhook:
      url: "https://your-webhook.example.com/notify"
      headers:
        Authorization: "Bearer xxx"
"""
from __future__ import annotations
import json
import logging
import os
import smtplib
import time
import urllib.request
import urllib.error
from email.mime.text import MIMEText
from typing import Dict, Optional

log = logging.getLogger("notifier")


def _fmt_elapsed(sec: float) -> str:
    if sec < 60:
        return f"{int(sec)}s"
    if sec < 3600:
        return f"{int(sec // 60)}m{int(sec % 60)}s"
    return f"{int(sec // 3600)}h{int((sec % 3600) // 60)}m"


class Notifier:
    def __init__(self, cfg: Dict):
        self.notify_cfg = cfg.get("notify", {})
        self.enabled = self.notify_cfg.get("enabled", False)
        self.on_done = self.notify_cfg.get("on_done", True)
        self.on_failed = self.notify_cfg.get("on_failed", True)
        self.on_recover = self.notify_cfg.get("on_agent_recover", True)

    def notify_task_done(self, task_data: Dict):
        if not self.enabled or not self.on_done:
            return
        rc = task_data.get("return_code", 0)
        elapsed = _fmt_elapsed(task_data.get("elapsed_sec", 0))
        title = f"Task Done: {task_data.get('task_id', '?')}"
        body = (f"Task: {task_data.get('task_id')}\n"
                f"Host: {task_data.get('host')}\n"
                f"GPU: {task_data.get('gpu_ids')}\n"
                f"CMD: {task_data.get('cmd', '')[:120]}\n"
                f"Return Code: {rc}\n"
                f"Elapsed: {elapsed}")
        self._send(title, body, level="success")

    def notify_task_failed(self, task_data: Dict):
        if not self.enabled or not self.on_failed:
            return
        rc = task_data.get("return_code", -1)
        elapsed = _fmt_elapsed(task_data.get("elapsed_sec", 0))
        retry_info = ""
        if task_data.get("max_retries", 0) > 0:
            retry_info = f"\nRetry: {task_data.get('retry_count', 0)}/{task_data.get('max_retries', 0)}"
        tail_lines = task_data.get("tail", [])
        tail_text = "\n".join(tail_lines[-10:]) if tail_lines else "(no tail)"
        title = f"Task FAILED: {task_data.get('task_id', '?')}"
        body = (f"Task: {task_data.get('task_id')}\n"
                f"Host: {task_data.get('host')}\n"
                f"GPU: {task_data.get('gpu_ids')}\n"
                f"CMD: {task_data.get('cmd', '')[:120]}\n"
                f"Return Code: {rc}\n"
                f"Elapsed: {elapsed}{retry_info}\n"
                f"--- Last 10 lines ---\n{tail_text}")
        self._send(title, body, level="error")

    def notify_recovery(self, host: str, task_id: str, action: str):
        if not self.enabled or not self.on_recover:
            return
        title = f"Agent Recovery: {host}"
        body = f"Host: {host}\nTask: {task_id}\nAction: {action}"
        self._send(title, body, level="warning")

    def _send(self, title: str, body: str, level: str = "info"):
        feishu = self.notify_cfg.get("feishu", {})
        if feishu.get("webhook"):
            try:
                self._send_feishu(feishu["webhook"], title, body, level)
            except Exception as e:
                log.warning("feishu notify fail: %s", e)

        email_cfg = self.notify_cfg.get("email", {})
        if email_cfg.get("smtp_host") and email_cfg.get("to"):
            try:
                self._send_email(email_cfg, title, body)
            except Exception as e:
                log.warning("email notify fail: %s", e)

        wh = self.notify_cfg.get("webhook", {})
        if wh.get("url"):
            try:
                self._send_webhook(wh["url"], wh.get("headers", {}), title, body, level)
            except Exception as e:
                log.warning("webhook notify fail: %s", e)

    def _send_feishu(self, webhook: str, title: str, body: str, level: str):
        color_map = {"success": "green", "error": "red", "warning": "orange", "info": "blue"}
        color = color_map.get(level, "blue")
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": color,
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "plain_text", "content": body[:2000]}},
                    {"tag": "note", "elements": [
                        {"tag": "plain_text",
                         "content": f"SafeTransport Cluster | {time.strftime('%Y-%m-%d %H:%M:%S')}"}
                    ]},
                ],
            },
        }
        data = json.dumps(payload).encode("utf-8")
        webhook = self._resolve_env(webhook)
        req = urllib.request.Request(
            webhook, data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()

    @staticmethod
    def _resolve_env(val: str) -> str:
        """支持 ${ENV_VAR} 和 $ENV_VAR 语法引用环境变量,避免明文密码"""
        if not isinstance(val, str):
            return val
        import re
        def replacer(m):
            name = m.group(1) or m.group(2)
            return os.environ.get(name, m.group(0))
        return re.sub(r'\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)', replacer, val)

    def _send_email(self, cfg: Dict, title: str, body: str):
        smtp_user = self._resolve_env(cfg["smtp_user"])
        smtp_pass = self._resolve_env(cfg["smtp_pass"])
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[Cluster] {title}"
        msg["From"] = smtp_user
        msg["To"] = ", ".join(cfg["to"])
        if cfg.get("use_ssl", True):
            server = smtplib.SMTP_SSL(cfg["smtp_host"], int(cfg.get("smtp_port", 465)),
                                      timeout=10)
        else:
            server = smtplib.SMTP(cfg["smtp_host"], int(cfg.get("smtp_port", 587)),
                                  timeout=10)
            server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, cfg["to"], msg.as_string())
        server.quit()

    def _send_webhook(self, url: str, headers: Dict, title: str, body: str, level: str):
        payload = {"title": title, "body": body, "level": level,
                   "time": time.strftime("%Y-%m-%d %H:%M:%S")}
        data = json.dumps(payload).encode("utf-8")
        h = {"Content-Type": "application/json"}
        h.update(headers)
        url = self._resolve_env(url)
        req = urllib.request.Request(url, data=data, headers=h)
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
