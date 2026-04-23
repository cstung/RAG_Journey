import os
import smtplib
from email.message import EmailMessage


def _split_emails(value: str) -> list[str]:
    parts = [p.strip() for p in (value or "").replace(";", ",").split(",")]
    return [p for p in parts if p]


def get_notify_emails() -> list[str]:
    return _split_emails(os.getenv("NOTIFY_EMAILS", ""))


def send_email(to_emails: list[str], subject: str, body: str) -> None:
    host = os.getenv("SMTP_HOST", "").strip()
    if not host:
        raise RuntimeError("SMTP_HOST is not set")

    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    sender = os.getenv("SMTP_FROM", "").strip() or username
    use_tls = os.getenv("SMTP_TLS", "true").strip().lower() not in ("0", "false", "no")

    if not sender:
        raise RuntimeError("SMTP_FROM (or SMTP_USERNAME) is required")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(host=host, port=port, timeout=20) as smtp:
        smtp.ehlo()
        if use_tls:
            smtp.starttls()
            smtp.ehlo()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(msg)
