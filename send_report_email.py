import argparse
import os
import re
import smtplib
import ssl
from datetime import datetime, timedelta
from email.message import EmailMessage

import duckdb

# Default configuration (mirrors etf_ingest.py)
DEFAULT_DB = 'etf_data.duckdb'
DEFAULT_TABLE = 'etf_prices'
DEFAULT_PERIOD_DAYS = 14
DEFAULT_SUBJECT = 'ETF scanner report - {start} to {end}'


def parse_column(name: str):
    """Split a stored column name into (field, symbol).

    Ingested frames can carry flattened MultiIndex labels such as
    "('Close', 'SPY')"; plain labels such as 'Close' yield an empty symbol.
    """
    text = str(name).strip()
    m = re.fullmatch(r"\((.*),(.*)\)", text)
    if m:
        field = m.group(1).strip().strip('\'"')
        symbol = m.group(2).strip().strip('\'"')
    else:
        field, symbol = text, ''
    return field.strip().lower(), symbol.strip()


def describe_table(con, table_name: str):
    """Return (date_column, {symbol: {field: column}}) for the price table."""
    cols = [row[0] for row in con.execute(f'DESCRIBE {table_name}').fetchall()]
    date_col = None
    by_symbol = {}
    for col in cols:
        field, symbol = parse_column(col)
        if 'date' in field:
            if date_col is None:
                date_col = col
            continue
        by_symbol.setdefault(symbol, {})[field] = col
    if date_col is None and cols:
        date_col = cols[0]
    return date_col, by_symbol


def quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def summarize_symbol(con, table_name: str, date_col: str, fields: dict, start: datetime, end: datetime):
    """Aggregate one symbol over [start, end]; returns None when no rows exist."""
    close_col = fields.get('adj close') or fields.get('close')
    if close_col is None:
        return None

    d = quote(date_col)
    c = quote(close_col)
    high = quote(fields['high']) if 'high' in fields else c
    low = quote(fields['low']) if 'low' in fields else c
    volume = quote(fields['volume']) if 'volume' in fields else 'NULL'

    row = con.execute(
        f'SELECT COUNT(*), MIN(CAST({d} AS DATE)), MAX(CAST({d} AS DATE)), '
        f'MAX({high}), MIN({low}), AVG({volume}) '
        f'FROM {table_name} WHERE CAST({d} AS DATE) BETWEEN ? AND ? AND {c} IS NOT NULL',
        [start.date(), end.date()],
    ).fetchone()

    count, first_date, last_date, period_high, period_low, avg_volume = row
    if not count:
        return None

    first_close = con.execute(
        f'SELECT {c} FROM {table_name} WHERE CAST({d} AS DATE) = ? AND {c} IS NOT NULL LIMIT 1',
        [first_date],
    ).fetchone()[0]
    last_close = con.execute(
        f'SELECT {c} FROM {table_name} WHERE CAST({d} AS DATE) = ? AND {c} IS NOT NULL LIMIT 1',
        [last_date],
    ).fetchone()[0]

    change_pct = (last_close / first_close - 1) * 100 if first_close else None
    return {
        'trading_days': count,
        'first_date': first_date,
        'last_date': last_date,
        'first_close': first_close,
        'last_close': last_close,
        'change_pct': change_pct,
        'high': period_high,
        'low': period_low,
        'avg_volume': avg_volume,
    }


def build_report(db_file: str, table_name: str, period_days: int, end: str | None = None):
    """Collect per-symbol stats for the trailing `period_days` window."""
    if not os.path.exists(db_file):
        raise FileNotFoundError(f'DuckDB file not found: {db_file}')

    con = duckdb.connect(db_file, read_only=True)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [table_name.lower()],
        ).fetchone()[0]
        if not exists:
            raise RuntimeError(f'Table {table_name} not found in {db_file}')

        date_col, by_symbol = describe_table(con, table_name)
        d = quote(date_col)
        if end:
            end_dt = datetime.fromisoformat(end)
        else:
            max_date = con.execute(f'SELECT MAX(CAST({d} AS DATE)) FROM {table_name}').fetchone()[0]
            if max_date is None:
                raise RuntimeError(f'Table {table_name} is empty')
            end_dt = datetime(max_date.year, max_date.month, max_date.day)
        start_dt = end_dt - timedelta(days=period_days)

        rows = []
        for symbol in sorted(by_symbol):
            stats = summarize_symbol(con, table_name, date_col, by_symbol[symbol], start_dt, end_dt)
            if stats is None:
                continue
            stats['symbol'] = symbol or 'ETF'
            rows.append(stats)

        return {
            'db_file': db_file,
            'table': table_name,
            'start': start_dt.strftime('%Y-%m-%d'),
            'end': end_dt.strftime('%Y-%m-%d'),
            'period_days': period_days,
            'rows': rows,
        }
    finally:
        con.close()


def fmt_num(value, digits=2):
    return '-' if value is None else f'{value:,.{digits}f}'


def fmt_pct(value):
    return '-' if value is None else f'{value:+.2f}%'


def render_text(report: dict) -> str:
    lines = [
        f"ETF scanner report for {report['start']} to {report['end']} "
        f"(trailing {report['period_days']} days)",
        '',
    ]
    if not report['rows']:
        lines.append('No price data in this window.')
    for r in report['rows']:
        lines.extend([
            f"{r['symbol']}",
            f"  Close       {fmt_num(r['last_close'])} (from {fmt_num(r['first_close'])})",
            f"  Change      {fmt_pct(r['change_pct'])}",
            f"  High / Low  {fmt_num(r['high'])} / {fmt_num(r['low'])}",
            f"  Avg volume  {fmt_num(r['avg_volume'], 0)}",
            f"  Days        {r['trading_days']} ({r['first_date']} .. {r['last_date']})",
            '',
        ])
    lines.append(f"Source: {report['db_file']} table {report['table']}")
    return '\n'.join(lines)


def render_html(report: dict) -> str:
    header = (
        f"<h2>ETF scanner report</h2>"
        f"<p>{report['start']} to {report['end']} (trailing {report['period_days']} days)</p>"
    )
    if not report['rows']:
        return f"<html><body>{header}<p>No price data in this window.</p></body></html>"

    cells = []
    for r in report['rows']:
        color = '#c0392b' if (r['change_pct'] or 0) < 0 else '#1e8449'
        cells.append(
            '<tr>'
            f"<td>{r['symbol']}</td>"
            f"<td align='right'>{fmt_num(r['last_close'])}</td>"
            f"<td align='right' style='color:{color}'>{fmt_pct(r['change_pct'])}</td>"
            f"<td align='right'>{fmt_num(r['high'])}</td>"
            f"<td align='right'>{fmt_num(r['low'])}</td>"
            f"<td align='right'>{fmt_num(r['avg_volume'], 0)}</td>"
            f"<td align='right'>{r['trading_days']}</td>"
            '</tr>'
        )
    return (
        "<html><body style=\"font-family:Arial,Helvetica,sans-serif\">"
        f"{header}"
        "<table cellpadding='6' style='border-collapse:collapse' border='1'>"
        "<tr style='background:#f0f0f0'><th>Symbol</th><th>Close</th><th>Change</th>"
        "<th>High</th><th>Low</th><th>Avg volume</th><th>Days</th></tr>"
        f"{''.join(cells)}"
        "</table>"
        f"<p style='color:#777;font-size:12px'>Source: {report['db_file']} table {report['table']}</p>"
        "</body></html>"
    )


def split_recipients(value: str):
    return [addr.strip() for addr in re.split(r'[,;]', value or '') if addr.strip()]


def send_email(subject: str, text_body: str, html_body: str, sender: str, recipients: list,
               host: str, port: int, user: str | None, password: str | None, use_ssl: bool,
               use_starttls: bool):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype='html')

    if use_ssl:
        smtp = smtplib.SMTP_SSL(host, port, context=ssl.create_default_context())
    else:
        smtp = smtplib.SMTP(host, port)
    try:
        if use_starttls and not use_ssl:
            smtp.starttls(context=ssl.create_default_context())
        if user and password:
            smtp.login(user, password)
        smtp.send_message(msg, from_addr=sender, to_addrs=recipients)
    finally:
        smtp.quit()


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ('1', 'true', 'yes', 'on')


def main():
    parser = argparse.ArgumentParser(description='Email a periodic ETF summary built from DuckDB')
    parser.add_argument('--db', default=os.environ.get('ETF_DB', DEFAULT_DB), help='DuckDB file')
    parser.add_argument('--table', default=os.environ.get('ETF_TABLE', DEFAULT_TABLE), help='Table name')
    parser.add_argument('--period-days', type=int, default=int(os.environ.get('ETF_PERIOD_DAYS', DEFAULT_PERIOD_DAYS)),
                        help='Length of the reporting window in days (default 14, i.e. two weeks)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD). Defaults to the latest date in the table.')
    parser.add_argument('--to', default=os.environ.get('EMAIL_TO'), help='Recipient(s), comma separated')
    parser.add_argument('--from', dest='sender', default=os.environ.get('EMAIL_FROM'), help='Sender address')
    parser.add_argument('--subject', default=os.environ.get('EMAIL_SUBJECT', DEFAULT_SUBJECT),
                        help='Subject template, supports {start} and {end}')
    parser.add_argument('--smtp-host', default=os.environ.get('SMTP_HOST', 'localhost'))
    parser.add_argument('--smtp-port', type=int, default=int(os.environ.get('SMTP_PORT', 587)))
    parser.add_argument('--smtp-user', default=os.environ.get('SMTP_USER'))
    parser.add_argument('--smtp-password', default=os.environ.get('SMTP_PASSWORD'))
    parser.add_argument('--ssl', dest='use_ssl', action='store_true', default=env_flag('SMTP_SSL', False),
                        help='Connect with implicit TLS (usually port 465)')
    parser.add_argument('--no-starttls', dest='use_starttls', action='store_false',
                        default=env_flag('SMTP_STARTTLS', True), help='Disable STARTTLS')
    parser.add_argument('--dry-run', action='store_true', help='Print the report instead of sending it')
    args = parser.parse_args()

    report = build_report(args.db, args.table, args.period_days, args.end)
    text_body = render_text(report)
    html_body = render_html(report)
    subject = args.subject.format(start=report['start'], end=report['end'])

    if args.dry_run:
        print(f'Subject: {subject}')
        print()
        print(text_body)
        return

    if not args.sender:
        parser.error('--from (or EMAIL_FROM) is required when not using --dry-run')
    recipients = split_recipients(args.to)
    if not recipients:
        parser.error('--to (or EMAIL_TO) is required when not using --dry-run')

    send_email(subject, text_body, html_body, args.sender, recipients, args.smtp_host,
               args.smtp_port, args.smtp_user, args.smtp_password, args.use_ssl, args.use_starttls)
    print(f"Sent '{subject}' to {', '.join(recipients)} ({len(report['rows'])} symbol(s))")


if __name__ == '__main__':
    main()
