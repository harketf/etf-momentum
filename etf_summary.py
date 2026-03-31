# -*- coding: utf-8 -*-
"""
ETF动量策略 - 每日汇总邮件
每天14:30发送，汇总三个策略的持仓动量数据及建议
"""

import akshare as ak
import pandas as pd
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# ==================== 配置 ====================

# 邮件配置
EMAIL_SENDER = "2338110918@qq.com"
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', 'bnskozorvfutdigc')
EMAIL_RECEIVER = "2338110918@qq.com"
SMTP_SERVER = "smtp.qq.com"
SMTP_PORT = 465

# 状态文件
STATE_FILES = {
    1: "etf_state.json",
    2: "etf_state2.json",
    3: "etf_state3.json",
}

# 策略配置
STRATEGIES = {
    1: {
        "name": "1号纳创黄油",
        "pool": {
            "159941": "纳指ETF",
            "159915": "创业板100ETF",
            "162719": "石油LOF",
            "518880": "黄金ETF",
        },
        "threshold_min": -0.011,
        "threshold_max": 0.222,
        "momentum_days": 20,
    },
    2: {
        "name": "2号创黄",
        "pool": {
            "159915": "创业板100ETF",
            "518880": "黄金ETF",
        },
        "threshold_min": -0.058,
        "threshold_max": 0.178,
        "momentum_days": 20,
    },
    3: {
        "name": "3号纳创恒油",
        "pool": {
            "159941": "纳指ETF",
            "159915": "创业板100ETF",
            "162719": "石油LOF",
            "159920": "恒生ETF",
        },
        "threshold_min": -0.030,
        "threshold_max": 0.222,
        "momentum_days": 20,
    },
}

# ETF名称映射（展示用）
ETF_NAMES = {
    "159941": "纳指ETF",
    "159915": "创业板100ETF",
    "162719": "石油LOF",
    "518880": "黄金ETF",
    "159920": "恒生ETF",
}

# LOF基金代码（使用单独接口）
LOF_SYMBOLS = {"162719"}

# ==================== 数据获取 ====================

def get_trade_days_count(start_date_str):
    """计算从买入次日到今天的交易日天数（买入当天不算）"""
    try:
        start = (datetime.fromisoformat(start_date_str) + timedelta(days=1)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal["trade_date"] = pd.to_datetime(trade_cal["trade_date"])
        mask = (trade_cal["trade_date"] >= start) & (trade_cal["trade_date"] <= end)
        return int(mask.sum())
    except Exception:
        days = (datetime.now() - datetime.fromisoformat(start_date_str)).days - 1
        return max(0, int(days * 5 / 7))


def is_trade_day():
    """判断今天是否是交易日"""
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal["trade_date"] = pd.to_datetime(trade_cal["trade_date"])
        today = datetime.now().strftime("%Y%m%d")
        return not trade_cal[trade_cal["trade_date"] == today].empty
    except Exception:
        return datetime.now().weekday() < 5


def get_realtime_price(symbol):
    """获取实时价格（交易时间内有效）"""
    try:
        if symbol in LOF_SYMBOLS:
            df = ak.fund_lof_spot_em()
        else:
            df = ak.fund_etf_spot_em()
        row = df[df["代码"] == symbol]
        if not row.empty:
            return float(row.iloc[0]["最新价"])
    except Exception as e:
        print(f"获取{symbol}实时价失败: {e}")
    return None


def get_etf_data(symbol, days=60):
    """获取ETF历史数据，交易时间内用实时价替换当天"""
    try:
        start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        df = ak.fund_etf_hist_em(
            symbol=symbol, period="daily",
            start_date=start, end_date=end, adjust="qfq"
        )
        if df is not None and len(df) > 0:
            df = df.sort_values("日期").reset_index(drop=True)
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            is_trading_time = now.weekday() < 5 and (
                (9, 30) <= (now.hour, now.minute) <= (15, 0)
            )
            if is_trading_time:
                realtime = get_realtime_price(symbol)
                if realtime:
                    if df.iloc[-1]["日期"] == today_str:
                        df.loc[df.index[-1], "收盘"] = realtime
                    else:
                        new_row = df.iloc[-1].copy()
                        new_row["日期"] = today_str
                        new_row["收盘"] = realtime
                        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
        return df
    except Exception as e:
        print(f"获取{symbol}数据失败: {e}")
        return None


def calc_momentum(symbol, days=20):
    """计算指定ETF的N日动量"""
    df = get_etf_data(symbol, days=days * 3)
    if df is None or len(df) < days + 1:
        return None
    df = df.tail(days + 1)
    start_price = df.iloc[0]["收盘"]
    end_price = df.iloc[-1]["收盘"]
    return (end_price - start_price) / start_price


def load_state(strategy_id):
    """加载策略持仓状态"""
    path = STATE_FILES[strategy_id]
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"current_hold": None, "hold_start_date": None}

# ==================== 邮件发送 ====================

def send_email(subject, body):
    """发送HTML邮件"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg.attach(MIMEText(body, "html", "utf-8"))
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print("汇总邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {e}")

# ==================== 主逻辑 ====================

def main():
    print(f"\n{'='*50}")
    print(f"ETF策略汇总执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    if not is_trade_day():
        print("今天非交易日，跳过执行。")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")

    # 缓存已拉取的动量，避免重复请求
    momentum_cache = {}

    def get_momentum_cached(symbol):
        if symbol not in momentum_cache:
            momentum_cache[symbol] = calc_momentum(symbol, days=20)
        return momentum_cache[symbol]

    # 收集各策略数据
    strategy_results = []

    for sid, cfg in STRATEGIES.items():
        state = load_state(sid)
        current_hold = state.get("current_hold")
        hold_start = state.get("hold_start_date", "")

        # 持有交易日天数
        hold_days = get_trade_days_count(state["hold_start_date"]) if state.get("hold_start_date") else 0

        # 当前持仓动量
        hold_momentum = get_momentum_cached(current_hold) if current_hold else None

        # ETF池全部动量
        pool_data = []
        for code, name in cfg["pool"].items():
            mom = get_momentum_cached(code)
            pool_data.append({
                "code": code,
                "name": name,
                "momentum": mom,
                "is_hold": (code == current_hold),
            })

        strategy_results.append({
            "id": sid,
            "name": cfg["name"],
            "current_hold": current_hold,
            "hold_name": ETF_NAMES.get(current_hold, current_hold) if current_hold else "空仓",
            "hold_start": hold_start[:10] if hold_start else "-",
            "hold_days": hold_days,
            "hold_momentum": hold_momentum,
            "threshold_min": cfg["threshold_min"],
            "threshold_max": cfg["threshold_max"],
            "pool_data": pool_data,
        })

    # 按持仓动量排名
    valid = [s for s in strategy_results if s["hold_momentum"] is not None]
    valid_sorted = sorted(valid, key=lambda x: x["hold_momentum"], reverse=True)
    rank_map = {s["id"]: i + 1 for i, s in enumerate(valid_sorted)}

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    # 建议策略
    best = valid_sorted[0] if valid_sorted else None

    # ==================== 构建邮件HTML ====================

    def momentum_tag(mom, tmin, tmax):
        """生成动量状态标签"""
        if mom is None:
            return '<span style="color:#999">数据缺失</span>'
        pct = f"{mom:+.2%}"
        if mom < tmin:
            return f'<span style="color:#52c41a">{pct} ⚠️低于下限</span>'
        elif mom > tmax:
            return f'<span style="color:#ff4d4f">{pct} ⚠️超上限</span>'
        else:
            return f'<span style="color:#1890ff">{pct} ✅</span>'

    blocks = ""
    for s in strategy_results:
        rank = rank_map.get(s["id"], "-")
        medal = medals.get(rank, "")
        hold_mom_str = f"{s['hold_momentum']:+.2%}" if s["hold_momentum"] is not None else "N/A"
        hold_days = s["hold_days"]
        min_hold = 5
        days_left = max(0, min_hold - hold_days)
        if days_left > 0:
            hold_days_html = f'<span style="color:#fa8c16">已持有 <b>{hold_days}</b> 交易日，还需 {days_left} 天可换仓</span>'
        else:
            hold_days_html = f'<span style="color:#52c41a">已持有 <b>{hold_days}</b> 交易日，✅ 可换仓</span>'

        # ETF池明细行
        pool_rows = ""
        for p in s["pool_data"]:
            hold_mark = "← 当前持仓" if p["is_hold"] else ""
            mom_html = momentum_tag(p["momentum"], s["threshold_min"], s["threshold_max"])
            bg = "#fffbe6" if p["is_hold"] else "white"
            pool_rows += f"""
            <tr style="background:{bg}">
                <td style="padding:4px 10px">{p['name']}</td>
                <td style="padding:4px 10px;color:#666">{p['code']}</td>
                <td style="padding:4px 10px">{mom_html}</td>
                <td style="padding:4px 10px;color:#fa8c16;font-size:12px">{hold_mark}</td>
            </tr>"""

        blocks += f"""
        <div style="margin-bottom:20px;border:1px solid #e8e8e8;border-radius:8px;overflow:hidden">
            <div style="background:#f5f5f5;padding:10px 16px;font-weight:bold;font-size:15px">
                {medal} {s['name']}
                &nbsp;&nbsp;
                <span style="font-weight:normal;font-size:13px;color:#666">
                    持仓: {s['hold_name']}（{s['hold_start']}买入）&nbsp;|&nbsp;
                    {hold_days_html}&nbsp;|&nbsp;
                    持仓20日动量: <b style="color:#cf1322">{hold_mom_str}</b>&nbsp;|&nbsp;
                    阈值区间: [{s['threshold_min']:.1%}, {s['threshold_max']:.1%}]
                </span>
            </div>
            <table style="width:100%;border-collapse:collapse;font-size:13px">
                <tr style="background:#fafafa;color:#999;font-size:12px">
                    <th style="padding:4px 10px;text-align:left">基金名称</th>
                    <th style="padding:4px 10px;text-align:left">代码</th>
                    <th style="padding:4px 10px;text-align:left">20日动量</th>
                    <th style="padding:4px 10px;text-align:left"></th>
                </tr>
                {pool_rows}
            </table>
        </div>"""

    # 建议区块
    if best:
        suggest_html = f"""
        <div style="background:#e6f7ff;border:1px solid #91d5ff;border-radius:8px;padding:12px 16px;font-size:14px">
            🎯 <b>建议侧重：{best['name']}</b>
            &nbsp;（当前持仓20日动量最强：{best['hold_momentum']:+.2%}）
            <br><span style="color:#888;font-size:12px">仅供参考，以各策略实际信号为准</span>
        </div>"""
    else:
        suggest_html = '<div style="color:#999">数据不足，无法给出建议</div>'

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:680px;margin:0 auto;padding:16px">
        <h2 style="border-bottom:2px solid #1890ff;padding-bottom:8px;color:#1890ff">
            📊 ETF策略每日汇总 · {today_str}
        </h2>
        {blocks}
        {suggest_html}
        <p style="color:#bbb;font-size:11px;margin-top:20px">
            动量计算基于前复权收盘价，交易时间内使用实时价 | 自动发送，请勿回复
        </p>
    </div>
    """

    subject = f"ETF策略汇总 {today_str} | 建议侧重: {best['name'] if best else '无'}"
    send_email(subject, html)


if __name__ == "__main__":
    main()
