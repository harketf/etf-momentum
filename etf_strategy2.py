#!/usr/bin/env python3
"""
ETF动量轮动策略 - 2号创黄策略
ETF池：创业板100ETF(159915)、黄金ETF(518880)
备选（空仓时买入）：黄金ETF(518880)
每天14:30执行，发送交易信号到邮箱
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os

# ========== 配置 ==========
ETF_POOL = {
    '159915': '创业板100ETF',
    '518880': '黄金ETF',
}
FALLBACK_ETF = '518880'  # 空仓时买入备选

MOMENTUM_DAYS = 20       # 动量周期（实际20交易日）
HOLD_DAYS = 5            # 最小持有天数
THRESHOLD_MIN = -0.058   # 动量阈值下限
THRESHOLD_MAX = 0.178    # 动量阈值上限

# 邮箱配置
SMTP_SERVER = 'smtp.qq.com'
SMTP_PORT = 465
SENDER_EMAIL = '2338110918@qq.com'
SENDER_PASSWORD = os.environ.get('EMAIL_PASSWORD', 'bnskozorvfutdigc')
RECEIVER_EMAIL = '2338110918@qq.com'

# 状态文件
STATE_FILE = 'etf_state2.json'

# LOF基金代码（用单独接口）
LOF_SYMBOLS = {'162719'}

def is_trade_day():
    """判断今天是否是交易日"""
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
        today = datetime.now().strftime('%Y%m%d')
        return not trade_cal[trade_cal['trade_date'] == today].empty
    except Exception:
        return datetime.now().weekday() < 5

def get_trade_days_count(start_date_str):
    """计算买入次日到今天的交易日天数（买入当天不算）"""
    try:
        start = (datetime.fromisoformat(start_date_str) + timedelta(days=1)).strftime('%Y%m%d')
        end = datetime.now().strftime('%Y%m%d')
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
        mask = (trade_cal['trade_date'] >= start) & (trade_cal['trade_date'] <= end)
        return int(mask.sum())
    except Exception:
        days = (datetime.now() - datetime.fromisoformat(start_date_str)).days - 1
        return max(0, int(days * 5 / 7))

def get_realtime_price(symbol):
    """获取实时价格（交易时间内）"""
    try:
        if symbol in LOF_SYMBOLS:
            df = ak.fund_lof_spot_em()
        else:
            df = ak.fund_etf_spot_em()
        row = df[df['代码'] == symbol]
        if not row.empty:
            return float(row.iloc[0]['最新价'])
    except Exception as e:
        print(f"获取{symbol}实时价失败: {e}")
    return None

def get_etf_data(symbol, days=60):
    """获取ETF历史数据，交易时间内用实时价替换当天"""
    try:
        start = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        end = datetime.now().strftime('%Y%m%d')
        df = ak.fund_etf_hist_em(symbol=symbol, period="daily",
                                  start_date=start, end_date=end,
                                  adjust="qfq")
        if df is not None and len(df) > 0:
            df = df.sort_values('日期').reset_index(drop=True)
            now = datetime.now()
            today_str = now.strftime('%Y-%m-%d')
            is_trading_time = now.weekday() < 5 and (
                (9, 30) <= (now.hour, now.minute) <= (15, 0)
            )
            if is_trading_time:
                realtime = get_realtime_price(symbol)
                if realtime:
                    if df.iloc[-1]['日期'] == today_str:
                        df.loc[df.index[-1], '收盘'] = realtime
                    else:
                        new_row = df.iloc[-1].copy()
                        new_row['日期'] = today_str
                        new_row['收盘'] = realtime
                        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
        return df
    except Exception as e:
        print(f"获取{symbol}数据失败: {e}")
        return None

def calculate_momentum(symbol):
    """计算20交易日动量"""
    df = get_etf_data(symbol, days=60)
    if df is None or len(df) < MOMENTUM_DAYS + 1:
        print(f"{symbol} 数据不足: {len(df) if df is not None else 0}行")
        return None
    df = df.tail(MOMENTUM_DAYS + 1)
    start_price = df.iloc[0]['收盘']
    end_price = df.iloc[-1]['收盘']
    momentum = (end_price - start_price) / start_price
    return {
        'symbol': symbol,
        'name': ETF_POOL[symbol],
        'momentum': momentum,
        'current_price': end_price,
    }

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {'current_hold': None, 'hold_start_date': None}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def generate_signal():
    """生成交易信号"""
    results = []
    for symbol in ETF_POOL:
        data = calculate_momentum(symbol)
        if data:
            results.append(data)
    if not results:
        return None, "数据获取失败"

    results.sort(key=lambda x: x['momentum'], reverse=True)

    state = load_state()
    current_hold = state.get('current_hold')
    hold_start_date = state.get('hold_start_date')
    hold_days = get_trade_days_count(hold_start_date) if hold_start_date else 0

    # 找动量在阈值内的最高ETF
    best_etf = None
    for etf in results:
        if THRESHOLD_MIN <= etf['momentum'] <= THRESHOLD_MAX:
            best_etf = etf
            break

    # 没有ETF在阈值内，买入备选
    if best_etf is None:
        fallback = next((e for e in results if e['symbol'] == FALLBACK_ETF), None)
        if fallback:
            best_etf = fallback
            reason_prefix = "无ETF在阈值内，买入备选"
        else:
            return results, "无有效信号"
    else:
        reason_prefix = None

    # 判断是否切换
    if current_hold is None:
        should_trade = True
        reason = "无持仓，买入"
    elif hold_days < HOLD_DAYS:
        should_trade = False
        reason = f"持仓未满{HOLD_DAYS}天（当前{hold_days}天）"
    elif current_hold == best_etf['symbol']:
        should_trade = False
        reason = "当前持仓已是最优"
    else:
        should_trade = True
        reason = reason_prefix or "满足切换条件"

    if should_trade:
        state['current_hold'] = best_etf['symbol']
        state['hold_start_date'] = datetime.now().isoformat()
        save_state(state)

    return {
        'momentum_data': results,
        'best_etf': best_etf,
        'current_hold': current_hold,
        'hold_days': hold_days,
        'should_trade': should_trade,
        'reason': reason,
        'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
    }, None

def format_email(signal):
    md = signal['momentum_data']
    best = signal['best_etf']
    hold = signal['current_hold']
    hold_days = signal['hold_days']
    should_trade = signal['should_trade']
    reason = signal['reason']
    date = signal['date']

    html = f"""
    <h2>ETF动量轮动 2号创黄策略 - {date}</h2>
    <h3>今日信号</h3>
    <p><strong>目标ETF:</strong> {best['name']} ({best['symbol']})</p>
    <p><strong>20日动量:</strong> {best['momentum']:.2%}</p>
    <p><strong>当前价格:</strong> ¥{best['current_price']:.3f}</p>
    <h3>动量排名</h3>
    <table border="1" cellpadding="8" style="border-collapse:collapse;">
        <tr style="background:#f0f0f0;">
            <th>排名</th><th>ETF</th><th>代码</th><th>20日动量</th><th>当前价格</th>
        </tr>
    """
    for i, etf in enumerate(md, 1):
        bg = '#90EE90' if etf['symbol'] == best['symbol'] else ''
        html += f"""
        <tr style="background:{bg};">
            <td>{i}</td><td>{etf['name']}</td><td>{etf['symbol']}</td>
            <td>{etf['momentum']:.2%}</td><td>¥{etf['current_price']:.3f}</td>
        </tr>"""
    html += "</table>"
    html += f"""
    <h3>持仓状态</h3>
    <p><strong>当前持仓:</strong> {ETF_POOL.get(hold, '无持仓')} {f'({hold})' if hold else ''}</p>
    <p><strong>已持有交易日:</strong> {hold_days}天</p>
    <h3>交易建议</h3>
    <p style="font-size:18px; color:{'red' if should_trade else 'green'};">
        <strong>{'建议买入/切换' if should_trade else '继续持有'}</strong>
    </p>
    <p>原因: {reason}</p>
    <hr>
    <p style="color:gray;font-size:12px;">
        策略参数: 动量周期={MOMENTUM_DAYS}交易日 | 最小持有={HOLD_DAYS}交易日 | 阈值=[{THRESHOLD_MIN:.1%}, {THRESHOLD_MAX:.1%}] | 备选={ETF_POOL[FALLBACK_ETF]}
    </p>
    """
    return html

def send_email(subject, content):
    try:
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(content, 'html', 'utf-8'))
        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        server.quit()
        print(f"[OK] 邮件发送成功: {subject}")
        return True
    except Exception as e:
        print(f"[FAIL] 邮件发送失败: {e}")
        return False

def main():
    print(f"\n{'='*50}")
    print(f"2号创黄策略执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    if not is_trade_day():
        print("今天非交易日，跳过执行。")
        return

    signal, error = generate_signal()
    if error:
        subject = f"[2号策略错误] {datetime.now().strftime('%m-%d')}"
        content = f"<h2>策略执行失败</h2><p>{error}</p>"
    else:
        content = format_email(signal)
        action = "买入" if signal['should_trade'] else "持有"
        subject = f"[2号{action}] {signal['best_etf']['name']} | 动量{signal['best_etf']['momentum']:.1%} | {datetime.now().strftime('%m-%d')}"

    send_email(subject, content)
    print(f"\n{'='*50}\n执行完成\n{'='*50}\n")

if __name__ == '__main__':
    main()
