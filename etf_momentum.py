#!/usr/bin/env python3
"""
ETF动量轮动策略 - 次方量化策略复现
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
    '159941': '纳指ETF',
    '159915': '创业板100ETF', 
    '162719': '石油LOF',
    '518880': '黄金ETF'
}

MOMENTUM_DAYS = 20  # 动量周期（次方量化实际按20交易日计算）
HOLD_DAYS = 5       # 最小持有天数
THRESHOLD_MIN = -0.011  # 动量阈值下限
THRESHOLD_MAX = 0.222   # 动量阈值上限

# 邮箱配置
SMTP_SERVER = 'smtp.qq.com'
SMTP_PORT = 465
SENDER_EMAIL = '2338110918@qq.com'
SENDER_PASSWORD = os.environ.get('EMAIL_PASSWORD', 'bnskozorvfutdigc')  # 优先从环境变量读取
RECEIVER_EMAIL = '2338110918@qq.com'

# 状态文件路径
STATE_FILE = 'etf_state.json'

# LOF基金代码列表（用单独接口获取实时价）
LOF_SYMBOLS = {'162719'}

def get_realtime_price(symbol):
    """获取实时价格（交易时间内有效）"""
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
    """获取ETF历史数据，并用当天实时价替换最后一行"""
    try:
        start = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        end = datetime.now().strftime('%Y%m%d')
        df = ak.fund_etf_hist_em(symbol=symbol, period="daily",
                                  start_date=start, end_date=end,
                                  adjust="qfq")
        if df is not None and len(df) > 0:
            df = df.sort_values('日期').reset_index(drop=True)
            # 用实时价替换当天收盘价（交易时间内）
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
                        # 当天数据还没入库，追加一行
                        new_row = df.iloc[-1].copy()
                        new_row['日期'] = today_str
                        new_row['收盘'] = realtime
                        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
        return df
    except Exception as e:
        print(f"获取{symbol}数据失败: {e}")
        return None

def calculate_momentum(symbol, days=MOMENTUM_DAYS):
    """计算动量（区间涨幅）"""
    df = get_etf_data(symbol, days=60)
    if df is None or len(df) < days + 1:
        print(f"{symbol} 数据不足: {len(df) if df is not None else 0}行，需要{days+1}行")
        return None
    
    # 取最近days+1个交易日（首尾相差days个区间）
    df = df.tail(days + 1)
    start_price = df.iloc[0]['收盘']
    end_price = df.iloc[-1]['收盘']
    momentum = (end_price - start_price) / start_price
    
    return {
        'symbol': symbol,
        'name': ETF_POOL[symbol],
        'momentum': momentum,
        'current_price': end_price,
        'start_price': start_price
    }

def get_all_momentum():
    """获取所有ETF的动量数据"""
    results = []
    for symbol in ETF_POOL:
        data = calculate_momentum(symbol)
        if data:
            results.append(data)
    
    # 按动量排序
    results.sort(key=lambda x: x['momentum'], reverse=True)
    return results

def get_trade_days_count(start_date_str):
    """计算从买入次日到今天的交易日天数（买入当天不算）"""
    try:
        import akshare as ak
        # 买入次日开始算
        start = (datetime.fromisoformat(start_date_str) + timedelta(days=1)).strftime('%Y%m%d')
        end = datetime.now().strftime('%Y%m%d')
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
        mask = (trade_cal['trade_date'] >= start) & (trade_cal['trade_date'] <= end)
        return int(mask.sum())
    except Exception:
        days = (datetime.now() - datetime.fromisoformat(start_date_str)).days - 1
        return max(0, int(days * 5 / 7))

def load_state():
    """加载持仓状态"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        'current_hold': None,  # 当前持仓
        'hold_start_date': None,  # 持仓开始日期
        'hold_days': 0  # 已持有天数
    }

def save_state(state):
    """保存持仓状态"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def should_switch(current_hold, best_etf, hold_days, state):
    """判断是否应该切换"""
    # 如果没有持仓，直接买入
    if current_hold is None:
        return True, "无持仓，买入信号"
    
    # 检查动量是否在阈值范围内
    if best_etf['momentum'] < THRESHOLD_MIN or best_etf['momentum'] > THRESHOLD_MAX:
        return False, f"动量{best_etf['momentum']:.2%}超出阈值范围"
    
    # 检查最小持有天数
    if hold_days < HOLD_DAYS:
        return False, f"持仓未满{HOLD_DAYS}天（当前{hold_days}天）"
    
    # 如果当前持仓就是最优的，不切换
    if current_hold == best_etf['symbol']:
        return False, "当前持仓已是最优"
    
    return True, "满足切换条件"

def generate_signal():
    """生成交易信号"""
    momentum_data = get_all_momentum()
    if not momentum_data:
        return None, "数据获取失败"
    
    state = load_state()
    current_hold = state.get('current_hold')
    hold_start_date = state.get('hold_start_date')
    
    # 计算已持有交易日天数
    hold_days = 0
    if hold_start_date:
        hold_days = get_trade_days_count(hold_start_date)
    
    # 找到动量最高且在阈值范围内的ETF
    best_etf = None
    for etf in momentum_data:
        if THRESHOLD_MIN <= etf['momentum'] <= THRESHOLD_MAX:
            best_etf = etf
            break
    
    if best_etf is None:
        return momentum_data, "无ETF在动量阈值范围内，建议空仓"
    
    # 判断是否应该切换
    should_trade, reason = should_switch(current_hold, best_etf, hold_days, state)
    
    signal = {
        'momentum_data': momentum_data,
        'best_etf': best_etf,
        'current_hold': current_hold,
        'hold_days': hold_days,
        'should_trade': should_trade,
        'reason': reason,
        'date': datetime.now().strftime('%Y-%m-%d %H:%M')
    }
    
    # 如果需要交易，更新状态
    if should_trade:
        state['current_hold'] = best_etf['symbol']
        state['hold_start_date'] = datetime.now().isoformat()
        save_state(state)
    
    return signal, None

def format_email_content(signal):
    """格式化邮件内容"""
    if isinstance(signal, str):
        return f"<h2>ETF动量策略 - 错误</h2><p>{signal}</p>"
    
    momentum_data = signal['momentum_data']
    best_etf = signal['best_etf']
    current_hold = signal['current_hold']
    hold_days = signal['hold_days']
    should_trade = signal['should_trade']
    reason = signal['reason']
    date = signal['date']
    
    html = f"""
    <h2>📊 ETF动量轮动策略 - {date}</h2>
    
    <h3>🎯 今日信号</h3>
    <p><strong>目标ETF:</strong> {best_etf['name']} ({best_etf['symbol']})</p>
    <p><strong>21日动量:</strong> {best_etf['momentum']:.2%}</p>
    <p><strong>当前价格:</strong> ¥{best_etf['current_price']:.3f}</p>
    
    <h3>📈 动量排名</h3>
    <table border="1" cellpadding="8" style="border-collapse: collapse;">
        <tr style="background-color: #f0f0f0;">
            <th>排名</th>
            <th>ETF</th>
            <th>代码</th>
            <th>21日动量</th>
            <th>当前价格</th>
        </tr>
    """
    
    for i, etf in enumerate(momentum_data, 1):
        bg_color = '#90EE90' if etf['symbol'] == best_etf['symbol'] else ''
        html += f"""
        <tr style="background-color: {bg_color};">
            <td>{i}</td>
            <td>{etf['name']}</td>
            <td>{etf['symbol']}</td>
            <td>{etf['momentum']:.2%}</td>
            <td>¥{etf['current_price']:.3f}</td>
        </tr>
        """
    
    html += "</table>"
    
    html += f"""
    <h3>💼 持仓状态</h3>
    <p><strong>当前持仓:</strong> {ETF_POOL.get(current_hold, '无持仓')} {f'({current_hold})' if current_hold else ''}</p>
    <p><strong>已持有天数:</strong> {hold_days}天</p>
    
    <h3>⚡ 交易建议</h3>
    <p style="font-size: 18px; color: {'red' if should_trade else 'green'};">
        <strong>{'🔴 建议买入/切换' if should_trade else '🟢 继续持有'}</strong>
    </p>
    <p>原因: {reason}</p>
    
    <hr>
    <p style="color: gray; font-size: 12px;">
        策略参数: 动量周期={MOMENTUM_DAYS}交易日 | 最小持有={HOLD_DAYS}交易日 | 阈值=[{THRESHOLD_MIN:.1%}, {THRESHOLD_MAX:.1%}]
    </p>
    """
    
    return html

def send_email(subject, content):
    """发送邮件"""
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

def is_trade_day():
    """判断今天是否是交易日"""
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
        today = datetime.now().strftime('%Y%m%d')
        return not trade_cal[trade_cal['trade_date'] == today].empty
    except Exception:
        # 降级：只判断是否是工作日
        return datetime.now().weekday() < 5

def main():
    """主函数"""
    print(f"\n{'='*50}")
    print(f"ETF动量策略执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    if not is_trade_day():
        print("今天非交易日，跳过执行。")
        return
    
    # 生成信号
    signal, error = generate_signal()
    
    if error:
        print(f"错误: {error}")
        subject = f"❌ ETF策略错误 - {datetime.now().strftime('%m-%d')}"
        content = f"<h2>策略执行失败</h2><p>{error}</p>"
    else:
        # 格式化邮件内容
        content = format_email_content(signal)
        
        # 生成邮件主题
        best_etf = signal['best_etf']
        should_trade = signal['should_trade']
        action = "买入" if should_trade else "持有"
        subject = f"[{action}] {best_etf['name']} | 动量{best_etf['momentum']:.1%} | {datetime.now().strftime('%m-%d')}"
    
    # 发送邮件
    send_email(subject, content)
    
    print(f"\n{'='*50}")
    print("执行完成")
    print(f"{'='*50}\n")

if __name__ == '__main__':
    main()
