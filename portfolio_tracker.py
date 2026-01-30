import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz

# ==================== 配置 ====================
PORTFOLIO_FILE = 'portfolio.csv'      # 你的实仓/虚拟持仓账本
DATA_DIR = 'stock_data'
REPORT_FILE = 'results/持仓监控报告.csv'
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

def get_latest_price(stock_code):
    """从本地最新的数据文件中获取当前价格"""
    file_path = os.path.join(DATA_DIR, f"{stock_code}.csv")
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if not df.empty:
            latest = df.iloc[-1]
            return latest['收盘'], latest['日期'], latest['涨跌幅']
    return None, None, None

def calculate_advice(row, current_price):
    """结合回测逻辑给出建议"""
    hold_days = (datetime.now(SHANGHAI_TZ).date() - pd.to_datetime(row['买入日期']).date()).days
    profit = (current_price - row['买入价']) / row['买入价'] * 100
    
    # 逻辑：回测显示30天收益最高，20-30天是收割区
    if profit < -8: # 硬性止损位
        return "急！止损卖出"
    elif hold_days >= 30:
        return "满期！建议止盈"
    elif hold_days >= 20 and profit > 5:
        return "达标！择机止盈"
    elif hold_days < 5:
        return "新仓！观察磨底"
    else:
        return "持有中"

def main():
    if not os.path.exists(PORTFOLIO_FILE):
        # 初始化账本（示例：你可以手动在CSV里添加元利科技）
        df_init = pd.DataFrame(columns=['代码', '名称', '买入日期', '买入价', '数量', '类型'])
        df_init.to_csv(PORTFOLIO_FILE, index=False, encoding='utf_8_sig')
        print(f"⚠️ 账本 {PORTFOLIO_FILE} 不存在，已为你创建空账本。请手动填入持仓。")
        return

    portfolio = pd.read_csv(PORTFOLIO_FILE, dtype={'代码': str})
    results = []

    print(f"🚀 正在监控持仓状态... ({datetime.now(SHANGHAI_TZ).strftime('%Y-%m-%d')})")

    for _, row in portfolio.iterrows():
        code = row['代码'].zfill(6)
        curr_price, last_date, daily_change = get_latest_price(code)
        
        if curr_price:
            profit_total = (curr_price - row['买入价']) / row['买入价'] * 100
            advice = calculate_advice(row, curr_price)
            
            results.append({
                '代码': code,
                '名称': row['名称'],
                '买入价': row['买入价'],
                '现价': curr_price,
                '今日涨跌%': daily_change,
                '累计盈亏%': f"{round(profit_total, 2)}%",
                '持有天数': (datetime.now(SHANGHAI_TZ).date() - pd.to_datetime(row['买入日期']).date()).days,
                '操作建议': advice,
                '数据更新': last_date
            })

    if results:
        df_report = pd.DataFrame(results)
        os.makedirs('results', exist_ok=True)
        df_report.to_csv(REPORT_FILE, index=False, encoding='utf_8_sig')
        print("\n📊 当前持仓盈亏概览：")
        print(df_report.to_string(index=False))
    else:
        print("📭 当前无活跃持仓。")

if __name__ == "__main__":
    main()
