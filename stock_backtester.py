import pandas as pd
import numpy as np
import os
import glob
from multiprocessing import Pool, cpu_count

# ==================== 回测参数配置 ====================
HOLD_PERIODS = [1, 3, 5, 7, 14, 20, 30]  # 虚拟持仓天数
DATA_DIR = 'stock_data'
BACKTEST_REPORT = 'results/虚拟持仓账本.csv'
SUMMARY_REPORT = 'results/策略胜率统计.csv'

def calculate_indicators(df):
    """复用主脚本核心指标计算逻辑"""
    close = df['收盘']
    # 均线
    df['ma5'] = close.rolling(5).mean()
    # RSI (6)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
    df['rsi6'] = 100 - (100 / (1 + gain/loss.replace(0, np.nan)))
    # KDJ
    low_list = df['最低'].rolling(9).min()
    high_list = df['最高'].rolling(9).max()
    rsv = (close - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_gold'] = (df['kdj_k'] > df['kdj_d']) & (df['kdj_k'].shift(1) <= df['kdj_d'].shift(1))
    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df['diff'] = ema12 - ema26
    df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['diff'] - df['dea']) * 2
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)
    
    return df

def backtest_stock(file_path):
    stock_code = os.path.basename(file_path).split('.')[0]
    try:
        df = pd.read_csv(file_path)
        if len(df) < 100: return []
        df = calculate_indicators(df)
        
        signals = []
        # 从第60天开始回测，留足计算空间
        for i in range(60, len(df) - max(HOLD_PERIODS)):
            row = df.iloc[i]
            prev_row = df.iloc[i-1]
            
            # --- 判定逻辑：点火启动(即买即涨) ---
            is_oversold = row['rsi6'] < 25
            is_ignition = is_oversold and row['收盘'] > row['ma5'] and row['成交量'] > prev_row['成交量'] and row['macd_improving']
            
            if is_ignition:
                entry_price = df.iloc[i+1]['开盘'] # 信号次日开盘买入
                entry_date = df.iloc[i]['日期']
                
                res = {'代码': stock_code, '信号日期': entry_date, '买入价': round(entry_price, 2)}
                
                # 记录不同周期的收益
                for p in HOLD_PERIODS:
                    exit_price = df.iloc[i+p]['收盘']
                    profit = (exit_price - entry_price) / entry_price * 100
                    res[f'{p}天收益%'] = round(profit, 2)
                
                signals.append(res)
        return signals
    except:
        return []

def main():
    os.makedirs('results', exist_ok=True)
    files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    print(f"🧬 启动历史回测，目标文件数: {len(files)}")
    
    with Pool(cpu_count()) as p:
        all_signals = p.map(backtest_stock, files)
    
    # 平铺结果
    flat_signals = [s for sub in all_signals for s in sub]
    if not flat_signals:
        print("❌ 未发现任何历史点火信号")
        return

    ledger_df = pd.DataFrame(flat_signals)
    ledger_df.to_csv(BACKTEST_REPORT, index=False, encoding='utf_8_sig')
    
    # 统计胜率
    summary = []
    for p in HOLD_PERIODS:
        col = f'{p}天收益%'
        win_rate = (ledger_df[col] > 0).mean() * 100
        avg_profit = ledger_df[col].mean()
        summary.append({'周期': f'持有{p}天', '胜率%': round(win_rate, 2), '平均收益%': round(avg_profit, 2)})
    
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(SUMMARY_REPORT, index=False, encoding='utf_8_sig')
    
    print("\n" + "="*30)
    print("📊 策略实战价值报告")
    print("-" * 30)
    print(summary_df.to_string(index=False))
    print("="*30)

if __name__ == "__main__":
    main()
