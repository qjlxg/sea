import pandas as pd
import numpy as np
import os
import glob
from multiprocessing import Pool, cpu_count

# ==================== 回测参数配置 ====================
HOLD_PERIODS = [1, 3, 5, 7, 14, 20, 30]  # 虚拟持仓周期
DATA_DIR = 'stock_data'
BACKTEST_REPORT = 'results/虚拟持仓账本.csv'
SUMMARY_REPORT = 'results/策略胜率统计.csv'

def calculate_indicators(df):
    """计算核心指标 (同主脚本逻辑)"""
    df = df.reset_index(drop=True)
    close = df['收盘']
    
    # 均线系统
    df['ma5'] = close.rolling(5).mean()
    
    # RSI6
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
    df['rsi6'] = 100 - (100 / (1 + gain/loss.replace(0, np.nan)))
    
    # KDJ (9,3,3)
    low_9 = df['最低'].rolling(9).min()
    high_9 = df['最高'].rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    
    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (ema12 - ema26 - (ema12 - ema26).ewm(span=9, adjust=False).mean()) * 2
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)
    
    return df

def backtest_single_stock(file_path):
    """单只股票的回测逻辑函数 (被并行调用)"""
    stock_code = os.path.basename(file_path).split('.')[0]
    try:
        df = pd.read_csv(file_path)
        if len(df) < 100: return []
        df = calculate_indicators(df)
        
        stock_signals = []
        # 寻找信号点：由于要计算30天后的收益，索引结束点需留出余量
        for i in range(60, len(df) - max(HOLD_PERIODS)):
            curr = df.iloc[i]
            prev = df.iloc[i-1]
            
            # --- 命中“点火启动”条件 ---
            # 条件：RSI超跌 + 站上MA5 + 较昨日放量 + MACD改善
            is_oversold = curr['rsi6'] < 25
            is_ignition = is_oversold and curr['收盘'] > curr['ma5'] and curr['成交量'] > prev['成交量'] and curr['macd_improving']
            
            if is_ignition:
                entry_date = curr['日期']
                # 模拟次日开盘买入 (更贴近实战)
                buy_price = df.iloc[i+1]['开盘'] 
                
                res = {'代码': stock_code, '信号日期': entry_date, '买入价': round(buy_price, 2)}
                
                # 计算各周期后的收盘价收益
                for p in HOLD_PERIODS:
                    target_row = df.iloc[i+p]
                    profit = (target_row['收盘'] - buy_price) / buy_price * 100
                    res[f'{p}天收益%'] = round(profit, 2)
                
                stock_signals.append(res)
        return stock_signals
    except:
        return []

def main():
    os.makedirs('results', exist_ok=True)
    files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    print(f"🧬 并行回测启动 | CPU核心数: {cpu_count()} | 总任务数: {len(files)}")
    
    # 使用并行池加快处理速度
    with Pool(processes=cpu_count()) as pool:
        results_list = pool.map(backtest_single_stock, files)
    
    # 汇总所有信号
    all_signals = [s for sublist in results_list for s in sublist]
    
    if not all_signals:
        print("⚠️ 未发现符合条件的成交记录")
        return

    # 生成详细账本
    ledger_df = pd.DataFrame(all_signals)
    ledger_df.to_csv(BACKTEST_REPORT, index=False, encoding='utf_8_sig')
    
    # 计算胜率统计表
    stats = []
    for p in HOLD_PERIODS:
        col = f'{p}天收益%'
        win_rate = (ledger_df[col] > 0).mean() * 100
        avg_ret = ledger_df[col].mean()
        stats.append({'周期': f'持有{p}天', '胜率%': f"{win_rate:.2f}%", '平均收益%': f"{avg_ret:.2f}%"})
    
    pd.DataFrame(stats).to_csv(SUMMARY_REPORT, index=False, encoding='utf_8_sig')
    print(f"✅ 回测完成！账本已保存至: {BACKTEST_REPORT}")
    print(pd.DataFrame(stats).to_string(index=False))

if __name__ == "__main__":
    main()
