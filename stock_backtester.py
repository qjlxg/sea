import pandas as pd
import numpy as np
import os
import glob
from multiprocessing import Pool, cpu_count
import warnings

# 忽略计算中的运行时警告，脚本内部会手动处理异常值
warnings.filterwarnings('ignore')

# ==================== 回测参数配置 ====================
HOLD_PERIODS = [1, 3, 5, 7, 14, 20, 30]
DATA_DIR = 'stock_data'
BACKTEST_REPORT = 'results/虚拟持仓账本.csv'
SUMMARY_REPORT = 'results/策略胜率统计.csv'

def calculate_indicators(df):
    """计算核心指标，适配你的 CSV 格式"""
    df = df.reset_index(drop=True)
    close = df['收盘']
    
    # 均线
    df['ma5'] = close.rolling(5).mean()
    
    # RSI6
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(6).mean()
    df['rsi6'] = 100 - (100 / (1 + gain/loss.replace(0, np.nan)))
    
    # MACD 能量改善
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_diff = ema12 - ema26
    macd_dea = macd_diff.ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (macd_diff - macd_dea) * 2
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)
    
    return df

def backtest_single_stock(file_path):
    stock_code = os.path.basename(file_path).split('.')[0]
    try:
        # 指定列名读取，确保与你的数据格式一致
        df = pd.read_csv(file_path)
        if len(df) < 100: return []
        
        df = calculate_indicators(df)
        stock_signals = []
        
        # 遍历历史
        for i in range(60, len(df) - max(HOLD_PERIODS)):
            curr = df.iloc[i]
            prev = df.iloc[i-1]
            
            # --- 命中“点火启动”条件 ---
            is_oversold = curr['rsi6'] < 25
            is_ignition = is_oversold and curr['收盘'] > curr['ma5'] and curr['成交量'] > prev['成交量'] and curr['macd_improving']
            
            if is_ignition:
                # 信号次日开盘买入
                buy_price = df.iloc[i+1]['开盘']
                
                # 核心修复：防止买入价为0或负数导致的计算错误
                if buy_price <= 0:
                    continue
                
                res = {'代码': stock_code, '信号日期': curr['日期'], '买入价': round(buy_price, 3)}
                
                for p in HOLD_PERIODS:
                    target_close = df.iloc[i+p]['收盘']
                    # 计算收益并过滤无穷大数值
                    profit = (target_close - buy_price) / buy_price * 100
                    res[f'{p}天收益%'] = round(profit, 2) if np.isfinite(profit) else 0.0
                
                stock_signals.append(res)
        return stock_signals
    except Exception:
        return []

def main():
    os.makedirs('results', exist_ok=True)
    files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    print(f"🧬 并行回测启动 | CPU核心: {cpu_count()} | 任务总数: {len(files)}")
    
    with Pool(processes=cpu_count()) as pool:
        results_list = pool.map(backtest_single_stock, files)
    
    all_signals = [s for sublist in results_list for s in sublist]
    
    if not all_signals:
        print("⚠️ 未发现有效历史信号，请检查指标参数设置。")
        return

    ledger_df = pd.DataFrame(all_signals)
    ledger_df.to_csv(BACKTEST_REPORT, index=False, encoding='utf_8_sig')
    
    # 统计胜率
    stats = []
    for p in HOLD_PERIODS:
        col = f'{p}天收益%'
        # 排除 NaN 后的胜率统计
        valid_profits = ledger_df[col].dropna()
        if len(valid_profits) > 0:
            win_rate = (valid_profits > 0).mean() * 100
            avg_ret = valid_profits.mean()
            stats.append({'周期': f'持有{p}天', '胜率%': f"{win_rate:.2f}%", '平均收益%': f"{avg_ret:.2f}%"})
    
    summary_df = pd.DataFrame(stats)
    summary_df.to_csv(SUMMARY_REPORT, index=False, encoding='utf_8_sig')
    
    print("\n" + "="*40)
    print("📈 策略历史回测复盘汇总")
    print("-" * 40)
    print(summary_df.to_string(index=False))
    print("="*40)

if __name__ == "__main__":
    main()
