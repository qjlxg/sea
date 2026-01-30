import pandas as pd
import numpy as np
import os, glob, warnings
from multiprocessing import Pool, cpu_count

warnings.filterwarnings('ignore')

# ==================== 回测配置 ====================
HOLD_PERIODS = [1, 3, 5, 7, 14, 20, 30]
DATA_DIR = 'stock_data'
BACKTEST_REPORT = 'results/多周期虚拟账本.csv'
SUMMARY_REPORT = 'results/多周期胜率对比.csv'
WEEKLY_RSI_THRESHOLD = 35  # 周线RSI安全边际

def calculate_indicators(df):
    """日线指标计算"""
    close = df['收盘']
    df['ma5'] = close.rolling(5).mean()
    # RSI6
    delta = close.diff()
    df['rsi6'] = 100 - (100 / (1 + (delta.where(delta > 0, 0).rolling(6).mean() / 
                                  abs(delta).rolling(6).mean().replace(0, np.nan))))
    # MACD改善
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    df['macd_hist'] = (ema12 - ema26 - (ema12 - ema26).ewm(span=9).mean()) * 2
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)
    return df

def get_weekly_rsi(df_daily):
    """日线转周线并计算RSI"""
    # 确保日期是datetime格式
    df_daily['日期'] = pd.to_datetime(df_daily['日期'])
    # 按周重采样：开盘价取第一天，最高价取区间最大，收盘价取最后一天
    df_weekly = df_daily.resample('W', on='日期').agg({
        '收盘': 'last'
    }).dropna()
    
    delta = df_weekly['收盘'].diff()
    gain = delta.where(delta > 0, 0).rolling(6).mean()
    loss = abs(delta).rolling(6).mean()
    df_weekly['w_rsi6'] = 100 - (100 / (1 + gain/loss.replace(0, np.nan)))
    return df_weekly

def backtest_single_stock(file_path):
    stock_code = os.path.basename(file_path).split('.')[0]
    try:
        df = pd.read_csv(file_path)
        if len(df) < 150: return []
        
        df = calculate_indicators(df)
        # 获取周线数据快照
        df_w = get_weekly_rsi(df.copy())
        
        stock_signals = []
        for i in range(100, len(df) - max(HOLD_PERIODS)):
            curr = df.iloc[i]
            # 基础日线点火判定
            is_ignition = curr['rsi6'] < 25 and curr['收盘'] > curr['ma5'] and curr['macd_improving']
            
            if is_ignition:
                # 匹配当天的周线状态
                current_date = pd.to_datetime(curr['日期'])
                # 寻找该日期所属周的周线RSI (向前找最近的一周)
                w_status = df_w[:current_date].iloc[-1]
                is_resonance = w_status['w_rsi6'] < WEEKLY_RSI_THRESHOLD
                
                buy_price = df.iloc[i+1]['开盘']
                if buy_price <= 0: continue
                
                res = {
                    '代码': stock_code, 
                    '信号日期': curr['日期'], 
                    '共振等级': 'SSS-日周共振' if is_resonance else 'B-日线点火',
                    '周线RSI': round(w_status['w_rsi6'], 1)
                }
                
                for p in HOLD_PERIODS:
                    target_close = df.iloc[i+p]['收盘']
                    res[f'{p}天收益%'] = round((target_close - buy_price) / buy_price * 100, 2)
                
                stock_signals.append(res)
        return stock_signals
    except:
        return []

def main():
    os.makedirs('results', exist_ok=True)
    files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    print(f"🧬 多周期并行回测 | CPU核心: {cpu_count()} | 任务数: {len(files)}")
    
    with Pool(cpu_count()) as pool:
        results = pool.map(backtest_single_stock, files)
    
    all_signals = [s for sub in results for s in sub]
    if not all_signals: return

    df_res = pd.DataFrame(all_signals)
    df_res.to_csv(BACKTEST_REPORT, index=False, encoding='utf_8_sig')
    
    # 统计对比：普通 vs 共振
    summary = []
    for level in ['B-日线点火', 'SSS-日周共振']:
        sub_df = df_res[df_res['共振等级'] == level]
        if sub_df.empty: continue
        for p in HOLD_PERIODS:
            col = f'{p}天收益%'
            win_rate = (sub_df[col] > 0).mean() * 100
            avg_ret = sub_df[col].mean()
            summary.append({
                '类型': level, '周期': f'{p}天', 
                '胜率%': round(win_rate, 2), '平均收益%': round(avg_ret, 2),
                '样本数': len(sub_df)
            })
    
    pd.DataFrame(summary).to_csv(SUMMARY_REPORT, index=False, encoding='utf_8_sig')
    print("\n" + "="*50)
    print("📊 多周期共振回测报告 (对比结论)")
    print("-" * 50)
    print(pd.DataFrame(summary).to_string(index=False))

if __name__ == "__main__":
    main()
