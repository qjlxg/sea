import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count, Manager
import numpy as np

# ==================== 2026“多周期+均线共振”参数 ===================
MIN_PRICE = 5.0              
MAX_AVG_TURNOVER_30 = 2.5    
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 0.85      
MAX_TODAY_CHANGE = 1.5       

# --- 极度超跌与多周期共振 ---
RSI6_MAX = 25                
RSI14_MAX = 35               
KDJ_K_MAX = 30               
MIN_PROFIT_POTENTIAL = 15    
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    df = df.reset_index(drop=True)
    close = df['收盘']
    delta = close.diff()
    
    # 1. RSI
    def get_rsi(period):
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))
    df['rsi6'] = get_rsi(6)
    df['rsi14'] = get_rsi(14)
    
    # 2. KDJ
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_gold'] = (df['kdj_k'] > df['kdj_d']) & (df['kdj_k'].shift(1) <= df['kdj_d'].shift(1))
    
    # 3. MACD
    df['ema12'] = close.ewm(span=12, adjust=False).mean()
    df['ema26'] = close.ewm(span=26, adjust=False).mean()
    df['diff'] = df['ema12'] - df['ema26']
    df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['diff'] - df['dea']) * 2
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)

    # 4. 均线系统 (新增 MA10, MA20)
    df['ma5'] = close.rolling(window=5).mean()
    df['ma10'] = close.rolling(window=10).mean()
    df['ma20'] = close.rolling(window=20).mean()
    df['ma60'] = close.rolling(window=60).mean()
    
    # 均线共振判定：MA5走平或上拐 且 股价站在MA5之上
    df['ma5_up'] = df['ma5'] >= df['ma5'].shift(1)
    # 均线聚拢：MA5与MA10的距离缩窄（成本趋同）
    df['ma_converge'] = abs(df['ma5'] - df['ma10']) / df['ma10'] < 0.03

    # 5. 量能
    df['avg_turnover_30'] = df['换手率'].rolling(window=30).mean()
    df['vol_ma5'] = df['成交量'].shift(1).rolling(window=5).mean()
    df['vol_ratio'] = df['成交量'] / df['vol_ma5']
    df['vol_increase'] = df['成交量'] > df['成交量'].shift(1)
    return df

def process_single_stock(args):
    file_path, name_map, stats_dict = args
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = name_map.get(stock_code, "未知")
    if "ST" in stock_name.upper(): return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        df = calculate_indicators(df_raw)
        latest = df.iloc[-1]
        
        # 统计关卡 (保留)
        stats_dict['total_scanned'] += 1
        if latest['收盘'] < MIN_PRICE:
            stats_dict['fail_price'] += 1
            return None
        if latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            stats_dict['fail_turnover'] += 1
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        
        is_oversold = latest['rsi6'] <= RSI6_MAX and latest['rsi14'] <= RSI14_MAX and latest['kdj_k'] <= KDJ_K_MAX
        is_shrink_vol = MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO
        is_small_body = abs(change) <= MAX_TODAY_CHANGE

        strategy_tag = ""

        # --- 增强版：点火启动 + 均线共振 ---
        # 逻辑：在原有点火基础上，要求MA5开始走平或上拐，且股价收复MA5
        if is_oversold and latest['收盘'] > latest['ma5'] and latest['macd_improving']:
            if latest['ma5_up'] and latest['vol_ratio'] > 0.5:
                strategy_tag = "0-均线共振点火(最强)"
        
        # --- 1级：多指标金叉共振 ---
        if strategy_tag == "" and is_oversold and latest['kdj_gold'] and latest['macd_improving']:
            strategy_tag = "1-多指标共振金叉"

        # --- 2级：极致潜伏 ---
        if strategy_tag == "" and is_oversold and is_shrink_vol and is_small_body and potential >= MIN_PROFIT_POTENTIAL:
            strategy_tag = "2-极致缩量潜伏"

        # --- 3级：观察池 ---
        elif strategy_tag == "" and is_oversold and potential >= 10.0:
            strategy_tag = "3-准入选观察池"

        if strategy_tag:
            # 增加均线状态描述
            ma_status = "MA5上拐" if latest['ma5_up'] else "MA5承压"
            return {
                '类型': strategy_tag, '代码': stock_code, '名称': stock_name,
                '现价': round(latest['收盘'], 2), '量比': round(latest['vol_ratio'], 2),
                '指标状态': f"{'金叉' if latest['kdj_gold'] else '底位'}/{ma_status}",
                'RSI6/14': f"{round(latest['rsi6'],1)}/{round(latest['rsi14'],1)}",
                '距60日线': f"{round(potential, 1)}%", '今日涨跌': f"{round(change, 1)}%"
            }
    except:
        return None
    return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 均线共振+多指标点火扫描开始...")
    
    manager = Manager()
    stats_dict = manager.dict({
        'total_scanned': 0, 'fail_price': 0, 'fail_turnover': 0,
        'fail_potential': 0, 'fail_rsi_kdj': 0, 'fail_volume': 0, 'fail_shape': 0
    })

    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        n_df = pd.read_csv(NAME_MAP_FILE, dtype={'code': str})
        name_map = dict(zip(n_df['code'].str.zfill(6), n_df['name']))

    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    tasks = [(f, name_map, stats_dict) for f in file_list]

    with Pool(processes=cpu_count()) as pool:
        raw_results = pool.map(process_single_stock, tasks)

    results = [r for r in raw_results if r is not None]
    
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values(by=['类型', '距60日线'], ascending=[True, False])
        print(f"\n🎯 选出结果 ({len(results)} 只):")
        print(df_result.to_string(index=False))
        
        os.makedirs("results", exist_ok=True)
        file_name = f"均线共振点火版_{now_shanghai.strftime('%Y%m%d_%H%M')}.csv"
        df_result.to_csv(os.path.join("results", file_name), index=False, encoding='utf_8_sig')
    else:
        print("\n😱 暂无标的通过三重共振筛选。")

if __name__ == "__main__":
    main()
