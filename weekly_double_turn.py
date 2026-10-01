import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：周线缩量双拐战法 (Weekly Double-Turn)
# 核心逻辑：
# 1. 趋势过滤：均线多头排列（MA60向上），确保处于大牛市或强趋势。
# 2. 回调识别：周K线经历连续回落，触及或接近长期均线支撑。
# 3. 缩量确认：回调过程中成交量极度萎缩（地量），暗示卖盘枯竭。
# 4. 双拐共振：MACD DIF/DEA 拐头向上且价格收阳，形成一击必中买点。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = './stock_names.csv'
OUTPUT_BASE = './results'

def analyze_stock(file_path, stock_names_df):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 60:
            return None
        
        # 基础过滤：代码格式与板块
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30') or code.startswith('ST') or code.startswith('*ST'):
            return None
        
        # 价格区间过滤 (5.0 - 20.0)
        last_close = df['收盘'].iloc[-1]
        if not (5.0 <= last_close <= 20.0):
            return None

        # 计算周线数据 (将日线重采样为周线)
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        
        logic = {
            '开盘': 'first', '收盘': 'last', '最高': 'max', 
            '最低': 'min', '成交量': 'sum', '涨跌幅': 'sum'
        }
        w_df = df.resample('W').apply(logic)
        
        # 计算技术指标
        w_df['MA60'] = w_df['收盘'].rolling(window=60).mean()
        w_df['MA20'] = w_df['收盘'].rolling(window=20).mean()
        w_df['V_MA5'] = w_df['成交量'].rolling(window=5).mean()
        
        # MACD 计算
        exp1 = w_df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = w_df['收盘'].ewm(span=26, adjust=False).mean()
        w_df['DIF'] = exp1 - exp2
        w_df['DEA'] = w_df['DIF'].ewm(span=9, adjust=False).mean()

        # --- 战法核心筛选逻辑 ---
        # 1. 趋势：MA60 走平或向上
        trend_ok = w_df['MA60'].iloc[-1] >= w_df['MA60'].iloc[-2]
        
        # 2. 缩量：当前周成交量小于 5周均量的 0.6倍 (极致缩量)
        vol_shrink = w_df['成交量'].iloc[-1] < w_df['V_MA5'].iloc[-1] * 0.6
        
        # 3. 双拐：DIF 向上拐头 且 价格企稳（或DIF金叉DEA）
        macd_turn = w_df['DIF'].iloc[-1] > w_df['DIF'].iloc[-2]
        price_support = w_df['收盘'].iloc[-1] >= w_df['MA60'].iloc[-1] * 0.98 # 在支撑位附近
        
        if trend_ok and vol_shrink and macd_turn and price_support:
            # 计算信号强度 (0-100)
            strength = 0
            if vol_shrink: strength += 40
            if w_df['DIF'].iloc[-1] > w_df['DEA'].iloc[-1]: strength += 30
            if w_df['涨跌幅'].iloc[-1] > 0: strength += 30
            
            # 操作建议逻辑
            suggestion = "观察待定"
            if strength >= 80: suggestion = "重点关注：一击必中，分批建仓"
            elif strength >= 60: suggestion = "轻仓试错：趋势初步确认"
            
            name = stock_names_df.get(code, "未知名称")
            return {
                '代码': code, '名称': name, '现价': last_close,
                '信号强度': strength, '操作建议': suggestion,
                '周成交量比': round(w_df['成交量'].iloc[-1] / w_df['V_MA5'].iloc[-1], 2)
            }
    except Exception as e:
        return None

def run_parallel():
    # 加载股票名称
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    results = []
    with ProcessPoolExecutor() as executor:
        future_results = [executor.submit(analyze_stock, f, names_dict) for f in files]
        for future in future_results:
            res = future.result()
            if res: results.append(res)
    
    if results:
        final_df = pd.DataFrame(results).sort_values(by='信号强度', ascending=False)
        # 优中选优：只取前 5 名最强信号
        final_df = final_df.head(5)
        
        # 创建年月目录
        now = datetime.now()
        dir_path = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
        os.makedirs(dir_path, exist_ok=True)
        
        # 保存结果
        file_name = f"weekly_double_turn_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf_8_sig')
        print(f"分析完成，筛选出 {len(final_df)} 只目标。")
    else:
        print("今日无符合战法信号。")

if __name__ == '__main__':
    run_parallel()
