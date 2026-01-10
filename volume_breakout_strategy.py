import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：量价突破擒龙战法 (Volume Breakout Strategy)
# 核心逻辑：
# 1. 识别放量：股价在相对低位或突破位，单日成交量显著超过前期均量（主力进场）。
# 2. 识别缩量：放量后股价不暴跌，成交量快速萎缩，波动减小（洗盘结束）。
# 3. 价格支撑：股价在MA5附近获得支撑。
# 4. 择机入场：缩量后量能再次异动或回踩关键位。
# ==========================================

# 配置参数
INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30:
            return None
        
        # 基础过滤：代码格式 (排除30开头、排除ST通过文件名或名称过滤)
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None
        
        # 最新价过滤
        last_price = df['收盘'].iloc[-1]
        if not (MIN_PRICE <= last_price <= MAX_PRICE): return None

        # 计算技术指标
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['Vol_MA10'] = df['成交量'].rolling(window=10).mean()
        
        # --- 战法逻辑实现 ---
        # 1. 寻找最近10日内的“显著放量日”（定义为成交量 > 2倍MA10）
        # 2. 寻找放量后的“缩量日”（成交量显著下降且价格站稳MA5）
        
        curr_vol = df['成交量'].iloc[-1]
        prev_vol = df['成交量'].iloc[-2]
        ma5 = df['MA5'].iloc[-1]
        
        # 简化版量能逻辑：昨日大幅缩量（洗盘），今日量能微增且站稳MA5
        is_shrunk = prev_vol < df['Vol_MA10'].iloc[-2] * 0.8
        is_support = last_price >= ma5 * 0.98  # 靠近或高于MA5
        
        # 计算信号强度 (0-100)
        signal_score = 0
        if is_shrunk and is_support:
            signal_score += 60
            if curr_vol > prev_vol: signal_score += 20 # 量能回升
            if df['涨跌幅'].iloc[-1] > 0: signal_score += 10 # 价格收红
        
        if signal_score < 60: return None

        # 操作建议生成
        advice = "暂时观察"
        if signal_score >= 80:
            advice = "重仓出击 (放量回踩完美)"
        elif signal_score >= 70:
            advice = "轻仓试错 (缩量形态确立)"

        return {
            "code": code,
            "last_price": last_price,
            "score": signal_score,
            "advice": advice,
            "change_pct": df['涨跌幅'].iloc[-1]
        }
    except Exception as e:
        return None

def main():
    stock_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    
    results = []
    # 并行处理提高效率
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_stock, stock_files):
            if res:
                # 匹配名称并排除ST
                name_row = names_df[names_df['code'] == res['code']]
                if not name_row.empty:
                    name = name_row.iloc[0]['name']
                    if "ST" in name: continue
                    res['name'] = name
                    results.append(res)

    # 结果排序：按信号强度从高到低
    results = sorted(results, key=lambda x: x['score'], reverse=True)
    
    # 导出结果
    if results:
        final_df = pd.DataFrame(results)
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        
        file_name = f"volume_breakout_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_path, file_name)
        final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，结果已保存至: {save_path}")
    else:
        print("今日无符合战法信号的个股。")

if __name__ == "__main__":
    main()
