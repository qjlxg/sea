import pandas as pd
import os
import glob
from datetime import datetime
from joblib import Parallel, delayed

# ==========================================
# 战法名称：冲高回落试盘战法 (High_Limit_Retrace)
# 逻辑要领：
# 1. 寻找盘中强力上攻（>7%）但收盘受阻（<2%）的股票。
# 2. 核心在于识别“仙人指路”还是“派发见顶”。
# 3. 买入逻辑：关注低位放量后的冲高回落，次日若能回踩不破影线中轴可试错。
# ==========================================

def process_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 5:
            return None
        
        # 获取最新一行数据
        latest = df.iloc[-1]
        code = str(latest['股票代码']).zfill(6)
        
        # --- 基础筛选条件 ---
        # 1. 价格区间 [5, 20]
        if not (5.0 <= latest['收盘'] <= 20.0):
            return None
        # 2. 排除 ST (通过文件名或代码判断，这里假设代码前缀)
        if "ST" in file_path:
            return None
        # 3. 排除 30 开头 (创业板)
        if code.startswith('30'):
            return None
        # 4. 仅限沪深A股 (00, 60 开头)
        if not (code.startswith('00') or code.startswith('60')):
            return None

        # --- 战法核心形态逻辑 ---
        # A. 当天最高涨幅 >= 7%
        high_pct = latest['涨跌幅'] + (latest['最高'] - latest['收盘']) / latest['收盘'] * 100 # 粗略计算
        # 更精确：(最高-昨收)/昨收
        prev_close = latest['收盘'] / (1 + latest['涨跌幅']/100)
        actual_high_pct = (latest['最高'] - prev_close) / prev_close * 100
        actual_close_pct = latest['涨跌幅']

        if actual_high_pct >= 7.0 and actual_close_pct <= 2.0:
            # --- 优中选优逻辑（自动复盘） ---
            vol_ratio = latest['成交量'] / df.iloc[-5:-1]['成交量'].mean() # 量比近5日均值
            
            # 信号强度评估
            score = 0
            suggestion = "观察"
            
            if vol_ratio > 2.0: score += 40  # 倍量试盘加分
            if latest['收盘'] > latest['开盘']: score += 20 # 收阳线说明多头尚存
            if actual_close_pct > 0: score += 20 # 拒绝收绿
            
            # 操作建议
            if score >= 80:
                suggestion = "重点关注：强力试盘，次日择机试错"
            elif score >= 50:
                suggestion = "分批建仓：小幅回落，形态尚可"
            else:
                suggestion = "暂时放弃：抛压过重或资金撤退"

            name = stock_names.get(code, "未知")
            return {
                "日期": latest['日期'],
                "代码": code,
                "名称": name,
                "最高涨幅%": round(actual_high_pct, 2),
                "收盘涨幅%": round(actual_close_pct, 2),
                "换手率": latest['换手率'],
                "量比": round(vol_ratio, 2),
                "买入信号强度": score,
                "操作建议": suggestion
            }
    except Exception as e:
        return None

def main():
    # 1. 加载股票名称字典
    names_df = pd.read_csv('stock_names.csv')
    names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))

    # 2. 扫描数据目录
    files = glob.glob('stock_data/*.csv')
    
    # 3. 并行处理
    results = Parallel(n_jobs=-1)(delayed(process_stock)(f, names_dict) for f in files)
    results = [r for r in results if r is not None]
    
    if results:
        result_df = pd.DataFrame(results)
        # 按信号强度排序
        result_df = result_df.sort_values(by="买入信号强度", ascending=False)
        
        # 4. 创建保存目录
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        os.makedirs(dir_path, exist_ok=True)
        
        # 5. 保存结果
        filename = f"High_Limit_Retrace_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(os.path.join(dir_path, filename), index=False, encoding='utf-8-sig')
        print(f"成功筛选出 {len(result_df)} 只目标股。")
    else:
        print("今日无符合战法条件的股票。")

if __name__ == "__main__":
    main()
