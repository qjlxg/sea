import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：龙头二板首阴 (Dragon Double-Board First Yin)
操作要领：
1. 寻找二连板股票，代表该股已确立板块龙头或强势地位。
2. 重点观察二板是否为“一字板”，一字板代表主力高度控盘。
3. 首阴回踩：次日出现放量阴线洗盘，但收盘或最低价不得回补前一涨停缺口。
4. 买入逻辑：在洗盘阴线当日尾盘或次日缩量企稳时介入，博弈随后的缩量反包拉升。
"""

def analyze_stock(file_path, names_dict):
    try:
        # 获取纯数字代码
        code_raw = os.path.basename(file_path).replace('.csv', '')
        # 兼容性处理：提取代码中的数字部分
        code = ''.join(filter(str.isdigit, code_raw))
        
        # --- 严格过滤条件 ---
        # 1. 排除创业板(30)、科创板(68)
        if code.startswith(('30', '68')):
            return None
        
        stock_name = names_dict.get(code, names_dict.get(int(code) if code.isdigit() else "", "未知"))
        # 2. 排除 ST
        if "ST" in str(stock_name).upper():
            return None

        df = pd.read_csv(file_path)
        if len(df) < 5: return None
        
        # 统一列名处理（防止CSV编码或空格导致读取失败）
        df.columns = [c.strip() for c in df.columns]
        df = df.sort_values('日期')
        
        # 3. 价格过滤 (最新收盘价在 5-20 元)
        last_price = df.iloc[-1]['收盘']
        if not (5.0 <= last_price <= 20.0):
            return None

        # --- 战法核心筛选逻辑 ---
        # 获取最近4天数据用于判断：前前前(T-3), 前前(T-2), 前(T-1), 今(T)
        data = df.tail(4)
        if len(data) < 4: return None
        
        t0 = data.iloc[-1]  # 今日 (首阴日)
        t1 = data.iloc[-2]  # 昨日 (二连板)
        t2 = data.iloc[-3]  # 前日 (一连板)
        
        # A. 判断二连板 (涨幅 > 9.5%)
        is_double_board = (t1['涨跌幅'] >= 9.5) and (t2['涨跌幅'] >= 9.5)
        if not is_double_board:
            return None

        # B. 判断是否为首阴 (今日收盘 < 开盘 或 涨跌幅 < 0)
        is_yin = t0['收盘'] < t0['开盘'] or t0['涨跌幅'] < 0
        if not is_yin:
            return None

        # C. 核心：不回补缺口 (今日最低价 > T-2日的最高价/收盘价)
        # 缺口支撑是判断真假洗盘的关键
        gap_price = t2['收盘']
        is_gap_safe = t0['最低'] > gap_price
        if not is_gap_safe:
            return None

        # --- 智能复盘与评分体系 ---
        score = 50
        details = []

        # 1. 强度项：二板是一字板 (开=收=高=低)
        if t1['开盘'] == t1['收盘'] == t1['最高']:
            score += 25
            details.append("二板一字极致强势")
        
        # 2. 成交量项：首阴放量 (放量说明换手充分)
        vol_ratio = t0['成交量'] / t1['成交量']
        if vol_ratio > 1.8:
            score += 15
            details.append("巨量洗盘换手充分")
        elif vol_ratio < 0.8:
            score -= 10
            details.append("缩量阴线动力不足")

        # 3. 换手率项
        if 5 <= t0['换手率'] <= 15:
            score += 10
            details.append("换手率适中")
        elif t0['换手率'] > 25:
            score -= 20
            details.append("换手过高警惕出货")

        # --- 最终决策输出 ---
        if score >= 80:
            op_advice = "【重点关注】形态完美，主力空中加油，建议逢低布局"
            signal_type = "强烈买入信号"
        elif score >= 65:
            op_advice = "【轻仓试错】符合战法，但强度一般，观察次日是否反包"
            signal_type = "观察买入信号"
        else:
            op_advice = "【暂时放弃】形态虽在但细节偏弱，容易走弱"
            signal_type = "弱信号"

        return {
            "代码": code,
            "名称": stock_name,
            "最新价": last_price,
            "今日涨跌": f"{t0['涨跌幅']}%",
            "换手率": t0['换手率'],
            "量比": round(vol_ratio, 2),
            "评分": score,
            "信号等级": signal_type,
            "复盘分析": " / ".join(details),
            "操作建议": op_advice
        }

    except Exception:
        return None

def main():
    # 1. 加载代码映射表
    try:
        names_df = pd.read_csv('stock_names.csv')
        # 确保代码是字符串且补齐6位
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        names_dict = dict(zip(names_df['code'], names_df['name']))
    except:
        names_dict = {}

    # 2. 扫描数据并并行筛选
    stock_files = glob.glob('stock_data/*.csv')
    print(f"开始分析，共计 {len(stock_files)} 只股票...")
    
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_stock, f, names_dict) for f in stock_files]
        for future in futures:
            res = future.result()
            if res:
                results.append(res)
    
    # 3. 输出结果
    if results:
        final_df = pd.DataFrame(results).sort_values(by="评分", ascending=False)
        
        # 路径处理
        dir_path = datetime.now().strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"dragon_return_strategy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        
        final_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成！筛选出 {len(results)} 只符合战法标的。")
        print(f"结果已存入：{full_path}")
    else:
        print("今日未发现符合【龙头二板首阴】战法的标的。")

if __name__ == "__main__":
    main()
