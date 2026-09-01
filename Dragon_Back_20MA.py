import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：龙回头-20日线稳健战法
# 核心逻辑：
# 1. 强力启动：前期有高质量放量涨停（主力介入标志）。
# 2. 趋势向上：股价位于20日均线（MA20）上方，且MA20斜率向上。
# 3. 缩量回踩：股价回调至MA20附近，未跌破或回踩确认支撑。
# 4. 止跌信号：出现小阳线或十字星，买点出现。
# 5. 分批风控：2天确认原则，半仓进出。
# ==========================================

# 配置参数
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path, names_dict):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 基础清洗
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 1. 硬性条件筛选
        if not (code.startswith('60') or code.startswith('00')): return None # 仅限深沪A股，排除30
        last_row = df.iloc[-1]
        if not (PRICE_MIN <= last_row['收盘'] <= PRICE_MAX): return None
        
        # 2. 计算指标
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        df['MA20_Slope'] = df['MA20'].diff(3) # 趋势斜率（3日增量）
        
        # 3. 识别近期涨停 (过去15个交易日内是否有涨停)
        # 涨幅 > 9.5% 视为涨停（考虑精度）
        df['Is_Limit_Up'] = df['涨跌幅'] >= 9.8
        recent_limit_up = df.iloc[-15:-3]['Is_Limit_Up'].any() 
        
        # 4. 战法逻辑判断
        curr_price = last_row['收盘']
        ma20 = last_row['MA20']
        slope = last_row['MA20_Slope']
        
        # 条件A: 整体趋势向上 (MA20向上且价格在MA20附近)
        is_uptrend = slope > 0 and curr_price >= ma20 * 0.98
        
        # 条件B: 回踩确认 (当前价格距离MA20在正负3%以内)
        on_support = abs(curr_price - ma20) / ma20 <= 0.03
        
        # 条件C: 止跌信号 (今日收阳线或振幅收敛)
        is_stop_drop = last_row['收盘'] >= last_row['开盘']
        
        if recent_limit_up and is_uptrend and on_support:
            # 5. 评分与复盘建议
            score = "高" if is_stop_drop and last_row['涨跌幅'] > 0 else "中"
            
            # 回测模拟：如果10天前符合条件，现在的收益如何？
            # 这里简化为信号强度逻辑
            advice = ""
            if score == "高":
                advice = "重点关注：回踩确认且止跌，建议分批试错，止损位设为有效跌破MA20。"
            else:
                advice = "观察：虽有支撑但力度一般，待出现放量阳线后再考虑。"

            return {
                "代码": code,
                "名称": names_dict.get(code, "未知"),
                "当前价": curr_price,
                "MA20": round(ma20, 2),
                "近期是否有涨停": "是",
                "信号强度": score,
                "操作建议": advice,
                "日期": last_row['日期']
            }
    except Exception as e:
        return None

def main():
    # 读取股票名称
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # 并行处理
    results = []
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        futures = [executor.submit(analyze_stock, f, names_dict) for f in files]
        for future in futures:
            res = future.result()
            if res: results.append(res)
    
    # 结果排序：优中选优（按信号强度）
    output_df = pd.DataFrame(results)
    if not output_df.empty:
        output_df = output_df.sort_values(by="信号强度", ascending=False).head(10) # 仅保留最精选的10只
        
        # 创建目录
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        if not os.path.exists(dir_path): os.makedirs(dir_path)
        
        # 保存文件
        file_name = f"Dragon_Back_20MA_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        output_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，选出 {len(output_df)} 只潜力股。结果已保存至 {full_path}")
    else:
        print("今日无符合战法条件的股票。")

if __name__ == '__main__':
    main()
