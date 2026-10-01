import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：分歧转一致（16字口诀战法）
# 核心逻辑：
# 1. 涨停放量：前期有过涨停且量能释放，确立强势地位。
# 2. 缩量企稳：洗盘期成交量萎缩，股价不破关键位（前阳线实体）。
# 3. 破位上车：利用急跌洗出浮筹，随后快速收复，形成“黄金坑”。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"
OUTPUT_BASE = "Screening_Results"

def get_signal_strength(row, df):
    """
    基于逻辑深度打分：
    - 换手率健康度
    - 收盘价是否站稳前阳线顶部
    - 回调时的缩量程度
    """
    score = 0
    # 逻辑1：缩量越极致，反转潜力越大
    if row['换手率'] < df['换手率'].tail(10).mean() * 0.6: score += 40
    # 逻辑2：价格处于5.0-20.0区间
    if 8.0 <= row['收盘'] <= 15.0: score += 20 # 优选黄金价格区间
    # 逻辑3：近期有涨停经历
    if (df['涨跌幅'].tail(15) > 9.5).any(): score += 40
    
    if score >= 80: return "极强（一击必中）", "重点关注，分批建仓"
    if score >= 60: return "中等（试错观察）", "轻仓介入，等待破位拉起"
    return "一般", "暂时放弃"

def process_single_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30: return None
        
        # 基础数据清洗与排序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        last_row = df.iloc[-1]
        code = str(last_row['股票代码']).zfill(6)
        
        # --- 基础条件过滤 ---
        # 1. 排除ST, 30开头, 5-20元价格区间
        if "ST" in file_path or code.startswith('30'): return None
        if not (5.0 <= last_row['收盘'] <= 20.0): return None
        
        # --- 战法逻辑核心筛选 ---
        # A. 寻找最近5天内的放量涨停
        recent_5 = df.tail(5)
        has_limit_up = (recent_5['涨跌幅'] > 9.8).any()
        
        # B. 缩量判定：当前量能小于近5日平均量能的70%
        is_shrinking = last_row['成交量'] < df['成交量'].tail(5).mean() * 0.7
        
        # C. 企稳判定：收盘价不破5日均线
        ma5 = df['收盘'].tail(5).mean()
        is_stable = last_row['收盘'] >= ma5
        
        if has_limit_up and is_shrinking and is_stable:
            strength, advice = get_signal_strength(last_row, df)
            return {
                "代码": code,
                "日期": last_row['日期'].strftime('%Y-%m-%d'),
                "当前价": last_row['收盘'],
                "换手率": last_row['换手率'],
                "涨跌幅": last_row['涨跌幅'],
                "信号强度": strength,
                "操作建议": advice
            }
    except Exception as e:
        return None

def main():
    # 1. 扫描文件
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"开始扫描 {len(csv_files)} 个股票文件...")
    
    # 2. 并行处理加速
    with Pool(cpu_count()) as pool:
        results = pool.map(process_single_stock, csv_files)
    
    # 3. 结果汇总
    valid_results = [r for r in results if r is not None]
    final_df = pd.DataFrame(valid_results)
    
    # 4. 匹配名称
    if not final_df.empty and os.path.exists(NAMES_FILE):
        names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        names_df['code'] = names_df['code'].str.zfill(6)
        final_df = final_df.merge(names_df, left_on="代码", right_on="code", how="left")
        final_df = final_df[['代码', 'name', '当前价', '涨跌幅', '信号强度', '操作建议']]
        final_df.rename(columns={'name': '股票名称'}, inplace=True)
        
        # 优中选优：只取信号强度为“中等”以上的
        final_df = final_df[final_df['信号强度'] != "一般"]

    # 5. 保存结果（按年月目录）
    now = datetime.now()
    dir_path = os.path.join(now.strftime('%Y'), now.strftime('%m'))
    os.makedirs(dir_path, exist_ok=True)
    
    file_name = f"Strong_Signal_Screener_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    if not final_df.empty:
        final_df.to_csv(save_path, index=False, encoding='utf_8_sig')
        print(f"筛选完成，找到 {len(final_df)} 只潜力股。结果已保存至: {save_path}")
    else:
        with open(save_path, "w") as f: f.write("今日无符合强信号条件的股票")
        print("今日未发现符合条件的股票。")

if __name__ == "__main__":
    main()
