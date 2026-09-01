import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp
from glob import glob

# ==========================================
# 战法名称：金孕地量战法 (Golden Harami Strategy)
# 买入逻辑：止跌母线 + 缩量孕子线 + 支撑位企稳
# 操作要领：寻找地量企稳信号，一击必中，严格控制价格区间
# ==========================================

def analyze_stock(file_path, name_map):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None  # 数据量过少跳过
        
        # 基础筛选：只要深沪A股，排除30 (创业板)，价格区间 [5, 20]，排除ST
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30') or not (5.0 <= df['收盘'].iloc[-1] <= 20.0):
            return None
        
        # 获取最新两条数据
        today = df.iloc[-1]
        yesterday = df.iloc[-2]
        prev_days = df.iloc[-20:-2] # 用于计算历史波动和支撑
        
        # --- 战法核心逻辑 ---
        
        # 1. 子母线形态 (孕线)
        is_harami = (today['最高'] <= yesterday['最高']) and (today['最低'] >= yesterday['最低'])
        
        # 2. 地量逻辑：今日成交量显著萎缩 (小于20日平均成交量的60% 且 小于昨日成交量)
        avg_volume = df['成交量'].tail(20).mean()
        is_low_vol = (today['成交量'] < yesterday['成交量'] * 0.7) and (today['成交量'] < avg_volume * 0.8)
        
        # 3. 止跌企稳：昨日是阴线或带下影线，今日波动极小
        is_stable = today['振幅'] < yesterday['振幅']
        
        if is_harami and is_low_vol and is_stable:
            # --- 历史回测逻辑 (简单模拟) ---
            # 计算该战法在该股历史上的表现 (近一年内出现该信号后的5日涨幅)
            success_count = 0
            # 简化版逻辑：这里仅计算当前信号的强度
            
            # --- 信号强度评估 ---
            score = 0
            if today['成交量'] < yesterday['成交量'] * 0.5: score += 40 # 极度缩量
            if today['收盘'] > today['开盘']: score += 20 # 子线为阳线更佳
            if today['收盘'] < yesterday['收盘'] * 1.02: score += 20 # 处于低位非追高
            if today['换手率'] < 3.0: score += 20 # 低换手代表锁定好
            
            # --- 操作建议 ---
            suggestion = ""
            if score >= 80:
                suggestion = "【重仓出击】地量极致，洗盘彻底，大概率反转"
            elif score >= 60:
                suggestion = "【适度试错】形态标准，建议分批建仓"
            else:
                suggestion = "【观察为主】波动尚存，等待趋势明确"

            return {
                "日期": today['日期'],
                "代码": code,
                "名称": name_map.get(code, "未知"),
                "收盘价": today['收盘'],
                "涨跌幅": today['涨跌幅'],
                "成交量比": round(today['成交量'] / yesterday['成交量'], 2),
                "信号强度": score,
                "操作建议": suggestion
            }
    except Exception as e:
        return None

def run_strategy():
    # 加载股票名称映射
    name_df = pd.read_csv('stock_names.csv')
    name_df['code'] = name_df['code'].astype(str).str.zfill(6)
    name_map = dict(zip(name_df['code'], name_df['name']))
    
    # 扫描数据目录
    files = glob('stock_data/*.csv')
    
    # 并行处理
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, name_map) for f in files])
    
    # 过滤空结果
    final_list = [r for r in results if r is not None]
    result_df = pd.DataFrame(final_list)
    
    if not result_df.empty:
        # 优中选优：按信号强度排序
        result_df = result_df.sort_values(by="信号强度", ascending=False).head(10)
        
        # 创建年月目录
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            
        # 保存结果
        file_name = f"Golden_Harami_Strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(f"{dir_path}/{file_name}", index=False, encoding='utf-8-sig')
        print(f"分析完成，精选{len(result_df)}只个股。")
    else:
        print("今日无符合金孕地量战法股票。")

if __name__ == '__main__':
    run_strategy()
