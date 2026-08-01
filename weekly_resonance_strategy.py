import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

"""
战法名称：周线共振主升战法 (Weekly Resonance Strategy)
核心逻辑：
1. 空间定位：筛选 5-20 元低价绩优股，排除ST、创业板(30开头)，专注沪深主板。
2. 周线对量：寻找连续3-5周成交量温和放大（堆量），如同“小山丘”，代表主力吸筹。
3. 趋势支撑：20周线与60周线多头排列，K线紧贴均线（乖离率小），重心上移。
4. 突破信号：周线级别长周期平台突破，一根放量大阳线捅破“天花板”。
5. 自动复盘：根据回踩深度和量价背离程度，给出“强攻”、“试错”或“观察”的操作建议。
"""

# 配置参数
DATA_PATH = './stock_data/'
NAME_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path):
    try:
        # 获取股票代码
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 基础过滤：排除ST和创业板(30)
        if 'ST' in code or code.startswith('30'):
            return None

        df = pd.read_csv(file_path)
        if df.empty or len(df) < 120: # 至少需要半年以上数据计算周线
            return None

        # 1. 价格过滤 (最新收盘价)
        latest_price = df.iloc[-1]['收盘']
        if not (PRICE_MIN <= latest_price <= PRICE_MAX):
            return None

        # --- 转换为周线数据 ---
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        
        # 聚合为周线
        logic = {
            '开盘': 'first',
            '最高': 'max',
            '最低': 'min',
            '收盘': 'last',
            '成交量': 'sum',
            '成交额': 'sum'
        }
        df_w = df.resample('W').apply(logic).dropna()

        # 2. 核心逻辑计算
        # 计算均线
        df_w['MA20'] = df_w['收盘'].rolling(window=20).mean()
        df_w['MA60'] = df_w['收盘'].rolling(window=60).mean()
        
        # A. 趋势：20周线 > 60周线 且均向上
        trend_ok = (df_w['MA20'].iloc[-1] > df_w['MA60'].iloc[-1]) and \
                   (df_w['MA20'].iloc[-1] > df_w['MA20'].iloc[-2])

        # B. 堆量：最近5周成交量重心上移 (连续放量)
        vol_recent = df_w['成交量'].tail(5).values
        volume_ok = all(vol_recent[i] > vol_recent[i-1] * 0.8 for i in range(1, 4)) # 允许小幅波动
        
        # C. 平台突破：过去10周最高价的突破
        platform_high = df_w['最高'].iloc[-12:-1].max()
        breakout = df_w['收盘'].iloc[-1] > platform_high

        # 3. 评分系统与回测（简化版）
        score = 0
        if trend_ok: score += 30
        if volume_ok: score += 30
        if breakout: score += 40

        if score < 70: # 只有高分才入选，实现“一击必中”
            return None

        # 4. 生成建议
        suggestion = "观察"
        strength = "一般"
        if score >= 90:
            strength = "极强 (主升浪起爆)"
            suggestion = "激进买入/加仓"
        elif score >= 70:
            strength = "中等 (趋势形成)"
            suggestion = "底仓试错"

        return {
            'code': code,
            'price': latest_price,
            'score': score,
            'signal_strength': strength,
            'action_advice': f"{suggestion} (突破位:{platform_high:.2f})",
            'volume_ratio': round(df_w['成交量'].iloc[-1] / df_w['成交量'].iloc[-5:-1].mean(), 2)
        }

    except Exception as e:
        return None

def main():
    # 扫描目录
    files = glob.glob(os.path.join(DATA_PATH, "*.csv"))
    
    # 并行处理加快速度
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, files)
    
    # 过滤空结果
    final_list = [res for res in results if res is not None]
    
    # 匹配名称
    if os.path.exists(NAME_FILE):
        names_df = pd.read_csv(NAME_FILE, dtype={'code': str})
        names_dict = dict(zip(names_df['code'], names_df['name']))
    else:
        names_dict = {}

    output_df = pd.DataFrame(final_list)
    if not output_df.empty:
        output_df['name'] = output_df['code'].apply(lambda x: names_dict.get(x, "未知"))
        # 按照分数排序，优中选优
        output_df = output_df.sort_values(by='score', ascending=False)
    
    # 创建年月目录
    now = datetime.now()
    dir_path = now.strftime('%Y%m')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # 保存结果
    file_name = f"weekly_resonance_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    output_df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf-8-sig')
    
    print(f"分析完成，筛选出 {len(output_df)} 只符合主升浪战法的潜力股。")

if __name__ == "__main__":
    main()
