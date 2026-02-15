# coding:gbk
"""
L2大单统计策略 - 交易版
功能：基于L2大单统计数据生成交易信号，自动筛选主力流入强劲的股票

【重要说明】
净流入数据需要通过 主买-主卖 计算，因为netInflow字段可能返回0或NaN

交易策略：
1. 筛选条件：DDX > 0 且 主力净流入 > 阈值 且 大单买入占比 > 阈值
2. 买入信号：满足筛选条件且当前无持仓
3. 卖出信号：DDX连续为负或主力大幅流出
4. 持仓控制：最多持有N只股票

风险提示：本策略仅供学习参考，实盘交易请自行承担风险
"""

import pandas as pd
import numpy as np
import datetime
import time

# 全局变量
class G:
    stock_list = []           # 股票池
    hold_list = {}            # 持仓字典 {code: buy_price}
    last_check_time = None    # 上次检查时间
    
    # 策略参数
    max_holdings = 5          # 最大持仓数
    ddx_threshold = 0.5       # DDX阈值
    inflow_threshold = 100000 # 主力净流入阈值（股）
    big_buy_ratio_threshold = 0.3  # 大单买入占比阈值


def init(C):
    """初始化函数"""
    # 获取沪深A股列表（取前300只作为示例）
    all_stocks = C.get_stock_list_in_sector('沪深A股')
    G.stock_list = all_stocks[:300]
    
    print(f"=" * 60)
    print(f"L2大单统计交易策略初始化")
    print(f"=" * 60)
    print(f"股票池数量: {len(G.stock_list)}")
    print(f"最大持仓数: {G.max_holdings}")
    print(f"DDX阈值: {G.ddx_threshold}")
    print(f"主力净流入阈值: {G.inflow_threshold}")
    print(f"大单买入占比阈值: {G.big_buy_ratio_threshold}")
    print("【提示】净流入数据将通过 主买-主卖 计算")
    print(f"=" * 60)


def get_l2_data(C, stock_list):
    """获取L2大单统计数据"""
    try:
        data = C.get_market_data_ex(
            fields=[
                'ddx', 'ddy', 'ddz',
                'bidBigVolume',     # 主买大单成交量
                'offBigVolume',     # 主卖大单成交量
                'bidMediumVolume',  # 主买中单成交量
                'offMediumVolume',  # 主卖中单成交量
                'bidBigAmount',     # 主买大单成交额
                'bidTotalAmount'    # 主买累计成交额
            ],
            stock_code=stock_list,
            period='l2transactioncount',
            count=1
        )
        return data
    except Exception as e:
        print(f"获取L2数据失败: {e}")
        return {}


def analyze_stocks(C, l2_data):
    """分析股票，返回符合条件的买入候选"""
    candidates = []
    
    for code, df in l2_data.items():
        if df is None or df.empty:
            continue
            
        row = df.iloc[-1]
        
        # 获取原始数据（处理None和NaN）
        ddx = row.get('ddx', 0) or 0
        ddy = row.get('ddy', 0) or 0
        
        # 获取成交量数据
        bid_big_vol = row.get('bidBigVolume', 0) or 0
        off_big_vol = row.get('offBigVolume', 0) or 0
        bid_medium_vol = row.get('bidMediumVolume', 0) or 0
        off_medium_vol = row.get('offMediumVolume', 0) or 0
        
        # 获取成交额数据
        bid_big_amount = row.get('bidBigAmount', 0) or 0
        bid_total_amount = row.get('bidTotalAmount', 0) or 0
        
        # 【关键】计算净流入 = 主买 - 主卖
        net_inflow_big = bid_big_vol - off_big_vol
        net_inflow_medium = bid_medium_vol - off_medium_vol
        main_inflow = net_inflow_big + net_inflow_medium  # 主力净流入 = 大单 + 中单
        
        # 计算大单买入占比
        big_buy_ratio = bid_big_amount / bid_total_amount if bid_total_amount > 0 else 0
        
        # 综合评分（DDX权重40%，主力净流入权重40%，大单占比权重20%）
        score = ddx * 0.4 + (main_inflow / 100000) * 0.4 + big_buy_ratio * 100 * 0.2
        
        candidates.append({
            'code': code,
            'name': C.get_stock_name(code),
            'ddx': ddx,
            'ddy': ddy,
            'big_buy_ratio': big_buy_ratio,
            'net_inflow_big': net_inflow_big,
            'net_inflow_medium': net_inflow_medium,
            'main_inflow': main_inflow,
            'score': score,
            'meet_criteria': (ddx > G.ddx_threshold and 
                             main_inflow > G.inflow_threshold and 
                             big_buy_ratio > G.big_buy_ratio_threshold)
        })
    
    return pd.DataFrame(candidates) if candidates else pd.DataFrame()


def check_sell_signals(C, l2_data):
    """检查是否需要卖出"""
    sell_list = []
    
    for code in list(G.hold_list.keys()):
        if code not in l2_data:
            continue
            
        df = l2_data[code]
        if df is None or df.empty:
            continue
            
        row = df.iloc[-1]
        ddx = row.get('ddx', 0) or 0
        
        # 获取成交量计算净流入
        bid_big_vol = row.get('bidBigVolume', 0) or 0
        off_big_vol = row.get('offBigVolume', 0) or 0
        net_inflow_big = bid_big_vol - off_big_vol
        
        # 卖出条件：DDX < 0 或 大单净流出超过阈值
        if ddx < 0 or net_inflow_big < -G.inflow_threshold:
            sell_list.append(code)
            print(f"  卖出信号: {code} DDX={ddx:.2f}, 大单净流入={net_inflow_big:.0f}")
    
    return sell_list


def do_sell(C, code, reason=""):
    """执行卖出"""
    # 这里仅为示例，实际使用时需要传入account等参数
    # passorder(24, 1101, account, code, 5, -1, vol, 'L2策略', 2, f"卖出 {code} {reason}", C)
    if code in G.hold_list:
        del G.hold_list[code]
        print(f"  [模拟卖出] {code} - {reason}")


def do_buy(C, code, reason=""):
    """执行买入"""
    # 这里仅为示例，实际使用时需要传入account等参数
    # passorder(23, 1101, account, code, 5, -1, vol, 'L2策略', 2, f"买入 {code} {reason}", C)
    G.hold_list[code] = {'buy_time': datetime.datetime.now()}
    print(f"  [模拟买入] {code} - {reason}")


def handlebar(C):
    """主处理函数"""
    # 只处理最新K线
    if not C.is_last_bar():
        return
    
    # 控制频率（每30秒执行一次）
    now = datetime.datetime.now()
    if G.last_check_time and (now - G.last_check_time).seconds < 30:
        return
    G.last_check_time = now
    
    # 只在交易时间执行（9:30-11:30, 13:00-15:00）
    time_str = now.strftime('%H%M%S')
    if not (('093000' <= time_str <= '113000') or ('130000' <= time_str <= '150000')):
        return
    
    print(f"\n{'=' * 80}")
    print(f"[{now.strftime('%H:%M:%S')}] L2大单策略运行 - 当前持仓: {len(G.hold_list)}/{G.max_holdings}")
    print('=' * 80)
    
    # 获取L2数据
    l2_data = get_l2_data(C, G.stock_list)
    if not l2_data:
        print("未获取到L2数据")
        return
    
    # ========== 第一步：检查卖出信号 ==========
    print("\n【检查卖出信号】")
    sell_list = check_sell_signals(C, l2_data)
    for code in sell_list:
        do_sell(C, code, "DDX转负或主力流出")
    
    # ========== 第二步：分析买入候选 ==========
    print("\n【分析买入候选】")
    df_analysis = analyze_stocks(C, l2_data)
    
    if df_analysis.empty:
        print("无候选股票")
        return
    
    # 显示符合条件的股票
    qualified = df_analysis[df_analysis['meet_criteria'] == True]
    print(f"符合条件股票数: {len(qualified)}")
    
    if len(qualified) > 0:
        # 按评分排序
        top_candidates = qualified.nlargest(10, 'score')
        print("\n候选股票 TOP10:")
        for _, row in top_candidates.iterrows():
            print(f"  {row['code']} {row['name']:8s} 评分:{row['score']:8.2f} DDX:{row['ddx']:6.2f} 主力净流入:{row['main_inflow']:10.0f}")
    
    # ========== 第三步：执行买入 ==========
    # 检查是否有持仓空间
    available_slots = G.max_holdings - len(G.hold_list)
    if available_slots > 0 and len(qualified) > 0:
        print(f"\n【执行买入】可用仓位: {available_slots}")
        
        # 选择评分最高的且未持仓的股票
        top_candidates = qualified.nlargest(available_slots + 5, 'score')
        
        for _, row in top_candidates.iterrows():
            if available_slots <= 0:
                break
            code = row['code']
            if code not in G.hold_list:
                do_buy(C, code, f"评分{row['score']:.2f}, DDX{row['ddx']:.2f}")
                available_slots -= 1
    else:
        if available_slots <= 0:
            print("\n【执行买入】仓位已满")
        else:
            print("\n【执行买入】无符合条件的股票")
    
    # ========== 显示持仓状态 ==========
    print(f"\n【当前持仓】共 {len(G.hold_list)} 只:")
    for code in G.hold_list:
        name = C.get_stock_name(code)
        print(f"  {code} {name}")
    
    print(f"\n{'=' * 80}")
