# coding:gbk
"""
L2大单统计策略 - 简化版
功能：在HANDLEBAR()中读取沪深全市场股票的L2大单统计数据（简化版，快速查看）

【重要说明】
净流入数据需要通过 主买-主卖 计算，因为netInflow字段可能返回0或NaN

使用说明：
1. 在QMT模型交易界面运行此策略
2. 需要投研版L2行情权限
3. 建议在小周期（如1分钟）上运行，以获取实时更新
"""

import pandas as pd
import datetime
import time

# 全局变量
class G:
    stock_list = []      # 股票列表
    last_time = None     # 上次处理时间


def init(C):
    """初始化"""
    # 获取沪深A股列表
    G.stock_list = C.get_stock_list_in_sector('沪深A股')
    print(f"股票总数: {len(G.stock_list)}")
    
    # 只选取前500只活跃股票作为示例（全市场获取数据量太大）
    # 如需全市场，请注释掉下面这行
    G.stock_list = G.stock_list[:500]
    print(f"本次监控股票数: {len(G.stock_list)}")
    print("【提示】净流入数据将通过 主买-主卖 计算")


def handlebar(C):
    """主处理函数"""
    # 只处理最新K线
    if not C.is_last_bar():
        return
    
    # 控制频率（每10秒执行一次）
    now = datetime.datetime.now()
    if G.last_time and (now - G.last_time).seconds < 10:
        return
    G.last_time = now
    
    print(f"\n=== [{now.strftime('%H:%M:%S')}] 获取L2大单数据 ===")
    
    # 获取L2大单统计数据
    # 注意：获取 bidBigVolume 和 offBigVolume 来计算净流入
    l2_data = C.get_market_data_ex(
        fields=[
            'ddx', 'ddy', 'ddz',
            'bidBigVolume',    # 主买大单成交量
            'offBigVolume',    # 主卖大单成交量
            'bidMediumVolume', # 主买中单成交量
            'offMediumVolume'  # 主卖中单成交量
        ],
        stock_code=G.stock_list,
        period='l2transactioncount',
        count=1
    )
    
    # 整理数据
    results = []
    for code, df in l2_data.items():
        if df is not None and not df.empty:
            row = df.iloc[-1]
            
            # 获取原始数据（处理None和NaN）
            ddx = row.get('ddx', 0) or 0
            ddy = row.get('ddy', 0) or 0
            ddz = row.get('ddz', 0) or 0
            
            bid_big = row.get('bidBigVolume', 0) or 0
            off_big = row.get('offBigVolume', 0) or 0
            bid_medium = row.get('bidMediumVolume', 0) or 0
            off_medium = row.get('offMediumVolume', 0) or 0
            
            # 【关键】计算净流入 = 主买 - 主卖
            net_inflow_big = bid_big - off_big
            net_inflow_medium = bid_medium - off_medium
            main_net_inflow = net_inflow_big + net_inflow_medium  # 主力净流入 = 大单 + 中单
            
            results.append({
                'code': code,
                'name': C.get_stock_name(code),
                'ddx': ddx,
                'ddy': ddy,
                'ddz': ddz,
                'net_inflow_big': net_inflow_big,
                'net_inflow_medium': net_inflow_medium,
                'main_net_inflow': main_net_inflow
            })
    
    if not results:
        print("未获取到数据")
        return
    
    # 创建DataFrame并排序
    df_result = pd.DataFrame(results)
    
    # 显示DDX TOP5
    print("\n【DDX 排名 TOP5】")
    top5 = df_result.nlargest(5, 'ddx')
    for _, row in top5.iterrows():
        print(f"  {row['code']} {row['name']}: DDX={row['ddx']:.2f}, DDY={row['ddy']:.2f}, 主力净流入={row['main_net_inflow']:.0f}")
    
    # 显示主力净流入 TOP5（大单+中单）
    print("\n【主力净流入(大单+中单) TOP5】")
    top5_inflow = df_result.nlargest(5, 'main_net_inflow')
    for _, row in top5_inflow.iterrows():
        print(f"  {row['code']} {row['name']}: 主力净流入={row['main_net_inflow']:.0f} (大单:{row['net_inflow_big']:.0f} 中单:{row['net_inflow_medium']:.0f})")
    
    # 显示大单净流入 TOP5
    print("\n【大单净流入 TOP5】")
    top5_big = df_result.nlargest(5, 'net_inflow_big')
    for _, row in top5_big.iterrows():
        print(f"  {row['code']} {row['name']}: 大单净流入={row['net_inflow_big']:.0f}")
    
    print(f"\n=== 处理完成，共 {len(results)} 只股票 ===")
