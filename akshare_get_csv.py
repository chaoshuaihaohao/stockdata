import akshare as ak
import pandas as pd
import argparse
import sys
from datetime import datetime
import warnings
import os

warnings.filterwarnings('ignore')


def get_stock_data(symbol, adjust_type="qfq"):
    """
    获取 A 股股票历史数据，并保存为 CSV 到脚本所在目录的 data 子目录下，保留所有列
    :param symbol: 股票代码，如 '600519'
    :param adjust_type: 复权类型，'qfq'（前复权，默认）、'hfq'（后复权）、'none'（不复权）
    """
    print(f"正在获取股票 {symbol} 的历史数据（复权类型: {adjust_type}）...")

    # 映射用户输入到 akshare 所需的 adjust 参数
    if adjust_type.lower() in ["none", "no", "false", ""]:
        ak_adjust = ""
    elif adjust_type.lower() == "hfq":
        ak_adjust = "hfq"
    else:  # 默认包括 'qfq' 或其他有效值
        ak_adjust = "qfq"

    try:
        # 使用 ak.stock_zh_a_hist 接口
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date="19900101",
            end_date=datetime.now().strftime('%Y%m%d'),
            adjust=ak_adjust  # 传入处理后的复权参数
        )

        if df.empty:
            print(f"错误：未能获取到股票 {symbol} 的数据，请检查代码是否正确。")
            return

        # 数据清洗和格式标准化
        # 1. 重命名列名（中文转英文）
        df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'price_change',
            '换手率': 'turnover'
        }, inplace=True)

        # 2. 确保日期格式标准化 (YYYY-MM-DD)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 3. 数据类型转换：将数值列转为 numeric，非数值设为 NaN
        numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'amount',
                        'amplitude', 'pct_change', 'price_change', 'turnover']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. 删除在关键价格/成交量列中存在缺失的行（可选，根据需求调整）
        # 这里保留宽松策略：仅当 open/high/low/close/volume 全为空时才删
        essential_cols = ['open', 'high', 'low', 'close', 'volume']
        df = df.dropna(subset=essential_cols, how='all')

        # ========== 保存到 data 子目录 ==========
        # 1. 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 2. 拼接 data 目录路径
        data_dir = os.path.join(script_dir, "A_data")
        # 3. 若 data 目录不存在则自动创建
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"已创建 data 目录：{data_dir}")

        # 4. 生成文件名并拼接完整路径
        adj_suffix = "no_adj" if ak_adjust == "" else ak_adjust
        filename = f"{symbol}_{adj_suffix}_A_data.csv"
        full_file_path = os.path.join(data_dir, filename)

        # 5. 保存 CSV 到 data 目录
        df.to_csv(full_file_path, index=False, encoding='utf-8')

        print(f"成功！数据已保存至: {full_file_path}")
        print(f"数据总行数: {len(df)}")
        print("前5行数据预览：")
        print(df.head())

    except Exception as e:
        print(f"发生异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='获取 A 股历史数据工具（支持 qfq/hfq/不复权，保留所有列）')
    parser.add_argument('--code', type=str, required=True, help='股票代码，例如 600519')
    parser.add_argument('--adjust', type=str, default='qfq',
                        choices=['qfq', 'hfq', 'none'],
                        help='复权类型：qfq（前复权，默认）、hfq（后复权）、none（不复权）')

    args = parser.parse_args()
    get_stock_data(args.code, args.adjust)
