import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import OperationalError

# --- Database Connection Parameters ---
DB_PARAMS = {
    'host': '10.128.21.148',
    'port': '5431',
    'database': 'cas25_test',
    'user': 'readonly_cas25_test',
    'password': 'readonly_cas25_test'
}

# --- Database Extraction Functions ---

def get_data_from_db(val_method, sql_query, group_by_columns, table_name, additional_where_clause=""):
    """
    Connects to the database, executes a specified query, and returns a DataFrame.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print(f"数据库连接成功！正在查询 val_method = '{val_method}' 的数据...")
        
        query = f"""
        {sql_query}
        FROM
            {table_name}
        WHERE
            "val_month" = '202412' AND "val_method" = '{val_method}' {additional_where_clause}
        GROUP BY
            {', '.join(f'"{col}"' for col in group_by_columns)}
        """
        
        df = pd.read_sql_query(query, conn)
        print(f"val_method = '{val_method}' 查询完成！")
        return df
        
    except OperationalError as e:
        print(f"数据库连接失败: {e}")
        return None
    except Exception as e:
        print(f"查询 val_method = '{val_method}' 时发生错误: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()
            print("数据库连接已关闭。")

def save_to_excel(df, filename):
    """Saves a DataFrame to an Excel file."""
    if df is not None and not df.empty:
        try:
            print(f"正在将数据保存到 {filename}...")
            df.to_excel(filename, index=False)
            print(f"数据已成功保存到 {filename}")
        except Exception as e:
            print(f"保存到Excel文件 {filename} 时出错: {e}")
    else:
        print(f"没有为 {filename} 查询到数据，或查询出错。")

def execute_raw_query(sql_query, description):
    """
    Connects to the database, executes a raw SQL query, and returns a DataFrame.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print(f"数据库连接成功！正在执行: {description}...")
        
        df = pd.read_sql_query(sql_query, conn)
        print(f"查询 '{description}' 完成！")
        return df
        
    except OperationalError as e:
        print(f"数据库连接失败: {e}")
        return None
    except Exception as e:
        print(f"查询 '{description}' 时发生错误: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()
            print("数据库连接已关闭。")

# --- Data Processing Functions ---

def process_direct_business(df_direct, filter_enabled=False):
    """
    Processes direct business data to generate accounting entries.
    """
    print("正在处理直保业务...")

    i17_names = {
        '2606010801': "未到期责任负债-未来现金流-现金流/保费-保费收入",
        '2606011002': "未到期责任负债-未来现金流-现金流/获取费用-手续费及佣金支出/佣金",
        '2606011102': "未到期责任负债-未来现金流-保费分配法分摊的收入-保费收入/直接业务",
        '2606011603': "未到期责任负债-未来现金流-获取费用摊销计入支出-保费分配法/直接业务",
        '2606011202': "未到期责任负债-未来现金流-保费分配法亏损合同损益-亏损提转差/直接业务",
        '2606011302': "未到期责任负债-未来现金流-保险财务费用-当期计提利息/保费分配法/直接业务",
    }
    
    rules = [
        {'类型': '签单保费', '借贷方向': '贷', 'I17科目代码': '2606010801', '取数口径': '正数', '金额来源': '保费_本币', '符号': 1},
        {'类型': '获取费用', '借贷方向': '贷', 'I17科目代码': '2606011002', '取数口径': '负数', '金额来源': '保险获取现金流_本币', '符号': -1},
        {'类型': '已经过保费', '借贷方向': '贷', 'I17科目代码': '2606011102', '取数口径': '负数', '金额来源': '保险合同收入', '符号': -1},
        {'类型': '获取费用摊销', '借贷方向': '贷', 'I17科目代码': '2606011603', '取数口径': '正数', '金额来源': '当期确认的IACF', '符号': 1},
        {'类型': '亏损(保费不足)', '借贷方向': '贷', 'I17科目代码': '2606011202', '取数口径': '正数', '金额来源': '亏损部分', '符号': 1},
        {'类型': '计息', '借贷方向': '贷', 'I17科目代码': '2606011302', '取数口径': '正数', '金额来源': 'IACF计息', '符号': 1},
    ]
    skip_codes_when_filtered = {'2606010801', '2606011002'}
    
    measure_dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同分组编号', '险种代码', '险类代码', '合同组合编号']
    all_entries = []
    
    for rule in rules:
        if rule['金额来源'] not in df_direct.columns:
            print(f"警告：在直保数据中找不到源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
            continue

        # 起期>20241231的保单已在WHERE条件中排除，直接使用聚合结果生成分录
        source_df = df_direct

        temp_df = source_df[measure_dimension_cols].copy()
        temp_df['类型'] = rule['类型']
        temp_df['借贷方向'] = rule['借贷方向']
        temp_df['I17科目代码'] = rule['I17科目代码']
        temp_df['I17科目名称'] = i17_names.get(rule['I17科目代码'])
        temp_df['取数口径'] = rule['取数口径']
        temp_df['金额'] = source_df[rule['金额来源']] * rule['符号']
        
        all_entries.append(temp_df)
        
    if not all_entries:
        return pd.DataFrame()
        
    final_df = pd.concat(all_entries, ignore_index=True)
    
    print("直保业务处理完成。")
    return final_df

def process_assumed_reinsurance(df_assumed):
    """
    Processes assumed reinsurance data to generate accounting entries.
    """
    print("正在处理分入业务...")

    i17_names = {
        '2606010901': '未到期责任负债-未来现金流-现金流/分入保费-分保费收入/比例合同',
        '2606010904': '未到期责任负债-未来现金流-现金流/分入保费-分保费收入/比例临分',
        '2606010911': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/比例合同',
        '2606010913': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/比例临分',
        '2606010921': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/经纪费/比例合同',
        '2606010923': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/经纪费/比例临分',
        '2606010990': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/业务及管理费结转',
        '2606011101': '未到期责任负债-未来现金流-保费分配法分摊的收入-保费收入/分入业务',
        '2606011602': '未到期责任负债-未来现金流-获取费用摊销计入支出-保费分配法/分入业务',
        '2606011301': '未到期责任负债-未来现金流-保险财务费用-当期计提利息/保费分配法/分入业务',
        '2606011201': '未到期责任负债-未来现金流-保费分配法亏损合同损益-亏损提转差/分入业务'
    }

    # contract_flag: 1 is facultative (临分), 2 is contract (合同)
    df_assumed['is_contract'] = df_assumed['合同标识'].astype(str) == '2'

    rules = [
        {'类型': '分保费收入', '金额来源': '分保费收入', '符号': 1, '取数口径': '正数', 'contract_code': '2606010901', 'facultative_code': '2606010904'},
        {'类型': '分保费用', '金额来源': '分保费用', '符号': -1, '取数口径': '负数', 'contract_code': '2606010911', 'facultative_code': '2606010913'},
        {'类型': '经纪费', '金额来源': '经纪费', '符号': -1, '取数口径': '负数', 'contract_code': '2606010921', 'facultative_code': '2606010923'},
        {'类型': '业务及管理费结转', '金额来源': '业务及管理费结转', '符号': -1, '取数口径': '负数', 'code': '2606010990'},
        {'类型': '已经过保费', '金额来源': ['预收净保费摊销', '累积计息摊销'], '符号': -1, '取数口径': '负数', 'code': '2606011101'},
        {'类型': '获取费用摊销', '金额来源': '获取费用摊销', '符号': 1, '取数口径': '正数', 'code': '2606011602'},
        {'类型': '亏损', '金额来源': '亏损部分', '符号': 1, '取数口径': '正数', 'code': '2606011201'},
        {'类型': '计息', '金额来源': '计息', '符号': 1, '取数口径': '正数', 'code': '2606011301'},
    ]
    
    measure_dimension_cols = ['归属机构', '车辆种类', '使用性质代码', '合同组合编号', '合同分组编号', '评估方法', '险种代码', '险类代码', '合同标识', '临分类型', '合约类型', '分出类型']
    all_entries = []

    for rule in rules:
        # Check for multiple source columns
        if isinstance(rule['金额来源'], list):
            if not all(col in df_assumed.columns for col in rule['金额来源']):
                print(f"警告：在分入数据中找不到一个或多个源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
                continue
        else:
            if rule['金额来源'] not in df_assumed.columns:
                print(f"警告：在分入数据中找不到源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
                continue
        
        temp_df = df_assumed[measure_dimension_cols].copy()
        temp_df['类型'] = rule['类型']
        temp_df['借贷方向'] = '贷'
        
        if 'code' in rule:
            temp_df['I17科目代码'] = rule['code']
        else:
            temp_df['I17科目代码'] = np.where(df_assumed['is_contract'], rule['contract_code'], rule['facultative_code'])
            
        temp_df['I17科目名称'] = temp_df['I17科目代码'].map(i17_names)
        temp_df['取数口径'] = rule['取数口径']

        if isinstance(rule['金额来源'], list):
            # Sum up columns for the amount
            temp_df['金额'] = df_assumed[rule['金额来源']].sum(axis=1) * rule['符号']
        else:
            temp_df['金额'] = df_assumed[rule['金额来源']] * rule['符号']
        
        all_entries.append(temp_df)

    if not all_entries:
        return pd.DataFrame()

    final_df = pd.concat(all_entries, ignore_index=True)
    
    print("分入业务处理完成。")
    return final_df

def process_ceded_reinsurance(df_ceded):
    """
    Processes ceded reinsurance data to generate accounting entries.
    """
    print("正在处理分出业务...")

    i17_names = {
        '1252010501': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-直接业务/比例合同",
        '1252010503': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-直接业务/比例临分",
        '1252010511': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-分入业务/比例合同",
        '1252010513': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-分入业务/比例临分",
        '1252010521': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-摊回分保费用/直接业务/比例合同",
        '1252010523': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-摊回分保费用/直接业务/比例临分",
        '1252010531': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-摊回分保费用/分入业务/比例合同",
        '1252010533': "分保摊回未到期责任资产-未来现金流-现金流/分出保费-摊回分保费用/分入业务/比例临分",
        '1252010301': "分保摊回未到期责任资产-未来现金流-保费分配法分摊的分出保费-分出保费/直接业务",
        '1252010302': "分保摊回未到期责任资产-未来现金流-保费分配法分摊的分出保费-分出保费/分入业务",
        '1252010401': "分保摊回未到期责任资产-未来现金流-保费分配法亏损摊回调整-亏损摊回调整/直接业务",
        '1252010402': "分保摊回未到期责任资产-未来现金流-保费分配法亏损摊回调整-亏损摊回调整/分入业务",
        '1252010201': "分保摊回未到期责任资产-未来现金流-摊回赔付/投资成分-摊回赔付支出/直接业务/比例合同",
        '1252010202': "分保摊回未到期责任资产-未来现金流-摊回赔付/投资成分-摊回赔付支出/直接业务/比例临分",
        '1253010501': "分保摊回已发生赔款资产-未来现金流-摊回赔付/投资成分-应收分保账款/摊回分保赔款/直接业务/比例合同",
        '1253010502': "分保摊回已发生赔款资产-未来现金流-摊回赔付/投资成分-应收分保账款/摊回分保赔款/直接业务/比例临分",
        '1252010101': "分保摊回未到期责任资产-未来现金流-保险财务费用-计息及金融假设的变化/直接业务",
        '1252010102': "分保摊回未到期责任资产-未来现金流-保险财务费用-计息及金融假设的变化/分入业务"
    }

    df_ceded['分出类型'] = df_ceded['分出类型'].astype(str)
    # contract_flag: 1 is facultative (临分), 2 is contract (合同)
    df_ceded['is_contract'] = df_ceded['合同标识'].astype(str) == '2'

    rules = [
        {'类型': '分出保费', '金额来源': '分出保费', '符号': 1, '取数口径': '正数',
         'codes': {'1_True': '1252010501', '1_False': '1252010503', '2_True': '1252010511', '2_False': '1252010513'}},
        {'类型': '摊回分保费用', '金额来源': ['手续费_本币', '经纪费_本币'], '符号': -1, '取数口径': '负数',
         'codes': {'1_True': '1252010521', '1_False': '1252010523', '2_True': '1252010531', '2_False': '1252010533'}},
        {'类型': '分出保费的分摊', '金额来源': ['预收净保费摊销', '累积计息摊销'], '符号': -1, '取数口径': '负数',
         'codes': {'1': '1252010301', '2': '1252010302'}},
        {'类型': '亏损摊回', '金额来源': '亏损摊回部分', '符号': 1, '取数口径': '正数',
         'codes': {'1': '1252010401', '2': '1252010402'}},
        {'类型': '计息', '金额来源': '计息', '符号': 1, '取数口径': '正数',
         'codes': {'1': '1252010101', '2': '1252010102'}},
    ]
    
    dimension_cols = ['归属机构', '车辆种类', '使用性质代码', '合同组合编号', '合同分组编号', '评估方法', '险种代码', '险类代码', '合同标识', '临分类型', '合约类型', '分出类型']
    all_entries = []

    for rule in rules:
        # Check for multiple source columns
        if isinstance(rule['金额来源'], list):
            if not all(col in df_ceded.columns for col in rule['金额来源']):
                print(f"警告：在分出数据中找不到一个或多个源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
                continue
        else:
            if rule['金额来源'] not in df_ceded.columns:
                print(f"警告：在分出数据中找不到源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
                continue
            
        temp_df = df_ceded[dimension_cols].copy()
        temp_df['类型'] = rule['类型']
        temp_df['借贷方向'] = '借'

        if rule['类型'] in ['分出保费', '摊回分保费用']:
            key_series = temp_df['分出类型'] + '_' + df_ceded['is_contract'].astype(str)
            temp_df['I17科目代码'] = key_series.map(rule['codes'])
        elif rule['类型'] in ['分出保费的分摊', '亏损摊回', '计息']:
            temp_df['I17科目代码'] = temp_df['分出类型'].map(rule['codes'])
        
        temp_df['I17科目名称'] = temp_df['I17科目代码'].map(i17_names)
        temp_df['取数口径'] = rule['取数口径']
        
        if isinstance(rule['金额来源'], list):
            temp_df['金额'] = df_ceded[rule['金额来源']].sum(axis=1) * rule['符号']
        else:
            temp_df['金额'] = df_ceded[rule['金额来源']] * rule['符号']

        all_entries.append(temp_df)
        
    # Handle '投资成分' separately as it creates two entries
    if '投资成分' in df_ceded.columns:
        is_contract_series = df_ceded['is_contract']
        
        # Entry 1
        temp_df_1 = df_ceded[dimension_cols].copy()
        temp_df_1['类型'] = '投资成分'
        temp_df_1['借贷方向'] = '借'
        temp_df_1['I17科目代码'] = np.where(is_contract_series, '1252010201', '1252010202')
        temp_df_1['I17科目名称'] = temp_df_1['I17科目代码'].map(i17_names)
        temp_df_1['取数口径'] = '负数, 已摊销投资成分'
        temp_df_1['金额'] = df_ceded['投资成分'] * -1
        all_entries.append(temp_df_1)
        
        # Entry 2
        temp_df_2 = df_ceded[dimension_cols].copy()
        temp_df_2['类型'] = '投资成分'
        temp_df_2['借贷方向'] = '借'
        temp_df_2['I17科目代码'] = np.where(is_contract_series, '1253010501', '1253010502')
        temp_df_2['I17科目名称'] = temp_df_2['I17科目代码'].map(i17_names)
        temp_df_2['取数口径'] = '正数, 已摊销投资成分'
        temp_df_2['金额'] = df_ceded['投资成分']
        all_entries.append(temp_df_2)
    
    if not all_entries:
        return pd.DataFrame()

    final_df = pd.concat(all_entries, ignore_index=True)
    
    print("分出业务处理完成。")
    return final_df

def transform_to_final_format(df, insurance_type, mappings):
    """
    Transforms the generated entries into the final accounting format.
    """
    print(f"开始转换最终格式 (insurance_type={insurance_type})...")
    
    # Handle missing '业务渠道' for reinsurance data
    if '业务渠道' not in df.columns:
        df['业务渠道'] = '0'

    # Clean and standardize keys before mapping
    df['险种代码_str'] = df['险种代码'].astype(str).str.strip()
    df['归属机构_str'] = df['归属机构'].astype(str).str.strip()
    df['业务渠道_str'] = df['业务渠道'].astype(str).str.strip()
    df['使用性质代码_str'] = df['使用性质代码'].astype(str).str.strip()
    df['车辆种类_str'] = df['车辆种类'].astype(str).str.strip()

    # 1. Apply mappings
    df['product_segment'] = df['险种代码_str'].map(mappings['product'])
    df['org_segment'] = df['归属机构_str'].map(mappings['org'])
    df['cost_center_segment'] = df['归属机构_str'].map(mappings['cost_center'])
    df['channel_segment'] = df['业务渠道_str'].map(mappings['channel'])
    
    # Handle two-column key for car mapping
    df['car_key'] = df['使用性质代码_str'] + '_' + df['车辆种类_str']
    df['car_cash_segment'] = df['car_key'].map(mappings['car'])

    # 2. Add new columns based on rules
    df['sj_id'] = [f"RAND_{i}" for i in range(len(df))] # Placeholder for random ID
    df['account_period'] = '202412'
    df['dc_cd'] = df['借贷方向'].map({'借': 'D', '贷': 'C'})
    df['account_name'] = df['I17科目名称']
    df['agriculture_segment'] = '0'
    df['detail_segment'] = '0'
    df['coverage_segment'] = '0'
    df['reserve1'] = '0'
    df['reserve2'] = '0'
    df['portfolio_id'] = df['合同组合编号']
    df['insurance_contract_group_id'] = df['合同分组编号']
    df['origin_currency_code'] = 'CNY' # Assuming CNY from context
    df['origin_currency_amt'] = df['金额']
    df['exchange_rate'] = 1.00
    df['local_currency_code'] = 'CNY'
    df['local_currency_amt'] = df['origin_currency_amt']
    df['dc_local_currency_amt'] = np.where(df['dc_cd'] == 'C', -df['local_currency_amt'], df['local_currency_amt'])
    df['evaluate_method'] = '4'
    df['insurance_type'] = insurance_type
    df['origin_data_type'] = '9'

    # 3. Select and reorder final columns
    final_columns = [
        'sj_id', 'account_period', 'dc_cd', 'account_code', 'account_name', 'org_segment',
        'agriculture_segment', 'cost_center_segment', 'detail_segment', 'product_segment',
        'coverage_segment', 'channel_segment', 'car_cash_segment', 'reserve1', 'reserve2',
        'portfolio_id', 'insurance_contract_group_id', 'origin_currency_code', 'origin_currency_amt',
        'exchange_rate', 'local_currency_code', 'local_currency_amt', 'dc_local_currency_amt',
        'evaluate_method', 'insurance_type', 'origin_data_type'
    ]
    # Rename account code for final output
    df.rename(columns={'I17科目代码': 'account_code'}, inplace=True)
    
    final_df = df[final_columns]
    print("最终格式转换完成。")
    return final_df

# --- Main Execution Logic ---

def build_filter_condition(table_name, val_method, filter_enabled=True, sign_year='2024', start_month='202501'):
    """
    构建过滤条件：排除签单年在指定年份且起期在指定月份的数据
    
    Args:
        table_name: 表名（用于判断是直保还是再保）
        val_method: 评估方法（'8'=直保, '10'=分出, '11'=分入）
        filter_enabled: 是否启用过滤
        sign_year: 签单年份
        start_month: 起期月份（格式：yyyyMM）
    
    Returns:
        SQL条件字符串
    """
    # 分入业务（val_method=11）不做筛选
    if val_method == '11' or not filter_enabled:
        return ""
    
    if 'measure_cx_unexpired' in table_name:
        # 直保：ini_confirm是yyyy-mm-dd格式，start_date是yyyy-mm-dd格式
        # 签单年2024且起期在2025年1月的数据要排除
        start_year = start_month[:4]  # 从'202501'提取'2025'
        start_month_num = start_month[4:6]  # 从'202501'提取'01'
        next_month = f"{int(start_month_num) + 1:02d}"  # 计算下一个月'02'
        return f"""
        AND NOT (
            LEFT("ini_confirm", 4) = '{sign_year}'
            AND "start_date" >= '{start_year}-{start_month_num}-01'
            AND "start_date" < '{start_year}-{next_month}-01'
        )
        """
    else:
        # 再保（分出）：under_write_date/certi_write_date是yyyymmdd格式，start_date是yyyymmdd格式
        # 签单年2024且起期在2025年1月的数据要排除
        start_date_begin = start_month + '01'  # 如：20250101
        start_date_end = start_month + '32'    # 如：20250132（用于 < 比较）
        return f"""
        AND NOT (
            (
                ("certi_no" IS NULL OR "certi_no" = '') AND LEFT("under_write_date", 4) = '{sign_year}'
                OR
                ("certi_no" IS NOT NULL AND "certi_no" != '') AND LEFT("certi_write_date", 4) = '{sign_year}'
            )
            AND "start_date" >= '{start_date_begin}'
            AND "start_date" < '{start_date_end}'
        )
        """

def build_direct_filter_case(filter_enabled=True):
    """
    构建直保过滤条件（CASE表达式用）：当过滤启用时，起期大于20241231的保单不做2606010801和2606011002这两个分录。
    返回SQL片段，可放入CASE WHEN中。
    """
    if not filter_enabled:
        return "FALSE"

    return "\"start_date\" > '2024-12-31'"

def main():
    """
    Main function to orchestrate the entire process from data extraction to final report generation.
    """
    # --- SQL Queries and Groupby Definitions ---
    # 注意：起期大于20241231的保单已在WHERE条件中排除，不参与查询和聚合

    sql_8 = """
    SELECT
        "com_code" AS "归属机构", "business_nature" AS "业务渠道", "car_kind_code" AS "车辆种类",
        "use_nature_code" AS "使用性质代码", "portfolio_id" AS "合同组合编号", "group_id" AS "合同分组编号",
        "val_method" AS "评估方法", "risk_code" AS "险种代码", "class_code" AS "险类代码",
        SUM("total_premium") AS "保费_本币",
        SUM("total_iacf_amt") AS "保险获取现金流_本币",
        SUM("acc_confirmed_premium") AS "保险合同收入",
        SUM("acc_iacf_premium") AS "当期确认的IACF",
        SUM("lrc_loss_cost_policy") AS "亏损部分",
        SUM("ifie_amt") AS "IACF计息"
    """
    groupby_8 = [
        "com_code", "business_nature", "car_kind_code", "use_nature_code", "portfolio_id", 
        "group_id", "val_method", "risk_code", "class_code"
    ]
    
    sql_11 = """
    SELECT
        "com_code" AS "归属机构", "car_kind_code" AS "车辆种类", "use_nature_code" AS "使用性质代码", 
        "portfolio_id" AS "合同组合编号", "group_id" AS "合同分组编号", "val_method" AS "评估方法", 
        "risk_code" AS "险种代码", "class_code" AS "险类代码", "contract_flag" AS "合同标识", 
        "enquiry_type" AS "临分类型", "contract_type" AS "合约类型", "rein_type" AS "分出类型",
        SUM("premium") AS "分保费收入",
        SUM("commission") AS "分保费用",
        SUM("brokerage") AS "经纪费",
        SUM("net_premium_amortization") AS "预收净保费摊销",
        SUM("cumulative_ifie_amt_amortization") AS "累积计息摊销",
        SUM("cumulative_no_iacf_amortization") AS "获取费用摊销",
        SUM("no_iacf_cash_flow") AS "业务及管理费结转",
        SUM("loss_component_allocation") AS "亏损部分",
        SUM("cumulative_ifie_amt") AS "计息"
    """
    groupby_11 = [
        "com_code", "car_kind_code", "use_nature_code", "portfolio_id", "group_id", 
        "val_method", "risk_code", "class_code", "contract_flag", "enquiry_type", 
        "contract_type", "rein_type"
    ]

    sql_10 = """
    SELECT
        "com_code" AS "归属机构", "car_kind_code" AS "车辆种类", "use_nature_code" AS "使用性质代码", 
        "portfolio_id" AS "合同组合编号", "group_id" AS "合同分组编号", "val_method" AS "评估方法", 
        "risk_code" AS "险种代码", "class_code" AS "险类代码", "contract_flag" AS "合同标识", 
        "enquiry_type" AS "临分类型", "contract_type" AS "合约类型", "rein_type" AS "分出类型",
        SUM("premium") AS "分出保费",
        SUM("commission") AS "手续费_本币",
        SUM("brokerage") AS "经纪费_本币",
        SUM("net_premium_amortization") AS "预收净保费摊销",
        SUM("cumulative_ifie_amt_amortization") AS "累积计息摊销",
        SUM("loss_component") AS "亏损摊回部分",
        SUM("base_investment_amortization") AS "投资成分",
        SUM("cumulative_ifie_amt") AS "计息"
    """
    groupby_10 = [
        "com_code", "car_kind_code", "use_nature_code", "portfolio_id", "group_id", 
        "val_method", "risk_code", "class_code", "contract_flag", "enquiry_type", 
        "contract_type", "rein_type"
    ]

    # --- Step 1: Extract data from database and save for checking ---
    print("--- 步骤 1: 开始从数据库提取数据 ---")
    
    df_8 = get_data_from_db('8', sql_8, groupby_8, table_name='"measure_platform"."measure_cx_unexpired"', 
                           additional_where_clause='AND "end_date" > \'2024-12-31\' AND "start_date" <= \'2024-12-31\'')
    save_to_excel(df_8, 'measurement_results_8.xlsx')
    
    # 分入业务（val_method=11）不做筛选
    df_11 = get_data_from_db('11', sql_11, groupby_11, table_name='"measure_platform"."int_measure_cx_unexpired_rein"', 
                            additional_where_clause='AND "end_date" > \'20241231\'')
    save_to_excel(df_11, 'measurement_results_11.xlsx')
    
    # 分出业务不做任何额外筛选，只做前两步筛选（val_month='202412' 且 end_date > '20241231'）
    df_10 = get_data_from_db('10', sql_10, groupby_10, table_name='"measure_platform"."int_measure_cx_unexpired_rein"', 
                            additional_where_clause='AND "end_date" > \'20241231\'')
    save_to_excel(df_10, 'measurement_results_10.xlsx')
    
    # df_alloc = execute_raw_query(sql_alloc, "分摊结果查询") # No longer needed
    # save_to_excel(df_alloc, 'allocation_results.xlsx') # No longer needed
    print("--- 步骤 1: 数据库数据提取并保存完成 ---\n")

    # Check if all dataframes were created successfully
    if df_8 is None or df_11 is None or df_10 is None:
        print("错误：一个或多个数据提取步骤失败，程序终止。请检查数据库连接和查询。")
        return

    # --- Step 2: Load mappings, process data, and generate final report ---
    print("--- 步骤 2: 开始生成分录结果报告 ---")
    try:
        # Load mapping files
        print("正在加载映射文件...")
        map_product_df = pd.read_excel(
            '给翟总/财务段值转换/产品管理导出列表.xls', 
            header=None, 
            usecols=[0, 2], 
            names=['code', 'segment'],
            dtype=str
        )
        map_product_df.dropna(inplace=True)
        map_product_df.drop_duplicates(subset=['code'], inplace=True)
        map_product = map_product_df.set_index('code')['segment']
        
        map_org_cost = pd.read_excel(
            '给翟总/财务段值转换/机构&成本中心.xlsx', 
            header=None, 
            usecols=[0, 3, 4],
            names=['code', 'org', 'cost'],
            dtype=str
        )
        map_org_cost.dropna(inplace=True)
        map_org_cost.drop_duplicates(subset=['code'], inplace=True)
        map_org = map_org_cost.set_index('code')['org']
        map_cost = map_org_cost.set_index('code')['cost']
        
        map_channel_df = pd.read_excel(
            '给翟总/财务段值转换/渠道管理导出列表.xls', 
            header=None, 
            usecols=[0, 2], 
            names=['code', 'segment'],
            dtype=str
        )
        map_channel_df.dropna(inplace=True)
        map_channel_df.drop_duplicates(subset=['code'], inplace=True)
        map_channel = map_channel_df.set_index('code')['segment']

        map_car_df = pd.read_excel(
            '给翟总/财务段值转换/车型、使用性质映射表.xls', 
            header=None, 
            usecols=[0, 2, 4], 
            names=['use', 'type', 'segment'],
            dtype=str
        )
        map_car_df.dropna(inplace=True)
        map_car_df['key'] = map_car_df['use'].str.strip() + '_' + map_car_df['type'].str.strip()
        map_car_df.drop_duplicates(subset=['key'], inplace=True)
        map_car = map_car_df.set_index('key')['segment']

        mappings = {
            'product': map_product, 'org': map_org, 'cost_center': map_cost,
            'channel': map_channel, 'car': map_car
        }
        print("映射文件加载完成。")

    except FileNotFoundError as e:
        print(f"错误：映射文件未找到 - {e}")
        print("请确保所有映射文件都存在于 '给翟总/财务段值转换/' 目录下。")
        return
    except Exception as e:
        print(f"加载映射文件时发生错误: {e}")
        return

    # Process each business type
    direct_entries = process_direct_business(df_8, filter_enabled=False)
    assumed_entries = process_assumed_reinsurance(df_11)
    ceded_entries = process_ceded_reinsurance(df_10)
    
    # Transform to final format
    final_direct = transform_to_final_format(direct_entries, '1', mappings)
    final_assumed = transform_to_final_format(assumed_entries, '2', mappings)
    final_ceded = transform_to_final_format(ceded_entries, '2', mappings)

    # Write to a single Excel file with multiple sheets
    output_filename = '未到期分录结果.xlsx'
    print(f"正在写入最终结果到 {output_filename}...")
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        final_direct.to_excel(writer, sheet_name='直保', index=False)
        final_assumed.to_excel(writer, sheet_name='分入', index=False)
        final_ceded.to_excel(writer, sheet_name='分出', index=False)
    
    print("处理完成！")
    print("--- 步骤 2: 分录结果报告生成完毕 ---")


if __name__ == '__main__':
    main()
