import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import OperationalError

# --- Database Connection Parameters ---
DB_PARAMS = {
    'host': '10.128.21.148',
    'port': '5431',
    'database': 'cas25_uat',
    'user': 'readonly_cas25_uat',
    'password': 'readonly_cas25_uat'
}

# --- Database Extraction Functions ---

def get_data_from_db(val_method, sql_query, group_by_columns):
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
            "measure_platform"."measure_cf_result_info"
        WHERE
            "val_month" = '202312' AND "val_method" = '{val_method}'
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

def process_direct_business(df_direct, df_alloc):
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
    }
    
    # Ensure '评估方法' is string type for consistent comparison
    df_alloc['评估方法'] = df_alloc['评估方法'].astype(str)

    # 1. Process loss entries from allocation data
    df_alloc_direct = df_alloc[df_alloc['评估方法'] == '8'].copy()
    alloc_dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同分组编码', '合同组合编号(短)', '险种代码', '险类代码']
    
    loss_entries_list = []
    if not df_alloc_direct.empty:
        loss_entries = df_alloc_direct[alloc_dimension_cols].copy()
        loss_entries.rename(columns={'合同分组编码': '合同分组编号', '合同组合编号(短)': '合同组合编号'}, inplace=True)
        loss_entries['类型'] = '亏损(保费不足)'
        loss_entries['借贷方向'] = '贷'
        loss_entries['I17科目代码'] = '2606011202'
        loss_entries['I17科目名称'] = i17_names.get('2606011202')
        loss_entries['取数口径'] = '正数'
        loss_entries['金额'] = df_alloc_direct['亏损部分']
        loss_entries_list.append(loss_entries)
    
    # 2. Process other entries from measurement data
    rules = [
        {'类型': '签单保费', '借贷方向': '贷', 'I17科目代码': '2606010801', '取数口径': '正数', '金额来源': '保费_本币', '符号': 1},
        {'类型': '获取费用', '借贷方向': '贷', 'I17科目代码': '2606011002', '取数口径': '负数', '金额来源': '保险获取现金流_本币', '符号': -1},
        {'类型': '已经过保费', '借贷方向': '贷', 'I17科目代码': '2606011102', '取数口径': '负数', '金额来源': '保险合同收入', '符号': -1},
        {'类型': '获取费用摊销', '借贷方向': '贷', 'I17科目代码': '2606011603', '取数口径': '正数', '金额来源': '当期确认的IACF', '符号': 1},
    ]
    
    measure_dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同分组编号', '险种代码', '险类代码', '合同组合编号']
    measure_entries_list = []
    
    for rule in rules:
        if rule['金额来源'] not in df_direct.columns:
            print(f"警告：在直保数据中找不到源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
            continue

        temp_df = df_direct[measure_dimension_cols].copy()
        temp_df['类型'] = rule['类型']
        temp_df['借贷方向'] = rule['借贷方向']
        temp_df['I17科目代码'] = rule['I17科目代码']
        temp_df['I17科目名称'] = i17_names.get(rule['I17科目代码'])
        temp_df['取数口径'] = rule['取数口径']
        temp_df['金额'] = df_direct[rule['金额来源']] * rule['符号']
        
        measure_entries_list.append(temp_df)
        
    # 3. Combine all entries
    all_entries = loss_entries_list + measure_entries_list
    if not all_entries:
        return pd.DataFrame()
        
    final_df = pd.concat(all_entries, ignore_index=True)
    
    print("直保业务处理完成。")
    return final_df

def process_assumed_reinsurance(df_assumed, df_alloc):
    """
    Processes assumed reinsurance data to generate accounting entries.
    """
    print("正在处理分入业务...")

    # Ensure '评估方法' is string type for consistent comparison
    df_alloc['评估方法'] = df_alloc['评估方法'].astype(str)

    # 1. Process loss entries from allocation data
    df_alloc_assumed = df_alloc[df_alloc['评估方法'] == '11'].copy()
    alloc_dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同分组编码', '评估方法', '合同组合编号(短)', '险种代码', '险类代码']
    
    loss_entries_list = []
    if not df_alloc_assumed.empty:
        loss_entries = df_alloc_assumed[alloc_dimension_cols].copy()
        loss_entries.rename(columns={'合同分组编码': '合同分组编号', '合同组合编号(短)': '合同组合编号'}, inplace=True)
        loss_entries['类型'] = '亏损'
        loss_entries['借贷方向'] = '贷'
        loss_entries['I17科目代码'] = '2606011201'
        loss_entries['I17科目名称'] = '未到期责任负债-未来现金流-保费分配法亏损合同损益-亏损提转差/分入业务'
        loss_entries['取数口径'] = '正数'
        loss_entries['金额'] = df_alloc_assumed['亏损部分']
        loss_entries_list.append(loss_entries)

    # 2. Process other entries from measurement data
    i17_names = {
        '2606010901': '未到期责任负债-未来现金流-现金流/分入保费-分保费收入/比例合同',
        '2606010904': '未到期责任负债-未来现金流-现金流/分入保费-分保费收入/比例临分',
        '2606010911': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/比例合同',
        '2606010913': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/比例临分',
        '2606010921': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/经纪费/比例合同',
        '2606010923': '未到期责任负债-未来现金流-现金流/分入保费-分保费用/经纪费/比例临分',
        '2606011101': '未到期责任负债-未来现金流-保费分配法分摊的收入-保费收入/分入业务'
    }

    df_assumed['is_contract'] = df_assumed['合约类型'].notna()

    rules = [
        {'类型': '分保费收入', '金额来源': '保费_本币', '符号': 1, '取数口径': '正数', 'contract_code': '2606010901', 'facultative_code': '2606010904'},
        {'类型': '分保费用', '金额来源': '手续费_本币', '符号': -1, '取数口径': '负数', 'contract_code': '2606010911', 'facultative_code': '2606010913'},
        {'类型': '经纪费', '金额来源': '不含税经纪费_本币', '符号': -1, '取数口径': '负数', 'contract_code': '2606010921', 'facultative_code': '2606010923'},
        {'类型': '已经过保费', '金额来源': '保险合同收入', '符号': -1, '取数口径': '负数', 'code': '2606011101'},
    ]
    
    measure_dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同组合编号', '合同分组编号', '评估方法', '险种代码', '险类代码', '保修期', '临分类型', '合约类型', '分出类型']
    measure_entries_list = []

    for rule in rules:
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
        temp_df['金额'] = df_assumed[rule['金额来源']] * rule['符号']
        measure_entries_list.append(temp_df)

    # 3. Combine all entries
    all_entries = loss_entries_list + measure_entries_list
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
        '1253010501': "分保摊回已发生赔款资产-未来现金流-摊回赔付/投资成分-应收分保账款/摊回分保赔款/直接业务/比例合同"
    }

    df_ceded['分出类型'] = df_ceded['分出类型'].astype(str)

    rules = [
        {'类型': '分出保费', '金额来源': '保费_本币', '符号': 1, '取数口径': '正数',
         'codes': {'1_True': '1252010501', '1_False': '1252010503', '2_True': '1252010511', '2_False': '1252010513'}},
        {'类型': '摊回分保费用', '金额来源': '手续费_本币', '符号': -1, '取数口径': '负数',
         'codes': {'1_True': '1252010521', '1_False': '1252010523', '2_True': '1252010531', '2_False': '1252010533'}},
        {'类型': '分出保费的分摊', '金额来源': '当期确认的保费', '符号': -1, '取数口径': '负数',
         'codes': {'1': '1252010301', '2': '1252010302'}},
        {'类型': '亏损摊回', '金额来源': '亏损摊回部分', '符号': 1, '取数口径': '正数',
         'codes': {'1': '1252010401', '2': '1252010402'}},
    ]
    
    dimension_cols = ['归属机构', '业务渠道', '车辆种类', '使用性质代码', '合同组合编号', '合同分组编号', '评估方法', '险种代码', '险类代码', '保修期', '临分类型', '合约类型', '分出类型']
    all_entries = []

    for rule in rules:
        if rule['金额来源'] not in df_ceded.columns:
            print(f"警告：在分出数据中找不到源列 '{rule['金额来源']}'，跳过规则 '{rule['类型']}'。")
            continue
            
        temp_df = df_ceded[dimension_cols].copy()
        temp_df['类型'] = rule['类型']
        temp_df['借贷方向'] = '借'

        if rule['类型'] in ['分出保费', '摊回分保费用']:
            is_contract_series = temp_df['合约类型'].notna()
            key_series = temp_df['分出类型'] + '_' + is_contract_series.astype(str)
            temp_df['I17科目代码'] = key_series.map(rule['codes'])
        elif rule['类型'] in ['分出保费的分摊', '亏损摊回']:
            temp_df['I17科目代码'] = temp_df['分出类型'].map(rule['codes'])
        
        temp_df['I17科目名称'] = temp_df['I17科目代码'].map(i17_names)
        temp_df['取数口径'] = rule['取数口径']
        temp_df['金额'] = df_ceded[rule['金额来源']] * rule['符号']
        all_entries.append(temp_df)
        
    # Handle '投资成分' separately as it creates two entries
    if '当期确认的投资成分' in df_ceded.columns:
        # Entry 1
        temp_df_1 = df_ceded[dimension_cols].copy()
        temp_df_1['类型'] = '投资成分'
        temp_df_1['借贷方向'] = '借'
        temp_df_1['I17科目代码'] = '1252010201'
        temp_df_1['I17科目名称'] = i17_names['1252010201']
        temp_df_1['取数口径'] = '负数, 已摊销投资成分'
        temp_df_1['金额'] = df_ceded['当期确认的投资成分'] * -1
        all_entries.append(temp_df_1)
        # Entry 2
        temp_df_2 = df_ceded[dimension_cols].copy()
        temp_df_2['类型'] = '投资成分'
        temp_df_2['借贷方向'] = '借'
        temp_df_2['I17科目代码'] = '1253010501'
        temp_df_2['I17科目名称'] = i17_names['1253010501']
        temp_df_2['取数口径'] = '正数, 已摊销投资成分'
        temp_df_2['金额'] = df_ceded['当期确认的投资成分']
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
    df['account_period'] = '202312'
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

def main():
    """
    Main function to orchestrate the entire process from data extraction to final report generation.
    """
    # --- SQL Queries and Groupby Definitions ---
    sql_8 = """
    SELECT
        "com_code" AS "归属机构", "business_nature" AS "业务渠道", "car_kind_code" AS "车辆种类",
        "use_nature_code" AS "使用性质代码", "portfolio_id" AS "合同组合编号", "group_id" AS "合同分组编号",
        "val_method" AS "评估方法", "risk_code" AS "险种代码", "class_code" AS "险类代码",
        SUM("curr_serv_amt") AS "当期服务量", SUM("other_serv_amt") AS "当期及未来服务量",
        SUM("cur_rec_pct") AS "当期确认比例", SUM("prem_bop_un_rec_amt") AS "期初未确认的保费",
        SUM("prem_interest_amt") AS "期初保费计息", SUM("prem_cur_rec_amt") AS "当期确认的保费",
        SUM("prem_eop_un_rec_amt") AS "期末未确认的保费", SUM("iacf_bop_un_rec_amt") AS "期初未确认的IACF",
        SUM("iacf_interest_amt") AS "IACF计息", SUM("iacf_amort_amt") AS "当期确认的IACF",
        SUM("iacf_eop_un_rec") AS "期末未确认IACF", SUM("isr_amt") AS "保险合同收入",
        SUM("lrc_no_lc_amt") AS "未到期责任负债-非亏损部分", SUM("un_rec_prem_amt") AS "未经过保费",
        SUM("pv_rep_amt") AS "预期未来现金流现值", SUM("lrc_ra_amt") AS "未到期-金融风险调整",
        SUM("iacf_fol_cny") AS "保险获取现金流_本币", SUM("premium_cny") AS "保费_本币",
        SUM("iacf_actual") AS "实际获取费用", SUM("lrc_no_lc_amt_rein") AS "未到期责任负债-非亏损部分_再保",
        SUM("iacf_fol_cny_rein") AS "保险获取现金流_本币_再保", SUM("iacf_bop_un_rec_amt_rein") AS "期初未确认的IACF_再保",
        SUM("iacf_interest_amt_rein") AS "IACF计息_再保", SUM("iacf_actual_rein") AS "实际获取费用_再保",
        SUM("iacf_amort_amt_rein") AS "当期确认的IACF_再保", SUM("iacf_eop_un_rec_rein") AS "期末未确认IACF_再保",
        SUM("share_factor") AS "分摊因子", SUM("share_factor_rein") AS "分摊因子_再保"
    """
    groupby_8 = [
        "com_code", "business_nature", "car_kind_code", "use_nature_code", "portfolio_id", 
        "group_id", "val_method", "risk_code", "class_code"
    ]
    
    sql_11 = """
    SELECT
        "com_code" AS "归属机构", "business_nature" AS "业务渠道", "car_kind_code" AS "车辆种类",
        "use_nature_code" AS "使用性质代码", "portfolio_id" AS "合同组合编号", "group_id" AS "合同分组编号",
        "val_method" AS "评估方法", "risk_code" AS "险种代码", "warranty_period" AS "保修期",
        "enquiry_type" AS "临分类型", "contract_type" AS "合约类型", "rein_type" AS "分出类型", "class_code" AS "险类代码",
        SUM("rein_prem_ratio") AS "净分出保费比例", SUM("rein_prem") AS "净分出保费", SUM("init_ceded_out_prem") AS "分出保费",
        SUM("surplus_ratio") AS "盈余比例", SUM("rein_part_ratio") AS "再保互助比例", SUM("period_adj_ratio") AS "期调整因子",
        SUM("inv_comp") AS "投资成分", SUM("curr_iacf") AS "当月预期获取费用", SUM("curr_serv_amt") AS "当期服务量",
        SUM("other_serv_amt") AS "当期及未来服务量", SUM("cur_rec_pct") AS "当期确认比例", SUM("prem_bop_un_rec_amt") AS "期初未确认的保费",
        SUM("prem_interest_amt") AS "期初保费计息", SUM("prem_cur_rec_amt") AS "当期确认的保费", SUM("prem_eop_un_rec_amt") AS "期末未确认的保费",
        SUM("iacf_bop_un_rec_amt") AS "期初未确认的IACF", SUM("iacf_interest_amt") AS "IACF计息", SUM("iacf_amort_amt") AS "当期确认的IACF",
        SUM("iacf_eop_un_rec") AS "期末未确认IACF", SUM("isr_amt") AS "保险合同收入", SUM("lrc_no_lc_amt") AS "未到期责任负债-非亏损部分",
        SUM("un_rec_prem_amt") AS "未经过保费", SUM("pv_rep_amt") AS "预期未来现金流现值", SUM("lrc_ra_amt") AS "未到期-金融风险调整",
        SUM("pv_bop_rep_amt") AS "预期未来现金流现值_期初", SUM("lrc_bop_ra_amt") AS "未到期-金融风险调整_期初", SUM("lrc_bop_no_lc_amt") AS "未到期责任负债-非亏损部分-期初",
        SUM("rec_prem_amt") AS "经过保费", SUM("pv_eop_rep_amt") AS "未来现金流量现值_期末", SUM("pv_loss_amt") AS "预期赔付的现值",
        SUM("pv_maintain_amt") AS "预期维持费用", SUM("pv_loss_tot_amt") AS "预期总赔付的现值", SUM("rein_default_amt") AS "再保人违约不履约",
        SUM("lrc_recovery_eop_end") AS "未到期责任资产_摊回未到期_非亏_期末", SUM("pv_bel_last") AS "预期未来现金流出现值_上期", SUM("init_pv_bel") AS "预期未来现金流出_初始",
        SUM("pv_bel_last2") AS "预期未来现金流入现值_期初2", SUM("init_pv_ra") AS "预期未来非风险金融调整_初始", SUM("lrc_ra_last2") AS "未到期_金融风险调整_期初2",
        SUM("init_pv_bel_in") AS "预期未来现金流入现值_初始", SUM("init_lrc_lc") AS "未到期责任负债_亏损部分_初始", SUM("init_lrc_lc_rein") AS "未到期责任负债_亏损部分_初始(再保互动)",
        SUM("init_bel") AS "未来现金流(BEL)_初始", SUM("pv_loss_tot_amt_adj") AS "实际总赔付的现值(实际赔款)", SUM("pv_eop_rep_amt_adj") AS "预期赔付现值_期末(参数调整)",
        SUM("lrc_eop_ra_amt_adj") AS "非金融风险调整_期末(参数调整)", SUM("pv_bel_last1") AS "预期未来现金流入现值_期初1", SUM("lrc_ra_last") AS "未到期_金融风险调整_上期",
        SUM("lrc_ra_last1") AS "未到期_金融风险调整_期初1", SUM("init_csm") AS "合同服务边际_初始", SUM("init_csm_a") AS "合同服务边际_初始(考虑原保险合同亏损)",
        SUM("init_csm_b") AS "合同服务边际_初始(不考虑原保险合同亏损)", SUM("pv_lrc_ra_release_amt") AS "预期非金融风险调整的释放", SUM("isr_attr") AS "合同亏损情况",
        SUM("csm_last") AS "合同服务边际_上期", SUM("csm_release") AS "合同服务边际的释放", SUM("lrc_eop_ra_amt") AS "非金融风险调整_期末",
        SUM("csm_eop") AS "合同服务边际_期末", SUM("bel_ifie") AS "未来现金流量(BEL)_计息", SUM("ra_ifie") AS "非金融风险调整计息",
        SUM("csm_ifie") AS "合同服务边际计息", SUM("lrc_recovery_eop") AS "未到期责任负债-亏损原保险合同", SUM("lrc_lc_last") AS "合同服务边际_上期",
        SUM("lrc_lc_last_rein") AS "再保合同服务边际_上期", SUM("lrc_lc_ifie") AS "未到期_亏损部分计息", SUM("lc_release") AS "亏损部分的释放",
        SUM("amort_ceded_out_prem") AS "分出保费的分摊", SUM("lrc_lc_recovery_eop") AS "未到期责任资产_摊回未到期_亏损_期末", SUM("lrc_lc_amt") AS "未到期责任负债_亏损",
        SUM("lrc_eop_opex") AS "未到期责任负债_其他部分_期末", SUM("lrc_opex_adj") AS "未到期责任-其他部分(参数调整)", SUM("lrc_lc") AS "未到期责任负债-亏损(参数调整)",
        SUM("csm_amt_adj") AS "合同服务边际(参数调整)", SUM("invest_prop") AS "投资成分占比", SUM("iacf_fol_cny") AS "保险获取现金流_本币",
        SUM("premium_cny") AS "保费_本币", SUM("iacf_actual") AS "实际获取费用", SUM("opening_pv_premium") AS "期初保费未来现金流现值",
        SUM("opening_pv_paid_loss") AS "期初理赔未来现金流现值", SUM("opening_pv_maintenance_expense") AS "期初维持费用未来现金流现值", SUM("opening_pv_iacf") AS "期初获取费用未来现金流现值",
        SUM("opening_bel") AS "期初未来现金流现值", SUM("opening_ra") AS "期初非金融风险调整", SUM("opening_csm") AS "期初合约服务边际",
        SUM("opening_lc") AS "期初损失成分", SUM("current_pv_premium") AS "当期保费未来现金流现值", SUM("current_pv_paid_loss") AS "当期理赔未来现金流现值",
        SUM("current_pv_maintenance_expense") AS "当期维持费用未来现金流现值", SUM("current_pv_iacf") AS "当期获取费用未来现金流现值", SUM("current_bel") AS "当期未来现金流现值",
        SUM("current_ra") AS "当期非金融风险调整", SUM("current_pv_paid_loss_nop") AS "当期理赔未来现金流现值-保单变动", SUM("current_pv_maintenance_expense_nop") AS "当期维持费用未来现金流现值-保单变动",
        SUM("current_bel_nop") AS "当期未来现金流现值-保单变动", SUM("current_ra_nop") AS "当期非金融风险调整-保单变动", SUM("current_pv_paid_loss_chg") AS "当期理赔未来现金流现值-假设变动",
        SUM("current_pv_maintenance_expense_chg") AS "当期维持费用未来现金流现值-假设变动", SUM("current_bel_chg") AS "当期未来现金流现值-假设变动", SUM("current_ra_chg") AS "当期非金融风险调整-假设变动",
        SUM("current_pv_paid_loss_chg_int") AS "当期理赔未来现金流现值-金融假设变动", SUM("current_pv_maintenance_expense_chg_int") AS "当期维持费用未来现金流现值-金融假设变动", SUM("current_bel_chg_int") AS "当期未来现金流现值-金融假设变动",
        SUM("current_ra_chg_int") AS "当期非金融风险调整-金融假设变动", SUM("unearn_premium") AS "期初未满期保费", SUM("commission") AS "手续费_本币",
        SUM("brokerage_fee") AS "不含税经纪费_本币", SUM("ic_bop_un_rec_amt") AS "期初未确认的投资成分", SUM("ic_interest_amt") AS "期初投资成分计息",
        SUM("ic_paid_amt") AS "当期确认的投资成分", SUM("ic_eop_un_rec_amt") AS "期末未确认的投资成分", SUM("lrc_ifie_amt") AS "IFIE未到期利息",
        SUM("share_rate") AS "分出比例", SUM("net_premium_cny") AS "不含税净分出保费", SUM("lrc_lc_change_amt") AS "亏损摊回部分",
        SUM("lrc_no_lc_amt_rein") AS "未到期责任负债-非亏损部分_再保", SUM("iacf_fol_cny_rein") AS "保险获取现金流_本币_再保", SUM("iacf_bop_un_rec_amt_rein") AS "期初未确认的IACF_再保",
        SUM("iacf_interest_amt_rein") AS "IACF计息_再保", SUM("iacf_actual_rein") AS "实际获取费用_再保", SUM("iacf_amort_amt_rein") AS "当期确认的IACF_再保",
        SUM("iacf_eop_un_rec_rein") AS "期末未确认IACF_再保", SUM("current_pv_premium_chg_int") AS "当期保费未来现金流现值-金融假设变动", SUM("current_pv_iacf_chg_int") AS "当期获取费用未来现金流现值-金融假设变动",
        SUM("current_pv_premium_lock_int") AS "当期保费未来现金流现值-锁定期初利率", SUM("current_pv_paid_loss_lock_int") AS "当期理赔未来现金流现值-锁定期初利率", SUM("current_pv_maintenance_expense_lock_int") AS "当期维持费用未来现金流现值-锁定期初利率",
        SUM("current_pv_iacf_lock_int") AS "当期获取费用未来现金流现值-锁定期初利率", SUM("current_bel_lock_int") AS "当期未来现金流现值-锁定期初利率", SUM("current_pv_paid_loss_chg_lock_int") AS "当期理赔未来现金流现值-假设变动-锁定期初利率",
        SUM("current_pv_maintenance_expense_chg_lock_int") AS "当期维持费用未来现金流现值-假设变动-锁定期初利率", SUM("current_bel_chg_lock_int") AS "当期未来现金流现值-假设变动-锁定期初利率", SUM("current_ra_lock_int") AS "当期非金融风险调整-锁定期初利率",
        SUM("current_ra_chg_lock_int") AS "当期非金融风险调整-假设变动-锁定期初利率", SUM("share_factor") AS "分摊因子", SUM("share_factor_rein") AS "分摊因子_再保"
    """
    groupby_11 = [
        "com_code", "business_nature", "car_kind_code", "use_nature_code", "portfolio_id",
        "group_id", "val_method", "risk_code", "warranty_period", "enquiry_type",
        "contract_type", "rein_type", "class_code"
    ]

    sql_alloc = """
    SELECT
        "group_id" AS "合同分组编码", "com_code" AS "归属机构", "business_nature" AS "业务渠道",
        "car_kind_code" AS "车辆种类", "use_nature_code" AS "使用性质代码", "val_method" AS "评估方法",
        "portfolio_id" AS "合同组合编号(短)", "risk_code" AS "险种代码", "class_code" AS "险类代码",
        SUM("un_rec_prem_amt") AS "期初未经过保费", SUM("un_rec_prem_amt_group") AS "期初未经过保费(合同组）",
        SUM("lrc_lc_change_amt_group") AS "亏损部分(合同组）", SUM("lrc_lc_change_amt") AS "亏损部分",
        SUM("csm_bf_inv") AS "计息前CSM", SUM("csm_int_accret") AS "当期CSM的计息", SUM("csm_af_inv") AS "计息后的CSM",
        SUM("csm_adj_tot") AS "CSM总吸收项", SUM("csm_adj_ra_prop") AS "损失后续确认和转回RA占比",
        SUM("csm_adj_bel_prop") AS "损失后续确认和转回BEL占比", SUM("lc_bf_inv") AS "计息前的LC",
        SUM("lc_int_accret_pl") AS "LC的部分计息", SUM("lc_af_inv") AS "计息后的LC",
        SUM("csm_adj_amt") AS "CSM调整项", SUM("csm_bf_amort") AS "摊销前的CSM",
        SUM("csm_release_rate") AS "当期CSM摊销比例", SUM("csm_release") AS "CSM当期摊销额",
        SUM("csm_af_amort") AS "CSM摊后余额", SUM("csm_if_end") AS "CSM期末值",
        SUM("lc_adj_amt") AS "损失后续确认和转回", SUM("lc_adj_amt_bel") AS "损失后续确认和转回拆分至bel",
        SUM("lc_adj_amt_ra") AS "损失后续确认和转回拆分至ra", SUM("lc_bf_amort") AS "分摊前的LC",
        SUM("lc_amort_rate") AS "当期LC分摊比例", SUM("lc_amort_amt") AS "LC当期分摊(不含期末分摊调整）",
        SUM("lc_af_amort") AS "LC摊后余额", SUM("expc_ifie_gpv_oci") AS "计入其他综合的保险财务损益-GPV",
        SUM("expc_ifie_ra_oci") AS "计入其他综合收益的保险财务损益-RA", SUM("lc_int_oci_alloc_rate") AS "OCI分摊至LC部分的比例",
        SUM("lc_int_accret_oci") AS "计入其他综合收益的保险财务损益-LC部分", SUM("lc_amort_end") AS "LC期末分摊调整",
        SUM("lc_amort_tot") AS "亏损合同的分摊总数", SUM("lc_if_end") AS "LC期末余额",
        SUM("iacf_bf_inv") AS "计息前的IACF", SUM("iacf_int_accret") AS "IACF的利息",
        SUM("iacf_af_inv") AS "计息后的IACF", SUM("expc_iacf_out") AS "预期IACF发生数",
        SUM("actl_iacf_out") AS "财务实际IACF发生数", SUM("iacf_bf_amort") AS "调整后的IACF",
        SUM("iacf_release_rate") AS "IACF当期摊销比例", SUM("iacf_release") AS "IACF当期摊销",
        SUM("iacf_af_amort") AS "IACF摊后余额", SUM("iacf_if_end") AS "IACF期末值",
        SUM("lrc_lc_change_amt_group_rein") AS "亏损部分(合同组)_再保", SUM("judging_condition") AS "判断条件",
        SUM("lrc_lc_change_amt_rein") AS "亏损部分_再保", SUM("lc_amort") AS "LC余额",
        SUM("ra_if") AS "上一期期末ra", SUM("init_ra_nb") AS "当期新单初始确认的ra",
        SUM("ra_bf_inv") AS "计息前的ra", SUM("ra_int_accret") AS "ra的计息",
        SUM("expc_ifie_ra_pl") AS "预期RA 损益表IFIE", SUM("ra_af_inv") AS "计息后的ra",
        SUM("ra_release_rate") AS "ra当期摊销比例", SUM("ra_release") AS "ra当期摊销",
        SUM("ra_af_amort") AS "ra摊后余额", SUM("ra_adj_tot") AS "ra的吸收项",
        SUM("ra_bf_amort") AS "调整后的ra", SUM("ra_if_end") AS "ra期末值",
        SUM("lc_amort_amt_bel") AS "赔付与费用_亏损分摊_预期现金流", SUM("lc_amort_amt_ra") AS "赔付与费用_亏损分摊_非金融风险调整",
        SUM("curr_serv_amt") AS "当期服务量", SUM("curr_serv_amt_group") AS "当期服务量(合同组)",
        SUM("premium_cny") AS "保费_本币", SUM("share_factor") AS "分摊因子",
        SUM("share_factor_group") AS "分摊因子(合同组)", SUM("share_factor_rein") AS "分摊因子_再保",
        SUM("share_factor_rein_group") AS "分摊因子_再保(合同组)"
    FROM
        "measure_platform"."measure_result_allocation"
    WHERE
        "val_month" = '202312'
    GROUP BY
        "group_id", "com_code", "business_nature", "car_kind_code", "use_nature_code",
        "val_method", "portfolio_id", "risk_code", "class_code"
    """

    # --- Step 1: Extract data from database and save for checking ---
    print("--- 步骤 1: 开始从数据库提取数据 ---")
    df_8 = get_data_from_db('8', sql_8, groupby_8)
    save_to_excel(df_8, 'measurement_results_8.xlsx')
    
    df_11 = get_data_from_db('11', sql_11, groupby_11)
    save_to_excel(df_11, 'measurement_results_11.xlsx')
    
    df_10 = get_data_from_db('10', sql_11, groupby_11) # Reusing sql_11 and groupby_11
    save_to_excel(df_10, 'measurement_results_10.xlsx')
    
    df_alloc = execute_raw_query(sql_alloc, "分摊结果查询")
    save_to_excel(df_alloc, 'allocation_results.xlsx')
    print("--- 步骤 1: 数据库数据提取并保存完成 ---\n")

    # Check if all dataframes were created successfully
    if df_8 is None or df_11 is None or df_10 is None or df_alloc is None:
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
    direct_entries = process_direct_business(df_8, df_alloc)
    assumed_entries = process_assumed_reinsurance(df_11, df_alloc)
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
