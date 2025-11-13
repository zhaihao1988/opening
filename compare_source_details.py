import pandas as pd
import numpy as np

def compare_data(source_file, detail_file, detail_sheet, columns_to_sum, business_type):
    """
    Compares a source Excel file with a specific sheet in a detail Excel file.

    Args:
        source_file (str): Path to the source data Excel file.
        detail_file (str): Path to the detail/measurement Excel file.
        detail_sheet (str): The name of the sheet to compare in the detail file.
        columns_to_sum (list): A list of column names to sum and compare.
        business_type (str): A string describing the business type for printing.
    """
    print(f"--- 开始比较: {business_type} ---")
    
    try:
        df_source = pd.read_excel(source_file)
        print(f"成功读取源文件: {source_file}")
    except FileNotFoundError:
        print(f"错误: 源文件未找到: {source_file}")
        print(f"--- 比较结束: {business_type} ---\n")
        return

    try:
        # 如果 detail_sheet 为 None，则读取第一个 sheet
        df_detail = pd.read_excel(detail_file, sheet_name=detail_sheet)
        sheet_name_for_log = detail_sheet if detail_sheet is not None else "默认第一个Sheet"
        print(f"成功读取目标文件: {detail_file} (Sheet: {sheet_name_for_log})")
    except FileNotFoundError:
        print(f"错误: 目标文件未找到: {detail_file}")
        print(f"--- 比较结束: {business_type} ---\n")
        return
    except ValueError:
        print(f"错误: 在 {detail_file} 中找不到名为 '{detail_sheet}' 的工作表。")
        print(f"--- 比较结束: {business_type} ---\n")
        return

    # 1. 比较行数
    source_rows = len(df_source)
    detail_rows = len(df_detail)
    print(f"\n1. 行数比较:")
    print(f"   - 源数据 ({source_file}): {source_rows} 行")
    print(f"   - 目标数据 ({detail_file}): {detail_rows} 行")
    if source_rows == detail_rows:
        print("   - 结果: 行数一致 ✅")
    else:
        print(f"   - 结果: 行数不一致 ❌ (差异: {source_rows - detail_rows})")

    # 2. 比较金额汇总值
    print("\n2. 金额汇总值比较:")
    all_match = True
    for col in columns_to_sum:
        if col not in df_source.columns:
            print(f"   - 警告: 列 '{col}' 在源文件 {source_file} 中不存在，跳过比较。")
            continue
        if col not in df_detail.columns:
            print(f"   - 警告: 列 '{col}' 在目标文件 {detail_file} 中不存在，跳过比较。")
            continue

        sum_source = df_source[col].sum()
        sum_detail = df_detail[col].sum()

        print(f"   - 正在比较列: '{col}'")
        print(f"     - 源数据汇总:   {sum_source:,.2f}")
        print(f"     - 目标数据汇总: {sum_detail:,.2f}")
        
        # 使用 numpy.isclose 来处理浮点数比较的精度问题
        if np.isclose(sum_source, sum_detail):
            print("     - 结果: 金额一致 ✅")
        else:
            all_match = False
            print(f"     - 结果: 金额不一致 ❌ (差异: {sum_source - sum_detail:,.2f})")

    if all_match:
        print("\n   >>> 所有指定金额列的汇总值均一致。")
    else:
        print("\n   >>> 存在金额列汇总值不一致的情况。")
        
    print(f"--- 比较结束: {business_type} ---\n")


def compare_final_entries(new_file, original_file):
    """
    比较最终生成的分录结果文件。
    1. 逐个工作表比较行数。
    2. 逐个工作表按科目代码汇总金额进行比较。
    """
    print(f"--- 开始比较: 最终分录结果 ---")
    
    try:
        xls_new = pd.ExcelFile(new_file)
        xls_original = pd.ExcelFile(original_file)
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 {e.filename}，跳过此项比较。")
        print(f"--- 最终分录结果比较结束 ---\n")
        return

    common_sheets = sorted(list(set(xls_new.sheet_names) & set(xls_original.sheet_names)))
    
    if not common_sheets:
        print("错误: 两个文件中没有共同的工作表可供比较。")
        print(f"--- 最终分录结果比较结束 ---\n")
        return

    for sheet in common_sheets:
        print(f"\n--- 正在比较 Sheet: {sheet} ---")
        df_new = pd.read_excel(xls_new, sheet_name=sheet)
        df_original = pd.read_excel(xls_original, sheet_name=sheet)

        # 1. 比较行数
        rows_new = len(df_new)
        rows_original = len(df_original)
        print(f"1. 行数比较:")
        print(f"   - 新结果 ({new_file}): {rows_new} 行")
        print(f"   - 原始结果 ({original_file}): {rows_original} 行")
        if rows_new == rows_original:
            print("   - 结果: 行数一致 ✅")
        else:
            print(f"   - 结果: 行数不一致 ❌ (差异: {rows_new - rows_original})")

        # 2. 按科目代码汇总金额进行比较
        print("\n2. 按科目代码汇总金额比较:")
        
        required_cols = ['account_code', 'dc_local_currency_amt']
        if not all(col in df_new.columns for col in required_cols):
            print(f"   - 警告: 新结果文件 '{sheet}' 工作表中缺少必需的列 ('account_code' 或 'dc_local_currency_amt')，跳过金额比较。")
            continue
        if not all(col in df_original.columns for col in required_cols):
            print(f"   - 警告: 原始结果文件 '{sheet}' 工作表中缺少必需的列 ('account_code' 或 'dc_local_currency_amt')，跳过金额比较。")
            continue

        summary_new = df_new.groupby('account_code')['dc_local_currency_amt'].sum()
        summary_original = df_original.groupby('account_code')['dc_local_currency_amt'].sum()

        # 合并两个汇总结果以便比较
        comparison_df = pd.DataFrame({'新结果汇总': summary_new, '原始结果汇总': summary_original})
        comparison_df.fillna(0, inplace=True)
        comparison_df['差异'] = comparison_df['新结果汇总'] - comparison_df['原始结果汇总']
        
        # 找出有差异的行 (使用 numpy.isclose 处理浮点数精度问题)
        diff_df = comparison_df[~np.isclose(comparison_df['差异'], 0)]

        if diff_df.empty:
            print("   - 结果: 所有科目的汇总金额均一致 ✅")
        else:
            print("   - 结果: 发现金额差异 ❌")
            print("     差异详情 (仅显示有差异的科目):")
            diff_df.index.name = '科目代码'
            for account_code, row in diff_df.iterrows():
                print(f"       - 科目: {account_code}")
                print(f"         - 新结果汇总:   {row['新结果汇总']:,.2f}")
                print(f"         - 原始结果汇总: {row['原始结果汇总']:,.2f}")
                print(f"         - 差异:         {row['差异']:,.2f}")
    
    print(f"\n--- 最终分录结果比较结束 ---\n")


def main():
    """
    主函数，定义比较配置并执行所有比较。
    """
    # --- 配置文件路径 ---
    detail_file_path = '给翟总/202312计量明细.xlsx'

    # --- 定义每个业务类型的比较配置 ---
    comparisons = [
        {
            'business_type': '直保业务',
            'source_file': 'measurement_results_8.xlsx',
            'detail_sheet': '直保',
            'columns_to_sum': [
                '保费_本币', 
                '保险获取现金流_本币', 
                '保险合同收入', 
                '当期确认的IACF'
            ]
        },
        {
            'business_type': '分入业务',
            'source_file': 'measurement_results_11.xlsx',
            'detail_sheet': '分入',
            'columns_to_sum': [
                '保费_本币', 
                '手续费_本币', 
                '不含税经纪费_本币', 
                '保险合同收入'
            ]
        },
        {
            'business_type': '分出业务',
            'source_file': 'measurement_results_10.xlsx',
            'detail_sheet': '分出',
            'columns_to_sum': [
                '保费_本币', 
                '手续费_本币', 
                '当期确认的保费', 
                '亏损摊回部分', 
                '当期确认的投资成分'
            ]
        }
    ]

    # --- 新增：动态比较分摊计量文件 ---
    alloc_source_file = 'allocation_results.xlsx'
    alloc_detail_file = '给翟总/202312分摊计量.xlsx'
    
    try:
        print(f"--- 正在准备比较分摊计量文件 ---")
        df_alloc_source = pd.read_excel(alloc_source_file)
        df_alloc_detail = pd.read_excel(alloc_detail_file)
        
        # 找出共有的、且为数值类型的列
        common_cols = list(set(df_alloc_source.columns) & set(df_alloc_detail.columns))
        numeric_cols_to_sum = df_alloc_source[common_cols].select_dtypes(include=np.number).columns.tolist()
        
        if numeric_cols_to_sum:
            print(f"找到 {len(numeric_cols_to_sum)} 个共有的数值列进行比较。")
            alloc_comparison = {
                'business_type': '分摊计量',
                'source_file': alloc_source_file,
                'detail_file': alloc_detail_file,
                'detail_sheet': 0, # 修正：明确指定读取第一个 sheet，而不是 None
                'columns_to_sum': sorted(numeric_cols_to_sum) # 排序以保证顺序
            }
            # 将新的比较任务添加到列表的开头
            comparisons.insert(0, alloc_comparison)
        else:
            print("警告: 在两个分摊计量文件中没有找到可供比较的共有数值列。")
        print("-" * 20)

    except FileNotFoundError as e:
        print(f"错误: 无法找到分摊计量文件 {e.filename}，跳过此项比较。")
    except Exception as e:
        print(f"错误: 在处理分摊计量文件时发生未知错误: {e}，跳过此项比较。")


    # --- 执行所有比较 ---
    for config in comparisons:
        compare_data(
            source_file=config['source_file'],
            detail_file=config.get('detail_file', detail_file_path), # 兼容分摊计量的不同目标文件
            detail_sheet=config['detail_sheet'],
            columns_to_sum=config['columns_to_sum'],
            business_type=config['business_type']
        )
    
    # --- 最后，执行最终分录结果的比较 ---
    final_new_file = '未到期分录结果.xlsx'
    final_original_file = '给翟总/未到期分录结果.xlsx'
    compare_final_entries(final_new_file, final_original_file)

if __name__ == '__main__':
    main()
