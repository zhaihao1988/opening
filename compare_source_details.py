import pandas as pd
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

def get_distinct_risk_codes_from_db():
    """
    Connects to the database and fetches a unique list of risk codes
    for direct business from the measurement results table.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("数据库连接成功！正在查询直保业务的险种代码...")
        
        # This query fetches the distinct risk codes for the same data scope
        # as the main script's direct business processing.
        query = """
        SELECT DISTINCT "risk_code"
        FROM "measure_platform"."measure_cx_unexpired"
        WHERE "val_month" = '202412' AND "val_method" = '8' AND "end_date" > '20241231'
        """
        
        df = pd.read_sql_query(query, conn)
        print("险种代码查询完成！")
        
        # Filter out any potential None or NaN values and return as a set
        return set(df['risk_code'].dropna())
        
    except OperationalError as e:
        print(f"数据库连接失败: {e}")
        return None
    except Exception as e:
        print(f"查询数据库时发生错误: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()
            print("数据库连接已关闭。")

def get_product_mapping_codes():
    """
    Loads the product mapping from the Excel file and returns the codes.
    """
    try:
        print("正在加载产品段值转换映射文件...")
        map_product_df = pd.read_excel(
            '给翟总/财务段值转换/产品管理导出列表.xls', 
            header=None, 
            usecols=[0], 
            names=['code'],
            dtype=str
        )
        map_product_df.dropna(inplace=True)
        print("映射文件加载完成。")
        return set(map_product_df['code'])
    except FileNotFoundError:
        print("错误: 映射文件 '给翟总/财务段值转换/产品管理导出列表.xls' 未找到。")
        return None
    except Exception as e:
        print(f"加载映射文件时发生错误: {e}")
        return None

def main():
    """
    Main function to compare risk codes from the database against the mapping file.
    """
    print("--- 开始对比数据库险种代码与映射文件 ---")
    
    db_risk_codes = get_distinct_risk_codes_from_db()
    mapping_codes = get_product_mapping_codes()
    
    if db_risk_codes is None or mapping_codes is None:
        print("由于发生错误，无法进行对比。")
        return
        
    print(f"\n数据库中共有 {len(db_risk_codes)} 个不同的险种代码。")
    print(f"映射文件 '产品管理导出列表.xls' 中共有 {len(mapping_codes)} 个代码。")
    
    # Find codes that are in the database but not in the mapping file
    missing_codes = db_risk_codes - mapping_codes
    
    if not missing_codes:
        print("\n恭喜！所有来自数据库的险种代码都能在映射文件中找到。")
    else:
        print(f"\n警告！发现 {len(missing_codes)} 个险种代码存在于数据库但缺失于映射文件中:")
        for code in sorted(list(missing_codes)):
            print(f"  - {code}")
        print("\n请将以上缺失的代码添加到 '产品管理导出列表.xls' 文件中以解决 product_segment 空值问题。")
        
    print("\n--- 对比完成 ---")

if __name__ == '__main__':
    main()
