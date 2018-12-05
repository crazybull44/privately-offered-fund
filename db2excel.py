import pandas as pd
import pymysql

code_table_path = "source-data/ID.csv"
database_path = "database"

df = pd.read_csv(code_table_path, encoding='gbk', header=None)
df.sort_index(inplace=True)

db_conn = pymysql.connect(
    host="",  # ip
    database="",
    user="",  # 用户名
    password="",  # 密码
    port=,  # 端口号
    charset='utf8'
)

for i in range(len(df.loc[:, 0])):
    tableName = df.iloc[i, 0]
    print("read %s" % tableName)
    s = "select info.fund_id as fund_id,info.fund_name as fund_name,"
    s = s + "v2_fund_nv_data_zyyx.statistic_date as statistic_date,"
    s = s + "nav as nav,added_nav,swanav,sanav,pc,cpc,total_asset,total_nav,share,annualized_return,"
    s = s + "d7_annualized_return,"
    s = s + "income_value_per_ten_thousand,change_type_code,change_type,change_value,split_ratio,"
    s = s + "cash_ratio,cash_sum,v2_fund_asset_data.stock_ratio as stock_ratio,stock_sum,"
    s = s + "bond_ratio,bond_sum,v2_fund_asset_data.fund_ratio as fund_ratio,"
    s = s + "fund_sum,other_ratio,other_sum,asset_scale "
    s = s + "from v2_fund_info as info "
    s = s + "left join v2_fund_nv_data_zyyx on info.fund_id=v2_fund_nv_data_zyyx.fund_id "
    s = s + "left join v2_fund_change_data on info.fund_id=v2_fund_change_data.fund_id "
    s = s + "left join v2_fund_split_data on info.fund_id=v2_fund_change_data.fund_id "
    s = s + "left join v2_fund_asset_data on info.fund_id=v2_fund_split_data.fund_id "
    s = s + "left join v2_fund_asset_scale on info.fund_id=v2_fund_asset_scale.fund_id "
    s = s + "left join v2_fund_shareholding on info.fund_id=v2_fund_shareholding.fund_id "
    s = s + "left join v2_fund_fee_data on info.fund_id=v2_fund_fee_data.fund_id "
    s = s + "where info.fund_id = " + str(tableName)
    data = pd.read_sql(s, db_conn)
    data.to_csv(database_path+'/%s.csv' % tableName, encoding='gbk', index=False)
    print("end")
