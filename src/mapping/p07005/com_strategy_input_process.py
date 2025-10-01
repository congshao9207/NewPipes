from mapping.module_processor import ModuleProcessor
from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
import numpy as np
import json
import datetime
from util.mysql_reader import sql_to_df


# 判断是否存在指标，若取不到指标，返回-999
def get_value(df, vari_name):
    temp_df = df.loc[df['variable_name'] == vari_name]
    if temp_df.shape[0] > 0:
        temp_value = temp_df['variable_value'].values[0]
        if pd.isna(temp_value):
            return -999.0
        else:
            temp_value = json.loads(temp_value)
            if isinstance(temp_value,list):
                if len(temp_value) > 0:
                    return temp_value
                else:
                    return -999.0
            else:
                return temp_value
    else:
        return -999.0


# 企业客户征信入参变量加工
class ComStrategyInputProcessor(ModuleProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.df = None

    def process(self):
        self.variables['user_type'] = "COMPANY"
        self.query_credit_info()
        if self.df is not None:
            self.clean_variables()

    def query_credit_info(self):
        sql = '''select basic_id,variable_name,variable_value from info_union_credit_data_detail where basic_id = (
                                        select id from info_union_credit_data where credit_parse_no = %(credit_parse_no)s 
                                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1) '''
        creditParseReqNo = self.origin_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
        df = sql_to_df(sql=sql, params={"credit_parse_no": creditParseReqNo})
        if df.shape[0] > 0:
            df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            self.df = df

    def clean_variables(self):
        df = self.df.copy()
        # 存在未结清不良类信贷
        self.variables['ecredit_unsettled_bad_cnt'] = get_value(df, 'ecredit_unsettled_bad_cnt')
        # 存在未结清关注类信贷
        self.variables['ecredit_unsettled_focus_cnt'] = get_value(df, 'ecredit_unsettled_focus_cnt')
        # 存在已结清不良类信贷
        self.variables['ecredit_settled_bad_cnt'] = get_value(df, 'ecredit_settled_bad_cnt')
        # 存在已结清关注类信贷
        self.variables['ecredit_settled_focus_cnt'] = get_value(df, 'ecredit_settled_focus_cnt')
        # 未结清信贷业务机构数较多
        self.variables['ecredit_unsettled_loan_org_cnt'] = get_value(df, 'ecredit_unsettled_loan_org_cnt')
