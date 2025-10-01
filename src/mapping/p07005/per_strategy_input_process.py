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


# 个人客户征信入参变量加工
class PerStrategyInputProcessor(ModuleProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.df = None

    def process(self):
        self.variables['user_type'] = "PERSONAL"
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
        self.variables['public_sum_count'] = get_value(df, 'public_sum_count')
        self.variables['rhzx_business_loan_3year_overdue_cnt'] = get_value(df, 'rhzx_business_loan_3year_overdue_cnt')
        self.variables['loan_category_abnormal_status'] = get_value(df, 'loan_category_abnormal_status')
        self.variables['business_loan_average_3year_overdue_cnt'] = get_value(df, 'business_loan_average_3year_overdue_cnt')
        self.variables['loan_now_overdue_money'] = get_value(df, 'loan_now_overdue_money')
        self.variables['single_loan_overdue_2year_cnt'] = get_value(df, 'single_loan_overdue_2year_cnt')
        self.variables['extension_number'] = get_value(df, 'extension_number')
        self.variables['loan_status_abnorm_cnt'] = get_value(df, 'loan_status_abnorm_cnt')
        self.variables['loan_overdue_2year_total_cnt'] = get_value(df, 'loan_overdue_2year_total_cnt')
        self.variables['unsettled_business_loan_org_cnt'] = get_value(df, 'unsettled_business_loan_org_cnt')
        self.variables['large_loan_2year_overdue_cnt'] = get_value(df, 'large_loan_2year_overdue_cnt')
        self.variables['unsettled_consume_loan_org_cnt'] = get_value(df, 'unsettled_consume_loan_org_cnt')
        self.variables['unsettled_consume_total_cnt'] = get_value(df, 'unsettled_consume_total_cnt')
        self.variables['business_loan_type_cnt'] = get_value(df, 'business_loan_type_cnt')
        self.variables['unsettled_loan_agency_number'] = get_value(df, 'unsettled_loan_agency_number')
        self.variables['unsettled_consume_total_amount'] = get_value(df, 'unsettled_consume_total_amount')
        self.variables['rhzx_business_loan_3year_ago_overdue_cnt'] = get_value(df, 'rhzx_business_loan_3year_ago_overdue_cnt')
        self.variables['business_loan_average_3year_ago_overdue_cnt'] = get_value(df, 'business_loan_average_3year_ago_overdue_cnt')
        self.variables['credit_now_overdue_1k_money'] = get_value(df, 'credit_now_overdue_1k_money')
        # 贷记卡当前逾期
        # credit_now_overdue_money - credit_now_overdue_1k_money
        credit_now_overdue_1k_money_df = df.loc[df.variable_name == 'credit_now_overdue_1k_money']
        credit_now_overdue_money_df = df.loc[df.variable_name == 'credit_now_overdue_money']
        if credit_now_overdue_money_df.shape[0] > 0 and credit_now_overdue_1k_money_df.shape[0] > 0:
            credit_now_overdue_money = float(credit_now_overdue_1k_money_df.variable_value.values[0]) - float(credit_now_overdue_money_df.variable_value.values[0])
            self.variables['credit_now_overdue_money'] = credit_now_overdue_money
        self.variables['single_credit_overdue_2year_cnt'] = get_value(df, 'single_credit_overdue_2year_cnt')
        self.variables['credit_status_abnormal_cnt'] = get_value(df, 'credit_status_abnormal_cnt')
        # total_credit_usage_rate
        total_credit_usage_rate_df = df.loc[df.variable_name == 'total_credit_usage_rate']
        if total_credit_usage_rate_df.shape[0] > 0:
            total_credit_usage_rate = float(total_credit_usage_rate_df.variable_value.values[0])
            if total_credit_usage_rate > 0.8:
                temp_df = df.loc[df.variable_name == 'credit_min_payed_number']
                if temp_df.shape[0] > 0:
                    credit_overdrawn_2card = int(temp_df.variable_value.values[0])
                    self.variables['credit_overdrawn_2card'] = credit_overdrawn_2card
                else:
                    self.variables['credit_overdrawn_2card'] = 0
        self.variables['credit_overdue_2year_total_cnt'] = get_value(df, 'credit_overdue_2year_total_cnt')
        # min(total_credit_usage_rate,2)*credit_min_payed_number
        temp_df1 = df.loc[df.variable_name == 'total_credit_usage_rate']
        if temp_df1.shape[0] > 0:
            total_credit_usage_rate = float(temp_df1.variable_value.values[0])
            temp_df2 = df.loc[df.variable_name == 'credit_min_payed_number']
            if temp_df2.shape[0] > 0:
                credit_min_payed_number = int(temp_df2.variable_value.values[0])
                credit_financial_tension = min(total_credit_usage_rate, 2) * credit_min_payed_number
                self.variables['credit_financial_tension'] = credit_financial_tension
        self.variables['activated_credit_card_cnt'] = get_value(df, 'activated_credit_card_cnt')
        self.variables['credit_min_payed_number'] = get_value(df, 'credit_min_payed_number')
        self.variables['credit_org_cnt'] = get_value(df, 'credit_org_cnt')
        self.variables['credit_overdue_5year'] = get_value(df, 'djkyqnum')
        # max(single_loan_3year_overdue_max_month,single_credit_3year_overdue_max_month)
        temp_df1 = df.loc[df.variable_name == 'single_loan_3year_overdue_max_month']
        temp_df2 = df.loc[df.variable_name == 'single_credit_3year_overdue_max_month']
        if temp_df1.shape[0] > 0 and temp_df2.shape[0] > 0:
            loan_3year = int(temp_df1.variable_value.values[0])
            credit_3year = int(temp_df2.variable_value.values[0])
            single_credit_or_loan_3year_overdue_max_month = max(loan_3year, credit_3year)
            self.variables['single_credit_or_loan_3year_overdue_max_month']= single_credit_or_loan_3year_overdue_max_month
        self.variables['loan_scured_five_a_level_abnormality_cnt'] = get_value(df, 'loan_scured_five_a_level_abnormality_cnt')
        self.variables['loan_scured_five_b_level_abnormality_cnt'] = get_value(df, 'loan_scured_five_b_level_abnormality_cnt')
        self.variables['guar_loan_balance'] = get_value(df, 'guar_loan_balance')
        self.variables['force_execution_cnt'] = get_value(df, 'force_execution_cnt')
        self.variables['civil_judge_cnt'] = get_value(df, 'civil_judge_cnt')
        self.variables['loan_doubtful'] = get_value(df, 'loan_doubtful')
        self.variables['owing_tax_cnt'] = get_value(df, 'owing_tax_cnt')
        self.variables['admin_punish_cnt'] = get_value(df, 'admin_punish_cnt')
        self.variables['loan_credit_query_3month_cnt'] = get_value(df, 'loan_credit_query_3month_cnt')
        # 20230315新增
        self.variables['loan_now_overdue_cnt'] = get_value(df, 'loan_now_overdue_cnt')
        self.variables['single_loan_overdue_2year_cnt'] = get_value(df, 'single_loan_overdue_2year_cnt')

        # loan_non_mort_overdue_2year_total_cnt
        fwajdkyqnum = df.loc[df.variable_name == 'fwajdkyqnum'].variable_value.values[0]
        if self.variables['loan_overdue_2year_total_cnt'] != -999.0:
            loan_non_mort_overdue_2year_total_cnt = self.variables['loan_overdue_2year_total_cnt'] - int(fwajdkyqnum)
            self.variables['loan_non_mort_overdue_2year_total_cnt'] = loan_non_mort_overdue_2year_total_cnt

        # credit_and_mort_loan_3year_overdue_cnt
        credit_overdue_2year_total_cnt = get_value(df, 'credit_overdue_2year_total_cnt')
        if credit_overdue_2year_total_cnt != -999.0:
            credit_and_mort_loan_3year_overdue_cnt = credit_overdue_2year_total_cnt + int(fwajdkyqnum)
            self.variables['credit_and_mort_loan_3year_overdue_cnt'] = credit_and_mort_loan_3year_overdue_cnt

        #
        twoyearcarddueterm = df.loc[df.variable_name == 'twoyearcarddueterm'].variable_value.values[0]
        self.variables['credit_overdue_months_2year'] = int(twoyearcarddueterm)


