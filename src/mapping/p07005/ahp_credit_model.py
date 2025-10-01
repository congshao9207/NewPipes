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
            if isinstance(temp_value, list):
                if len(temp_value) > 0:
                    return temp_value
                else:
                    return -999.0
            else:
                return temp_value
    else:
        return -999.0


# ahp模型变量加工
class AhpCreditModel(ModuleProcessor):

    def __init__(self, strategy_param) -> None:
        super().__init__()
        self.df = None
        self.origin_data = strategy_param.get('queryData')
        self.variables = {
            # 新增ahp模型指标，默认值均在每个指标分值为0的分箱内，保证个人未授权情况下获取不到数据，总分为0
            "ahp_marriage_status": 0,  # 婚姻状况
            "ahp_live_address_type": 0,  # 个人房产数
            "ahp_all_loan_account_cnt": 0,  # 所有账户数合计
            "ahp_unsettled_business_loan_org_cnt": 10,  # 经营性贷款在贷机构家数
            "ahp_unsettled_consume_loan_org_cnt": 10,  # 消费性贷款在贷机构家数
            "ahp_bus_loan_credit_guar_balance_prop": 1.0,  # 信用保证类经营性贷款在贷金额占比
            "ahp_total_loan_change_rate": 1.0,  # 近一年贷款金额变化率
            "ahp_org_change_cnt": 10,  # 近一年机构变化家数
            "ahp_new_add_1y_loan_amount": 1000.0,  # 近一年新增机构借款金额
            "ahp_loan_approved_rate_1y": 0.0,  # 近一年贷款审批查询放款比例
            "ahp_loan_overdue_2year_total_cnt": 10,  # 总计贷款两年内逾期次数
            "ahp_credit_overdue_2year_total_cnt": 20,  # 总计贷记卡两年内逾期次数
            "ahp_overdue_5year_total_cnt": 20,  # 总计五年内逾期次数
            "ahp_single_loan_3year_overdue_max_month": 10,  # 单笔贷记卡近3年内出现连续逾期最大期数
            "ahp_abnormal_loans_and_external_guarantees_cnt": 10,  # 贷款及对外担保异常笔数
            "ahp_abnormal_credit_cards_cnt": 10,  # 非正常状态信用卡张数
            "ahp_loan_credit_query_3month_cnt": 10,  # 近3个月审批查询机构家数
            "ahp_total_credit_usage_rate": 1.0,  # 贷记卡总透支率
            "ahp_credit_min_payed_number": 10,  # 贷记卡最低还款张数
            "ahp_credit_quota_used_div_avg_used_rate": 10.0  # 已用额度/近六个月平均使用额
        }

    def process(self):
        self.variables['user_type'] = 'model'
        self.variables['segment_name'] = 'model'
        self.query_marriage_status()
        self.query_credit_info()
        if self.df is not None:
            self.clean_variables()

    def query_credit_info(self):
        sql = '''select basic_id,variable_name,variable_value from info_union_credit_data_detail where basic_id = (
                                        select id from info_union_credit_data where credit_parse_no = %(credit_parse_no)s 
                                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1) '''
        for origin_data in self.origin_data:
            if origin_data['userType'] == 'PERSONAL' and origin_data['relation'] == 'MAIN':
                creditParseReqNo = origin_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
                df = sql_to_df(sql=sql, params={"credit_parse_no": creditParseReqNo})
                if df.shape[0] > 0:
                    df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
                    self.df = df

    # 通过接口获取单人婚姻状况
    def query_marriage_status(self):
        """
        若为主体个人且婚姻信息为已婚，给1
        :return:
        """
        marriage_sql = """
        select id_card_no, marriage_status from info_marriage_status 
            where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            and channel_api_no = 37001
            """
        for origin_data in self.origin_data:
            if origin_data['userType'] == 'PERSONAL' and origin_data['relation'] == 'MAIN':
                idno = origin_data.get('idno')
                marriage_df = sql_to_df(sql=marriage_sql, params={'unique_id_no': idno})
                if marriage_df.shape[0] > 0:
                    if pd.notna(marriage_df['marriage_status'][0]) and marriage_df['marriage_status'][0] == '已婚':
                        self.variables['ahp_marriage_status'] = 1

    def clean_variables(self):
        df = self.df.copy()
        # ahp_live_address_type 个人房产数
        self.variables['ahp_live_address_type'] = int(get_value(df, 'live_address_type'))
        # ahp_all_loan_account_cnt 所有账户数合计
        self.variables['ahp_all_loan_account_cnt'] = int(get_value(df, 'all_loan_account_cnt'))
        # ahp_unsettled_business_loan_org_cnt 经营性贷款在贷机构家数
        self.variables['ahp_unsettled_business_loan_org_cnt'] = int(get_value(df, 'unsettled_business_loan_org_cnt'))
        # ahp_unsettled_consume_loan_org_cnt 消费性贷款在贷机构家数
        self.variables['ahp_unsettled_consume_loan_org_cnt'] = int(get_value(df, 'unsettled_consume_loan_org_cnt'))
        # ahp_bus_loan_credit_guar_balance_prop 信用保证类经营性贷款在贷金额占比
        self.variables['ahp_bus_loan_credit_guar_balance_prop'] = get_value(df, 'bus_loan_credit_guar_balance_prop')
        # ahp_total_loan_change_rate 近一年贷款金额变化率
        temp_df = df.loc[df['variable_name'].isin(['total_loan_amount_latest_year', 'total_loan_amount_last_year'])]
        temp_df1 = df.loc[(df['variable_name'] == 'total_loan_amount_latest_year') & pd.notna(df['variable_value'])]
        temp_df2 = df.loc[(df['variable_name'] == 'total_loan_amount_last_year') & pd.notna(df['variable_value'])]
        if temp_df1.shape[0] > 0 and temp_df2.shape[0] > 0:
            total_loan_amount_latest_year = float(temp_df1['variable_value'].values[0])
            total_loan_amount_last_year = float(temp_df2['variable_value'].values[0])
            self.variables['ahp_total_loan_change_rate'] = total_loan_amount_latest_year / total_loan_amount_last_year if total_loan_amount_last_year != 0 else 1
        # ahp_org_change_cnt  近一年机构变化家数
        self.variables['ahp_org_change_cnt'] = int(get_value(df, 'org_change_cnt'))
        # ahp_new_add_1y_loan_amount 近一年新增机构借款金额，单位：万元
        temp_df = df.loc[df['variable_name'] == 'new_add_1y_loan_amount']
        if temp_df.shape[0] > 0:
            if pd.notna(temp_df['variable_value'].values[0]):
                self.variables['ahp_new_add_1y_loan_amount'] = float(temp_df['variable_value'].values[0]) / 10000
            else:
                self.variables['ahp_new_add_1y_loan_amount'] = -999.0
        else:
            self.variables['ahp_new_add_1y_loan_amount'] = -999.0
        # ahp_loan_approved_rate_1y 近一年贷款审批查询放款比例
        self.variables['ahp_loan_approved_rate_1y'] = get_value(df, 'loan_approved_rate_1y')
        # ahp_loan_overdue_2year_total_cnt 总计贷款两年内逾期次数
        self.variables['ahp_loan_overdue_2year_total_cnt'] = int(get_value(df, 'loan_overdue_2year_total_cnt'))
        # ahp_credit_overdue_2year_total_cnt 总计贷记卡两年内逾期次数
        self.variables['ahp_credit_overdue_2year_total_cnt'] = int(get_value(df, 'credit_overdue_2year_total_cnt'))
        # ahp_overdue_5year_total_cnt 总计五年内逾期次数
        temp_df = df.loc[df['variable_name'].isin(['dkyqnum', 'djkyqnum']) & pd.notna(df['variable_value'])]
        if temp_df.shape[0] > 0:
            self.variables['ahp_overdue_5year_total_cnt'] = int(temp_df['variable_value'].astype(int).sum())
        else:
            self.variables['ahp_overdue_5year_total_cnt'] = -999
        # ahp_single_loan_3year_overdue_max_month 单笔贷记卡近3年内出现连续逾期最大期数
        self.variables['ahp_single_loan_3year_overdue_max_month'] = int(get_value(df, 'single_loan_3year_overdue_max_month'))
        # ahp_abnormal_loans_and_external_guarantees_cnt 贷款及对外担保异常笔数
        temp_df = df.loc[df['variable_name'].isin(['unsettled_category_abnormal_cnt', 'loan_scured_five_b_level_abnormality_cnt'])
                         & pd.notna(df['variable_value'])]
        if temp_df.shape[0] > 0:
            self.variables['ahp_abnormal_loans_and_external_guarantees_cnt'] = int(temp_df['variable_value'].astype(int).sum())
        else:
            self.variables['ahp_abnormal_loans_and_external_guarantees_cnt'] = -999
        # ahp_abnormal_credit_cards_cnt 非正常状态信用卡张数
        self.variables['ahp_abnormal_credit_cards_cnt'] = int(get_value(df, 'cardtypenum'))
        # ahp_loan_credit_query_3month_cnt 近3个月审批查询机构家数
        self.variables['ahp_loan_credit_query_3month_cnt'] = int(get_value(df, 'loan_credit_query_3month_cnt'))
        # ahp_total_credit_usage_rate 贷记卡总透支率
        self.variables[''] = get_value(df, 'total_credit_usage_rate')
        # ahp_credit_min_payed_number 贷记卡最低还款张数
        self.variables['ahp_credit_min_payed_number'] = int(get_value(df, 'credit_min_payed_number'))
        # ahp_credit_quota_used_div_avg_used_rate 已用额度/近六个月平均使用额
        temp_df1 = df.loc[(df['variable_name'] == 'total_credit_quota_used') & pd.notna(df['variable_value'])]
        temp_df2 = df.loc[(df['variable_name'] == 'total_credit_avg_used_6m') & pd.notna(df['variable_value'])]
        if temp_df1.shape[0] > 0 and temp_df2.shape[0] > 0:
            total_credit_quota_used = float(temp_df1['variable_value'].values[0])
            total_credit_avg_used_6m = float(temp_df2['variable_value'].values[0])
            self.variables['ahp_credit_quota_used_div_avg_used_rate'] = total_credit_quota_used / total_credit_avg_used_6m if total_credit_avg_used_6m != 0 else 2
