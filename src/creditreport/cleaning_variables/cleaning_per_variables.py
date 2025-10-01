import pandas as pd
import re
from creditreport.cleaning_variables.credit_config import feature_value
from pandas.tseries.offsets import *
import numpy as np
from util.mysql_reader import sql_to_df
import warnings
from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str
import json

warnings.filterwarnings('ignore')


# 保留中文
def find_chinese(param):
    pattern = re.compile(r'[^\u4e00-\u9fa5]')
    chinese = re.sub(pattern, '', param)
    return chinese


# 保留英文字母和数字
def find_en(param):
    pattern = re.compile(r'[^a-zA-Z0-9]')
    en = re.sub(pattern, '', str(param))
    return en


class CleaningPerVariables:
    def __init__(self, report_id, basic_id):
        self.variables = feature_value.copy()
        self.report_id = report_id
        self.basic_id = basic_id
        self.name = None
        self.certificate_no = None
        self.variables_list = []

    def process(self):
        self.loan_summary_info()
        self.combine_variables()
        transform_feature_class_str(self.variables_list, 'InfoUnionCreditDataDetail')

    # 主表,通过主体信息查询得到
    def credit_base_info(self):
        sql = f"select * from credit_base_info where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    # 贷款明细
    def pcredit_loan(self):
        sql = f"select * from pcredit_loan where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        if df.shape[0] > 0:
            df.loan_date = pd.to_datetime(df.loan_date)
        return df

    # 特殊交易记录
    def pcredit_special(self):
        sql = f"select * from pcredit_special where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    # 还款记录
    def repayment_info(self):
        sql = f"select * from pcredit_repayment where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    def pcredit_query_record(self):
        sql = f"select * from pcredit_query_record where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    # 20240115
    def pcredit_live(self):
        sql = f"select * from pcredit_live where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    def pcredit_info(self):
        sql = f"select * from pcredit_info where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    def pcredit_large(self):
        sql = f"select * from pcredit_large_scale where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    def pcredit_biz(self):
        sql = f"select * from pcredit_biz_info where report_id = {self.report_id}"
        df = sql_to_df(sql=sql)
        return df

    def combine_variables(self):
        for key, value in self.variables.items():
            # if key in ['annual_year']:
            temp_dict = {'basic_id': self.basic_id, 'variable_name': key, 'variable_value': value}
            self.variables_list.append(temp_dict)

    def loan_summary_info(self):
        """
        贷款统计信息
        :return:
        """
        base_info = self.credit_base_info()
        df = self.pcredit_loan()
        special_df = self.pcredit_special()
        repayment_df = self.repayment_info()
        query_record_df = self.pcredit_query_record()
        live_df = self.pcredit_live()

        # 征信报告编号	report_no
        self.variables['report_no'] = base_info.report_no.values[0]
        self.variables['report_time'] = pd.to_datetime(base_info.report_time.values[0])

        # 获取报告主体信息
        self.name = base_info.name.values[0]
        self.certificate_no = base_info.certificate_no.values[0]

        # 个人房产数
        self.variables["live_address_type"] = live_df[live_df['live_address_type'].isin(['01', '02', '06', '11'])].shape[0]

        # 经营、消费、按揭类贷款
        loan_account_type_df = df[df['account_type'].isin(['01', '02', '03'])]

        # 经营性贷款
        business_loan_base_df = df.query('account_type in ["01", "02", "03"] and '
                                         '((loan_type in ["01","07","15","22","99"]) or (loan_type in ["04","16"] and loan_amount >= 200000))')
        # 消费性贷款
        consume_loan_base_df = df.query('account_type in["01", "02", "03"] and '
                                        '((loan_type in ["04", "16"] and loan_amount < 200000) or loan_type in ["02"])')
        # 经营性贷款+消费性贷款
        bus_con_loan_base_df = df.query('account_type in ["01", "02", "03"] and '
                                        'loan_type in ["01","07","15","22","99", "04", "02", "16"]')
        # 贷记卡
        credit_card_df = df.query('account_type in ["04"] and currency in ["人民币元","CNY"]')
        # 准贷记卡
        quasi_credit_loan_df = df.query('account_type in ["05"] and currency in ["人民币元","CNY"]')
        # 贷记卡及准贷记卡
        credit_loan_df = df.query('account_type in ["04", "05"] and currency in ["人民币元","CNY"]')

        # 未结清经营性贷款机构家数 unsettled_business_loan_org_cnt
        # 经营性贷款余额	business_loan_type_balance
        # 经营性在贷贷款笔数	business_loan_type_cnt
        if business_loan_base_df.shape[0] > 0:
            unsettled_business_loan_org_df = business_loan_base_df.query('loan_balance > 0')
            if unsettled_business_loan_org_df.shape[0] > 0:
                count = unsettled_business_loan_org_df.dropna(subset=["account_org"])["account_org"].unique().size
                self.variables['unsettled_business_loan_org_cnt'] = count
                self.variables['business_loan_type_balance'] = unsettled_business_loan_org_df['loan_balance'].sum()
                self.variables['business_loan_type_cnt'] = unsettled_business_loan_org_df.shape[0]

        # 未结清消费性贷款机构家数
        # 消费性贷款余额	consume_loan_type_balance
        # 消费性在贷贷款笔数	consume_loan_type_cnt
        if consume_loan_base_df.shape[0] > 0:
            unsettled_consume_loan_org_df = consume_loan_base_df.query('loan_balance > 0')
            if unsettled_consume_loan_org_df.shape[0] > 0:
                count = unsettled_consume_loan_org_df.dropna(subset=["account_org"])["account_org"].unique().size
                self.variables['unsettled_consume_loan_org_cnt'] = count
                self.variables['consume_loan_type_balance'] = unsettled_consume_loan_org_df.loan_balance.sum()
                self.variables['consume_loan_type_cnt'] = unsettled_consume_loan_org_df.shape[0]

        # 未结清房贷笔数
        unsettled_house_loan_number_df = df.query('account_type in ["01", "02", "03"] and '
                                                  'loan_type in ["03", "05", "06"] and loan_balance > 0')
        self.variables['unsettled_house_loan_cnt'] = unsettled_house_loan_number_df.shape[0]

        # 未结清车贷笔数
        unsettled_car_loan_number_df = df.query('account_type in ["01", "02", "03"] and loan_type in ["02"] and loan_balance > 0')
        self.variables['unsettled_car_loan_cnt'] = unsettled_car_loan_number_df.shape[0]

        # 贷款账户状态异常笔数
        # 20240115 新增：剔除状态为空的
        loan_status_abnorm_df = df.loc[df['account_type'].isin(["01", "02", "03"]) &
                                       ((~df['account_status'].isin(['01', '04'])) & pd.notna(df['account_status']))]
        self.variables['loan_status_abnorm_cnt'] = loan_status_abnorm_df.shape[0]

        # 贷款五级分类异常笔数
        unsettled_category_abnormal_df = df.query('account_type in ["01", "02", "03"] and category in ["02", "03", "04", "05"]')
        self.variables['unsettled_category_abnormal_cnt'] = unsettled_category_abnormal_df.shape[0]

        # 贷款当前逾期笔数
        loan_now_overdue_df = df.query('account_type in ["01", "02", "03"] and overdue_amount > 0')
        self.variables['loan_now_overdue_cnt'] = loan_now_overdue_df.shape[0]

        # 贷款当前逾期金额
        self.variables['loan_now_overdue_money'] = loan_now_overdue_df.overdue_amount.sum()

        # 房贷按揭已归还	mort_settle_loan_date
        mort_settle_loan_df = df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"] and account_status == "04"')
        self.variables['mort_settle_loan_date'] = mort_settle_loan_df.loan_date.unique().tolist()

        # 房贷按揭未结清	mort_no_settle_loan_date
        mort_no_settle_loan_df = df.query(
            'account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"] and account_status not in ["04"]')
        self.variables['mort_no_settle_loan_date'] = mort_no_settle_loan_df.loan_date.unique().tolist()

        # 机构类型	per_org_type
        # 余额	per_balance
        # 占比	per_balance_prop
        loan_account_df = df.query('account_type in ["01", "02", "03", "04", "05"] and loan_balance > 0')
        if loan_account_df.shape[0] > 0:
            loan_account_df['account_org_ch'] = loan_account_df['account_org'].apply(lambda x: find_chinese(str(x)))
            duplicate_account_org_df = loan_account_df.groupby('account_org_ch')['loan_balance'].sum().reset_index()
            duplicate_account_org_df['balance_prop'] = duplicate_account_org_df.loan_balance / duplicate_account_org_df.loan_balance.sum()
            self.variables['per_org_type'] = duplicate_account_org_df.account_org_ch.tolist()
            self.variables['per_balance'] = duplicate_account_org_df.loan_balance.tolist()
            self.variables['per_balance_prop'] = duplicate_account_org_df.balance_prop.tolist()

        # 住房贷款余额	mortgage_loan_type_balance
        # 住房在贷贷款笔数	mortgage_loan_type_cnt
        mortgage_loan_df = df.query('account_type in["01", "02", "03"] and loan_type in ["05","06"]')
        if mortgage_loan_df.shape[0] > 0:
            self.variables['mortgage_loan_type_balance'] = mortgage_loan_df.loan_balance.sum()
            self.variables['mortgage_loan_type_cnt'] = mortgage_loan_df.query('loan_balance > 0').shape[0]

        # 商用住房贷款余额	bus_mortgage_loan_type_balance
        # 商用住房在贷贷款笔数	bus_mortgage_loan_type_cnt
        bus_mortgage_loan_df = df.query('account_type in["01", "02", "03"] and account_type in ["03"]')
        if bus_mortgage_loan_df.shape[0] > 0:
            self.variables['bus_mortgage_loan_type_balance'] = bus_mortgage_loan_df.loan_balance.sum()
            self.variables['bus_mortgage_loan_type_cnt'] = bus_mortgage_loan_df.query('loan_balance > 0').shape[0]

        # 消费性贷款借款金额最大值	max_unsettled_consume_amt
        if consume_loan_base_df.shape[0] > 0:
            self.variables['max_unsettled_consume_amt'] = consume_loan_base_df.loan_amount.max()

        # 房贷借款总额	house_total_loan_amt
        house_total_loan_df = df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"]')
        self.variables['house_total_loan_amt'] = house_total_loan_df.loan_amount.sum()

        # 房贷提前结清记录数量	if_house_pre_settled
        house_loan_df = df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"]')
        house_pre_settled_df = special_df.query('record_id in ' + str(list(house_loan_df.id)))
        if house_pre_settled_df.shape[0] > 0:
            self.variables['if_house_pre_settled'] = house_pre_settled_df.query('special_type in ["05"]').shape[0]

        # 年份	annual_year
        cur_year = self.variables['report_time'].year
        temp_list = [cur_year, cur_year - 1, cur_year - 2, cur_year - 3]
        annual_year = [str(i) for i in temp_list]
        self.variables['annual_year'] = '[' + ','.join(f'"{i}"' for i in temp_list) + ']'

        # 经营性借款金额	annual_bus_loan_amount
        # 消费性借款金额	annual_cousume_loan_amount
        # 机构家数	annual_org_cnt
        annual_bus_loan_amount = []
        annual_cousume_loan_amount = []
        annual_org_cnt = []
        for i in annual_year:
            bus_amount = business_loan_base_df.loc[business_loan_base_df.loan_date.dt.strftime('%Y') == i].loan_amount.sum()
            con_amount = consume_loan_base_df.loc[consume_loan_base_df.loan_date.dt.strftime('%Y') == i].loan_amount.sum()
            org_list = bus_con_loan_base_df.loc[bus_con_loan_base_df.loan_date.dt.strftime('%Y') == i].account_org.unique().tolist()
            org_cnt = len(org_list)
            annual_bus_loan_amount.append(bus_amount)
            annual_cousume_loan_amount.append(con_amount)
            annual_org_cnt.append(org_cnt)
        self.variables['annual_bus_loan_amount'] = '[' + ','.join(f'"{i}"' for i in annual_bus_loan_amount) + ']'
        self.variables['annual_cousume_loan_amount'] = '[' + ','.join(f'"{i}"' for i in annual_cousume_loan_amount) + ']'
        self.variables['annual_org_cnt'] = '[' + ','.join(f'"{i}"' for i in annual_org_cnt) + ']'

        # 近12个月放款总额	total_loan_amount_latest_year
        latest_year = self.variables['report_time'] - DateOffset(years=1)
        total_loan_amount_latest_year = bus_con_loan_base_df.loc[bus_con_loan_base_df.loan_date >= latest_year].loan_amount.sum()
        self.variables['total_loan_amount_latest_year'] = total_loan_amount_latest_year

        # 上一年放款总额	total_loan_amount_last_year
        last_year = self.variables['report_time'] - DateOffset(years=2)
        total_loan_amount_last_year = bus_con_loan_base_df.loc[
            (bus_con_loan_base_df.loan_date < latest_year) & (bus_con_loan_base_df.loan_date >= last_year)].loan_amount.sum()
        self.variables['total_loan_amount_last_year'] = total_loan_amount_last_year

        # 在贷余额	unsettled_loan_balance
        # unsettled_loan_df = df.query('account_type in ["01", "02", "03"]')
        unsettled_loan_balance = loan_account_type_df.loan_balance.sum()
        self.variables['unsettled_loan_balance'] = unsettled_loan_balance

        # 住房贷款余额	house_loan_total_balance
        # 商用住房贷款余额	bus_house_loan_total_balance
        self.variables['house_loan_total_balance'] = self.variables['mortgage_loan_type_balance']
        self.variables['bus_house_loan_total_balance'] = self.variables['bus_mortgage_loan_type_balance']

        # 车贷余额	car_loan_total_balance
        car_loan_total_df = df.query('account_type in ["01", "02", "03"] and loan_type == "02"')
        car_loan_total_balance = car_loan_total_df.loan_balance.sum()
        self.variables['car_loan_total_balance'] = car_loan_total_balance

        # 近12个月新增贷款笔数	new_add_1y_loan_cnt
        # 近12个月新增借款金额	new_add_1y_loan_amount
        # 近12个月新增在贷贷款笔数	new_add_1y_unsettle_loan_cnt
        # 近12个月新增在贷余额	new_add_1y_unsettled_loan_balance
        latest_year_df = df.loc[df.loan_date < latest_year]
        latest_year_org_list = latest_year_df.account_org.unique().tolist()
        new_add_loan_df = df.loc[(df.loan_date >= latest_year) & (~df.account_org.isin(latest_year_org_list))]
        self.variables['new_add_1y_loan_cnt'] = new_add_loan_df.shape[0]
        self.variables['new_add_1y_loan_amount'] = new_add_loan_df.loan_amount.sum()
        self.variables['new_add_1y_unsettle_loan_cnt'] = new_add_loan_df.query('loan_amount > 0').shape[0]
        self.variables['new_add_1y_unsettled_loan_balance'] = new_add_loan_df.loan_balance.sum()

        # 是否有3家（含）以上授信金额<=20万	if_loan_amount_lt_20w
        temp_df = new_add_loan_df.groupby('account_org').loan_amount.sum().reset_index()
        temp_df1 = temp_df.loc[temp_df.loan_amount <= 200000]
        if temp_df1.shape[0] >= 3:
            self.variables['if_loan_amount_lt_20w'] = 1

        # 机构变化家数	org_change_cnt
        new_add_loan_org_list = new_add_loan_df.account_org.unique().tolist()
        org_diff = len(latest_year_org_list) - len(new_add_loan_org_list)
        self.variables['org_change_cnt'] = org_diff

        # 担保方式	guarantee_type
        # 笔数	business_loan_guarantee_type_cnt
        # 余额	guarantee_type_balance
        # 余额占比	guarantee_type_balance_prop
        # 抵押组合类、信用保证类、质押、其他
        # 抵押组合类
        gua_mort_df = business_loan_base_df.loc[business_loan_base_df.loan_guarantee_type.isin(['2', '5', '6'])]
        # 信用保证类
        gua_credit_df = business_loan_base_df.loc[business_loan_base_df.loan_guarantee_type.isin(['3', '4', '7'])]
        # 质押
        gua_imp_df = business_loan_base_df.loc[business_loan_base_df.loan_guarantee_type.isin(['1'])]
        # 其他
        gua_other_df = business_loan_base_df.loc[business_loan_base_df.loan_guarantee_type.isin(['99'])]
        # 担保类型、余额、目前笔数
        guar_type_list = []
        guar_type_balance_list = []
        guar_type_cnt_list = []
        guar_type_balance_prop_list = []
        # 经营性贷款余额总和
        total_balance = business_loan_base_df['loan_balance'].sum()

        guar_type_list.append('抵押组合类')
        mort_balance = gua_mort_df['loan_balance'].sum()
        guar_type_balance_list.append(mort_balance)
        guar_type_cnt_list.append(gua_mort_df[gua_mort_df['loan_balance'] > 0].shape[0])
        guar_type_balance_prop_list.append('%.4f' % (mort_balance / total_balance) if total_balance > 0 else 0)

        guar_type_list.append("信用保证类")
        credit_balance = gua_credit_df.loc[:, 'loan_balance'].sum()
        guar_type_balance_list.append(credit_balance)
        guar_type_cnt_list.append(gua_credit_df[gua_credit_df['loan_balance'] > 0].shape[0])
        guar_type_balance_prop_list.append('%.4f' % (credit_balance / total_balance) if total_balance > 0 else 0)

        guar_type_list.append("质押")
        imp_balance = gua_imp_df.loc[:, 'loan_balance'].sum()
        guar_type_balance_list.append(imp_balance)
        guar_type_cnt_list.append(gua_imp_df[gua_imp_df['loan_balance'] > 0].shape[0])
        guar_type_balance_prop_list.append('%.4f' % (imp_balance / total_balance) if total_balance > 0 else 0)

        guar_type_list.append("其他")
        others_balance = gua_other_df.loc[:, 'loan_balance'].sum()
        guar_type_balance_list.append(others_balance)
        guar_type_cnt_list.append(gua_other_df[gua_other_df['loan_balance'] > 0].shape[0])
        guar_type_balance_prop_list.append('%.4f' % (others_balance / total_balance) if total_balance > 0 else 0)

        self.variables['guarantee_type'] = '[' + ','.join(f'"{i}"' for i in guar_type_list) + ']'
        self.variables['business_loan_guarantee_type_cnt'] = '[' + ','.join(f'"{i}"' for i in guar_type_cnt_list) + ']'
        self.variables['guarantee_type_balance'] = '[' + ','.join(f'"{i}"' for i in guar_type_balance_list) + ']'
        self.variables['guarantee_type_balance_prop'] = '[' + ','.join(f'"{i}"' for i in guar_type_balance_prop_list) + ']'

        # 经营性贷款质押笔数	bus_loan_pledge_cnt
        if gua_imp_df.shape[0] > 0:
            bus_loan_pledge_df = gua_imp_df.query('loan_balance > 0')
            self.variables['bus_loan_pledge_cnt'] = bus_loan_pledge_df.shape[0]

        # 经营性贷款信用保证类在贷金额占比	bus_loan_credit_guar_balance_prop
        if gua_credit_df.shape[0] > 0:
            bus_loan_credit_guar_df = gua_credit_df.query('loan_balance > 0')
            bus_loan_credit_guar_balance = bus_loan_credit_guar_df.loan_balance.sum()
            self.variables['bus_loan_credit_guar_balance_prop'] = bus_loan_credit_guar_balance / total_balance if total_balance > 0 else 0

        # 经营性贷款抵押组合类在贷金额	bus_loan_mort_balance
        # 经营性贷款抵押组合类余额占比	bus_loan_mort_balance_prop
        if gua_mort_df.shape[0] > 0:
            bus_loan_mort_balance = gua_mort_df.query('loan_balance > 0').loan_balance.sum()
            self.variables['bus_loan_mort_balance'] = bus_loan_mort_balance
            self.variables['bus_loan_mort_balance_prop'] = bus_loan_mort_balance / total_balance if total_balance > 0 else 0

        # 未结清消费性贷款笔数	unsettled_consume_total_cnt
        # 未结清消费性贷款总额	unsettled_consume_total_amount
        unsettled_consume_total_df = consume_loan_base_df.query('loan_balance > 0')
        self.variables['unsettled_consume_total_cnt'] = unsettled_consume_total_df.shape[0]
        self.variables['unsettled_consume_total_amount'] = unsettled_consume_total_df.loan_amount.sum()

        # 未结清贷款机构家数	unsettled_loan_agency_number
        unsettled_loan_agency_df = loan_account_type_df.query('loan_balance > 0')
        unsettled_loan_agency_df.account_org.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        self.variables['unsettled_loan_agency_number'] = unsettled_loan_agency_df.account_org.nunique()

        # 按揭贷款机构数	mort_org_cnt
        house_loan_number_df = df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"]')
        house_loan_number_df.account_org.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        self.variables['mort_org_cnt'] = house_loan_number_df.account_org.nunique()

        # 信用贷款家数	credit_guar_loan_cnt
        credit_guar_loan_df = loan_account_type_df.query('loan_guarantee_type == "4"')
        credit_guar_loan_df.account_org.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        self.variables['credit_guar_loan_cnt'] = credit_guar_loan_df.account_org.nunique()

        # 20240115
        # 贷记卡状态是否存在异常
        """账户状态异常判定：除开正常（01）、结清（04）、销户（07）、未激活（08）和空的，剩下的算作异常"""
        unusual_account_df = credit_loan_df.loc[(~credit_loan_df.account_status.isin(['01', '04', '07', '08']))
                                                & pd.notna(credit_loan_df.account_status)]
        if unusual_account_df.shape[0] > 0:
            self.variables['ifexception'] = 'T'

        # 贷记卡信息条数	if_credit_card_record
        self.variables['if_credit_card_record'] = credit_loan_df.shape[0]

        # 贷记卡激活张数	activated_credit_card_cnt
        self.variables['activated_credit_card_cnt'] = credit_card_df.query('account_status not in ["07" ,"08"]').shape[0]

        # 已激活张数	activated_credit_card_cnt
        self.variables['activated_credit_card_cnt'] = self.variables['activated_credit_card_cnt']

        # 贷记卡最低还款机构家数	credit_min_payed_number
        credit_min_payed = credit_card_df.loc[credit_card_df['amout_replay_amount'] < credit_card_df['repay_amount'] * 2]
        if credit_min_payed.shape[0] > 0:
            credit_min_payed.account_org.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            self.variables['credit_min_payed_number'] = credit_min_payed.account_org.nunique()

        # 发卡机构数量	credit_org_cnt
        # 贷记卡授信额度总额	total_credit_loan_amount
        # 已使用额度总额	total_credit_quota_used
        # 最近6个月平均使用额度总额	total_credit_avg_used_6m
        # 总使用率	total_credit_usage_rate
        credit_org_df = credit_card_df.query('account_status not in ["07", "08"]')
        if credit_org_df.shape[0] > 0:
            credit_org_df.account_org.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            self.variables['credit_org_cnt'] = credit_org_df.account_org.nunique()
            total_credit_loan_amount = credit_org_df.loan_amount.sum()
            total_credit_quota_used = credit_org_df.quota_used.sum()
            total_credit_avg_used_6m = credit_org_df.avg_overdraft_balance_6.sum()
            self.variables['total_credit_loan_amount'] = total_credit_loan_amount
            self.variables['total_credit_quota_used'] = total_credit_quota_used
            self.variables['total_credit_avg_used_6m'] = total_credit_avg_used_6m
            total_credit_usage_amount = max(total_credit_quota_used, total_credit_avg_used_6m)
            self.variables[
                'total_credit_usage_rate'] = total_credit_usage_amount / total_credit_loan_amount if total_credit_loan_amount > 0 else 0

        # 贷记卡账户状态存在“司法追偿”	credit_status_legal_cnt
        credit_status_legal_df = special_df.query('record_id in ' + str(list(credit_card_df.id)))
        if not house_pre_settled_df.empty:
            self.variables['credit_status_legal_cnt'] = credit_status_legal_df.query('special_type == "8"').shape[0]

        # 贷记卡账户状态存在“银行止付、冻结”	credit_status_b_level_cnt
        credit_status_b_level_cnt = credit_card_df.query('account_status in ["05", "09"]').shape[0]
        self.variables['credit_status_b_level_cnt'] = credit_status_b_level_cnt

        # 对外担保笔数
        loan_gurantee_df = df.query('account_type == "06"')
        self.variables['foreignassurenum'] = loan_gurantee_df.shape[0]

        # 对外担保五级分类存在“关注”笔数	loan_scured_five_b_level_abnormality_cnt
        loan_scured_five_b_level_abnormality_df = df.query('account_type == "06" and category == "02"')
        self.variables['loan_scured_five_b_level_abnormality_cnt'] = loan_scured_five_b_level_abnormality_df.shape[0]

        # 对外担保五级分类存在“次级、可疑、损失”笔数	loan_scured_five_a_level_abnormality_cnt
        loan_scured_five_a_level_abnormality_df = df.query('account_type == "06" and category in ["03", "04", "05"]')
        self.variables['loan_scured_five_a_level_abnormality_cnt'] = loan_scured_five_a_level_abnormality_df.shape[0]

        # 到期日在信用报告拉取时间后面2个月的余额	loan_balance_due_soon
        report_time = self.variables['report_time']
        target_time = report_time + DateOffset(months=2)
        loan_balance_due_soon_df = df.loc[(df.account_type == "06") & (report_time < df.end_date) & (df.end_date <= target_time)]
        self.variables['loan_balance_due_soon'] = loan_balance_due_soon_df.loan_balance.sum()

        # 近3个月审批查询机构家数	loan_credit_query_3month_cnt
        target_time = self.variables['report_time'] - DateOffset(months=3)
        loan_credit_query_3month_df = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(
                ["01", "02", "08", "09", "13", "11", "12", "20"])]
        loan_credit_query_3month_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['loan_credit_query_3month_cnt'] = loan_credit_query_3month_df.shape[0]

        # 近3个月信用卡查询机构家数	credit_query_3month_cnt
        credit_query_3month_df = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["02"])]
        credit_query_3month_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['credit_query_3month_cnt'] = credit_query_3month_df.shape[0]

        # 近3个月贷款审批查询机构家数	loan_query_3month_cnt
        loan_query_3month_df = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["01", "08", "09", "13", "11", "12", "20"])]
        loan_query_3month_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['loan_query_3month_cnt'] = loan_query_3month_df.shape[0]

        # 近3个月保前审查笔数	guar_query_cnt
        guar_query_df = query_record_df.loc[(pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["08"])]
        guar_query_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['guar_query_cnt'] = guar_query_df.shape[0]

        # 近3个月资信审查笔数	credit_review_query_cnt
        credit_review_query_df = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["13", "12"])]
        credit_review_query_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['credit_review_query_cnt'] = credit_review_query_df.shape[0]

        # 近6个月本行审批查询次数	self_query_6month_cnt
        target_time = self.variables['report_time'] - DateOffset(months=6)
        query_record_df['operator_en'] = query_record_df['operator'].apply(lambda x: find_en(x))
        self_query_6month_df = query_record_df.loc[(pd.to_datetime(query_record_df.jhi_time) > target_time) &
                                                   query_record_df.reason.isin(["01", "02", "08", "09", "13", "11", "12", "20"])
                                                   & (query_record_df.operator_en.str.len() > 2)]
        self.variables['self_query_6month_cnt'] = self_query_6month_df.shape[0]

        # 近6个月本行担保资格审查次数	self_guar_query_6month_cnt
        self_guar_query_6month_df = query_record_df.loc[(pd.to_datetime(query_record_df.jhi_time) > target_time) &
                                                        query_record_df.reason.isin(["03"]) & (query_record_df.operator_en.str.len() > 2)]
        self.variables['self_guar_query_6month_cnt'] = self_guar_query_6month_df.shape[0]

        # 近1年贷款审批查询机构家数	loan_query_1y_cnt
        target_time = self.variables['report_time'] - DateOffset(years=1)
        loan_query_1y_df = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["01", "08", "09", "13", "11", "12", "20"])]
        loan_query_1y_df.drop_duplicates(['operator', 'reason'], inplace=True)
        self.variables['loan_query_1y_cnt'] = loan_query_1y_df.shape[0]

        # 连续审批查询机构家数	max_query_cnt_15d
        max_query_cnt_15d = 0
        jhi_time_list = [pd.to_datetime(i) for i in query_record_df.jhi_time.unique().tolist()]
        for i in jhi_time_list:
            target_time = i - DateOffset(days=30)
            temp_df = query_record_df.loc[
                (target_time < pd.to_datetime(query_record_df.jhi_time)) & (pd.to_datetime(query_record_df.jhi_time) <= i)]
            query_cnt = temp_df.operator.nunique()
            if query_cnt > max_query_cnt_15d:
                max_query_cnt_15d = query_cnt
        self.variables['max_query_cnt_15d'] = max_query_cnt_15d

        # 是否白户	if_no_credit_record
        self.variables['no_loan'] = 1 if df[df['account_type'].isin(['01', '02', '03', '04', '05', '06'])].shape[0] == 0 else 0

        # 近3个月贷款放款机构家数	query_loan_approved_3m_org_cnt
        # 近3个月贷款审批查询放款比例	query_loan_approved_3m_prob
        target_time = self.variables['report_time'] - DateOffset(months=3)
        query_loan_approved_3m = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["01", "08", "09", "13", "11", "12", "20"])]
        # 将查询机构可能存在空字符串去掉
        query_loan_approved_3m.operator.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        total_org_cnt = query_loan_approved_3m.operator.nunique()
        org_cnt = 0
        temp_df = query_loan_approved_3m.groupby('operator').jhi_time.min().reset_index()
        for i in range(0, temp_df.shape[0]):
            temp_org = temp_df.iloc[i, 0]
            temp_date = pd.to_datetime(temp_df.iloc[i, 1])
            org_df = loan_account_type_df.loc[(loan_account_type_df.account_org == temp_org) & (loan_account_type_df.loan_date > temp_date)]
            if org_df.shape[0] > 0:
                org_cnt += 1
        self.variables['query_loan_approved_3m_org_cnt'] = org_cnt
        self.variables['query_loan_approved_3m_prob'] = org_cnt / total_org_cnt if total_org_cnt > 0 else 0

        # 近1年贷款查询放款比例	loan_approved_rate_1y
        target_time = self.variables['report_time'] - DateOffset(years=1)
        query_loan_approved_1y = query_record_df.loc[
            (pd.to_datetime(query_record_df.jhi_time) > target_time) & query_record_df.reason.isin(["01", "08", "09", "13", "11", "12", "20"])]
        # 将查询机构可能存在空字符串去掉
        query_loan_approved_1y.operator.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
        total_org_cnt = query_loan_approved_1y.operator.nunique()
        org_cnt = 0
        temp_df = query_loan_approved_1y.groupby('operator').jhi_time.min().reset_index()
        for i in range(0, temp_df.shape[0]):
            temp_org = temp_df.iloc[i, 0]
            temp_date = pd.to_datetime(temp_df.iloc[i, 1])
            org_df = loan_account_type_df.loc[(loan_account_type_df.account_org == temp_org) & (loan_account_type_df.loan_date > temp_date)]
            if org_df.shape[0] > 0:
                org_cnt += 1
        self.variables['loan_approved_rate_1y'] = org_cnt / total_org_cnt if total_org_cnt > 0 else 0

        # 20240115
        report_time = pd.to_datetime(base_info.report_time.values[0])
        business_loan_base_df['loan_date'] = pd.to_datetime(business_loan_base_df['loan_date'])
        business_loan_base_df['loan_end_date'] = pd.to_datetime(business_loan_base_df['loan_end_date'])
        if business_loan_base_df.shape[0] > 0:
            business_loan_base_df['repay_period'] = business_loan_base_df.apply(
                lambda x: x['repay_period'] if pd.notna(x['repay_period']) else
                (x['loan_end_date'].year - x['loan_date'].year) * 12 + x['loan_end_date'].month - x['loan_date'].month
                + (x['loan_end_date'].day - x['loan_date'].day - 1) // 100 + 1, axis=1)
        business_loan_base_df['one_third_amt'] = business_loan_base_df['loan_amount'].apply(lambda x: x / 3 if pd.notna(x) else 0)
        business_loan_base_df['avg_loan_amount'] = business_loan_base_df.apply(
            lambda x: x['loan_amount'] / int(x['repay_period']) if pd.notna(x['repay_period']) else 0, axis=1) if business_loan_base_df.shape[
                                                                                                                      0] > 0 else 0
        repayment_df['ym'] = repayment_df.apply(lambda x: f"{x['jhi_year']}-{x['month']}", axis=1) if repayment_df.shape[0] > 0 else None
        # 贷款逾期信息
        loan_overdue_df = repayment_df[
            repayment_df['record_id'].isin(loan_account_type_df['id'].unique().tolist())]
        # 经营性贷款逾期信息
        business_overdue_df = repayment_df[
            repayment_df['record_id'].isin(business_loan_base_df['id'].unique().tolist())]
        business_overdue_df = pd.merge(business_overdue_df,
                                       business_loan_base_df[['id', 'repay_amount', 'one_third_amt', 'avg_loan_amount']],
                                       left_on='record_id', right_on='id')
        # 消费性贷款逾期信息
        consume_overdue_df = repayment_df[
            repayment_df['record_id'].isin(consume_loan_base_df['id'].unique().tolist())]
        # 贷记卡逾期信息
        credit_overdue_df = repayment_df[
            (repayment_df['record_id'].isin(credit_card_df['id'].unique().tolist()))]
        # 准贷记卡逾期信息
        semi_credit_overdue_df = repayment_df[
            (repayment_df['record_id'].isin(quasi_credit_loan_df['id'].unique().tolist()))]
        # 最新月度信息
        # loan_lately_df = self.lately_df[
        #     self.lately_df['ACCTNO'].isin(loan_df['ACCTNO'].unique().tolist())]
        # loan_lately_df = pd.merge(loan_lately_df, loan_df[['ACCTNO', 'LOANAMT']], on='ACCTNO')
        # 近3年还款方式为分期偿还的经营性贷款连续逾期期数
        temp_df = business_overdue_df[(business_overdue_df['repay_amount'] > business_overdue_df['avg_loan_amount']) &
                                      (business_overdue_df['repay_amount'] < business_overdue_df['one_third_amt']) &
                                      (business_overdue_df['status'].astype(str).str.isdigit())]
        if not temp_df.empty:
            self.variables['business_loan_average_3year_overdue_cnt'] = temp_df['status'].astype(int).max()
        # 近3年经营性贷款本金逾期次数
        self.variables['rhzx_business_loan_3year_overdue_cnt'] = business_overdue_df[
            (business_overdue_df['ym'] >= format(report_time - DateOffset(months=36), '%Y-%m')) &
            (business_overdue_df['one_third_amt'] > 0) & (business_overdue_df['repayment_amt'] > business_overdue_df['one_third_amt'])].shape[0]
        # 经营性贷款到期未归还金额
        self.variables['settled_business_loan_overdue_amount'] = business_loan_base_df[
            (business_loan_base_df['loan_end_date'] < report_time)]['loan_balance'].sum()
        # 单笔贷款近3年内出现连续逾期最大期数
        temp_df = loan_overdue_df[(loan_overdue_df['ym'] >= format(report_time - DateOffset(months=36), '%Y-%m')) &
                                  (loan_overdue_df['status'].astype(str).str.isdigit())]
        if not temp_df.empty:
            self.variables['single_loan_3year_overdue_max_month'] = temp_df['status'].astype(int).max()
        # 单笔贷款近2年内最大逾期总次数
        temp_df = loan_overdue_df[(loan_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
                                  (loan_overdue_df['repayment_amt'] > 0)].groupby('record_id').agg({'status': 'count'})
        if not temp_df.empty:
            self.variables['single_loan_overdue_2year_cnt'] = temp_df['status'].max()
        # 单笔经营性贷款近2年内最大逾期次数
        temp_df = business_overdue_df[
            (business_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (business_overdue_df['status'].astype(str).str.isdigit()) &
            (business_overdue_df['repayment_amt'] > 0)].groupby('record_id').agg({'status': 'count'})
        if not temp_df.empty:
            self.variables['single_bus_loan_overdue_2year_cnt'] = temp_df['status'].max()
        # 单笔消费性贷款近2年内最大逾期次数
        temp_df = consume_overdue_df[
            (consume_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (consume_overdue_df['status'].astype(str).str.isdigit()) &
            (consume_overdue_df['repayment_amt'] > 0)].groupby('record_id').agg({'status': 'count'})
        if not temp_df.empty:
            self.variables['single_consume_loan_overdue_2year_cnt'] = temp_df['status'].max()
        # 总计贷款2年内逾期次数
        self.variables['loan_overdue_2year_total_cnt'] = loan_overdue_df[
            (loan_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (loan_overdue_df['repayment_amt'] > 0) &
            (loan_overdue_df['status'].astype(str).str.isdigit())].shape[0]
        # 经营性贷款2年内最大连续逾期期数
        temp_df = business_overdue_df[
            (business_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (business_overdue_df['status'].astype(str).str.isdigit())]
        if not temp_df.empty:
            self.variables['large_loan_2year_overdue_cnt'] = temp_df['status'].astype(int).max()
        # 疑似压贷笔数
        self.variables['loan_doubtful'] = self._loan_doubtful(loan_account_type_df, report_time)
        # 3年前经营性贷款本金逾期笔数
        self.variables['rhzx_business_loan_3year_ago_overdue_cnt'] = business_overdue_df[
            (business_overdue_df['ym'] < format(report_time - DateOffset(months=36), '%Y-%m')) &
            (business_overdue_df['one_third_amt'] > 0) &
            (business_overdue_df['repayment_amt'] > business_overdue_df['one_third_amt'])]['record_id'].nunique()
        # 3年前还款方式为分期偿还的经营性贷款最大连续逾期期数
        temp_df = business_overdue_df[
            (business_overdue_df['ym'] < format(report_time - DateOffset(months=36), '%Y-%m')) &
            (business_overdue_df['repay_amount'] > business_overdue_df['avg_loan_amount']) &
            (business_overdue_df['repay_amount'] < business_overdue_df['one_third_amt']) &
            (business_overdue_df['status'].astype(str).str.isdigit())]
        if not temp_df.empty:
            self.variables['business_loan_average_3year_ago_overdue_cnt'] = temp_df['status'].astype(int).max()
        # 单笔贷款两年内最大连续逾期期数
        temp_df = loan_overdue_df[
            (loan_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (loan_overdue_df['status'].astype(str).str.isdigit())]
        if not temp_df.empty:
            self.variables['single_loan_max_overdue_month'] = temp_df['status'].astype(int).max()
        # 贷记卡当前逾期金额
        semi_last_credit_overdue_df = \
            semi_credit_overdue_df.sort_values(by='ym').drop_duplicates('record_id', keep='last')
        semi_last_credit_overdue_df = semi_last_credit_overdue_df.loc[
            semi_last_credit_overdue_df['status'].astype(str).str.isdigit()]
        self.variables['credit_now_overdue_money'] = credit_card_df['overdue_amount'].sum() + semi_last_credit_overdue_df[
            (semi_last_credit_overdue_df['repayment_amt'] > 0) & (semi_last_credit_overdue_df['status'].astype(int) > 2)]['repayment_amt'].sum()
        # 贷记卡当前严重逾期金额
        self.variables['credit_now_overdue_1k_money'] = \
            credit_card_df[(credit_card_df['amout_replay_amount'] < credit_card_df['overdue_amount']) &
                           (credit_card_df['overdue_amount'] > 1000)]['overdue_amount'].sum() + \
            semi_last_credit_overdue_df[(semi_last_credit_overdue_df['repayment_amt'] > 1000) &
                                        (semi_last_credit_overdue_df['status'].astype(int) > 2)]['repayment_amt'].sum()
        # 单笔贷记卡近2年内总逾期次数
        single_credit_card_overdue_2y = credit_overdue_df[
            (credit_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (credit_overdue_df['status'].astype(str).str.isdigit())
            ].groupby('record_id').agg({'status': 'count'})
        if single_credit_card_overdue_2y.shape[0]:
            self.variables['single_credit_overdue_2year_cnt'] = single_credit_card_overdue_2y['status'].max()
        # 单笔贷记卡近3年内出现连续逾期最大期数
        single_credit_card_overdue_3y = credit_overdue_df[
            (credit_overdue_df['ym'] >= format(report_time - DateOffset(months=36), '%Y-%m')) &
            (credit_overdue_df['status'].astype(str).str.isdigit())]
        if single_credit_card_overdue_3y.shape[0] > 0:
            self.variables['single_credit_3year_overdue_max_month'] = single_credit_card_overdue_3y['status'].astype(int).max()
        # 贷记卡账户出现过“呆账”
        self.variables['credit_status_bad_cnt'] = credit_overdue_df[credit_overdue_df['status'] == 'B']['record_id'].nunique()
        # 总计贷记卡2年内逾期次数
        self.variables['credit_overdue_2year_total_cnt'] = credit_overdue_df[
            (credit_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (credit_overdue_df['status'].astype(str).str.isdigit()) &
            (credit_overdue_df['repayment_amt'] > 0)].shape[0]
        # 准贷记卡最大逾期期数
        semi_credit_overdue = semi_credit_overdue_df[semi_credit_overdue_df['status'].astype(str).str.isdigit()]
        if semi_credit_overdue.shape[0] > 0:
            self.variables['single_semi_credit_card_3year_overdue_max_month'] = semi_credit_overdue['status'].astype(int).max()
        # 单张准贷记卡近2年内最大逾期次数
        single_semi_credit_overdue_2y = semi_credit_overdue_df[
            (semi_credit_overdue_df['ym'] >= format(report_time - DateOffset(months=24), '%Y-%m')) &
            (semi_credit_overdue_df['status'].astype(str).str.isdigit()) &
            (semi_credit_overdue_df['status'].astype(str) > '2')].groupby('record_id').agg({'status': 'count'})
        if single_semi_credit_overdue_2y.shape[0] > 0:
            self.variables['single_semi_credit_overdue_2year_cnt'] = single_semi_credit_overdue_2y['status'].max()

        # 个人征信负债余额
        pcredit_info = self.pcredit_info()
        pcredit_large = self.pcredit_large()
        self.variables['per_debt_amt'] = loan_account_type_df['loan_balance'].sum() + pcredit_info['undestory_used_limit'].sum() + pcredit_info[
            'undestory_semi_overdraft'].sum() + pcredit_large[pcredit_large['end_date'] > report_time]['usedsum'].sum()
        # 贷款及其他账户总数
        pcredit_biz = self.pcredit_biz()
        self.variables['all_loan_account_cnt'] = pcredit_biz[pcredit_biz['biz_type'].isin(['01', '02', '03'])]['biz_counts'].sum()
        # 信用卡当前已用额度
        self.variables['used_creditcard_balance'] = pcredit_info['undestory_used_limit'].sum() + pcredit_info['undestory_semi_overdraft'].sum()

        # 申请人他行一年内到期信用类贷款余额
        timerange_downlimit = self.variables['report_time']
        timerange_uplimit = timerange_downlimit + DateOffset(years=1)
        loan_df = df.loc[df['account_type'].isin(['01', '02', '03'])]
        if loan_df.shape[0] > 0:
            loan_df['account_org_en'] = loan_df['account_org'].apply(lambda x: find_en(x))
            loan_df['end_date'] = pd.to_datetime(loan_df['end_date'])
            loan_df_1y = loan_df.loc[(loan_df['end_date'] <= timerange_uplimit) &
                                     (loan_df['end_date'] > timerange_downlimit) &
                                     (loan_df['loan_guarantee_type'] == '4') &
                                     (~loan_df['account_status'].isin(['04']))]
            if loan_df_1y.shape[0] > 0:
                self.variables['credit_loan_total_balance_1year'] = loan_df_1y.loan_balance.sum()

        # loan_credit_query_1month_orgcnt_others  征信近1个月他行‘贷款审批’+‘信用卡审批’查询机构数
        # loan_credit_query_3month_orgcnt_others  征信近3个月他行‘贷款审批’+‘信用卡审批’查询机构数
        # loan_credit_query_1month_cnt_others 征信近1个月他行‘贷款审批’+‘信用卡审批’查询次数
        # loan_credit_query_3month_cnt_others 征信近3个月他行‘贷款审批’+‘信用卡审批’查询次数
        # loan_credit_query_1month_cnt_own    征信近1个月本行‘贷款审批’+‘信用卡审批’查询次数
        # loan_credit_query_3month_cnt_own    征信近3个月本行‘贷款审批’+‘信用卡审批’查询次数
        target_time_1m = self.variables['report_time'] - DateOffset(months=1)
        target_time_3m = self.variables['report_time'] - DateOffset(months=3)
        query_record_df_en = query_record_df.copy()
        query_record_df_en['operator_en'] = query_record_df_en['operator'].apply(lambda x: find_en(x))
        query_record_1m = query_record_df.loc[(pd.to_datetime(query_record_df['jhi_time']) > target_time_1m) &
                                              (query_record_df.reason.isin(['2', '3', '02', '03']))]
        query_record_3m = query_record_df.loc[(pd.to_datetime(query_record_df['jhi_time']) > target_time_3m) &
                                              (query_record_df.reason.isin(['2', '3', '02', '03']))]
        self.variables['loan_credit_query_1month_orgcnt_others'] = \
            query_record_1m.loc[query_record_1m['operator_en'].str.len() <= 2]['operator_en'].nunique()
        self.variables['loan_credit_query_3month_orgcnt_others'] = \
            query_record_3m.loc[query_record_3m['operator_en'].str.len() <= 2]['operator_en'].nunique()

        self.variables['loan_credit_query_1month_cnt_others'] = query_record_1m.loc[query_record_1m['operator_en'].str.len() <= 2].shape[0]
        self.variables['loan_credit_query_3month_cnt_others'] = query_record_3m.loc[query_record_3m['operator_en'].str.len() <= 2].shape[0]
        self.variables['loan_credit_query_1month_cnt_own'] = query_record_1m.loc[query_record_1m['operator_en'].str.len() > 2].shape[0]
        self.variables['loan_credit_query_3month_cnt_own'] = query_record_3m.loc[query_record_3m['operator_en'].str.len() > 2].shape[0]

    @staticmethod
    # 疑似压贷笔数
    def _loan_doubtful(loan_df, report_time):
        loan_df = loan_df[(loan_df['account_type'].isin(['01', '02', '03'])) &
                          (((loan_df['loan_type'].isin(['01', '07', '99', '15', '16', '22'])) |
                            (loan_df['loan_type'].str.contains('融资租赁')) |
                            ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] >= 200000))) |
                           (loan_df['loan_guarantee_type'] == '3'))]
        loan_df = loan_df.filter(items=["account_org", "loan_date", "loan_amount", "loan_status_time"])
        loan_df = loan_df[pd.notna(loan_df["loan_date"])]
        loan_df["year"] = loan_df["loan_date"].transform(lambda x: x.year)
        loan_df["month"] = loan_df["loan_date"].transform(lambda x: x.month)
        loan_df["account_org"] = loan_df["account_org"].transform(lambda x: x.replace('"', ""))

        final_count = 0
        df = loan_df.groupby(["account_org", "year", "month"])["month"].count().reset_index(name="count")
        ignore_org_list = df.query('count >= 3')["account_org"].unique()

        all_org_list = list(loan_df.loc[:, "account_org"].unique())
        # 判断是否机构为空
        if len(all_org_list) == 0:
            return 0
        loan_doubtful_org = []

        for org_name in all_org_list:
            if org_name in ignore_org_list:
                continue
            express = 'account_org == "' + org_name + \
                      '" and (year > ' + str(report_time.year - 5) \
                      + ' or (year == ' + str(report_time.year - 5) \
                      + ' and month >= ' + str(report_time.month) + '))'

            item_df = loan_df.query(express)
            if item_df.shape[0] < 3:
                continue

            item_df = item_df.sort_values(by=["year", "month"], ascending=False)
            first_amt = item_df.iloc[0].loan_amount

            first_loan_date = pd.to_datetime(item_df.iloc[0].loan_date)
            second_amt = item_df.iloc[1].loan_amount
            second_loan_status_time = pd.to_datetime(item_df.iloc[1].loan_status_time)
            # 次新一笔账户关闭日期为空或者次新一笔账户关闭日期>最新一笔放款时间
            if pd.isna(second_loan_status_time) or (second_loan_status_time > pd.to_datetime(first_loan_date)):
                continue
            date_interval = (first_loan_date.year - second_loan_status_time.year) * 12 + first_loan_date.month - second_loan_status_time.month
            if first_amt and second_amt:
                ratio = first_amt / second_amt
                if ratio < 0.8 and date_interval <= 12:
                    temp_name = re.sub('"', '', org_name)
                    loan_doubtful_org.append(temp_name)
                    final_count = final_count + 1
        return final_count
