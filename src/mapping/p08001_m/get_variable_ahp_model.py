import pandas as pd
import numpy as np
from pandas.tseries import offsets
from mapping.trans_module_processor import TransModuleProcessor


class GetVariableAhpModel(TransModuleProcessor):
    """
    20221010:
    获取AHP模型需要的各类指标
    1、因原始画像没有对外投资和分红标签，需清洗画像标签
    2、分经营状况、资产状况、经营稳定性和异常流水分析四类指标
    """

    def __init__(self):
        super().__init__()
        self.df = None
        self.df_summary = None

    def process(self):
        self.get_trans_flow_detail()
        if self.df is not None:
            self.cal_month()
            self.investment()
            self.dividends()
            self.operation_status()
            self.asset_status()
            self.operation_stability()
            self.unusual_analysis()

    def cal_month(self):
        if self.df is not None:
            max_year = self.df['trans_date'].max().year
            max_month = self.df['trans_date'].max().month
            max_day = self.df['trans_date'].max().day
            # 取距最后交易日的相差月份，数字为n及为n月内
            self.df['cal_month'] = self.df['trans_date'].apply(
                lambda x: (max_year - x.year) * 12 + max_month - x.month + (max_day - x.day) // 100 + 1)

    def investment(self):
        """对外投资标签"""
        concat_list = ['opponent_name', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        # 将字符串列合并到一起
        self.df['concat_str'] = self.df.apply(lambda x: ';'.join(x[concat_list]), axis=1)
        self.df['year_month'] = self.df['trans_date'].apply(lambda x: format(x, '%Y-%m'))
        investment_df = self.df.loc[
            (self.df.concat_str.str.contains('对外投资')) & (self.df.trans_amt < 0) & pd.isna(self.df.relationship)]
        investment_list = investment_df.index.tolist()
        self.df.loc[investment_list, 'investment'] = '对外投资'

    def dividends(self):
        """分红标签"""
        dividends_df = self.df.loc[
            (self.df.concat_str.str.contains('分红')) & (self.df.trans_amt > 0) & pd.isna(self.df.relationship)]
        dividends_list = dividends_df.index.tolist()
        self.df.loc[dividends_list, 'dividends'] = '分红'

    def operation_status(self):
        """
        经营状况
        :return:
        """
        if self.df is not None:
            df = self.df.loc[(self.df.cal_month <= 12) & pd.isna(self.df.loan_type)
                             & pd.isna(self.df.unusual_trans_type) & pd.isna(self.df.relationship)]
            # 月均经营性进账
            ahp_operation_income_amt = df.loc[df.trans_amt > 0].trans_amt.sum()
            ahp_operation_income_month = df.loc[df.trans_amt > 0]['year_month'].nunique()
            # 月均经营性进账
            self.variables['ahp_income_mean_m'] = \
                ahp_operation_income_amt / ahp_operation_income_month if ahp_operation_income_month != 0 else 0
            # 近一年贷款金额/经营性进账
            loan_df = self.df.loc[pd.notna(self.df.loan_type)]
            loan_amt = loan_df.loc[loan_df.trans_amt >= 0].trans_amt.sum()
            self.variables['ahp_loan_income_amt_proportion'] = \
                round(loan_amt / ahp_operation_income_amt, 4) if ahp_operation_income_amt != 0 else 0
            # 进出帐资金调动能力
            normal_income_data = df.loc[df.trans_amt >= 0]
            expense_data = df.loc[df.trans_amt < 0]

            if normal_income_data.empty:
                income_loanable_90 = None
                income_loanable_95 = None
            else:
                income_loanable_90 = np.nanpercentile(normal_income_data.trans_amt, 90, interpolation='linear')
                income_loanable_95 = np.nanpercentile(normal_income_data.trans_amt, 95, interpolation='linear')
            if expense_data.empty:
                expense_loanable_90 = None
                expense_loanable_95 = None
            else:
                expense_data.trans_amt = expense_data.trans_amt.abs()
                expense_loanable_90 = np.nanpercentile(expense_data.trans_amt, 90, interpolation='linear')
                expense_loanable_95 = np.nanpercentile(expense_data.trans_amt, 95, interpolation='linear')

            self.variables['ahp_income_loanable_90'] = income_loanable_90
            self.variables['ahp_income_loanable_95'] = income_loanable_95
            self.variables['ahp_expense_loanable_90'] = expense_loanable_90
            self.variables['ahp_expense_loanable_95'] = expense_loanable_95

            # 资金调动能力为进出帐二者取大
            if income_loanable_90 is not None and expense_loanable_90 is not None:
                self.variables['ahp_loanable'] = max(income_loanable_90, expense_loanable_90)
            elif income_loanable_90 is None and expense_loanable_90 is not None:
                self.variables['ahp_loanable'] = expense_loanable_90
            elif income_loanable_90 is not None and expense_loanable_90 is None:
                self.variables['ahp_loanable'] = income_loanable_90

            # 关联关系出账金额占比
            relationship_df = self.df.loc[pd.notna(self.df.relationship)]
            relationship_expense_amt = relationship_df.loc[relationship_df.trans_amt < 0].trans_amt.abs().sum()
            expense_amt = self.df.loc[self.df.trans_amt < 0].trans_amt.abs().sum()
            self.variables['ahp_relationship_expense_amt_proportion'] = \
                round(relationship_expense_amt / expense_amt, 4) if expense_amt != 0 else 0

            # 关联关系进账金额占比
            relationship_income_amt = relationship_df.loc[relationship_df.trans_amt > 0].trans_amt.sum()
            income_amt = self.df.loc[self.df.trans_amt > 0].trans_amt.sum()
            self.variables['ahp_relationship_income_amt_proportion'] = \
                round(relationship_income_amt / income_amt, 4) if income_amt != 0 else 0

    def asset_status(self):
        """
        资产状况
        :return:
        """
        if self.df is not None:
            # 结息日均和余额日均
            sum_df = self.df_summary[self.df_summary['month'].str.contains('year')]
            if sum_df.shape[0] > 0:
                mean_interest_12 = sum_df['interest_amt'].tolist()[-1]
                mean_balance_12 = sum_df['balance_amt'].tolist()[-1]
            else:
                mean_interest_12 = None
                mean_balance_12 = None
            if mean_interest_12 is None:
                if mean_balance_12 is not None:
                    mean_interest_12 = mean_balance_12
            if mean_interest_12 is not None:
                self.variables[f"ahp_mean_interest_12m"] = mean_interest_12

            # 其他投资净值 / 经营性进账
            df = self.df.loc[(self.df.cal_month <= 12) & pd.isna(self.df.loan_type)
                             & pd.isna(self.df.unusual_trans_type) & pd.isna(self.df.relationship)]
            # 经营性进账
            ahp_operation_income_amt = df.loc[df.trans_amt > 0].trans_amt.sum()
            others_df = self.df.loc[(self.df.unusual_trans_type.astype(str).str.contains('股票期货')) | (
                self.df.usual_trans_type.astype(str).str.contains('理财行为'))]
            if others_df.empty:
                others_net_income_amt = None
            else:
                others_net_income_amt = others_df.trans_amt.sum() if others_df.trans_amt.sum() >= 0 else 0
            if others_net_income_amt is not None:
                self.variables['ahp_others_investment_net_amt_proportion'] = \
                    round(others_net_income_amt / ahp_operation_income_amt, 4) if ahp_operation_income_amt != 0 else 0

            # 其他投资总额/经营性进账
            others_net_income_m = others_df.year_month.nunique()
            if others_net_income_m != 0:
                others_income_amt = others_df.loc[others_df.trans_amt >= 0].trans_amt.sum() / others_net_income_m
                self.variables['ahp_others_investment_amt_proportion'] = round(others_income_amt / self.variables[
                    'ahp_income_mean_m'], 4) if self.variables['ahp_income_mean_m'] > 0 else 0

            # 对外投资金额
            investment_df = self.df.loc[pd.notna(self.df.investment)]
            self.variables['ahp_investment_amt'] = investment_df.trans_amt.abs().sum()

            # 分红金额
            dividends_df = self.df.loc[pd.notna(self.df.dividends)]
            self.variables['ahp_dividends'] = dividends_df.trans_amt.sum()

    def operation_stability(self):
        """
        经营稳定性
        :return:
        """
        if self.df is not None:
            # 年经营性进账变化率
            """上一年流水比今年流水，取同样的交易周期"""
            df = self.df.loc[
                pd.isna(self.df.loan_type) & pd.isna(self.df.relationship) & pd.isna(self.df.unusual_trans_type)]
            per_operation_df = df.loc[(df.cal_month <= 24) & (df.cal_month > 12)]
            if not per_operation_df.empty:
                per_trans_date_min = pd.to_datetime(per_operation_df.trans_date.min().date())
                cur_trans_date_min = per_trans_date_min + offsets.DateOffset(months=12)
                cur_operation_df = self.df.loc[(self.df.trans_date >= cur_trans_date_min)]
                per_trans_income_amt = per_operation_df.loc[per_operation_df.trans_amt >= 0].trans_amt.sum()
                cur_trans_income_amt = cur_operation_df.loc[cur_operation_df.trans_amt >= 0].trans_amt.sum()
                income_amt_diff = cur_trans_income_amt - per_trans_income_amt
                self.variables['ahp_income_amt_diff_proportion'] = \
                    round(income_amt_diff / per_trans_income_amt, 4) if per_trans_income_amt != 0 else 1

                # 年经营性前十大交易对手变化率
                """不用考虑同一交易周期的对比，仅对比上一年交易时段和近一年"""
                per_income_df = per_operation_df.loc[per_operation_df.trans_amt >= 0]
                per_expense_df = per_operation_df.loc[per_operation_df.trans_amt < 0]
                cur_income_df = df.loc[(df.cal_month <= 12) & (df.trans_amt >= 0)]
                cur_expense_df = df.loc[(df.cal_month <= 12) & (df.trans_amt < 0)]
                per_temp_df1 = per_income_df.loc[pd.notna(per_income_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=False).reset_index(
                    drop=True)
                per_temp_df2 = per_expense_df.loc[pd.notna(per_expense_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=True).reset_index(
                    drop=True)
                cur_temp_df1 = cur_income_df.loc[pd.notna(cur_income_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=False).reset_index(
                    drop=True)
                cur_temp_df2 = cur_expense_df.loc[pd.notna(cur_expense_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=True).reset_index(
                    drop=True)
                # 上一年前10大交易对手
                per_income_amt_top_10 = per_temp_df1.loc[per_temp_df1.index < 10].opponent_name.tolist()
                per_expense_amt_top_10 = per_temp_df2.loc[per_temp_df2.index < 10].opponent_name.tolist()
                # 近一年前10大交易对手
                cur_income_amt_top_10 = cur_temp_df1.loc[cur_temp_df1.index < 10].opponent_name.tolist()
                cur_expense_amt_top_10 = cur_temp_df2.loc[cur_temp_df2.index < 10].opponent_name.tolist()

                income_set = list(set(per_income_amt_top_10).intersection(set(cur_income_amt_top_10)))
                expense_set = list(set(per_expense_amt_top_10).intersection(set(cur_expense_amt_top_10)))

                per_cnt = len(per_income_amt_top_10) + len(per_expense_amt_top_10)
                diff_cnt = len(income_set) + len(expense_set)
                self.variables['ahp_top_10_change_rate'] = diff_cnt / per_cnt

            # 大额进账月份占比
            df = self.df
            df = df.loc[(df.cal_month <= 12) & (df.trans_flow_src_type == 0) &
                        pd.isna(df.loan_type) & pd.isna(df.relationship) & pd.isna(df.unusual_trans_type)]
            base_amt = self.variables.get('ahp_mean_interest_12m')
            # 总进账笔数、总进账周期
            total_income_period = df.loc[df.trans_amt > 0].year_month.nunique()
            if base_amt is not None and base_amt != 0:
                large_income_period = df.loc[df.trans_amt > (base_amt * 2)].trans_date.dt.month.nunique()
                self.variables['ahp_large_income_period_rate'] = \
                    large_income_period / total_income_period if total_income_period != 0 else 0

            # 经营性进账前五大交易对手交易金额占比、经营性出账前五大交易对手交易金额占比
            countparty_df = self.df.copy()
            if countparty_df is not None:
                # 取近一年数据
                countparty_df = countparty_df.loc[countparty_df.cal_month <= 12]
                income_df = countparty_df.loc[countparty_df.trans_amt > 0]
                income_amt = income_df.trans_amt.sum()
                expense_df = countparty_df.loc[countparty_df.trans_amt < 0]
                expense_amt = expense_df.trans_amt.abs().sum()
                # 前五大交易对手特征
                # 进账、出账表
                temp_df = income_df.loc[pd.notna(income_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=False).reset_index(
                    drop=True)
                temp_df2 = expense_df.loc[pd.notna(expense_df.opponent_name)].groupby(
                    'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                               ascending=True).reset_index(
                    drop=True)
                # 前n大交易对手进账金额、出账金额
                income_amt_top_5 = temp_df.loc[temp_df.index < 5].trans_amt.sum()
                expense_amt_top_5 = temp_df2.loc[temp_df2.index < 5].trans_amt.abs().sum()
                self.variables[
                    "ahp_income_amt_rate_top5"] = income_amt_top_5 / income_amt if income_amt != 0 else 0
                self.variables[
                    "ahp_expense_amt_rate_top5"] = expense_amt_top_5 / expense_amt if expense_amt != 0 else 0

    def unusual_analysis(self):
        """
        流水异常分析
        :return:
        """
        if self.df is not None:
            total_trans_cnt = self.df.shape[0]
            unusual_trans_cnt = self.df.loc[pd.notna(self.df.unusual_trans_type)].shape[0]
            self.variables['ahp_unusual_trans_cnt_proportion'] = round(unusual_trans_cnt / total_trans_cnt, 4)

            # 近半年非银机构交易次数
            # 金额均取进账
            loan_df = self.df.loc[pd.notna(self.df.loan_type) & (self.df.cal_month <= 6)]
            bank_trans_amt = loan_df.loc[
                (loan_df.loan_type.str.contains('银行')) & (loan_df.trans_amt >= 0)].trans_amt.sum()
            not_bank_trans_amt = loan_df.loc[
                (~loan_df.loan_type.str.contains('银行')) & (loan_df.trans_amt >= 0)].trans_amt.sum()
            not_bank_trans_cnt = \
                loan_df.loc[(~loan_df.loan_type.str.contains('银行'))].shape[0]
            not_bank_trans_organizations_cnt = loan_df.loc[
                (~loan_df.loan_type.str.contains('银行'))].loan_type.nunique()
            self.variables['ahp_not_bank_trans_cnt'] = not_bank_trans_cnt
            self.variables['ahp_not_bank_trans_amt'] = not_bank_trans_amt
            self.variables['ahp_bank_trans_amt'] = bank_trans_amt
            self.variables['ahp_not_bank_trans_org_cnt'] = not_bank_trans_organizations_cnt

            # 极端大额进账金额占比、极端大额出账金额占比
            df = self.df.loc[(self.df.cal_month <= 12) & pd.isna(self.df.loan_type)
                             & pd.isna(self.df.unusual_trans_type) & pd.isna(self.df.relationship)]
            if not df.empty:
                ahp_income_loanable_95 = self.variables['ahp_income_loanable_95']
                ahp_expense_loanable_95 = self.variables['ahp_expense_loanable_95']
                income_amt = df.loc[df.trans_amt >= 0].trans_amt.sum()
                expense_amt = df.loc[df.trans_amt < 0].trans_amt.abs().sum()
                if pd.notna(ahp_income_loanable_95):
                    extreme_income_amt = df.loc[df.trans_amt > ahp_income_loanable_95].trans_amt.sum()
                    self.variables['ahp_extreme_income_amt_proportion'] = round(extreme_income_amt / income_amt, 4)
                if pd.notna(ahp_expense_loanable_95):
                    extreme_expense_amt = df.loc[df.trans_amt < (ahp_expense_loanable_95 * (-1))].trans_amt.abs().sum()
                    self.variables['ahp_extreme_expense_amt_proportion'] = round(extreme_expense_amt / expense_amt, 4)

    def get_trans_flow_detail(self):
        df = self.trans_u_flow_portrait.copy()
        df_summary = self.trans_u_summary_portrait.copy()
        if df.empty:
            return
        df.trans_date = pd.to_datetime(df.trans_date)
        self.df = df
        self.df_summary = df_summary
