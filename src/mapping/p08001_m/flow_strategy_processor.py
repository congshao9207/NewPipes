from mapping.trans_module_processor import TransModuleProcessor
from pandas.tseries import offsets
import datetime
import pandas as pd


class FlowStrategyProcessor(TransModuleProcessor):

    def process(self):
        self.from_flow_portrait()
        self.from_portrait()
        self.from_loan_portrait()
        self.from_summary_portrait()

    def from_flow_portrait(self):
        ori_df = self.trans_u_flow_portrait

        self.variables["pawn_cnt"] = ori_df['unusual_trans_type'].str.contains("典当").fillna(False).sum()
        self.variables['medical_cnt'] = ori_df['unusual_trans_type'].str.contains("医疗").fillna(False).sum()
        self.variables['insure_cnt'] = ori_df['unusual_trans_type'].str.contains("保险理赔").fillna(False).sum()
        self.variables['fam_unstable_cnt'] = ori_df['unusual_trans_type'].str.contains("家庭不稳定").fillna(False).sum()

        self.variables["income_mean"] = round(ori_df[ori_df.trans_amt > 0].trans_amt.mean(), 4)
        income_mean = round(ori_df[ori_df.trans_amt > 0].trans_amt.mean(), 4)
        income_std = round(ori_df[ori_df.trans_amt > 0].trans_amt.std(), 4)
        self.variables['income_mean_sigma_2right'] = round(income_mean + 2 * income_std, 4)
        self.variables["income_std"] = round(ori_df[ori_df.trans_amt > 0].trans_amt.std(), 4)
        self.variables["expend_std"] = round(ori_df[ori_df.trans_amt < 0].trans_amt.std(), 4)
        self.variables["daily_max_income_mean"] = round(
            ori_df[ori_df.trans_amt > 0].groupby(by='trans_date')['trans_amt'].agg(
                'max').mean(), 4)
        self.variables['pty_company_income_amt'] = round(ori_df[(ori_df.trans_amt > 0)
                                                                & (
                                                                            ori_df.relationship.isin(["U_PER_SH_H_COMPANY","U_PER_SH_M_COMPANY","U_PER_SH_L_COMPANY"]))].trans_amt.sum(),
                                                         4)

        try:
            self.variables['pty_company_expense_cnt_prop'] = round(ori_df[(ori_df.trans_amt < 0)
                                                                          & (
                                                                                      ori_df.relationship.isin(["U_PER_SH_H_COMPANY","U_PER_SH_M_COMPANY","U_PER_SH_L_COMPANY"]))].shape[
                                                                       0] / \
                                                                   ori_df[(ori_df.trans_amt < 0)].shape[0], 4)
        except:
            self.variables['pty_company_expense_cnt_prop'] = 0
        self.daily_bal_mean(ori_df)

    def from_summary_portrait(self):
        portrait = self.trans_u_summary_portrait
        if portrait.empty:
            return

        half_year_portrait = portrait[portrait.month == 'half_year']
        if not half_year_portrait.empty:
            self.variables['half_year_bal_d_mean'] = half_year_portrait['balance_amt'].values[0]
            self.variables['inter_bal_prop_half_year'] = half_year_portrait['interest_balance_proportion'].values[0]
        year_portrait = portrait[portrait.month == 'year']
        if not year_portrait.empty:
            self.variables['year_inter_d_mean'] = year_portrait['interest_amt'].values[0]



    def from_portrait(self):
        portrait = self.trans_u_portrait
        if portrait.empty:
            return

        portrait['income_cnt_sum'] = portrait['income_0_to_5_cnt'] + \
                                     portrait['income_5_to_10_cnt'] + \
                                     portrait['income_10_to_30_cnt'] + \
                                     portrait['income_30_to_50_cnt'] + \
                                     portrait['income_50_to_100_cnt'] + \
                                     portrait['income_100_to_200_cnt'] + \
                                     portrait['income_above_200_cnt']

        income_0_to_5_cnt = portrait.loc[0, 'income_0_to_5_cnt']
        income_cnt_sum = portrait.loc[0, 'income_cnt_sum']
        try:
            self.variables['income_0_5_prop'] = round(income_0_to_5_cnt / income_cnt_sum, 4)
        except:
            self.variables['income_0_5_prop'] = 0
        self.variables['normal_income_mean'] = portrait.loc[0, 'normal_income_mean']
        self.variables['normal_income_amt_d_mean'] = portrait.loc[0, 'normal_income_d_mean']
        self.variables['balance_max_weight'] = portrait.loc[0, 'balance_weight_max']
        self.variables['income_max_weight'] = portrait.loc[0, 'income_weight_max']

        self.variables['daily_max_income_max'] = portrait.loc[0, 'income_weight_max']

    def from_loan_portrait(self):
        portrait = self.trans_u_loan_portrait

        time_dict = {
            '_r3m': '近3个月',
            '_r6m': '近6个月',
            '_r12m': '近12个月',
            '_his': '历史可查'
        }

        try:
            self.variables['consume_repay_cnt_r3m'] = portrait[(portrait.loan_type == '消金')
                                                               & (portrait.month == '近3个月')]['repay_cnt'].values[0]
        except:
            self.variables['consume_repay_cnt_r3m'] = -999
        try:
            self.variables['consume_repay_cnt_r6m'] = portrait[(portrait.loan_type == '消金')
                                                               & (portrait.month == '近6个月')]['repay_cnt'].values[0]
        except:
            self.variables['consume_repay_cnt_r6m'] = -999

        try:
            self.variables['third_loan_cnt_mean'] = portrait[(portrait.loan_type == '第三方支付')
                                                             & (~portrait.month.isin(list(time_dict.values())))][
                'loan_cnt'].mean()
        except:
            self.variables['third_loan_cnt_mean'] = -999

        try:
            self.variables['third_repay_cnt_mean'] = portrait[(portrait.loan_type == '第三方支付')
                                                              & (~portrait.month.isin(list(time_dict.values())))][
                'repay_cnt'].mean()
        except:
            self.variables['third_repay_cnt_mean'] = -999

        try:
            self.variables['unbank_repay_type_cnt_r3m'] = portrait[(portrait.month == '近3个月')
                                                                   & (portrait.loan_type != '银行')].loan_type.nunique()
        except:
            self.variables['unbank_repay_type_cnt_r3m'] = -999

        self.variables['private_loan_mean_his'] = portrait[(portrait.loan_type == '民间借贷') &
                                                           (portrait.month == '历史可查')].loan_amt.sum()
        self.variables['consume_loan_cnt_r6m'] = portrait[(portrait.loan_type == '消金') &
                                                          (portrait.month == '近6个月')].loan_cnt.sum()
        self.variables['bank_repay_cnt_r6m'] = portrait[(portrait.loan_type == '银行') &
                                                        (portrait.month == '近6个月')].repay_cnt.sum()

    def daily_bal_mean(self, df):
        account_id_list = df['account_id'].unique().tolist()
        end_date = df['trans_date'].max()
        balance_amt = 0
        for account_id in account_id_list:
            flow_df = df[df['account_id'] == account_id]
            if end_date not in flow_df['trans_date'].tolist():
                continue
            start_date = (pd.to_datetime(end_date) - offsets.DateOffset(months=12)).date()
            balance_df = flow_df[(flow_df.trans_date >= start_date) & (flow_df.trans_date <= end_date)]
            if balance_df.shape[0] == 0:
                continue
            balance_df.sort_values(by=['trans_date', 'flow_id'], ascending=True, inplace=True)
            balance_df['str_date'] = balance_df['trans_date'].apply(lambda x:
                                                                    x.date if type(x) == datetime.datetime else x)
            balance_df.drop_duplicates(subset='str_date', keep='last', inplace=True)
            str_date = balance_df['trans_date'].to_list()
            trans_amt = balance_df['account_balance'].to_list()
            length = len(str_date)
            diff_days = [(str_date[i + 1] - str_date[i]).days for i in range(length - 1)]
            diff_days.append(1)
            total_days = sum(diff_days)
            total_amt = [(diff_days[i] * trans_amt[i] if pd.notna(diff_days[i]) and pd.notna(trans_amt[i]) else 0)
                         for i in range(length)]
            balance_amt += sum(total_amt) / total_days if total_days != 0 else 0
        self.variables['daily_bal_mean'] = balance_amt
