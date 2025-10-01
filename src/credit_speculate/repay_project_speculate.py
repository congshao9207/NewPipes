import pandas as pd


class RepayProject:

    def __init__(self, input_variable):
        self.variables = input_variable
        self.role_list = []
        self.df = pd.DataFrame()

    def main(self):
        amt = self.variables.get('amt', None)
        balance = self.variables.get('balance', None)
        should_amt = self.variables.get('should_amt', None)
        rate = self.variables.get('real_rate', 0.1) if pd.notna(self.variables.get('real_rate')) else 0.1
        should_date = self.variables.get('should_date', None)
        start_date = self.variables.get('start_date', None)
        end_date = self.variables.get('end_date', None)
        repay_period = self.variables.get('repay_period', None)
        period_amt = self.variables.get('period_amt', None)
        repay_type = self.variables.get('repay_type', None)

        quar_map = {}
        # 还款频率为按季贷款，结息周期也为按季
        if repay_type == 'XB_QUARTER':
            quar_range = (pd.date_range(start_date, end_date, freq='QS-MAR') +
                          pd.offsets.DateOffset(day=start_date.day)).tolist() + [end_date]
            quar_last = [start_date] + quar_range[:-1]
            quar_map = {v: quar_last[i] for i, v in enumerate(quar_range)}
        date_range = pd.date_range(start_date, max(end_date, start_date + pd.offsets.DateOffset(months=1)), freq='m') \
            + pd.offsets.DateOffset(months=1, day=start_date.day)
        last_range = pd.date_range(start_date, max(end_date, start_date + pd.offsets.DateOffset(months=1)), freq='m') \
            + pd.offsets.DateOffset(day=start_date.day)
        self.df['repay_month'] = date_range
        self.df['last_month'] = self.df['repay_month'].apply(lambda x: x if x not in quar_map else quar_map[x]) \
            if repay_type == 'XB_QUARTER' else last_range
        length = self.df.shape[0] - 1
        self.df.loc[length, 'repay_month'] = end_date
        self.df['interest_days'] = (self.df['repay_month'] - self.df['last_month']).apply(lambda x: x.days)
        self.df['repay_cnt'] = self.df.index + 1
        self.df['account_status'] = 1
        self.df['settled'] = self.variables.get('is_end', None)
        self.df['record_id'] = self.variables.get('record_id', None)
        self.df['loan_repay_type'] = repay_type
        self.df['nominal_interest_rate'] = self.variables.get('nomi_rate', None)
        self.df['real_interest_rate'] = rate
        # 无法推算或者结清贷款均按照利率为10%的先息后本及等额本息方式进行推算
        if repay_type == '无法推算' or pd.isna(repay_type) or pd.isna(start_date) or pd.isna(should_date):
            temp_df = self.df.copy()
            temp_df['repay_amount'] = amt * rate / 12 / (1 - 1 / (1 + rate / 12) ** temp_df.shape[0])
            temp_df['repay_principal'] = temp_df.apply(
                lambda x: x['repay_amount'] / (1 + rate / 12) ** (length + 2 - x['repay_cnt']), axis=1)
            temp_df['loan_balance'] = amt - temp_df['repay_principal'].cumsum()
            temp_df.loc[length, 'loan_balance'] = 0
            temp_df['loan_repay_type'] = 'D_INTEREST'
            self.df['repay_amount'] = amt * rate / 360 * self.df['interest_days']
            self.df.loc[length, 'repay_amount'] = self.df.loc[self.df.shape[0] - 1, 'repay_amount'] + amt
            self.df['loan_balance'] = amt
            self.df.loc[length, 'loan_balance'] = 0
            self.df['loan_repay_type'] = 'XB_MONTH'
            self.df = pd.concat([self.df, temp_df], axis=0, ignore_index=True)
            self.df['account_status'] = 0
            return
        if repay_type in ['XB_MONTH', 'XB_QUARTER']:
            self.df['repay_amount'] = amt * rate / 360 * self.df['interest_days']
            self.df.loc[length, 'repay_amount'] = self.df.loc[self.df.shape[0] - 1, 'repay_amount'] + amt
            self.df['loan_balance'] = amt
        elif repay_type in ['D_INTEREST']:
            repay_month_cnt = (should_date.year - start_date.year) * 12 + should_date.month - start_date.month - 1
            res_month_cnt = length - repay_month_cnt + 1
            should_amt /= (1 + rate / 12) ** res_month_cnt
            repay_amt1 = (amt - balance - should_amt) / (1 - 1 / (1 + rate / 12) ** repay_month_cnt) * \
                (1 - 1 / (1 + rate / 12)) * ((1 + rate / 12) ** (res_month_cnt + 1)) \
                if rate != 0 and repay_month_cnt != 0 else (amt - balance - should_amt) / repay_month_cnt \
                if repay_month_cnt != 0 else 0
            repay_amt2 = (balance + should_amt) * rate / 12 / (1 - 1 / (1 + rate / 12) ** res_month_cnt) \
                if rate != 0 else (balance + should_amt) / res_month_cnt
            self.df.loc[self.df['repay_month'] <= should_date - pd.offsets.MonthBegin(), 'repay_amount'] = repay_amt1
            self.df.loc[self.df['repay_month'] > should_date - pd.offsets.MonthBegin(), 'repay_amount'] = repay_amt2
            self.df['repay_principal'] = self.df.apply(
                lambda x: x['repay_amount'] / (1 + rate / 12) ** (length + 2 - x['repay_cnt']), axis=1)
            self.df['loan_balance'] = amt - self.df['repay_principal'].cumsum()
        elif repay_type in ['D_INTEREST_PRINCIPAL']:
            repay_amt = amt * (rate / 12) / (1 - 1 / (1 + rate / 12) ** (length + 1))
            month_repay = amt / (length + 1)
            self.df['repay_amount'] = repay_amt
            self.df['loan_balance'] = amt - self.df['repay_cnt'] * month_repay
        elif repay_type in ['D_PRINCIPAL']:
            repay_months = length + 1
            month_repay = amt / repay_months
            self.df['repay_amount'] = month_repay * self.df['repay_cnt'] * rate * self.df['interest_days'] / 360 + \
                month_repay
            self.df['loan_balance'] = amt - self.df['repay_cnt'] * month_repay
        else:
            self.df['principal_month'] = self.df['repay_cnt'].apply(lambda x: 1 if x % repay_period == 0 else 0)
            self.df['res_amt'] = self.df['repay_cnt'].apply(
                lambda x: max(amt - (x - 1) // repay_period * period_amt, 0))
            self.df['repay_principal'] = self.df['res_amt'].apply(lambda x: min(x, period_amt))
            self.df.loc[self.df.shape[0] - 1, 'repay_principal'] = self.df.loc[self.df.shape[0] - 1, 'res_amt']
            self.df['repay_interest'] = self.df['interest_days'] * self.df['res_amt'] * rate / 360
            self.df['repay_amount'] = self.df['repay_principal'] + self.df['repay_interest']
            self.df['loan_balance'] = self.df['repay_cnt'].apply(
                lambda x: max(amt - x // repay_period * period_amt, 0))
        self.df.loc[length, 'loan_balance'] = 0
