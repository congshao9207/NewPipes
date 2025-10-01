# from util.mysql_reader import sql_to_df
import pandas as pd
from pandas.tseries import offsets
from credit_speculate.interest_speculate import InterestSpeculate
from credit_speculate.repay_project_speculate import RepayProject
from creditreport.tables import transform_class_str
import datetime


class CreditMain:

    def __init__(self, report_id, loan_df):
        self.report_id = report_id
        self.loan_df = loan_df

    @staticmethod
    def _get_value(row, col, default_val=None):
        res = getattr(row, col) if hasattr(row, col) and pd.notna(getattr(row, col)) else default_val
        return res

    def processor(self):
        loan_df = self.loan_df[(self.loan_df['account_type'].isin(['01', '02', '03'])) &
                               (self.loan_df['account_status'].isin(['01', '04', '正常', '结清']))]
        acc_df = pd.DataFrame()
        pro_df = pd.DataFrame()
        for row in loan_df.itertuples():
            record_id = self._get_value(row, 'id')
            acc_status = self._get_value(row, 'account_status')  # 账户状态
            amt = self._get_value(row, 'loan_amount', 0)  # 借款金额
            # 开立日期
            start_date = self._get_value(row, 'loan_date')
            start_date = pd.to_datetime(start_date) if pd.notna(start_date) else None
            # 到期日期
            end_date1 = self._get_value(row, 'end_date')
            end_date2 = self._get_value(row, 'loan_status_time')
            end_date = end_date1 if pd.notna(end_date1) else end_date2
            # 机构类型，1表示银行，0表示非银行
            org_type = 1 if '银行' in str(self._get_value(row, 'account_org')) else 0
            # 若开立日期或者到期日期为空或者借款金额为0，则不进行任何推算
            if pd.isna(start_date) or pd.isna(end_date) or amt == 0:
                continue
            end_date = pd.to_datetime(end_date)
            # 若账户状态为“结清”则直接到还款计划推算阶段，不进行利率推算
            if acc_status in ['04', '结清']:
                rep_spe = RepayProject({'start_date': start_date, 'end_date': end_date, 'amt': amt, 'is_end': 1,
                                        'record_id': record_id})
                rep_spe.main()
                pro_df = pd.concat([pro_df, rep_spe.df], axis=0, ignore_index=True)
                continue
            # 获取开立日期和到期日期之间的月份数
            month_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) \
                if pd.notna(start_date) and pd.notna(end_date) else 0
            # 若贷款期限超过360个月，不进行利率推算及还款计划推算
            if month_diff > 360:
                continue
            balance = self._get_value(row, 'loan_balance', 0)  # 余额
            freq = self._get_value(row, 'repay_frequency', '03')  # 还款频率
            freq = 1 if freq == '03' else 3 if freq == '04' else 2
            terms = self._get_value(row, 'repay_period', 0)  # 还款期数
            # 还款期数可能因为结息日期的原因多出来一期,因此用时间周期来判断
            if terms == 0:
                terms = (month_diff + freq - 1) // freq if freq in [1, 3] else month_diff
            else:
                if freq in [1, 3] and terms > (month_diff + freq - 1) // freq:
                    terms = (month_diff + freq - 1) // freq
            # 本月应还款,如果余额为0或者本月应还款为0就获取本月实还款
            should_amt = self._get_value(row, 'repay_amount', 0)
            real_amt = self._get_value(row, 'amout_replay_amount', -1)
            should_amt = real_amt if balance == 0 or should_amt == 0 or pd.isna(should_amt) else should_amt

            # 应还款日
            should_date = self._get_value(row, 'plan_repay_date')
            should_date = pd.to_datetime(should_date) if pd.notna(should_date) else None
            # 上月天数
            month_days = (should_date - (should_date - offsets.DateOffset(months=1))).days \
                if pd.notna(should_date) else 0
            # 上季度天数
            quar_days = (should_date - (should_date - offsets.DateOffset(months=3))).days \
                if pd.notna(should_date) else 0
            # 剩余还款期数
            res_terms = self._get_value(row, 'surplus_repay_period', 0)
            # 剩余月份数
            res_diff = (end_date.year - should_date.year) * 12 + (end_date.month - should_date.month) \
                if pd.notna(end_date) and pd.notna(should_date) else 0
            if res_terms == 0:
                res_terms = (res_diff + freq - 1) // freq if freq in [1, 3] else res_diff
            else:
                if freq in [1, 3] and res_terms > (res_diff + freq - 1) // freq:
                    res_terms = (res_diff + freq - 1) // freq

            # 实还款日
            real_date = self._get_value(row, 'lately_replay_date')
            real_date = pd.to_datetime(real_date) if pd.notna(real_date) else None
            # 实还款日距离开立日期天数
            real_days = (real_date - start_date).days if pd.notna(start_date) and pd.notna(real_date) else 0
            # 上月计息天数
            month_period = month_days if balance != 0 and balance != amt else max(min(real_days, month_days), 0)
            # 上季度计息天数
            quar_period = quar_days if balance != 0 and balance != amt else max(min(real_days, quar_days), 0)
            spec_param = {
                'record_id': record_id,
                'org_type': org_type,
                'amt': amt,
                'should_amt': should_amt,
                'balance': balance,
                'freq': freq,
                'terms': int(terms),
                'real_amt': real_amt,
                'res_terms': int(res_terms),
                'month_period': month_period,
                'quar_period': quar_period,
                'start_date': start_date,
                'end_date': end_date,
                'should_date': should_date
            }
            int_spe = InterestSpeculate(spec_param)
            int_spe.main()
            acc_df = acc_df.append(int_spe.params, ignore_index=True)
            rep_spe = RepayProject(int_spe.params)
            rep_spe.main()
            pro_df = pd.concat([pro_df, rep_spe.df], axis=0, ignore_index=True)
        if pro_df.shape[0] > 0:
            pro_df['report_id'] = self.report_id
            pro_df['repay_month'] = pro_df['repay_month'].apply(lambda x: format(x, '%Y-%m'))
            pro_df['create_time'] = format(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            role_list = pro_df.to_dict('records')
            transform_class_str(role_list, 'PcreditAccSpeculate')
