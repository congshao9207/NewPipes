import pandas as pd
import numpy as np
from mapping.trans_module_processor import TransModuleProcessor
from util.mysql_reader import sql_to_df
from pandas.tseries.offsets import *
from fileparser.trans_flow.trans_config import IGNORE_ACC_NO, IGNORE_ACC_NO_PATTERN, IGNORE_OPPO_NAME_PATTERN
import re


def multi_mapping_score(val, cnt, cut_list1, cut_list2, score_list):
    if pd.isna(val):
        return score_list[0]
    for i in range(1, len(cut_list1)):
        if cut_list1[i - 1] < val <= cut_list1[i]:
            if cnt < cut_list2[i - 1]:
                return score_list[2 * i - 2]
            else:
                return score_list[2 * i - 1]
    return score_list[0]


class GetVariableRules(TransModuleProcessor):
    """
    20221011:
    获取各类规则指标
    """

    def __init__(self):
        super().__init__()
        self.df = None
        self.df_summary = None

    def process(self):
        self.get_trans_flow_detail()
        if self.df is not None:
            self.cal_month()
            # self._add_unusual_type_label()
            self.ahp_operation_data_feature()
            self.operation_data_feature()
            self.counterparty_feature()
            self.int_and_bal_executor_feature()
            self.loan_type_feature()
            self.unusual_trans_feature()
            self.confidence_feature()

    def cal_month(self):
        if self.df is not None:
            max_year = self.df['trans_date'].max().year
            max_month = self.df['trans_date'].max().month
            max_day = self.df['trans_date'].max().day
            # 取距最后交易日的相差月份，数字为n及为n月内
            self.df['cal_month'] = self.df['trans_date'].apply(
                lambda x: (max_year - x.year) * 12 + max_month - x.month + (max_day - x.day) // 100 + 1)

    def _add_unusual_type_label(self):
        """异常标签新增房产买卖和赡养抚养"""
        self.df['op_name'] = self.df.opponent_name
        concat_list = ['opponent_name', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        # 将字符串列合并到一起
        self.df['concat_str'] = self.df.apply(lambda x: ';'.join(x[concat_list]), axis=1)
        for row in self.df.itertuples():
            # 将合并列拉出来
            concat_str = getattr(row, 'concat_str')
            op_name = getattr(row, 'op_name')
            unusual_trans_type = getattr(row, 'unusual_trans_type')
            # 异常交易类型
            unusual_type = []
            # 房产买卖
            if re.search(r'买房|卖房|首付|售房|房地产|置业|购房|认筹', concat_str) and op_name != "":
                unusual_type.append("房产买卖")
            # 赡养抚养
            if re.search(r'抚养|赡养', concat_str) and op_name != "":
                unusual_type.append('赡养抚养')

            # 添加标签到df
            if len(unusual_type) > 0:
                unusual_type.append(unusual_trans_type) if pd.notna(unusual_trans_type) else unusual_type
                self.df.loc[row.Index, 'unusual_trans_type'] = ';'.join(unusual_type)

    def ahp_operation_data_feature(self):
        """
        进出帐资金调动能力
        :return:
        """
        if self.df is not None:
            df = self.df.loc[(self.df.cal_month <= 12) & pd.isna(self.df.loan_type)
                             & pd.isna(self.df.unusual_trans_type) & pd.isna(self.df.relationship) &
                             (self.df.trans_flow_src_type != 1)]
            # 进账资金调动能力
            normal_income_data = df.loc[df.trans_amt >= 0]
            if normal_income_data.empty:
                income_loanable = 0.
            else:
                income_loanable = np.nanpercentile(normal_income_data.trans_amt, 90, interpolation='linear')

            self.variables['ahp_income_loanable'] = income_loanable

    def operation_data_feature(self):
        """
        流水余额各金额区间占比、流水进账各金额区间占比、流水出账各金额区间占比，区间划分[0,1],(1,5]
        只传出流水进账0-1w区间占比
        :return:
        """
        df = self.df.copy()
        if df is not None:
            df = df.loc[df.cal_month <= 12]
            # 经营性数据进出帐
            operation_income_df = df.loc[
                (df.trans_amt > 0) & pd.isna(df.relationship) & pd.isna(df.loan_type) & pd.isna(df.unusual_trans_type)
                & (df.trans_flow_src_type != 1)]
            operation_income_cnt = operation_income_df.shape[0]
            income_cnt1 = operation_income_df.loc[
                (operation_income_df.trans_amt >= 0) & (operation_income_df.trans_amt <= 10000)].shape[0]
            self.variables[
                f"income_rate_0_to_1"] = income_cnt1 / operation_income_cnt if operation_income_cnt != 0 else 0

    def counterparty_feature(self):
        df = self.df.copy()
        if df is not None:
            # 取近一年数据
            df = df.loc[df.cal_month <= 12]
            income_df = df.loc[df.trans_amt > 0]
            income_amt = income_df.trans_amt.sum()
            # 前五大交易对手特征
            # 进账、出账表
            temp_df = income_df.loc[pd.notna(income_df.opponent_name)].groupby(
                'opponent_name').trans_amt.sum().reset_index().sort_values('trans_amt',
                                                                           ascending=False).reset_index(drop=True)
            # 前五大交易对手进账金额
            income_amt_top_5 = temp_df.loc[temp_df.index < 5].trans_amt.sum()
            # 前五大交易对手进账金额占比
            self.variables[f"top5_income_rate"] = income_amt_top_5 / income_amt if income_amt != 0 else 0

            # 关联交易进出账金额占比
            relation_income_df = df.loc[(df.trans_amt > 0) & pd.notna(df.relationship)]
            relation_income_amt = relation_income_df.trans_amt.sum()
            self.variables[
                f"relationship_income_rate"] = relation_income_amt / income_amt if income_amt != 0 else 0

    def int_and_bal_executor_feature(self):
        df = self.df.copy()
        if df is not None:
            sum_df = self.df_summary[self.df_summary['month'].str.contains('year')]
            if sum_df.shape[0] > 0:
                mean_interest_12 = sum_df['interest_amt'].tolist()[-1]
                mean_balance_12 = sum_df['balance_amt'].tolist()[-1]
            else:
                mean_interest_12 = None
                mean_balance_12 = None
            if mean_balance_12 is None:
                if mean_interest_12 is not None:
                    mean_balance_12 = mean_interest_12
            if mean_balance_12 is not None:
                self.variables[f"mean_balance_12m"] = mean_balance_12

    def loan_type_feature(self):
        if self.df is not None:
            df_12m = self.df.loc[self.df.cal_month <= 12]
            df_6m = df_12m.loc[df_12m.cal_month <= 6]
            df_3m = df_12m.loc[df_12m.cal_month <= 3]
            # 近12个月保理出账次数
            self.variables['factoring_expense_cnt_12m'] = \
                df_12m.loc[(df_12m['loan_type'] == '保理') & (df_12m.trans_amt < 0)].shape[0]
            # 近12个月融资租赁最小出账金额
            temp_df = df_12m.loc[(df_12m['loan_type'] == '融资租赁') & (df_12m.trans_amt < 0)]
            if temp_df.shape[0] > 0:
                self.variables['financial_leasing_min_expense_amt_12m'] = temp_df.trans_amt.abs().min()

            # 近6个月小贷出账次数、近6个月小贷进账次数、近6个月民间借贷出账次数
            self.variables['petty_loan_expense_cnt_6m'] = \
                df_6m.loc[(df_6m['loan_type'] == '小贷') & (df_6m.trans_amt < 0)].shape[0]
            self.variables['petty_loan_income_cnt_6m'] = \
                df_6m.loc[(df_6m['loan_type'] == '小贷') & (df_6m.trans_amt > 0)].shape[0]
            self.variables['private_lending_expense_cnt_6m'] = df_6m.loc[
                (df_6m['loan_type'] == '民间借贷') & (df_6m.trans_amt < 0)].shape[0]
            # 近6个月消金进账金额
            self.variables['consumption_income_amt_6m'] = df_6m.loc[
                (df_6m['loan_type'] == '消金') & (df_6m.trans_amt > 0)].trans_amt.sum()

            # 近3个月非银机构总进账次数
            not_bank_df_3m = df_3m.loc[pd.notna(df_3m.loan_type) & (~(df_3m.loan_type == '银行'))]
            self.variables['total_income_cnt_3m'] = not_bank_df_3m.loc[not_bank_df_3m.trans_amt > 0].shape[0]

            # 近3个月还款非银机构类型数
            self.variables['unbank_repay_type_cnt_r3m'] = not_bank_df_3m.loc[
                not_bank_df_3m.trans_amt < 0].loan_type.nunique()
            # 近3个月银行最大进账金额
            temp_df = df_3m.loc[(df_3m['loan_type'] == '银行') & (df_3m.trans_amt > 0)]
            if temp_df.shape[0] > 0:
                self.variables['bank_max_income_amt_3m'] = temp_df.trans_amt.max()

    def unusual_trans_feature(self):
        if self.df is not None:
            df_expense_12m = self.df.loc[(self.df.cal_month <= 12) & (self.df.trans_amt < 0)]
            df_6m = self.df.loc[self.df.cal_month <= 6]
            # 近12个月医院出账次数、近12个月房产买卖出账次数
            self.variables['hospital_expense_cnt_12m'] = \
                df_expense_12m.loc[df_expense_12m['unusual_trans_type'].astype(str).str.contains('医院')].shape[0]
            self.variables['house_sale_expense_cnt_12m'] = \
                df_expense_12m.loc[df_expense_12m['usual_trans_type'].astype(str).str.contains('房产买卖')].shape[0]

            # 近6个月案件纠纷交易笔数
            self.variables['court_cnt_6m'] = \
                df_6m.loc[df_6m['unusual_trans_type'].astype(str).str.contains('案件纠纷')].shape[0]

    def get_trans_flow_detail(self):
        df = self.trans_u_flow_portrait.copy()
        df_summary = self.trans_u_summary_portrait.copy()
        if df.empty:
            return
        df.trans_date = pd.to_datetime(df.trans_date)
        self.df = df
        self.df_summary = df_summary

    def confidence_feature(self):
        basic_sql = """
                SELECT ap.related_name AS relatedName, acc.id as account_id, 
                ap.relationship AS relation, ap.account_id as unique_id,
                ac.bank AS bankName,ac.account_no AS bankAccount,
                acc.start_time, acc.end_time, ta.trans_flow_src_type, ap.id_card_no
                FROM trans_apply ap
                left join trans_account ac
                on ap.account_id = ac.id
                left join trans_account acc
                on ac.account_no = acc.account_no and ac.bank = acc.bank and ac.risk_subject_id = acc.risk_subject_id
                left join trans_parse_task ta
                on acc.id = ta.account_id
                where ap.report_req_no =  %(report_req_no)s
            """
        basic_df = sql_to_df(sql=basic_sql, params={"report_req_no": self.reqno})
        basic_df['relatedName'] = basic_df['relatedName'].apply(lambda x: re.sub(r'([\u0000\s\\/*.+?^$])', r'\\\1', x))
        relation_list = basic_df['relatedName'].unique().tolist()
        year_ago = pd.to_datetime((basic_df['end_time'].max() - DateOffset(months=12)).date())
        basic_df = basic_df[(basic_df['start_time'] >= year_ago) | (basic_df['end_time'] >= year_ago) |
                            pd.isna((basic_df['account_id']))]
        basic_df.loc[basic_df['start_time'] < year_ago, 'start_time'] = year_ago
        account_list = list(map(str, basic_df[pd.notna(basic_df['account_id'])]['account_id'].unique().tolist()))

        flow_sql = f"""select * from trans_flow where account_id in ({','.join(account_list)})"""
        total_flow = sql_to_df(sql=flow_sql)
        total_flow = total_flow[total_flow['trans_time'] >= year_ago]
        credible_score = 100
        if not total_flow.empty:
            account_df = basic_df[pd.notna(basic_df.account_id)]
            acc_info = \
                account_df.drop_duplicates(subset=['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id'],
                                           )[['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id']]
            unique_id_list = acc_info.unique_id.unique().tolist()
            for ind in acc_info.index:
                # a. 遍历所有!银行账户流水!进行可信度分析
                bank, bank_acc = acc_info.loc[ind, 'bankName'], acc_info.loc[ind, 'bankAccount']
                name, idno = acc_info.loc[ind, 'relatedName'], acc_info.loc[ind, 'id_card_no']
                unique_id = acc_info.loc[ind, 'unique_id']
                temp_df = account_df[(account_df['id_card_no'] == idno) & (account_df['bankName'] == bank) &
                                     (account_df['bankAccount'] == bank_acc) &
                                     (~account_df['trans_flow_src_type'].isin([2, 3]))]
                # 若id_str中包含微信支付宝流水（或当前账户为微信支付宝流水），跳过
                if temp_df.empty:
                    continue
                temp_df.sort_values(by='start_time', inplace=True, ascending=True)
                temp_df['start_time'] = temp_df['start_time'].apply(lambda x: x.date())
                temp_df['end_time'] = temp_df['end_time'].apply(lambda x: x.date())
                # 当前账户流水数据
                df = total_flow[total_flow['account_id'].isin(temp_df['account_id'].unique().tolist())]
                if df.empty:
                    continue
                # 初始化参数
                total_score = 0
                # 银行账号处理
                handled_acc_no = ''.join([_ for _ in bank_acc if _.isnumeric()])
                if len(handled_acc_no) < 4:
                    # 若银行卡不规范，则任意赋值
                    acc_no = '!!!!!!!!!!'
                else:
                    acc_no = handled_acc_no[-4:]

                # 取近一年流水进行可信度分析
                total_score += self.balance_constance(df)
                total_score += self.opponent_name_check(df)
                total_score += self.relationship_check(relation_list, df)
                total_score += self.intact_check(df)

                # 银行流水交叉验证
                if basic_df.unique_id.nunique() != 1 and len(unique_id_list) != 1:
                    flow_cross_score = 0
                    for _ in unique_id_list:
                        # 不与本身进行校验
                        if _ != unique_id:
                            refer_df = total_flow[total_flow['account_id'].isin(
                                basic_df[basic_df['unique_id'] == _]['account_id'].tolist())]
                            if refer_df.empty:
                                continue
                            score = self.flow_cross_verify(df, refer_df, acc_no, bank, name)
                            flow_cross_score += score
                    total_score += flow_cross_score / (len(unique_id_list) - 1)
                else:
                    total_score += 7

                # 结息分析
                id_str = ','.join(list(map(str, basic_df[
                    basic_df['unique_id'] == unique_id]['account_id'].tolist())))
                single_summary_sql = """select distinct account_id, month, interest_amt, balance_amt,
                                        interest_balance_proportion from trans_single_summary_portrait 
                                        where account_id in (%s) and report_req_no = '%s'""" % (id_str, self.reqno)
                single_df = sql_to_df(single_summary_sql)
                single_df = single_df[~single_df['month'].str.isnumeric()]
                total_score += self.interest_analyse(single_df, year_ago)
                benford_coefficient = self.benford_ratio(df)
                if benford_coefficient < 0.5:
                    total_score += 1
                elif 0.5 <= benford_coefficient < 0.8:
                    total_score += 2
                elif 0.8 <= benford_coefficient < 0.9:
                    total_score += 3
                else:
                    total_score += 4

                # 20240112 若银行为青岛农商行，赋值分数95分
                if bank in ['青岛农商行', '青岛农商银行']:
                    total_score = 95
                credible_score = min(credible_score, total_score)
        self.variables['credible_score'] = credible_score

    @staticmethod
    def opponent_name_check(flow):
        df = flow.copy()
        # 剔除特殊交易对手和交易账号后， 汇总所有账号数量
        df = df[pd.notna(df.opponent_account_no)
                & pd.notna(df.opponent_name)
                & (~df.opponent_account_no.isin(IGNORE_ACC_NO))
                & (~df.opponent_name.astype(str).str.contains(IGNORE_OPPO_NAME_PATTERN))
                & (~df.opponent_account_no.astype(str).str.contains(IGNORE_ACC_NO_PATTERN))]
        total_cnt = df.opponent_account_no.nunique()
        # 匹配包含“T10(已修复)/T07(异常)”标签
        unusual_df = df[(df.verif_label.astype(str).str.contains('T07')) &
                        (~df.verif_label.astype(str).str.contains('T10'))][['opponent_name', 'opponent_account_no']]
        unusual_cnt = unusual_df.opponent_account_no.nunique()
        # 计算异常占比
        unusual_proportion = 0 if total_cnt == 0 else unusual_cnt / total_cnt
        resp = multi_mapping_score(unusual_proportion, unusual_cnt, [-1, 0, 0.1, 0.3, 1],
                                   [0, 2, 5, 0], [17, 17, 15, 12, 9, 6, 0, 0])
        return resp

    @staticmethod
    def relationship_check(relation_list, df):
        if len(relation_list) == 0:
            resp = 4
        else:
            relation_df = df[df['opponent_name'].astype(str).str.contains('|'.join(relation_list))]
            if relation_df.empty:
                resp = 2
            else:
                if relation_df.shape[0] / df.shape[0] >= 0.1:
                    resp = 4
                else:
                    resp = 3
        return resp

    @staticmethod
    def interest_analyse(single_df, max_time):
        if single_df.empty:
            resp = 0
        else:
            single_df = single_df[single_df['month'] >= str(max_time-DateOffset(months=13))[:7]]
            # 页面展示，结息次数
            real_cnt = single_df[pd.notna(single_df.interest_amt)].shape[0]
            should_cnt = single_df[(~single_df['month'].str.contains(r'\*')) |
                                   ((single_df['month'].str.contains(r'\*')) &
                                    (pd.notna(single_df.interest_amt)))].shape[0]
            should_cnt = real_cnt if real_cnt > should_cnt else should_cnt
            # 页面展示，结息金额具体状态
            single_df.where(single_df.notnull(), None)
            if should_cnt - real_cnt >= 2:
                if single_df.shape[0] >= 4:
                    score1 = 3
                else:
                    score1 = 0
            elif should_cnt - real_cnt == 1:
                if single_df.shape[0] >= 4:
                    score1 = 10
                else:
                    score1 = 7
            else:
                score1 = 14
            # 分数2：若结息日均/余额日均不在[0.8, 1.2]
            interest_cnt = single_df[pd.notna(single_df.interest_balance_proportion)
                                     & (0.8 <= single_df.interest_balance_proportion)
                                     & (single_df.interest_balance_proportion <= 1.2)].shape[0]
            if interest_cnt / single_df.shape[0] >= 0.75:
                score2 = 8
            elif 0.5 <= (interest_cnt / single_df.shape[0]) < 0.75:
                score2 = 6
            elif 0.25 <= (interest_cnt / single_df.shape[0]) < 0.5:
                score2 = 3
            else:
                score2 = 0
            resp = score1 + score2
        return resp

    @staticmethod
    def intact_check(df):
        # 缺失：交易对手/摘要为空
        unusual_oppo_cnt = df[(pd.isna(df.opponent_name) | (df.opponent_name == ''))].shape[0]
        unusual_remark_cnt = df[(pd.isna(df.remark) | (df.remark == ''))].shape[0]
        if (unusual_oppo_cnt / df.shape[0]) > 0.5:
            score1 = 1
        else:
            score1 = 2
        if (unusual_remark_cnt / df.shape[0]) > 0.5:
            score2 = 1
        elif (unusual_remark_cnt / df.shape[0]) == 1:
            score2 = 2
        else:
            score2 = 3
        resp = score1 + score2
        return resp

    @staticmethod
    def flow_cross_verify(flow, other_flow, acc_no, bank, user_name):
        df, refer_df = flow.copy(), other_flow.copy()
        # 取银行流水和微信流水交集
        start_time, check_end_time = max((min(df.trans_time)), min(refer_df.trans_time)), \
            min(max(df.trans_time), max(refer_df.trans_time))
        score = 7
        if start_time < check_end_time:
            df = df[(df.trans_time >= start_time) & (df.trans_time <= check_end_time)]
            refer_df = refer_df[(refer_df.trans_time >= start_time) & (refer_df.trans_time <= check_end_time)]
            trans_record = refer_df[
                (refer_df.trans_type.astype(str).str.contains(acc_no)
                 & ~refer_df.trans_type.astype(str).str.contains('余额')
                 & refer_df.trans_type.astype(str).str.contains(bank))
                | (refer_df.opponent_account_no.astype(str).str.contains(acc_no)
                   | (refer_df.opponent_name.astype(str).str.contains(acc_no)))
                & (refer_df.opponent_name.astype(str).str.contains('|'.join([user_name, bank]))
                   | refer_df.opponent_account_bank.astype(str).str.contains(bank))]
            # 1.交易类型包含银行名称以及银行账户，但不包含“余额” （针对微信支付宝流水）
            # 2.交易对手账号包含银行账户 且 交易对手包含用户名|银行名称 或 交易银行名包含银行名称
            if not trans_record.empty:
                unusual_cnt = 0
                trans_record.index = [_ for _ in range(trans_record.shape[0])]
                for ind in trans_record.index:
                    check_start_time = trans_record.loc[ind, 'trans_time']
                    check_end_time = check_start_time + DateOffset(days=1)
                    # 参照银行账户金额
                    record_amt = abs(trans_record.loc[ind, 'trans_amt'])
                    check_df = df[(df.trans_time <= check_end_time) & (df.trans_time >= check_start_time)
                                  & (abs(df.trans_amt) >= record_amt * 0.998)
                                  & (abs(df.trans_amt) <= record_amt * 1.002)]
                    if check_df.empty:
                        unusual_cnt += 1
                unsual_proportion = unusual_cnt / trans_record.shape[0]
                score = multi_mapping_score(unsual_proportion, unusual_cnt, [-1, 0.2499, 0.4999, 0.7499, 1],
                                            [0, 9, 6, 3], [8, 8, 6, 4, 4, 2, 2, 0])
        return score

    @staticmethod
    def balance_constance(df):
        # 余额不连续占比
        unusual_cnt = df[df.verif_label.astype(str).str.contains('T01')].shape[0]
        unusual_proportion = unusual_cnt / df.shape[0]
        if df[df.verif_label.astype(str).str.contains('T11|T12')].shape[0] > 0:
            resp = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
                                       [0, 5, 10, 20, 20], [40, 40, 35, 30, 25, 20, 15, 5, 10, 0])
        else:
            resp = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
                                       [0, 10, 15, 20, 30], [40, 40, 35, 30, 25, 20, 15, 5, 10, 0])
        return resp

    @staticmethod
    def benford_ratio(df):
        expect_frequency = [0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046]
        first_num_list = [str(abs(_))[:1] for _ in df[abs(df.trans_amt) >= 1].trans_amt.tolist()]
        rate = 0
        for _ in range(1, 10):
            actual_frequency = first_num_list.count(str(_)) / len(first_num_list) if len(first_num_list) > 0 else 0
            if actual_frequency == 0:
                actual_frequency = 1e-6
            rate += (actual_frequency - expect_frequency[_-1]) * np.log(actual_frequency/expect_frequency[_-1])
        return 1 - rate
