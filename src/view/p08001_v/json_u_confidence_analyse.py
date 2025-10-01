from view.TransFlow import TransFlow
import numpy as np
import pandas as pd
import re
from util.mysql_reader import sql_to_df
from pandas.tseries.offsets import *
from fileparser.trans_flow.trans_config import IGNORE_ACC_NO, IGNORE_ACC_NO_PATTERN, IGNORE_OPPO_NAME_PATTERN


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


def union_date(start, end):
    res = []
    for i, v in enumerate(start):
        if i == 0:
            res.append([v, end[i]])
        else:
            if v <= res[-1][-1]:
                if end[i] > res[-1][-1]:
                    res[-1][-1] = end[i]
            else:
                res.append([v, end[i]])
    return res


class JsonUnionConfidenceAnalyse(TransFlow):
    def process(self):
        self.variables['confidence_analyse'] = []
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
                where ap.report_req_no =  %(report_req_no)s and acc.bank not in ('青岛农商行', '青岛农商银行')
            """
        basic_df = sql_to_df(sql=basic_sql, params={"report_req_no": self.reqno})
        if basic_df.shape[0] > 0:
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
        else:
            total_flow = pd.DataFrame()

        if not total_flow.empty:
            account_df = basic_df[pd.notna(basic_df.account_id)]
            acc_info = \
                account_df.drop_duplicates(subset=['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id'],
                                           )[['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id']]
            unique_id_list = acc_info.unique_id.unique().tolist()
            overview_confidence_tips = ""
            suggest_confidence_tips = ""
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
                temp_start = pd.to_datetime(temp_df['start_time']).tolist()
                temp_end = pd.to_datetime(temp_df['end_time']).tolist()
                temp_date_list = union_date(temp_start, temp_end)
                # 当前账户流水数据
                df = total_flow[total_flow['account_id'].isin(temp_df['account_id'].unique().tolist())]
                if df.empty:
                    continue
                # 初始化参数
                tmp_dict = {'account_name': f"{name}—{bank}", 'total_score': None}
                # 银行账号处理
                handled_acc_no = ''.join([_ for _ in bank_acc if _.isnumeric()])
                if len(handled_acc_no) < 4:
                    tmp_dict['account_name'] += f'（{bank_acc}）'
                    # 若银行卡不规范，则任意赋值
                    acc_no = '!!!!!!!!!!'
                else:
                    acc_no = handled_acc_no[-4:]
                    tmp_dict['account_name'] += f'（{bank_acc}）'

                # 取近一年流水进行可信度分析
                tmp_dict['balance_constance'] = self.balance_constance(df, temp_date_list)
                tmp_dict['opponent_check'] = self.opponent_name_check(df)
                tmp_dict['relation_check'] = self.relationship_check(relation_list, df)
                tmp_dict['intact_check'] = self.intact_check(df)

                # 银行流水交叉验证
                if basic_df.unique_id.nunique() == 1 or len(unique_id_list) == 1:
                    tmp_dict['flow_cross_verify'] = {'score': 7, 'risk_tips': '主体用户未上传其他账户流水，无法进行交叉验证'}
                else:
                    risk_list = []
                    flow_cross_score = 0
                    for _ in unique_id_list:
                        # 不与本身进行校验
                        if _ != unique_id:
                            refer_df = total_flow[total_flow['account_id'].isin(
                                basic_df[basic_df['unique_id'] == _]['account_id'].tolist())]
                            if refer_df.empty:
                                continue
                            refer_acc_no = acc_info[acc_info['unique_id'] == _]['bankAccount'].tolist()[0]
                            refer_bank = acc_info[acc_info['unique_id'] == _]['bankName'].tolist()[0]
                            refer_name = acc_info[acc_info['unique_id'] == _]['relatedName'].tolist()[0]
                            score = self.flow_cross_verify(df, refer_df, acc_no, bank, name)
                            if score < 7:
                                risk_list.append(f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                 f"（{refer_acc_no}）交叉验证失败")
                            elif score == 7:
                                risk_list.append(f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                 f"（{refer_acc_no}）无交易记录")
                            else:
                                risk_list.append(f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                 f"（{refer_acc_no}）交叉验证成功")
                            flow_cross_score = flow_cross_score + score
                    tmp_dict['flow_cross_verify'] = {'score': flow_cross_score / (len(unique_id_list) - 1),
                                                     'risk_tips': ';'.join(risk_list)}

                # 结息分析
                id_str = ','.join(list(map(str, basic_df[
                    basic_df['unique_id'] == unique_id]['account_id'].tolist())))
                single_summary_sql = """select distinct account_id, month, interest_amt, balance_amt,
                                        interest_balance_proportion from trans_single_summary_portrait 
                                        where account_id in (%s) and report_req_no = '%s'""" % (id_str, self.reqno)
                single_df = sql_to_df(single_summary_sql)
                single_df = single_df[~single_df['month'].str.isnumeric()]
                tmp_dict['interest_analyse'] = self.interest_analyse(single_df, year_ago)
                tmp_dict['benford_coefficient'] = self.benford_ratio(df)
                if tmp_dict['benford_coefficient'] < 0.5:
                    benford_score = 1
                elif 0.5 <= tmp_dict['benford_coefficient'] < 0.8:
                    benford_score = 2
                elif 0.8 <= tmp_dict['benford_coefficient'] < 0.9:
                    benford_score = 3
                else:
                    benford_score = 4

                # 所有模块报文封装
                tmp_dict['total_score'] = round(tmp_dict['opponent_check']['score']
                                                + tmp_dict['relation_check']['score']
                                                + tmp_dict['interest_analyse']['score']
                                                + tmp_dict['balance_constance']['score']
                                                + tmp_dict['flow_cross_verify']['score']
                                                + tmp_dict['intact_check']['score']
                                                + benford_score)
                del tmp_dict['relation_check']
                # 每个模块添加status空字符
                module_name = ['balance_constance', 'flow_cross_verify', 'intact_check', 'interest_analyse',
                               'opponent_check']
                for m_name in module_name:
                    tmp_dict[m_name]['status'] = ''
                if tmp_dict['total_score'] < 70:
                    tmp_dict['total_status'] = '可信度为低'
                    suggest_confidence_tips = "存在可信度为低的流水，建议谨慎授信"
                elif 70 <= tmp_dict['total_score'] < 90:
                    tmp_dict['total_status'] = '可信度为中'
                else:
                    tmp_dict['total_status'] = '可信度为高'
                overview_confidence_tips += f"{name}—{bank}（{bank_acc}）{tmp_dict['total_status']};"
                self.variables['confidence_analyse'].append(tmp_dict)
            self.variables['trans_report_overview']['trans_general_info']['confidence_analyse'][
                'risk_tips'] = overview_confidence_tips
            self.variables['suggestion_and_guide']['trans_general_info']['confidence_analyse'][
                'risk_tips'] = suggest_confidence_tips

    @staticmethod
    def opponent_name_check(flow):
        df = flow.copy()
        resp = {'trans_detail': []}
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
        if unusual_proportion != 0:
            resp['unusual_proportion'] = unusual_proportion
            for _ in unusual_df.opponent_account_no.unique().tolist():
                tmp_dict = dict()
                tmp_dict['oppo_no'] = _
                tmp_dict['oppo_names'] = '/'.join(unusual_df[unusual_df.opponent_account_no == _].
                                                  opponent_name.unique().tolist())
                resp['trans_detail'].append(tmp_dict)
        resp['score'] = multi_mapping_score(unusual_proportion, unusual_cnt, [-1, 0, 0.1, 0.3, 1],
                                            [0, 2, 5, 0], [17, 17, 15, 12, 9, 6, 0, 0])
        return resp

    @staticmethod
    def relationship_check(relation_list, df):
        resp = dict()
        if len(relation_list) == 0:
            resp['score'] = 4
            resp['risk_tips'] = '用户未填写关联关系或关联关系查询失败'
        else:
            relation_df = df[df['opponent_name'].astype(str).str.contains('|'.join(relation_list))]
            if relation_df.empty:
                resp['score'] = 2
                resp['risk_tips'] = '与关联人不存在交易记录'
            else:
                if relation_df.shape[0] / df.shape[0] >= 0.1:
                    resp['score'] = 4
                else:
                    resp['score'] = 3
        return resp

    @staticmethod
    def interest_analyse(single_df, year_ago):
        resp = {'score': 24, 'interest_cnt': '', 'trans_detail': []}
        if single_df.empty or single_df[single_df['month'] >= str(year_ago)[:7]].shape[0] == 0:
            resp['interest_cnt'] = '0/0'
            resp['score'] = 0
        else:
            single_df = single_df[single_df['month'] >= str(year_ago)[:7]]
            # 页面展示，结息次数
            real_cnt = single_df[pd.notna(single_df.interest_amt)].shape[0]
            should_cnt = single_df[(~single_df['month'].str.contains(r'\*')) |
                                   ((single_df['month'].str.contains(r'\*')) &
                                    (pd.notna(single_df.interest_amt)))].shape[0]
            should_cnt = real_cnt if real_cnt > should_cnt else should_cnt
            resp['interest_cnt'] = f'{real_cnt}/{should_cnt}'
            # 页面展示，结息金额具体状态
            single_df.where(single_df.notnull(), None)
            resp['interest_detail'] = single_df[['month', 'interest_amt', 'balance_amt']].to_dict('records')
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
            resp['score'] = score1 + score2
        return resp

    @staticmethod
    def intact_check(df):
        resp = dict()
        # 缺失：交易对手/摘要为空
        unusual_oppo_cnt = df[(pd.isna(df.opponent_name) | (df.opponent_name == ''))].shape[0]
        unusual_remark_cnt = df[(pd.isna(df.remark) | (df.remark == ''))].shape[0]
        if ((unusual_oppo_cnt / df.shape[0]) > 0.5) or ((unusual_remark_cnt / df.shape[0]) > 0.5):
            resp['risk_tips'] = '主体银行流水摘要、备注等交易信息较少'
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
        resp['score'] = score1 + score2
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
    def balance_constance(df, date_list):
        resp = {'trans_detail': []}
        df['trans_date'] = df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d'))
        month_list = pd.date_range(df['trans_time'].min().date(), df['trans_time'].max().date() + MonthEnd(0), freq='M')
        max_date = df['trans_time'].max()
        d_ind = 0
        for i, m in enumerate(month_list):
            tmp_dict = dict()
            tmp_dict['order'] = i + 1
            m_end = m + MonthEnd(0)
            m_start = m_end - MonthBegin(1)
            tmp_dict['start_date'], tmp_dict['end_date'] = [], []
            while True:
                tmp_min = max(m_start, pd.to_datetime(date_list[d_ind][0]))
                tmp_max = min(m_end, pd.to_datetime(date_list[d_ind][-1]), max_date)
                if tmp_min <= tmp_max:
                    tmp_dict['start_date'].append(format(tmp_min, '%Y-%m-%d'))
                    tmp_dict['end_date'].append(format(tmp_max, '%Y-%m-%d'))
                if m_end < pd.to_datetime(date_list[d_ind][-1]) or m_end >= pd.to_datetime(date_list[-1][-1]):
                    break
                d_ind += 1
            tmp_dict['unusual_date'] = df[(df.verif_label.astype(str).str.contains('T01'))
                                          & (m_start <= df.trans_time)
                                          & (df.trans_time < m_end + DateOffset(days=1))
                                          ].trans_date.astype(str).unique().tolist()
            resp['trans_detail'].append(tmp_dict)

        # 余额不连续占比
        unusual_cnt = df[df.verif_label.astype(str).str.contains('T01')].shape[0]
        unusual_proportion = unusual_cnt / df.shape[0]
        if df[df.verif_label.astype(str).str.contains('T11|T12')].shape[0] > 0:
            resp['score'] = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
                                                [0, 5, 10, 20, 20], [40, 40, 35, 30, 25, 20, 15, 5, 10, 0])
        else:
            resp['score'] = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
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
