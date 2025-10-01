import pandas as pd
import numpy as np
import datetime
import re
from fileparser.trans_flow.trans_config import *
from portrait.transflow.single_account_portrait.portrait_config import *
from util.mysql_reader import sql_to_df


def get_trans_flow_src_type_by_account_id(account_id, df_trans_parse):
    df_temp = df_trans_parse[df_trans_parse['account_id'] == account_id]
    if not df_temp.empty:
        return df_temp['trans_flow_src_type'].tolist()[0]
    else:
        return None


class TransSingleLabel:
    """
    单账户标签画像表清洗并落库
    author:汪腾飞
    created_time:20200706
    updated_time_v1:20201125,夜间交易风险和家庭不稳定风险以及民间借贷风险逻辑调整
    updated_time_v2:20210207,民间借贷剔除关联关系，当特殊交易类型命中医院时，将命中医院关键字的字符加到备注列里面
    """

    def __init__(self, trans_flow):
        self.query_data_array = trans_flow.query_data_array
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.df = trans_flow.trans_flow_df
        self.user_name = trans_flow.user_name
        self.user_type = trans_flow.user_type
        self.create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.label_list = []
        self.spouse_name = 'None'

    def process(self):
        if self.df is None:
            return
        self._choose_index()
        self._relationship_dict()
        # 新增，提前处理关联关系
        self._isrelationship()
        self._wxzfb_relation_label()

        if pd.isnull(self.df.trans_flow_src_type.values[0]) or self.df.trans_flow_src_type.values[0] == 1:
            self._loan_type_label()
            self._unusual_type_label()
        elif self.df.trans_flow_src_type.values[0] in (2, 3):
            self._loan_type_label_third_pay()
            self._unusual_type_label_third_pay()

        self._in_out_order()
        self.usual_trans_type()
        df = self.save_raw_data()
        if 'cost_type' not in df.columns:
            df['cost_type'] = None
        return df

    def _choose_index(self):
        """
        剔除冲正、抹账相关数据
        """
        temp_df = self.df
        concat_list = ['trans_channel', 'trans_use', 'remark']
        temp_df[concat_list] = temp_df[concat_list].fillna('').astype(str)
        temp_df['text'] = temp_df['trans_channel'] + temp_df['trans_use'] + temp_df['remark']
        index_list1 = temp_df[temp_df.text.str.contains(BIG_IN_OUT_EXCEPT)].index.tolist()
        index_list2 = []
        for left_i in index_list1:
            row1 = temp_df.loc[left_i, :]
            if left_i > 0:
                row2 = temp_df.loc[left_i - 1, :]
            else:
                continue
            if getattr(row1, 'opponent_name') == getattr(row2, 'opponent_name') and \
                    getattr(row1, 'trans_amt') + getattr(row2, 'trans_amt') == 0:
                index_list2.append(left_i - 1)
                index_list2.append(left_i)
        self.df = self.df.drop(index=index_list2).reset_index(drop=True)

    def _relationship_dict(self):
        """
        生成姓名和关联关系对应的字典,需要将编码形式的关联关系转化为中文关联关系
        v1.2,忽略掉全部担保人
        v2 担保人不忽略
        :return:
        """
        length = len(self.query_data_array)
        self.relation_dict = dict()
        for i in range(length):
            temp = self.query_data_array[i]
            # base_type_detail = base_type_mapping.get(temp['baseTypeDetail'])
            # 保存英文base_type
            base_type_detail = temp['baseTypeDetail']
            # if base_type_detail != '担保人':
            name = re.sub(r'([\u0000\s\\/*.+?^$])', r'\\\1', temp['name'])
            if name in ['*', '', '.', '?']:
                name = f'\\{name}'
            self.relation_dict[name] = base_type_detail

            # if base_type_detail in ['借款人配偶', '借款企业实际控制人配偶']:
            #     self.spouse_name = str(temp['name'])
            if base_type_detail in ['U_PER_SP_PERSONAL', 'U_COM_CT_SP_PERSONAL']:
                self.spouse_name = str(temp['name'])

    def _isrelationship(self):
        """
        本模块获取关联关系
        """
        for i, v in self.relation_dict.items():
            self.df.loc[self.df['opponent_name'].astype(str).str.contains(i), 'relationship'] = v

    def _loan_type_label(self):
        """
        包括交易对手类型标签opponent_type,贷款类型标签loan_type,是否还款标签is_repay,是否结息标签is_interest
        是否结息前一周标签is_before_interest_repay
        v1.1,调整先后顺序，补充部分字符
        v1.2, 调整字符
        :return:
        """
        concat_list = ['opponent_name', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        # 交易对手标签赋值,1个人,2企业,其他为空
        self.df['opponent_type'] = self.df['opponent_name'].apply(self._opponent_type)
        self.df['year_month'] = self.df['trans_time'].apply(lambda x: format(x, '%Y-%m'))
        self.df['year'] = self.df['trans_time'].apply(lambda x: x.year)
        self.df['month'] = self.df['trans_time'].apply(lambda x: x.month)
        self.df['day'] = self.df['trans_time'].apply(lambda x: x.day)
        # 将字符串列合并到一起
        # self.df['concat_str'] = self.df.apply(lambda x: ';'.join(x[concat_list]), axis=1)
        self.df['concat_str'] = self.df['opponent_name'] + ';' + self.df['trans_channel'] + ';' + \
                                self.df['trans_type'] + ';' + self.df['trans_use'] + ';' + self.df['remark']
        # 贷款类型赋值,优先级从上至下
        # 我司相关机构需从多头中剔除
        # our_inst = "重庆中金同盛小额贷款|磁石供应链商业保理|晋福融资担保|孚厘|中金同盛商业保理"
        our_inst = "￥￥￥$$$$"  # 占位符，必不可能命中
        # 消金
        self.df.loc[(self.df['concat_str'].str.contains(CONSUME_FINANCE)) &
                    (~self.df['concat_str'].str.contains(CONSUME_FINANCE_EXCEPT)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '消金'
        # 融资租赁
        self.df.loc[(self.df['concat_str'].str.contains(FINANCE_LEASE)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '融资租赁'
        # 担保
        self.df.loc[(self.df['concat_str'].str.contains(GUARANTEE)) &
                    (~self.df['concat_str'].str.contains(GUARANTEE_EXCEPT)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '担保'
        # 保理
        self.df.loc[(self.df['concat_str'].str.contains(FACTORING)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '保理'
        # 小贷
        self.df.loc[~(self.df['concat_str'].str.contains(SMALL_LOAN_EXCEPT)) &
                    (self.df['concat_str'].str.contains(SMALL_LOAN)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '小贷'

        # 银行放款
        self.df.loc[(self.df.trans_amt > 0) &
                    ((self.df.opponent_name.str.contains(BANK_LOAN_OPPONENT_NAME)) |
                     ((self.df.opponent_name.isin([self.user_name, ""])) &
                      (self.df.remark.str.contains(BANK_LOAN_REMARK)))) &
                    ((~self.df.concat_str.str.contains(BANK_LOAN_CONCAT_STR_EXCEPT)) |
                     (self.df.concat_str.str.contains(BANK_LOAN_CONCAT_STR_COMPATIBLE))) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.astype(str).str.contains(BANK_LOAN_OPPONENT_NAME_EXCEPT)), 'bank_loan'] = 1
        # 银行还款
        self.df.loc[(self.df.trans_amt < 0) &
                    ((self.df.opponent_name.str.contains(BANK_REPAY_OPPONENT_NAME)) |
                     ((self.df.opponent_name.isin([self.user_name, ""])) &
                      (self.df.concat_str.str.contains(BANK_REPAY_CONCAT_STR)))) &
                    ((~self.df.concat_str.str.contains(BANK_REPAY_CONCAT_STR_EXCEPT)) |
                     (self.df.concat_str.str.contains(BANK_REPAY_CONCAT_STR_COMPATIBLE))) &
                    (~self.df.opponent_name.astype(str).str.contains(BANK_REPAY_OPPONENT_NAME_EXCEPT)) &
                    (pd.isnull(self.df.loan_type)), 'bank_repay'] = 1
        # 受托支付
        self.df.loc[(self.df.trans_amt < 0) &
                    (self.df.concat_str.str.contains(ENTRUSTED_PAY)), 'entrust_pay'] = 1
        bank_income = self.df[self.df.bank_loan == 1]
        entrust_list = set()
        if not bank_income.empty:
            for row in bank_income.itertuples():
                temp_amt = getattr(row, 'trans_amt')
                temp_dt = getattr(row, 'trans_time')
                day3_after = pd.to_datetime(temp_dt + datetime.timedelta(days=3))
                temp_df = self.df[(self.df.trans_time > temp_dt) &
                                  (self.df.trans_time < day3_after) &
                                  (self.df.trans_amt == -temp_amt) &
                                  (~self.df.concat_str.str.contains(ENTRUSTED_PAY)) &
                                  (self.df.opponent_name == "")]
                if not temp_df.empty:
                    temp_df.reset_index(drop=False, inplace=True)
                    temp_index = temp_df.loc[0, 'index']
                    entrust_list.add(temp_index)
        self.df.loc[list(entrust_list), 'entrust_pay'] = 1

        # 银行
        self.df.loc[(self.df.bank_loan == 1) |
                    ((self.df.bank_repay == 1)
                     & (self.df.entrust_pay != 1)), 'loan_type'] = "银行"
        # 第三方支付
        self.df.loc[(self.df['concat_str'].str.contains(THIRD_REPAY_1)) &
                    (self.df['concat_str'].str.contains(THIRD_REPAY_2)) &
                    (~self.df['concat_str'].str.contains(THIRD_REPAY_EXCEPT)) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '第三方支付'
        # 其他金融
        self.df.loc[
            (((self.df['concat_str'].str.contains(OTHER_FINANCE)) &
              (~self.df['concat_str'].str.contains(OTHER_FINANCE_EXCEPT))) |
             ((self.df['concat_str'].str.contains(OTHER_FINANCE_PER)) & (self.df.opponent_type != 1)) |
             ((self.df['opponent_name'].str.contains('|'.join(self.relation_dict.keys()))) &
              (self.df['concat_str'].str.contains(OTHER_FINANCE_RELATION)))) &
            (~self.df['trans_channel'].str.contains(OTHER_FINANCE_CHANNEL_EXCEPT)) &
            (pd.isnull(self.df.loan_type)) &
            (~self.df.opponent_name.str.contains(OTHER_FINANCE_ENT_EXCEPT)), 'loan_type'] = '其他金融'
        # 民间借贷
        self.df.loc[(self.df['trans_amt'].apply(lambda x: abs(x)) > 500) &
                    (~self.df['concat_str'].str.contains('|'.join(self.relation_dict.keys()))) &
                    ((((self.df['concat_str'].str.contains(PRIVATE_LENDING)) |
                       ((self.df['concat_str'].str.contains(PRIVATE_LENDING_COMPATIBLE)) &
                        (self.df['opponent_name'] != ''))) &
                      (~self.df['concat_str'].str.contains(PRIVATE_LENDING_EXCEPT_1))) |
                     ((self.df['trans_amt'] < 0) & (self.df['concat_str'].str.contains(PRIVATE_LENDING_INTEREST)) &
                      (~self.df['concat_str'].str.contains(PRIVATE_LENDING_INTEREST_EXCEPT)))) &
                    (~self.df['opponent_name'].str.contains(PRIVATE_LENDING_OPPONENT_NAME)) &
                    (self.df['opponent_name'] != '') &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '民间借贷'
        amt_group = self.df[
            (self.df['trans_amt'].apply(lambda x: abs(x)) > MIN_PRIVATE_LENDING) &
            (~self.df['concat_str'].str.contains('|'.join(self.relation_dict.keys()))) &
            (pd.notnull(self.df['opponent_type'])) &
            (~self.df['concat_str'].str.contains(PRIVATE_LENDING_EXCEPT_2)) &
            (self.df['opponent_name'] != '') &
            (~self.df['opponent_name'].str.contains(PRIVATE_LENDING_OPPONENT_NAME)) &
            (pd.isnull(self.df.loan_type))
            ].groupby(['opponent_name', 'trans_amt'], as_index=False).agg({'month': len})
        amt_group = amt_group[amt_group['month'] >= MIN_CONTI_MONTHS]
        if amt_group.shape[0] > 0:
            for row in amt_group.itertuples():
                temp_name = getattr(row, 'opponent_name')
                temp_amt = getattr(row, 'trans_amt')
                temp_df = self.df[(self.df['opponent_name'] == temp_name) &
                                  (self.df['trans_amt'] == temp_amt)]
                temp_df.reset_index(drop=False, inplace=True)
                last_month = temp_df['trans_time'].tolist()[0]
                temp_df.loc[0, 'conti'] = 1
                conti_list = set()
                temp_cnt = 1
                now_index = 1
                for index in temp_df.index.tolist()[1:]:
                    this_month = temp_df.loc[index, 'trans_time']
                    temp_interval = (this_month.year - last_month.year) * 12 + this_month.month - last_month.month
                    if temp_interval <= 1:
                        temp_df.loc[index, 'conti'] = now_index
                        if temp_interval == 1:
                            temp_cnt += 1
                            if temp_cnt >= MIN_CONTI_MONTHS:
                                conti_list.add(now_index)
                    else:
                        now_index += 1
                        temp_df.loc[index, 'conti'] = now_index
                        temp_cnt = 1
                    last_month = this_month
                conti_list = list(conti_list)
                if len(conti_list) == 0:
                    continue
                for i in conti_list:
                    conti_df = temp_df[temp_df['conti'] == i]
                    max_interval = conti_df['day'].max() - conti_df['day'].min()
                    if max_interval <= MAX_INTERVAL_DAYS:
                        if 'loan_type' not in conti_df.columns:
                            loan_type = '民间借贷'
                        else:
                            conti_type_df = conti_df[pd.notna(conti_df['loan_type'])]
                            if conti_type_df.shape[0] > 0:
                                loan_type = conti_type_df['loan_type'].tolist()[0]
                            else:
                                loan_type = '民间借贷'
                        self.df.loc[conti_df['index'].tolist(), 'loan_type'] = loan_type

        # 是否还款标签
        self.df.loc[(pd.notnull(self.df['loan_type'])) & (self.df['trans_amt'] < 0), 'is_repay'] = 1

        # 是否结息标签
        interest_df = self.df.loc[(self.df.month % 3 == 0) &
                                  (self.df.day.isin([20, 21])) &
                                  (self.df.trans_amt > 0) &
                                  ((self.df.opponent_name == '') |
                                   (self.df.opponent_name == self.user_name) |
                                   (self.df.opponent_name.str.contains(INTEREST_OPPO_KEY_WORD))) &
                                  (self.df.concat_str.str.contains(INTEREST_KEY_WORD)) &
                                  (~self.df.concat_str.str.contains(NON_INTEREST_KEY_WORD))]
        interest_df.reset_index(drop=False, inplace=True)
        group_df = interest_df.groupby(by=['year', 'month'], as_index=False).agg({'trans_amt': min})
        index_list = interest_df.loc[group_df.index.tolist(), 'index'].tolist()
        self.df.loc[index_list, 'is_interest'] = 1
        if self.df[self.df.is_interest == 1].empty and self.df[pd.notna(self.df.remark)
                                                               & (self.df.remark != "")].empty:
            interest_df = self.df.loc[(self.df.month % 3 == 0) &
                                      (self.df.day.isin([20, 21, 22])) &
                                      (self.df.trans_amt > 0) &
                                      (self.df.opponent_name == '')]
            interest_df.reset_index(drop=False, inplace=True)
            group_df = interest_df.groupby(by=['year', 'month'], as_index=False).agg({'trans_amt': min})
            index_list = interest_df.loc[group_df.index.tolist(), 'index'].tolist()
            self.df.loc[index_list, 'is_interest'] = 1

        # 是否还款到期前一周标签
        repay_date_list = self.df[(self.df['is_repay'] == 1)]['trans_time'].tolist()
        for repay_date in repay_date_list:
            seven_days_ago = pd.to_datetime((repay_date - datetime.timedelta(days=7)).date())
            self.df.loc[(self.df.trans_time < repay_date) &
                        (self.df.trans_time >= seven_days_ago), 'is_before_interest_repay'] = 1
        self.df.drop(['year', 'month', 'day'], axis=1, inplace=True)

    def _loan_type_label_third_pay(self):
        concat_list = ['opponent_name', 'trans_type', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        # 交易对手标签赋值,1个人,2企业,其他为空
        self.df['opponent_type'] = self.df['opponent_name'].apply(self._opponent_type)
        self.df['year_month'] = self.df['trans_time'].apply(lambda x: format(x, '%Y-%m'))
        self.df['year'] = self.df['trans_time'].apply(lambda x: x.year)
        self.df['month'] = self.df['trans_time'].apply(lambda x: x.month)
        self.df['day'] = self.df['trans_time'].apply(lambda x: x.day)
        # 将字符串列合并到一起
        # self.df['concat_str'] = self.df.apply(lambda x: ';'.join(x[concat_list]), axis=1)
        self.df['concat_str'] = self.df['opponent_name'] + ';' + self.df['trans_type'] + ';' + self.df['remark']
        # 贷款类型赋值,优先级从上至下
        # 我司相关机构需从多头中剔除
        our_inst = "￥￥￥$$$$"

        # 借呗花呗交易（支付宝）-> 小贷
        self.df.loc[(self.df['concat_str'].str.contains(WXZFB_SMALL_LOAN)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '小贷'
        # 消金
        self.df.loc[(pd.isnull(self.df.verif_label)) &
                    (self.df.trans_amt < 0) &
                    (self.df['concat_str'].str.contains(WXZFB_CONSUME_FINANCE)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '消金'
        # 网商银行（剔除余利宝）-> 银行
        self.df.loc[(self.df['concat_str'].str.contains(WXZFB_BANK_LOAN)) &
                    (~self.df['remark'].str.contains(WXZFB_BANK_LOAN_REMARK_EXCEPT)) &
                    (pd.isnull(self.df.loan_type)) &
                    (~self.df.opponent_name.str.contains(our_inst)), 'loan_type'] = '银行'

    def _unusual_type_label(self):
        self.df['op_name'] = self.df.opponent_name
        no_channel_list = ['opponent_name', 'trans_type', 'trans_use', 'remark']
        no_oppo_channel_list = ['trans_type', 'trans_use', 'remark']
        self.df[no_channel_list] = self.df[no_channel_list].fillna('').astype(str)
        self.df[no_oppo_channel_list] = self.df[no_oppo_channel_list].fillna('').astype(str)
        # 将字符串列合并到一起
        # self.df['no_channel_str'] = self.df.apply(lambda x: ';'.join(x[no_channel_list]), axis=1)
        # self.df['no_oppo_channel_str'] = self.df.apply(lambda x: ';'.join(x[no_oppo_channel_list]), axis=1)
        self.df['no_channel_str'] = self.df['opponent_name'] + ';' + self.df['trans_type'] + ';' + \
                                    self.df['trans_use'] + ';' + self.df['remark']
        self.df['no_oppo_channel_str'] = self.df['trans_type'] + ';' + self.df['trans_use'] + ';' + self.df['remark']
        self.df['user_type'] = self.user_type
        self.df['unusual_trans_type'] = \
            pd.Series(np.where((self.df['concat_str'].str.contains(GAMBLE)) &
                               ((self.df['trans_amt'] < 0) |
                                ((self.df['no_oppo_channel_str'].str.contains(GAMBLE_INCOME)) &
                                 (self.df['trans_amt'] > 0))), '博彩', '')) + ';' + \
            pd.Series(np.where((self.df['concat_str'].str.contains(AMUSEMENT)) &
                               (self.df['trans_amt'] < 0) &
                               (~self.df['concat_str'].str.contains(AMUSEMENT_EXCEPT)) &
                               (self.df['op_name'] != ""), '娱乐', '')) + ';' + \
            pd.Series(np.where((self.df['op_name'].str.contains(CASE_DISPUTES)) &
                               (self.df['trans_amt'] < 0), '案件纠纷', '')) + ';' + \
            pd.Series(np.where(((self.df['no_channel_str'].str.contains(SECURITY_FINES)) &
                                (~self.df['concat_str'].str.contains(SECURITY_FINES_EXCEPT))) |
                               ((self.df['op_name'].str.contains(SECURITY_EXPENSE_FINES)) &
                                (self.df['trans_amt'] < 0)), '治安罚款', '')) + ';' + \
            pd.Series(np.where((self.df['concat_str'].str.contains(INSURANCE_CLAIMS)) &
                               (self.df['op_name'] != ""), '保险理赔', '')) + ';' + \
            pd.Series(np.where((self.df['op_name'].str.contains(STOCK_OPPONENT_NAME)) &
                               (self.df['remark'].str.contains(STOCK_REMARK)), '股票期货', '')) + ';' + \
            pd.Series(np.where((self.df['user_type'] != 'PERSONAL') &
                               (self.df['trans_amt'] < 0) &
                               (self.df['concat_str'].str.contains(HOSPITAL)) &
                               (~self.df['concat_str'].str.contains(HOSPITAL_EXCEPT)) &
                               (self.df['op_name'] != ""), '医院', '')) + ';' + \
            pd.Series(np.where((((self.df['loan_type'] == '担保') &
                                 (self.df['concat_str'].str.contains(LOAN_GUAR_ABNORMAL))) |
                                (self.df['concat_str'].str.contains(LOAN_ABNORMAL))) &
                               (self.df['trans_amt'] < 0) &
                               (self.df['op_name'] != ""), '贷款异常', '')) + ';' + \
            pd.Series(np.where((self.df['remark'].str.contains(GUAR_ABNORMAL)) &
                               (self.df['trans_amt'] < 0) &
                               (self.df['op_name'] != ""), '对外担保异常', '')) + ';' + \
            pd.Series(np.where((self.df['concat_str'].str.contains(NORITOMO)) &
                               (self.df['op_name'] != ""), '典当', ''))
        # 全部未命中的行赋值为空值
        self.df['unusual_trans_type'] = np.where(self.df['unusual_trans_type'].str.replace(';', '') == '', None, self.df['unusual_trans_type'])
        self.df['cost_type'] = np.where(
            (self.df['no_channel_str'].str.contains(SALARY)) & (self.df['trans_amt'] < 0), '工资', np.where(
                (self.df['no_channel_str'].str.contains(UTILITIES)) & (self.df['trans_amt'] < 0), '水电', np.where(
                    (self.df['no_channel_str'].str.contains(TAX)) & (self.df['trans_amt'] < 0), '税费', np.where(
                        (self.df['no_channel_str'].str.contains(RENT)) & (self.df['trans_amt'] < 0), '房租', np.where(
                            (self.df['no_channel_str'].str.contains(INSURANCE)) & (self.df['trans_amt'] < 0), '保险',
                            np.where((self.df['no_channel_str'].str.contains(VARIABLE_COST)) &
                                     (self.df['trans_amt'] < 0), '可变成本', None))))))
        # for row in self.df.itertuples():
        #     # 将合并列拉出来
        #     concat_str = getattr(row, 'concat_str')
        #     no_channel_str = getattr(row, 'no_channel_str')
        #     no_oppo_channel_str = getattr(row, 'no_oppo_channel_str')
        #     op_name = getattr(row, 'op_name')
        #     trans_amt = getattr(row, 'trans_amt')
        #     # loan_type = getattr(row, 'loan_type')
        #     remark = getattr(row, 'remark')
        #     # 异常交易类型
        #     unusual_type = []
        #     # 博彩
        #     if (re.search(GAMBLE, concat_str) and
        #         (trans_amt < 0 or (trans_amt > 0 and re.search(GAMBLE_INCOME, no_oppo_channel_str)))) \
        #             and op_name != "":
        #         unusual_type.append('博彩')
        #     # 娱乐
        #     if (re.search(AMUSEMENT, concat_str) and trans_amt < 0 and
        #         re.search(AMUSEMENT_EXCEPT, concat_str) is None) \
        #             and op_name != "":
        #         unusual_type.append('娱乐')
        #     # 案件纠纷
        #     if re.search(CASE_DISPUTES, op_name) and trans_amt < 0:
        #         unusual_type.append('案件纠纷')
        #     # 治安罚款
        #     if (re.search(SECURITY_EXPENSE_FINES, op_name) and (trans_amt < 0)) \
        #             or (re.search(SECURITY_FINES, no_channel_str) and
        #                 re.search(SECURITY_FINES_EXCEPT, concat_str) is None):
        #         unusual_type.append('治安罚款')
        #     # 保险理赔
        #     if re.search(INSURANCE_CLAIMS, concat_str) and op_name != "":
        #         unusual_type.append('保险理赔')
        #     # 股票期货
        #     if re.search(STOCK_OPPONENT_NAME, op_name) and re.search(STOCK_REMARK, remark):
        #         unusual_type.append('股票期货')
        #     # 医院
        #     if self.user_type == "PERSONAL" and trans_amt < 0 and re.search(HOSPITAL, concat_str) and \
        #             re.search(HOSPITAL_EXCEPT, concat_str) is None and op_name != "":
        #         unusual_type.append('医院')
        #
        #     # 贷款异常
        #     if trans_amt < 0 and ((hasattr(row, 'loan_type') and
        #                            getattr(row, 'loan_type') == '担保' and
        #                            re.search(LOAN_GUAR_ABNORMAL, concat_str)) or
        #                           (re.search(LOAN_ABNORMAL, concat_str))) \
        #             and op_name != "":
        #         unusual_type.append('贷款异常')
        #     # 对外担保异常
        #     if trans_amt < 0 and re.search(GUAR_ABNORMAL, remark) and op_name != "":
        #         unusual_type.append('对外担保异常')
        #     # 典当
        #     if re.search(NORITOMO, concat_str) and op_name != "":
        #         unusual_type.append('典当')
        #
        #     # 添加标签到df
        #     if len(unusual_type) > 0:
        #         self.df.loc[row.Index, 'unusual_trans_type'] = ';'.join(unusual_type)
        #     else:
        #         self.df.loc[row.Index, 'unusual_trans_type'] = None
        #
        #     # 成本支出类别标签
        #     if re.search(SALARY, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '工资'
        #     elif re.search(UTILITIES, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '水电'
        #     elif re.search(TAX, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '税费'
        #     elif re.search(RENT, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '房租'
        #     elif re.search(INSURANCE, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '保险'
        #     elif re.search(VARIABLE_COST, no_channel_str):
        #         self.df.loc[row.Index, 'cost_type'] = '可变成本'
        #     else:
        #         self.df.loc[row.Index, 'cost_type'] = None

    def usual_trans_type(self):
        self.df['date'] = self.df['trans_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
        no_oppo_list = ['trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[no_oppo_list] = self.df[no_oppo_list].fillna('').astype(str)
        # 将字符串列合并到一起
        # self.df['no_oppo_str'] = self.df.apply(lambda x: ';'.join(x[no_oppo_list]), axis=1)
        self.df['no_oppo_str'] = self.df['trans_channel'] + ';' + self.df['trans_type'] + ';' + \
                                 self.df['trans_use'] + ';' + self.df['remark']
        # 其他画像之是否整进整出标签
        # 20220913 整进整出金额调整为10万的整万
        big_in_out_df = self.df[(self.df.trans_amt.apply(lambda x: abs(x)) >= 100000) &
                                (self.df.trans_amt.apply(lambda x: x % 10000) == 0) &
                                (~self.df.concat_str.str.contains(BIG_IN_OUT_EXCEPT))]
        big_in_date = big_in_out_df[big_in_out_df.trans_amt > 0]['date'].tolist()
        big_out_date = big_in_out_df[big_in_out_df.trans_amt < 0]['date'].tolist()
        big_in_out_date = list(set(big_in_date).intersection(set(big_out_date)))
        big_in_out_list = big_in_out_df[big_in_out_df.date.isin(big_in_out_date)].index.tolist()
        self.df.loc[big_in_out_list, 'big_in_out'] = "整进整出"

        # 其他画像之快进快出标签
        # 20220913 快进快出金额调整为20万
        fast_in_out_df = self.df[(self.df.trans_amt.apply(lambda x: abs(x)) >= 200000) &
                                 (self.df.opponent_name != '') &
                                 (~self.df.opponent_name.str.contains(FAST_IN_OUT_OPPONENT_NAME_EXCEPT)) &
                                 (~self.df.concat_str.str.contains(FAST_IN_OUT_EXCEPT))]
        fast_in_date = fast_in_out_df[fast_in_out_df.trans_amt > 0]['date'].tolist()
        fast_out_date = fast_in_out_df[fast_in_out_df.trans_amt < 0]['date'].tolist()
        fast_in_out_date = list(set(fast_in_date).intersection(set(fast_out_date)))
        fast_in_out_list = fast_in_out_df[fast_in_out_df.date.isin(fast_in_out_date)].index.tolist()
        self.df.loc[fast_in_out_list, 'fast_in_out'] = "快进快出"

        # 其他画像之家庭不稳定标签
        self.df.loc[(self.df['opponent_name'] != self.spouse_name) & (pd.isna(self.df['relationship'])) &
                    (self.df['opponent_type'] == 1) &
                    (self.df['trans_amt'].abs().isin(FAMILY_RISK_AMT)), 'family_risk'] = '家庭不稳定'

        # 其他画像之理财行为标签
        # 理财行为取值逻辑：组合列含有到期、赎回、理财收益 且交易金额 >0 或含有认购、买入、理财本金 且交易金额 < 0
        financing_df = self.df[((self.df.trans_amt > 0) & (self.df.no_oppo_str.str.contains(FINANCING_INCOME))) |
                               ((self.df.trans_amt < 0) & (self.df.no_oppo_str.str.contains(FINANCING_EXPENSE)))]
        financing_list = financing_df.index.tolist()
        self.df.loc[financing_list, 'financing'] = "理财行为"

        # 其他画像之房产买卖
        # 房产买卖取值逻辑：组合列含有买房、卖房、首付、售房、房地产、置业、购房、认筹、房产评估费、房屋评估费、房款 且不包含装修 且交易对手不包含买房、卖房、首付、售房、房地产、置业、购房、认筹 且交易对手不为空
        house_sale_df = self.df[(self.df.concat_str.str.contains(HOUSE_TRADE)) &
                                (~self.df.concat_str.str.contains(HOUSE_TRADE_EXCEPT_1)) &
                                (self.df.opponent_name.astype(str).str.contains(HOUSE_OPPO)) &
                                (self.df.trans_amt.abs() >= 2e4)]
        house_sale_list = house_sale_df.index.tolist()
        self.df.loc[house_sale_list, 'house_sale'] = "房产买卖"
        self.df['usual_trans_type'] = self.df.apply(
            lambda x: ','.join([y for y in [x['big_in_out'], x['fast_in_out'], x['family_risk'],
                                            x['financing'], x['house_sale']] if pd.notna(y)]), axis=1)
        self.df.drop(['big_in_out', 'fast_in_out', 'family_risk', 'financing', 'house_sale'], axis=1, inplace=True)

    def _unusual_type_label_third_pay(self):
        self.df['date'] = self.df['trans_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
        self.df['unusual_trans_type'] = \
            pd.Series(np.where((self.df['opponent_name'].str.contains(GAMBLE)) |
                               (self.df['remark'].str.contains(GAMBLE)), '博彩', '')) + ';' + \
            pd.Series(np.where((self.df['opponent_name'].str.contains(AMUSEMENT)) |
                               (self.df['remark'].str.contains(AMUSEMENT)), '娱乐', '')) + ';' + \
            pd.Series(np.where(((self.df['opponent_name'].str.contains(STOCK_OPPONENT_NAME)) |
                                (self.df['remark'].str.contains(STOCK_REMARK))) &
                               (self.df['opponent_name'].str.contains(WXZFB_STOCK_OPPONENT_NAME_EXCEPT)),
                               '股票期货', '')) + ';' + \
            pd.Series(np.where((self.df['trans_amt'] < 0) &
                               (((self.df['opponent_name'].str.contains(HOSPITAL)) &
                                 (~self.df['opponent_name'].str.contains(HOSPITAL_EXCEPT))) |
                                (self.df['remark'].str.contains(HOSPITAL))), '医院', ''))
        # 全部未命中的行赋值为空值
        self.df['unusual_trans_type'] = np.where(self.df['unusual_trans_type'].str.replace(';', '') == '', None, self.df['unusual_trans_type'])

        # for row in self.df.itertuples():
        #     opponent_name = getattr(row, 'opponent_name')
        #     remark = getattr(row, 'remark')
        #     trans_amt = getattr(row, 'trans_amt')
        #     # 异常交易类型
        #     unusual_type = []
        #     # 医院
        #     if ((re.search(HOSPITAL, opponent_name) and re.search(HOSPITAL_EXCEPT, opponent_name) is None) or
        #         re.search(HOSPITAL, remark)) and trans_amt < 0:
        #         unusual_type.append('医院')
        #     # 娱乐
        #     if re.search(AMUSEMENT, opponent_name) or re.search(AMUSEMENT, remark):
        #         unusual_type.append('娱乐')
        #     # 博彩
        #     if re.search(GAMBLE, opponent_name) or re.search(GAMBLE, remark):
        #         unusual_type.append('博彩')
        #     # 股票期货
        #     if (re.search(STOCK_OPPONENT_NAME, opponent_name) or re.search(STOCK_REMARK, remark)) \
        #             and re.search(WXZFB_STOCK_OPPONENT_NAME_EXCEPT, opponent_name) is None:
        #         unusual_type.append('股票期货')
        #     # 写入原df
        #     if len(unusual_type) > 0:
        #         self.df.loc[row.Index, 'unusual_trans_type'] = ';'.join(unusual_type)
        #     else:
        #         self.df.loc[row.Index, 'unusual_trans_type'] = None

    @staticmethod
    def _opponent_type(op_name):
        if len(op_name) > 6 and re.search(ENT_TYPE, op_name) is not None:
            return 2
        else:
            if len(op_name) <= 15:
                cleaned_name = re.sub(TYPE_EXCEPT_1, '', op_name)
                if re.match(TYPE_START_1, cleaned_name):
                    cleaned_name = re.sub(TYPE_EXCEPT_2, '', cleaned_name)
                elif re.match(TYPE_START_2, cleaned_name):
                    cleaned_name = cleaned_name.split()[-1]
                else:
                    cleaned_name = re.sub(r' ', '', cleaned_name)
                if 2 <= len(cleaned_name) <= 3:
                    if re.search(TYPE_EXCEPT_3, cleaned_name) is None and \
                            re.match(TYPE_EXCEPT_4, cleaned_name) is None:
                        return 1

    def _in_out_order(self):
        income_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 1) & (pd.isna(self.df.loan_type)) & (
                                    pd.isna(self.df.unusual_trans_type)) &
                                (~self.df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) & (
                                    ~self.df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
        expense_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 1) & (pd.isna(self.df.loan_type)) & (
                                     pd.isna(self.df.unusual_trans_type)) &
                                 (~self.df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) & (
                                     ~self.df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
        income_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 2) & (pd.isna(self.df.loan_type)) & (
                                    pd.isna(self.df.unusual_trans_type))]
        income_com_df = income_com_df[
            (~income_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~income_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        expense_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 2) & (pd.isna(self.df.loan_type)) & (
                                     pd.isna(self.df.unusual_trans_type))]
        expense_com_df = expense_com_df[
            (~expense_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~expense_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        income_per_cnt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
                                  sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        income_per_amt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
                                  sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_per_cnt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
                                   sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_per_amt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
                                   sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
        income_com_cnt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
                                  sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        income_com_amt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
                                  sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_com_cnt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
                                   sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_com_amt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
                                   sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
        for i in range(len(income_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(income_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(expense_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(expense_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(income_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(income_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(expense_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_amt_list[i], 'expense_amt_order'] = i + 1
        for i in range(len(expense_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_amt_list[i], 'expense_amt_order'] = i + 1

    def save_raw_data(self):
        # 原始数据列名
        self.df.rename({'id': 'flow_id'}, axis=1, inplace=True)
        self.df['account_id'] = self.account_id
        self.df['report_req_no'] = self.report_req_no
        self.df['trans_flow_src_type'] = self.df['trans_flow_src_type'].apply(lambda x: 1 if x in (2, 3) else 0)
        self.df['trans_date'] = self.df['trans_time'].apply(lambda x: x.date())
        self.df['trans_time'] = self.df['trans_time'].apply(lambda x: format(x, '%H:%M:%S'))
        self.df['remark_type'] = self.df['remark']
        self.df['is_financing'] = None
        self.df['phone'] = None
        # self.df['is_sensitive'] = self.df.apply(
        #     lambda x: 1 if pd.notna(x['loan_type']) or pd.notna(x['unusual_trans_type']) else None, axis=1)
        self.df['is_sensitive'] = np.where((pd.notna(self.df['loan_type'])) | (pd.notna(self.df['unusual_trans_type'])), 1, None)
        return self.df

    def bank_transform(self, bank):
        bank = re.sub(r'[^\u4e00-\u9fa5]|流水|商业|村镇', '', bank)
        # 中国银行 中国建设银行 中国建设银行股份有限公司
        if bank.find('中国') != -1 and bank.find('中国银行') == -1:
            bank = re.sub(r'中国', '', bank)
        # 农行
        bank_dic = {'农行': '农业银行', '工行': '工商银行', '招行': '招商银行', '中行': '中国银行',
                    '建行': '建设银行', '邮政储蓄': '邮储', '邮政': '邮储', '浦东发展': '浦发'
                    }
        for key, value in bank_dic.items():
            bank = re.sub(key, value, bank)
        # 上饶农商行
        bank = re.sub(r'农商行|农村银行|农村信用合作社|农村信用社|农商银行|农信社', '农信', bank)
        # 民生
        bank = bank + '银行' if len(bank) == 2 else bank
        if bank.find('银行') != -1:
            bank = bank[bank.index(r'银行') - 2:bank.index(r'银行') + 2]
        elif bank.find('农信') != -1:
            bank = bank[bank.index(r'农信') - 2:bank.index(r'农信') + 2]
        return bank

    def _wxzfb_relation_label(self):
        # 充值提现零钱通类型处理
        concat_list = ['opponent_name', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        self.df['concat_str'] = self.df['opponent_name'] + ';' + self.df['trans_channel'] + ';' + \
                                self.df['trans_type'] + ';' + self.df['trans_use'] + ';' + self.df['remark']
        culling = r'转入零钱通|零钱通转出|零钱提现|零钱充值|经营账户提现|微信提现'
        self.df.loc[self.df['concat_str'].str.contains(culling), 'relationship'] = '充值提现'
        # # 微信银行卡重复类关键词
        # condition_culling = ['群收款', '扫二维码付款', '付款码付款', '商户消费', '微信红包（单发）', '微信红包（群红包）',
        #                      '微信红包（面对面红包）', '企业微信红包', '信用卡还款', '分付还款', '赞赏码', '亲属卡交易', '购买理财通']
        # 所有账户下的开户行以及卡号后四位信息
        risk_subject_id_list = [i['riskSubjectId'] for i in self.query_data_array]
        if len(risk_subject_id_list) > 0:
            sql = """
                select account_name, bank, account_no, start_time, end_time from trans_account 
                    where risk_subject_id in %(risk_subject_id_list)s
            """
            temp_df = sql_to_df(sql=sql, params={"risk_subject_id_list": risk_subject_id_list})
            # 只保留为纯数字且长度大于等于4的账户号
            temp_df['account_no'] = temp_df['account_no'].apply(lambda x: re.sub(r'\D', '', x)[-4:])
            temp_df = temp_df[(temp_df['account_no'].str.len() == 4)]
            if temp_df.shape[0] > 0:
                # 将用户填写的银行名转换为四字形式
                temp_df['bank'] = temp_df.bank.apply(self.bank_transform)
                temp_df['bank_account'] = temp_df['bank'] + '(' + temp_df['account_no'] + ')'
                temp_df.drop_duplicates(inplace=True)

                # 微信银行卡重复类剔除微信流水，只在微信部分打标签
                if self.df['trans_flow_src_type'][0] == 3:
                    for row in temp_df.itertuples():
                        bank_account = getattr(row, 'bank_account')
                        statr_time, end_time = getattr(row, 'start_time'), getattr(row, 'end_time')
                        self.df.loc[(self.df['trans_type'] == bank_account) &
                                    (~self.df['trans_use'].str.contains(culling)) &
                                    ((self.df['trans_time'] >= statr_time) &
                                     (self.df['trans_time'] <= end_time)), 'relationship'] = '交叉剔除'
