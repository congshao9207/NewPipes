import xmltodict
import datetime
import pandas as pd
from creditreport.company.enumerate import *
from creditreport.tables import transform_enumerate, transform_amount, transform_count, \
    transform_date, transform_class_str, transform_not_null, transform_dict
from util.mysql_reader import DB_ENGINE
from sqlalchemy.orm import sessionmaker, scoped_session
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_dataframe(msg, keys):
    res = pd.DataFrame()
    for key in keys:
        if key in msg:
            data = msg[key]
            if type(data) != list:
                data = [data]
            res = pd.concat([res, pd.DataFrame(data)], axis=1)
    return res


class ECreditReport:

    def __init__(self, xml_data, report_id):
        self.report_msg = xmltodict.parse(xml_data.getvalue().decode('utf-8'))['Document']
        # self.res_code = self.report_msg['ResultCode']
        # if self.res_code not in ['AAA000', 'AAA001']:
        #     raise Exception('该主体无已查得征信报告')
        self.msg = self.report_msg
        self.report_id = report_id
        self.now = format(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.session = scoped_session(session_factory=sessionmaker(bind=DB_ENGINE))
        self.dfs = {}

    def save_data(self):
        self.head_info()
        self.summary_info()
        self.basic_info()
        self.loan_info()
        self.repay_duty_info()
        self.public_info()
        self.financial_info()
        self.credit_parse_request()
        for key, value in self.dfs.items():
            value['report_id'] = self.report_id
            transform_class_str(value.to_dict('records'), key)
        return self.dfs

    def head_info(self):
        # 报告头信息
        logger.info('开始处理报告头信息')
        report_head = self.msg['EAA']['EA01']
        head_df = to_dataframe(report_head, ['EA01A', 'EA01B', 'EA01C'])
        if 'EA01CH' in head_df:
            head_df.drop('EA01CH', axis=1, inplace=True)
            id_df = to_dataframe(report_head['EA01C'], ['EA01CH'])
            id_df = id_df.set_index('EA01CD01').T.reset_index(drop=True)
            head_df = pd.concat([head_df, id_df], axis=1)
        head_df.rename(column_mapping, axis=1, inplace=True)
        transform_date(head_df, ['report_time'])
        head_df['created_by'] = 'Pipes'
        head_df['created_date'] = self.now
        head_df['last_modified_date'] = self.now
        self.dfs['EcreditBaseInfo'] = head_df

    def summary_info(self):
        # 信贷交易提示信息及各类汇总信息
        logger.info('开始处理信贷交易提示信息及各类汇总信息')
        unsettled_his_df = pd.DataFrame()  # 借贷交易+担保交易均放入此表
        settled_his_df = pd.DataFrame()
        # 信用提示信息
        if 'EBA' in self.msg and 'EB01' in self.msg['EBA']:
            summary_info = self.msg['EBA']['EB01']
            summary_df = to_dataframe(summary_info, ['EB01A', 'EB01B'])
            if summary_df.shape[0] > 0:
                summary_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(summary_df, ['loan_bal', 'loan_recover_bal', 'loan_special_mentioned_bal',
                                              'loan_non_performing_bal', 'secured_bal', 'secured_special_mentioned_bal',
                                              'secured_non_performing_bal'])
                transform_count(summary_df, ['loan_org_num', 'remain_loan_org_num', 'not_loan_account_num',
                                             'owing_taxes_num', 'judge_num', 'enforce_num', 'admin_penalty_num'])
                self.dfs['EcreditInfoOutline'] = summary_df
        # 借贷交易汇总信息
        if 'EBB' in self.msg and 'EB02' in self.msg['EBB']:
            loan_smy_info = self.msg['EBB']['EB02']
            # 未结清借贷交易汇总
            unsettled_smy_df = to_dataframe(loan_smy_info, ['EB02A'])
            if unsettled_smy_df.shape[0] > 0:
                if unsettled_smy_df.shape[1] > 2:
                    unsettled_smy_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(unsettled_smy_df, ['dispose_balance', 'advance_balance', 'overdue_amt',
                                                        'overdue_principal', 'overdue_interest'])
                    transform_count(unsettled_smy_df, ['dispose_account_num', 'advance_account_num'])
                    transform_date(unsettled_smy_df, ['last_dispose_date', 'last_repay_date'])
                    self.dfs['EcreditAssetsOutline'] = unsettled_smy_df
                if 'EB02AH' in unsettled_smy_df:
                    unsettled_loan_df = to_dataframe(loan_smy_info['EB02A'], ['EB02AH'])
                    unsettled_loan_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(unsettled_loan_df, ['balance'])
                    transform_count(unsettled_loan_df, ['account_num'])
                    unsettled_his_df = pd.concat([unsettled_his_df, unsettled_loan_df], axis=0, ignore_index=True)
            # 已结清借贷交易汇总 todo:汇总信息未落库
            settled_smy_df = to_dataframe(loan_smy_info, ['EB02B'])
            if settled_smy_df.shape[0] > 0:
                if settled_smy_df.shape[1] > 2:
                    settled_smy_df.rename(column_mapping, axis=1, inplace=True)
                    if settled_smy_df.shape[0] > 0:
                        transform_amount(settled_smy_df, ['dispose_balance', 'advance_balance'])
                        transform_count(settled_smy_df, ['dispose_account_num', 'advance_account_num'])
                        transform_date(settled_smy_df, ['dispose_end_date', 'loan_end_date'])
                        # self.dfs['EcreditSettledOutline'] = settled_smy_df
                if 'EB02BH' in settled_smy_df:
                    settled_loan_df = to_dataframe(loan_smy_info['EB02B'], ['EB02BH'])
                    settled_loan_df.rename(column_mapping, axis=1, inplace=True)
                    transform_count(settled_loan_df, ['account_num'])
                    # transform_enumerate(settled_loan_df, ['status_type'], [status_type_mapping], ['9'])
                    type_list = settled_loan_df.loan_type.unique().tolist()
                    for t in type_list:
                        temp_df = settled_loan_df[settled_loan_df['loan_type'] == t]
                        temp_df.drop(['loan_type'], axis=1, inplace=True)
                        temp_df = temp_df.set_index('status_type').T.reset_index(drop=True)
                        temp_df['loan_type'] = t
                        temp_df.rename(column_mapping, axis=1, inplace=True)
                        settled_his_df = pd.concat([settled_his_df, temp_df], axis=0, ignore_index=True)
            # 负债历史汇总 todo:存在冗余信息
            if 'EB02C' in loan_smy_info and 'EB02CH' in loan_smy_info['EB02C']:
                debt_smy_df = to_dataframe(loan_smy_info['EB02C'], ['EB02CH'])
                if debt_smy_df.shape[0] > 0:
                    debt_smy_df.rename(column_mapping, axis=1, inplace=True)
                    df1 = debt_smy_df[(pd.notna(debt_smy_df['account_num1'])) & (debt_smy_df['account_num1'] != '0')] \
                        if 'account_num1' in debt_smy_df else pd.DataFrame()
                    df2 = debt_smy_df[(pd.notna(debt_smy_df['account_num2'])) & (debt_smy_df['account_num2'] != '0')] \
                        if 'account_num2' in debt_smy_df else pd.DataFrame()
                    df3 = debt_smy_df[(pd.notna(debt_smy_df['account_num3'])) & (debt_smy_df['account_num3'] != '0')] \
                        if 'account_num3' in debt_smy_df else pd.DataFrame()
                    df1.rename({'account_num1': 'account_num', 'balance1': 'balance'}, axis=1, inplace=True)
                    df1['status_type'] = '全部负债'
                    df2.rename({'account_num2': 'account_num', 'balance2': 'balance'}, axis=1, inplace=True)
                    df2['status_type'] = '关注类负债'
                    df3.rename({'account_num3': 'account_num', 'balance3': 'balance'}, axis=1, inplace=True)
                    df3['status_type'] = '不良类负债'
                    debt_smy_df = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
                    if debt_smy_df.shape[0] > 0:
                        transform_amount(debt_smy_df, ['balance'])
                        transform_count(debt_smy_df, ['account_num'])
                        self.dfs['EcreditDebtHistor'] = debt_smy_df
        # 担保交易汇总信息
        if 'EBC' in self.msg and 'EB03' in self.msg['EBC']:
            guar_smy_info = self.msg['EBC']['EB03']
            # 未结清担保
            if 'EB03A' in guar_smy_info and 'EB03AH' in guar_smy_info['EB03A']:
                unsettled_guar_df = to_dataframe(guar_smy_info['EB03A'], ['EB03AH'])
                if unsettled_guar_df.shape[0] > 0:
                    unsettled_guar_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(unsettled_guar_df, ['balance'])
                    transform_count(unsettled_guar_df, ['account_num'])
                    unsettled_his_df = pd.concat([unsettled_his_df, unsettled_guar_df], axis=0, ignore_index=True)
            # 已结清担保
            if 'EB03B' in guar_smy_info and 'EB03BH' in guar_smy_info['EB03B']:
                settled_guar_df = to_dataframe(guar_smy_info['EB03B'], ['EB03BH'])
                if settled_guar_df.shape[0] > 0:
                    settled_guar_df.rename(column_mapping, axis=1, inplace=True)
                    transform_count(settled_guar_df, ['account_num'])
                    # transform_enumerate(settled_guar_df, ['status_type'], [status_type_mapping], ['9'])
                    type_list = settled_guar_df.loan_type.unique().tolist()
                    for t in type_list:
                        temp_df = settled_guar_df[settled_guar_df['loan_type'] == t]
                        temp_df.drop(['loan_type'], axis=1, inplace=True)
                        temp_df = temp_df.set_index('status_type').T.reset_index(drop=True)
                        temp_df['loan_type'] = t
                        temp_df.rename(column_mapping, axis=1, inplace=True)
                        settled_his_df = pd.concat([settled_his_df, temp_df], axis=0, ignore_index=True)
        if unsettled_his_df.shape[0] > 0:
            self.dfs['EcreditUnclearedOutline'] = unsettled_his_df
        if settled_his_df.shape[0] > 0:
            self.dfs['EcreditSettleOutline'] = settled_his_df
        # 授信协议汇总信息
        if 'EBD' in self.msg and 'EB04' in self.msg['EBD']:
            agreement_smy_df = to_dataframe(self.msg['EBD'], ['EB04'])
            agreement_smy_df.rename(column_mapping, axis=1, inplace=True)
            transform_amount(agreement_smy_df, ['acyclic_total_amt', 'acyclic_used_amt', 'acyclic_surplus_amt',
                                                'cycle_total_amt', 'cycle_used_amt', 'cycle_surplus_amt'])
            self.dfs['EcreditCreditOutline'] = agreement_smy_df
        # 相关还款责任汇总信息
        if 'EBE' in self.msg and 'EB05' in self.msg['EBE']:
            duty_smy_info = self.msg['EBE']['EB05']
            # 借贷交易相关还款责任
            loan_duty_df = to_dataframe(duty_smy_info, ['EB05A'])
            if loan_duty_df.shape[0] > 0:
                loan_duty_df.rename(column_mapping, axis=1, inplace=True)
                df1 = loan_duty_df[pd.notna(loan_duty_df['duty_amt1'])] \
                    if 'duty_amt1' in loan_duty_df else pd.DataFrame()
                df2 = loan_duty_df[pd.notna(loan_duty_df['duty_amt2'])] \
                    if 'duty_amt2' in loan_duty_df else pd.DataFrame()
                df1['business_type'] = '被追偿业务'
                df2['business_type'] = '其他借贷交易'
                df1.drop(['duty_amt2', 'account_num2', 'balance2', 'non_performing_bal', 'special_mentioned_bal'],
                         axis=1, inplace=True, errors='ignore')
                df2.drop(['duty_amt1', 'account_num1', 'balance1'], axis=1, inplace=True, errors='ignore')
                df1.rename({'duty_amt1': 'duty_amt', 'account_num1': 'account_num', 'balance1': 'balance'},
                           axis=1, inplace=True)
                df2.rename({'duty_amt2': 'duty_amt', 'account_num2': 'account_num', 'balance2': 'balance'},
                           axis=1, inplace=True)
                loan_duty_df = pd.concat([df1, df2], axis=0, ignore_index=True)
            # 担保交易相关还款责任
            guar_duty_df = to_dataframe(duty_smy_info, ['EB05B'])
            if guar_duty_df.shape[0] > 0:
                guar_duty_df.rename(column_mapping, axis=1, inplace=True)
                guar_duty_df['business_type'] = '担保交易'
                loan_duty_df = pd.concat([loan_duty_df, guar_duty_df], axis=0, ignore_index=True)
            if loan_duty_df.shape[0] > 0:
                transform_amount(loan_duty_df, ['duty_amt', 'balance', 'non_performing_bal', 'special_mentioned_bal'])
                transform_count(loan_duty_df, ['account_num'])
                self.dfs['EcreditRepayDutyOutline'] = loan_duty_df

    def basic_info(self):
        # 基本信息
        logger.info('正在处理基本信息')
        if 'ECA' in self.msg:
            # 企业基本信息
            basic_info = self.msg['ECA']
            face_df = to_dataframe(basic_info, ['EC01'])
            if face_df.shape[0] > 0:
                face_df.rename(column_mapping, axis=1, inplace=True)
                # transform_date(face_df, ['reg_cert_duedate'])
                self.dfs['EcreditGeneralizeInfo'] = face_df
            # 投资人信息
            investor_df = to_dataframe(basic_info, ['EC02'])
            if investor_df.shape[0] > 0:
                investor_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(investor_df, ['registered_capital'])
                transform_date(investor_df, ['update_date'])
                self.dfs['EcreditBaseInfo']['registered_capital'] = investor_df.loc[0, 'registered_capital']
                update_date = investor_df.loc[0, 'update_date']
                if 'EC020H' in investor_df:
                    investor_df = to_dataframe(basic_info['EC02'], ['EC020H'])
                    investor_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(investor_df, ['investor_rate'])
                    investor_df['investor_rate'] = investor_df['investor_rate'] / 100 \
                        if 'investor_rate' in investor_df else 0
                    investor_df['update_date'] = update_date
                    self.dfs['EcreditInvestorInfo'] = investor_df
            # 股东信息
            shareholder_df = to_dataframe(basic_info, ['EC03'])
            if shareholder_df.shape[0] > 0:
                shareholder_df.rename(column_mapping, axis=1, inplace=True)
                transform_date(shareholder_df, ['update_date'])
                update_date = shareholder_df.loc[0, 'update_date']
                if 'EC030H' in shareholder_df:
                    shareholder_df = to_dataframe(basic_info['EC03'], ['EC030H'])
                    shareholder_df.rename(column_mapping, axis=1, inplace=True)
                    shareholder_df['update_date'] = update_date
                    self.dfs['EcreditPersonConstituteInfo'] = shareholder_df
            # 上级机构信息
            superior_df = to_dataframe(basic_info, ['EC04'])
            if superior_df.shape[0] > 0:
                superior_df.rename(column_mapping, axis=1, inplace=True)
                transform_date(superior_df, ['update_date'])
                update_date = superior_df.loc[0, 'update_date']
                if 'EC040H' in superior_df:
                    superior_df = to_dataframe(basic_info['EC04'], ['EC040H'])
                    superior_df.rename(column_mapping, axis=1, inplace=True)
                    superior_df['update_date'] = update_date
                    self.dfs['EcreditSuperiorOrg'] = superior_df
            # 实际控制人信息
            control_df = to_dataframe(basic_info, ['EC05'])
            if control_df.shape[0] > 0:
                control_df.rename(column_mapping, axis=1, inplace=True)
                transform_date(control_df, ['update_date'])
                update_date = control_df.loc[0, 'update_date']
                if 'EC050H' in control_df:
                    control_df = to_dataframe(basic_info['EC05'], ['EC050H'])
                    control_df.rename(column_mapping, axis=1, inplace=True)
                    control_df['update_date'] = update_date
                    self.dfs['EcreditControlsPerson'] = control_df

    def loan_info(self):
        # 借贷账户信息
        logger.info('正在处理借贷账户信息')
        credit_biz_df = pd.DataFrame()
        draft_lc_df = pd.DataFrame()
        if 'EDA' in self.msg:
            loan_acc_info = self.msg['EDA']
            # 借贷账户信息
            if 'ED01' in loan_acc_info:
                loan_info = loan_acc_info['ED01']
                if type(loan_info) != list:
                    loan_info = [loan_info]
                loan_df = pd.DataFrame()
                history_df = pd.DataFrame()
                for i, each in enumerate(loan_info):
                    loan_dict = {'index': i}
                    if 'ED01A' in each:
                        loan_dict.update(each['ED01A'])
                    if 'ED01B' in each and 'ED01BH' in each['ED01B']:
                        temp_df = to_dataframe(each['ED01B'], ['ED01BH'])
                        temp_df['index'] = i
                        history_df = pd.concat([history_df, temp_df], axis=0, ignore_index=True)
                        temp_list = each['ED01B']['ED01BH']
                        if type(temp_list) == list:
                            temp_list = temp_list[0] if len(temp_list) > 0 else {}
                        loan_dict.update(temp_list)
                    if 'ED01C' in each and 'ED01CH' in each['ED01C']:
                        temp_list = each['ED01C']['ED01CH']
                        if type(temp_list) == list:
                            temp_list = temp_list[0] if len(temp_list) > 0 else {}
                        loan_dict.update(temp_list)
                    loan_dict = {k: v[0] if type(v) == list and len(v) > 0 else v for k, v in loan_dict.items()}
                    loan_df = loan_df.append(loan_dict, ignore_index=True)
                if loan_df.shape[0] > 0:
                    loan_df.drop(['ED01BR01'], axis=1, inplace=True, errors='ignore')
                    loan_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(loan_df, ['amount', 'balance', 'overdue_amt', 'overdue_principal'])
                    transform_count(loan_df, ['overdue_period', 'surplus_repay_period'])
                    transform_date(loan_df, ['loan_date', 'end_date', 'stats_date', 'last_repay_date'])
                    if 'settle_status' not in loan_df:
                        loan_df['settle_status'] = None
                    loan_df.loc[(loan_df['account_type'].astype(str).str.contains("被追偿")) &
                                (pd.isna(loan_df['settle_status'])), 'settle_status'] = '被追偿业务'
                    transform_not_null(loan_df, ['settle_status', 'account_type'], ['未知', '未知'])
                    loan_df['report_id'] = self.report_id
                    role_list = loan_df.to_dict('records')
                    role_map = {}
                    for role in role_list:
                        ecredit_loan = transform_dict(role, 'EcreditLoan')
                        self.session.add(ecredit_loan)
                        self.session.commit()
                        role_map[role['index']] = ecredit_loan.id
                    if history_df.shape[0] > 0:
                        history_df.rename(column_mapping, axis=1, inplace=True)
                        transform_amount(history_df, ['balance', 'last_actual_payamount', 'last_agreed_amt',
                                                      'overdue_amt', 'overdue_principal'])
                        transform_count(history_df, ['overdue_period', 'surplus_repay_period'])
                        transform_date(history_df, ['stats_date', 'bal_update_date', 'category_date',
                                                    'last_actual_paydate', 'last_agreed_paydate'])
                        history_df['loan_id'] = history_df['index'].map(role_map)
                        self.dfs['EcreditHistorPerfo'] = history_df
            # 贴现账户分机构汇总信息
            if 'ED02' in loan_acc_info:
                discount_df = to_dataframe(loan_acc_info, ['ED02'])
                if discount_df.shape[0] > 0:
                    discount_df.rename(column_mapping, axis=1, inplace=True)
                    df1 = discount_df[pd.notna(discount_df['balance'])] if 'balance' in discount_df else pd.DataFrame()
                    df2 = discount_df[pd.notna(discount_df['discount_amt'])] \
                        if 'discount_amt' in discount_df else pd.DataFrame()
                    df1.drop(['account_num2', 'discount_amt'], axis=1, inplace=True, errors='ignore')
                    df2.drop(['account_num1', 'balance', 'overdue_amt', 'overdue_principal'],
                             axis=1, inplace=True, errors='ignore')
                    df1.rename({'account_num1': 'account_num'}, axis=1, inplace=True)
                    df2.rename({'account_num2': 'account_num'}, axis=1, inplace=True)
                    df1['settle_status'] = '未结清信贷'
                    df2['settle_status'] = '已结清信贷'
                    credit_biz_df = pd.concat([credit_biz_df, df1, df2], axis=0, ignore_index=True)
                    credit_biz_df['ignore_label'] = 1  # 这里的数据没办法和draft_lc表格对应
            # 欠息信息ED03 todo:无数据未落库
        # 担保账户
        if 'EDB' in self.msg:
            guar_acc_info = self.msg['EDB']
            if 'ED04' in guar_acc_info:
                guar_info = guar_acc_info['ED04']
                for each in guar_info:
                    guar_dict = {}
                    if 'ED04A' in each:
                        guar_dict.update(each['ED04A'])
                    if 'ED04B' in each:
                        guar_dict.update(each['ED04B'])
                    draft_lc_df = draft_lc_df.append(guar_dict, ignore_index=True)
            if 'ED05' in guar_acc_info:
                discount_df = to_dataframe(guar_acc_info, ['ED05'])
                if discount_df.shape[0] > 0:
                    discount_df.rename(column_mapping, axis=1, inplace=True)
                    df2 = discount_df[pd.notna(discount_df['discount_amt'])] \
                        if 'discount_amt' in discount_df else pd.DataFrame()
                    df1 = discount_df[~discount_df.index.isin(df2.index)]
                    df1.drop(['account_num2', 'discount_amt'], axis=1, inplace=True, errors='ignore')
                    df2.drop(['account_num1', 'balance', 'overdue_amt', 'overdue_principal'],
                             axis=1, inplace=True, errors='ignore')
                    df1.rename({'account_num1': 'account_num'}, axis=1, inplace=True)
                    df2.rename({'account_num2': 'account_num'}, axis=1, inplace=True)
                    df1['settle_status'] = '未结清信贷'
                    df2['settle_status'] = '已结清信贷'
                    credit_biz_df = pd.concat([credit_biz_df, df1, df2], axis=0, ignore_index=True)
        if credit_biz_df.shape[0] > 0:
            transform_amount(credit_biz_df, ['balance', 'overdue_amt', 'overdue_principal', 'discount_amt',
                                             'end_30_bal', 'end_60_bal', 'end_90_bal', 'end_more_90_bal'])
            transform_count(credit_biz_df, ['account_num'])
            credit_biz_df['account_type'] = credit_biz_df['biz_type'].map(account_type_mapping)
            transform_not_null(credit_biz_df, ['account_type'], ['未知'])
            credit_biz_df['report_id'] = self.report_id
            role_list = credit_biz_df.to_dict('records')
            biz_map = {}
            for role in role_list:
                credit_biz = transform_dict(role, 'EcreditCreditBiz')
                self.session.add(credit_biz)
                self.session.commit()
                if role.get('ignore_label') != 1 and pd.notna(role.get('account_org')) \
                        and role.get('account_org') != '':
                    biz_map[role['account_org']] = credit_biz.id
            if draft_lc_df.shape[0] > 0:
                draft_lc_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(draft_lc_df, ['amount', 'balance', 'exposure_bal'])
                transform_count(draft_lc_df, ['deposit_rate'])
                transform_date(draft_lc_df, ['loan_date', 'end_date', 'stats_date', 'finish_date'])
                draft_lc_df['biz_id'] = draft_lc_df['account_org'].map(biz_map) \
                    if 'account_org' in draft_lc_df else None
                draft_lc_df['biz_id'].fillna(0, inplace=True)
                self.dfs['EcreditDraftLc'] = draft_lc_df
        # 授信协议信息
        if 'EDC' in self.msg and 'ED06' in self.msg['EDC']:
            credit_df = to_dataframe(self.msg['EDC'], ['ED06'])
            credit_df.rename(column_mapping, axis=1, inplace=True)
            transform_amount(credit_df, ['amount', 'used_amt', 'jhi_quota'])
            transform_date(credit_df, ['loan_date', 'end_date', 'stats_date'])
            self.dfs['EcreditCreditInfo'] = credit_df

    def repay_duty_info(self):
        # 相关还款责任信息
        logger.info('正在处理相关还款责任信息')
        if 'EDD' in self.msg:
            repay_duty_info = self.msg['EDD']
            repay_duty_biz_df = to_dataframe(repay_duty_info, ['ED07'])
            if repay_duty_biz_df.shape[0] > 0:
                repay_duty_biz_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(repay_duty_biz_df, ['duty_amt', 'balance', 'overdue_amt',
                                                     'overdue_principal', 'amount'])
                transform_count(repay_duty_biz_df, ['debt_status', 'surplus_repay_period'])
                transform_date(repay_duty_biz_df, ['biz_date', 'end_date', 'stats_date'])
                self.dfs['EcreditRepayDutyBiz'] = repay_duty_biz_df
            repay_discount_df = to_dataframe(repay_duty_info, ['ED08'])
            repay_guar_df = to_dataframe(repay_duty_info, ['ED09'])
            repay_discount_df.rename(column_mapping, axis=1, inplace=True)
            repay_guar_df.rename(column_mapping, axis=1, inplace=True)
            repay_other_df = pd.concat([repay_discount_df, repay_guar_df], axis=0, ignore_index=True)
            if repay_other_df.shape[0] > 0:
                repay_other_df['account_type'] = '为担保交易承担的相关还款责任'
                transform_amount(repay_other_df, ['duty_amt', 'balance', 'amount', 'overdue_amt', 'overdue_principal'])
                transform_count(repay_other_df, ['account_num'])
                self.dfs['EcreditRepayDutyDiscount'] = repay_other_df

    def public_info(self):
        # 公共信息明细
        logger.info('正在处理公共信息明细')
        # 公用事业缴费账户信息EEA、欠税信息EFA、获得认证/奖励相关信息EFE、进出口检验相关信息EFF、融资规模控制信息EFG todo:未解析落库
        # 民事判决信息
        if 'EFB' in self.msg and 'EF02' in self.msg['EFB']:
            judge_df = to_dataframe(self.msg['EFB'], ['EF02'])
            if judge_df.shape[0] > 0:
                judge_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(judge_df, ['target_amt'])
                transform_date(judge_df, ['filing_date'])
                self.dfs['EcreditCivilJudgments'] = judge_df
        # 强制执行信息
        if 'EFB' in self.msg and 'EF03' in self.msg['EFB']:
            execute_df = to_dataframe(self.msg['EFB'], ['EF03'])
            if execute_df.shape[0] > 0:
                execute_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(execute_df, ['target_amt'])
                transform_date(execute_df, ['filing_date'])
                self.dfs['EcreditForceExecution'] = execute_df
        # 行政处罚信息
        if 'EFC' in self.msg and 'EF04' in self.msg['EFC']:
            admin_df = to_dataframe(self.msg['EFC'], ['EF04'])
            if admin_df.shape[0] > 0:
                admin_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(admin_df, ['penalty_amt'])
                transform_date(admin_df, ['penalty_date'])
                self.dfs['EcreditPunishment'] = admin_df
        # 住房公积金参缴记录
        if 'EFD' in self.msg and 'EF05' in self.msg['EFD']:
            house_df = to_dataframe(self.msg['EFD'], ['EF05'])
            if house_df.shape[0] > 0:
                house_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(house_df, ['base_amt', 'arrearage_amt'])
                transform_count(house_df, ['staff_num'])
                transform_date(house_df, ['last_date'])
                self.dfs['EcreditHouseFund'] = house_df

    def financial_info(self):
        # 财务信息
        logger.info('正在处理财务信息')
        financial_df = pd.DataFrame()
        if 'EGA' in self.msg:
            financial_info = self.msg['EGA']
            for key in ["EG01", "EG02", "EG03", "EG04", "EG05", "EG06", "EG07", "EG08", "EG09", "EG10"]:
                if key in financial_info:
                    temp_info = financial_info[key]
                    if type(temp_info) != list:
                        temp_info = [temp_info]
                    for each in temp_info:
                        df1 = to_dataframe(each, [key + 'A'])
                        df2 = to_dataframe(each, [key + 'B'])
                        if df2.shape[0] > 0:
                            df2 = df2.T.reset_index(drop=False).set_axis(['report_item', 'report_value'], axis=1)
                            if type(df2) == pd.DataFrame and df2.shape[0] > 0:
                                # transform_enumerate(df2, ['report_item'], [column_mapping], ['未知'])
                                df1.rename(column_mapping, axis=1, inplace=True)
                                for col in ['org_code', 'report_year', 'report_type', 'report_nature']:
                                    if col in df1 and df1.shape[0] > 0:
                                        df2[col] = df1.loc[0, col]
                                df2['report_name'] = financial_table_mapping[key]
                                financial_df = pd.concat([financial_df, df2], axis=0, ignore_index=True)
        if financial_df.shape[0] > 0:
            transform_amount(financial_df, ['report_value'])
            self.dfs['EcreditFinancialSheet'] = financial_df

    def credit_parse_request(self):
        logger.info('开始保存企业征信报告解析请求')
        request_df = pd.DataFrame()
        request_df.loc[0, 'app_id'] = '0000000000'
        request_df['out_req_no'] = self.report_id
        request_df['provider'] = 'CENTRAL'
        request_df['credit_type'] = 'ENT'
        request_df['credit_version'] = 'SECOND'
        request_df['report_id'] = self.report_id
        request_df['process_status'] = 'DONE'
        request_df['process_memo'] = '成功'
        request_df['create_time'] = self.now
        request_df['update_time'] = self.now
        self.dfs['CreditParseRequest'] = request_df
