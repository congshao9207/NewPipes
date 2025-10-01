import xmltodict
import datetime
import pandas as pd
from creditreport.person.enumerate import *
from creditreport.tables import transform_class_str, transform_enumerate, transform_amount, transform_count, \
    transform_date, transform_dict, transform_org, choose_one
from util.mysql_reader import DB_ENGINE
from sqlalchemy.orm import sessionmaker, scoped_session
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_dataframe(data):
    if type(data) != list:
        data = [data]
    return pd.DataFrame(data)


class PCreditReport:

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
        logger.info('开始保存征信报告数据')
        self.report_head()
        self.identity_info()
        self.summary_info()
        self.loan_info()
        self.pub_info()
        self.query_info()
        self.credit_parse_request()
        for key, value in self.dfs.items():
            value['report_id'] = self.report_id
            # pcredit_loan已经落库了，此处不重新落库
            if key != 'PcreditLoan':
                transform_class_str(value.to_dict('records'), key)
        return self.dfs

    def report_head(self):
        logger.info('开始保存征信报告头信息')
        # 报告头信息
        report_head = self.msg['PRH']['PA01']
        head_info = pd.DataFrame()
        query_info = pd.DataFrame()
        if 'PA01A' in report_head:
            head_info = to_dataframe(report_head['PA01A'])
        if 'PA01B' in report_head:
            query_info = to_dataframe(report_head['PA01B'])
        head_info = pd.concat([head_info, query_info], axis=1)
        head_info['create_time'] = self.now
        head_info['update_time'] = self.now
        head_info['report_type'] = '0201'
        head_info['credit_type'] = '01'
        head_info.rename(column_mapping, inplace=True, axis=1)
        transform_date(head_info, ['report_time'])
        transform_enumerate(head_info, ['certificate_type', 'query_reason'],
                            [cert_type_mapping, query_reason_mapping], ['99', '99'])
        self.dfs['CreditBaseInfo'] = head_info

        if 'PA01C' in report_head and 'PA01CH' in report_head['PA01C']:
            other_id_info = report_head['PA01C']['PA01CH']
            other_id_df = to_dataframe(other_id_info)
            other_id_df.rename(column_mapping, axis=1, inplace=True)
            self.dfs['PcreditCertInfo'] = other_id_df

        if 'PA01D' in report_head:
            warn_info = report_head['PA01D']
            warn_df = to_dataframe(warn_info)
            warn_df.rename(column_mapping, axis=1, inplace=True)
            transform_date(warn_df, ['effective_date', 'expiry_date'])
            self.dfs['PcreditWarnInfo'] = warn_df

    def identity_info(self):
        logger.info('开始保存征信报告身份信息')
        # 一、个人基本信息（身份、配偶、手机号、居住、职业信息）
        survey_df = pd.DataFrame()
        spouse_df = pd.DataFrame()
        phone_df = pd.DataFrame()
        residence_df = pd.DataFrame()
        occupation_df = pd.DataFrame()
        if 'PIM' in self.msg and pd.notna(self.msg['PIM']) and 'PB01' in self.msg['PIM']:
            survey_info = self.msg['PIM']['PB01']
            if 'PB01A' in survey_info:
                survey_df = to_dataframe(survey_info['PB01A'])
            if 'PB01B' in survey_info and 'PB01BH' in survey_info['PB01B']:
                phone_df = to_dataframe(survey_info['PB01B']['PB01BH'])
        if 'PMM' in self.msg and pd.notna(self.msg['PMM']) and 'PB02' in self.msg['PMM']:
            spouse_info = self.msg['PMM']['PB02']
            spouse_df = to_dataframe(spouse_info)
        if 'PRM' in self.msg and pd.notna(self.msg['PRM']) and 'PB03' in self.msg['PRM']:
            residence_df = to_dataframe(self.msg['PRM']['PB03'])
        if 'POM' in self.msg and pd.notna(self.msg['POM']) and 'PB04' in self.msg['POM']:
            occupation_df = to_dataframe(self.msg['POM']['PB04'])
        if survey_df.shape[0] > 0:
            survey_df = pd.concat([survey_df, spouse_df], axis=1)
            survey_df.rename(column_mapping, axis=1, inplace=True)
            transform_enumerate(survey_df, ['sex', 'marriage_status', 'education',
                                            'jhi_degree', 'employment', 'spouse_certificate_type'],
                                [sex_mapping, marriage_status_mapping, education_mapping, degree_mapping,
                                 employment_mapping, cert_type_mapping],
                                ['9', '99', '99', '98', '98', '99'])
            self.dfs['PcreditPersonInfo'] = survey_df

        if phone_df.shape[0] > 0:
            phone_df['no'] = phone_df.index + 1
            phone_df.rename(column_mapping, axis=1, inplace=True)
            transform_date(phone_df, ['update_time'])
            self.dfs['PcreditPhoneHis'] = phone_df

        if residence_df.shape[0] > 0:
            residence_df['no'] = residence_df.index + 1
            residence_df.rename(column_mapping, axis=1, inplace=True)
            transform_enumerate(residence_df, ['live_address_type'], [residence_status_mapping], ['08'])
            transform_date(residence_df, ['update_time'])
            self.dfs['PcreditLive'] = residence_df

        if occupation_df.shape[0] > 0:
            occupation_df['no'] = occupation_df.index + 1
            occupation_df.rename(column_mapping, axis=1, inplace=True)
            # transform_enumerate(occupation_df,
            #                     ['work_type', 'profession', 'industry', 'duty', 'duty_title'],
            #                     [company_type_mapping, occupation_mapping, industry_mapping,
            #                      position_mapping, post_title_mapping],
            #                     ['99', 'Z', '9', '9', '9'])
            transform_enumerate(occupation_df,
                                ['work_type', 'profession'],
                                [company_type_mapping, occupation_mapping],
                                ['99', 'Z'])
            transform_date(occupation_df, ['enter_date', 'update_time'])
            self.dfs['PcreditProfession'] = occupation_df

    def summary_info(self):
        logger.info('开始保存征信报告概要信息')
        # 二、信息概要
        # 评分信息
        if 'PSM' in self.msg and 'PC01' in self.msg['PSM']:
            score_df = to_dataframe(self.msg['PSM']['PC01'])
            score_df.rename(column_mapping, axis=1, inplace=True)
            self.dfs['PcreditScoreInfo'] = score_df
        # 信贷交易信息概要
        if 'PCO' in self.msg and pd.notna(self.msg['PCO']) and 'PC02' in self.msg['PCO']:
            credit_smy_info = self.msg['PCO']['PC02']
            if 'PC02A' in credit_smy_info and 'PC02AH' in credit_smy_info['PC02A']:
                credit_smy_df = to_dataframe(credit_smy_info['PC02A']['PC02AH'])
                credit_smy_df.rename(column_mapping, axis=1, inplace=True)
                transform_enumerate(credit_smy_df, ['biz_type', 'biz_sub_type'],
                                    [business_type_mapping, business_subtype_mapping], ['00', '0000'])
                self.dfs['PcreditBizInfo'] = credit_smy_df
            default_df = pd.DataFrame()
            if 'PC02B' in credit_smy_info and 'PC02BH' in credit_smy_info['PC02B']['PC02BH']:
                temp_df = to_dataframe(credit_smy_info['PC02B']['PC02BH']).rename(column_mapping, axis=1)
                temp_df['default_type'] = '01'
                default_df = pd.concat([default_df, temp_df], axis=0)
            if 'PC02C' in credit_smy_info:
                temp_df = to_dataframe(credit_smy_info['PC02C']).rename(column_mapping, axis=1)
                temp_df['default_type'] = '02'
                default_df = pd.concat([default_df, temp_df], axis=0)
            if 'PC02D' in credit_smy_info and 'PC02DH' in credit_smy_info['PC02D']['PC02DH']:
                temp_df = to_dataframe(credit_smy_info['PC02D']['PC02DH']).rename(column_mapping, axis=1)
                temp_df['default_type'] = '03'
                default_df = pd.concat([default_df, temp_df], axis=0)
            if default_df.shape[0] > 0:
                transform_enumerate(default_df, ['default_subtype'], [default_subtype_mapping], [None])
                transform_count(default_df, ['default_count', 'default_month', 'max_overdue_month'])
                transform_amount(default_df, ['default_balance', 'max_overdue_sum'])
                self.dfs['PcreditDefaultInfo'] = default_df

            other_smy_df = pd.DataFrame()
            for key in ['PC02E', 'PC02F', 'PC02G', 'PC02H', 'PC02I']:
                if key in credit_smy_info:
                    temp_df = to_dataframe(credit_smy_info[key]).rename(column_mapping, axis=1)
                    other_smy_df = pd.concat([other_smy_df, temp_df], axis=1)
            if 'PC02K' in credit_smy_info and 'PC02KH' in credit_smy_info['PC02K']:
                guar_smy_df = to_dataframe(credit_smy_info['PC02K']['PC02KH'])
                # transform_enumerate(guar_smy_df, ['PC02KD01', 'PC02KD02'],
                #                     [response_id_type_mapping, response_subtype_mapping], ['1', '1'])
                for i in guar_smy_df.index:
                    temp_str = guar_smy_df.loc[i, 'PC02KD01'] + guar_smy_df.loc[i, 'PC02KD02']
                    temp_df = guar_smy_df.loc[[i], ['PC02KS02', 'PC02KJ01', 'PC02KJ02']]
                    transform_amount(temp_df, ['PC02KJ01', 'PC02KJ02'])
                    transform_count(temp_df, ['PC02KS02'])
                    temp_df.rename(lambda x: temp_str + x, axis=1, inplace=True)
                    temp_df.rename(column_mapping, axis=1, inplace=True)
                    temp_df.reset_index(drop=True, inplace=True)
                    other_smy_df = pd.concat([other_smy_df, temp_df], axis=1)
            transform_amount(other_smy_df,
                             ['non_revolloan_totalcredit', 'non_revolloan_balance', 'non_revolloan_repayin_6_m',
                              'revolcredit_totalcredit', 'revolcredit_balance', 'revolcredit_repayin_6_m',
                              'revolloan_totalcredit', 'revolloan_balance', 'revolloan_repayin_6_m',
                              'undestroy_limit', 'undestory_max_limit', 'undestory_min_limt', 'undestory_used_limit',
                              'undestory_avg_use', 'undestory_semi_limit', 'undestory_semi_max_limit',
                              'undestory_semi_min_limt', 'undestory_semi_overdraft', 'undestory_semi_avg_overdraft',
                              'ind_guarantee_sum', 'ind_guarantee_balance', 'ind_repay_sum', 'ind_repay_balance',
                              'ent_guarantee_sum', 'ent_guarantee_balance', 'ent_repay_sum', 'ent_repay_balance'])
            transform_count(other_smy_df, ['non_revolloan_org_count', 'non_revolloan_accountno',
                                           'revolcredit_org_count', 'revolcredit_account',
                                           'revolloan_org_count', 'revolloan_account_no',
                                           'undestroy_org_count', 'undestroy_count',
                                           'undestory_semi_org_count', 'undestory_semi_count',
                                           'ind_guarantee_count', 'ind_repay_count',
                                           'ent_guarantee_count', 'ent_repay_count'])
            self.dfs['PcreditInfo'] = other_smy_df

        # 非信贷信息概要
        if 'PNO' in self.msg and pd.notna(self.msg['PNO']) and 'PC03' in self.msg['PNO'] and pd.notna(self.msg['PNO']['PC03']) and 'PC030H' in self.msg['PNO']['PC03']:
            non_credit_df = to_dataframe(self.msg['PNO']['PC03']['PC030H']).rename(column_mapping, axis=1)
            transform_enumerate(non_credit_df, ['noncredit_type'], [noncredit_business_type_mapping], ['09'])
            transform_amount(non_credit_df, ['noncredit_sum'])
            transform_count(non_credit_df, ['noncredit_count'])
            self.dfs['PcreditNoncreditInfo'] = non_credit_df
        if 'PPO' in self.msg and pd.notna(self.msg['PPO']) and 'PC04' in self.msg['PPO'] and pd.notna(self.msg['PPO']['PC04']) and 'PC040H' in self.msg['PPO']['PC04']:
            pub_smy_df = to_dataframe(self.msg['PPO']['PC04']['PC040H']).rename(column_mapping, axis=1)
            transform_enumerate(pub_smy_df, ['pub_type'], [public_type_mapping], [None])
            transform_amount(pub_smy_df, ['pub_sum'])
            transform_count(pub_smy_df, ['pub_count'])
            self.dfs['PcreditPubInfo'] = pub_smy_df
        if 'PQO' in self.msg and 'PC05' in self.msg['PQO']:
            query_smy_info = self.msg['PQO']['PC05']
            query_smy_df = pd.DataFrame()
            for key in ['PC05A', 'PC05B']:
                if key in query_smy_info:
                    temp_df = to_dataframe(query_smy_info[key])
                    query_smy_df = pd.concat([query_smy_df, temp_df], axis=1)
            query_smy_df.rename(column_mapping, axis=1, inplace=True)
            transform_date(query_smy_df, ['last_query_time'])
            transform_enumerate(query_smy_df, ['last_query_type'], [query_type_mapping], [None])
            transform_enumerate(query_smy_df, ['last_query_org1'], [account_org_type_mapping], ['其他'])
            transform_org(query_smy_df, 'last_query_org')
            transform_count(query_smy_df, ['loan_org_1', 'credit_org_1', 'loan_times_1', 'credit_times_1',
                                           'self_times_1', 'loan_times_2', 'guarantee_times_2', 'agreement_times_2'])
            self.dfs['PcreditQueryTimes'] = query_smy_df

    def loan_info(self):
        logger.info('开始保存征信报告贷款信息')
        # 三、信贷交易信息明细
        loan_df = pd.DataFrame()
        repay_df = pd.DataFrame()
        special_trade_df = pd.DataFrame()
        # special_issue_df = pd.DataFrame()
        large_scale_df = pd.DataFrame()
        credit_cont_df = pd.DataFrame()
        response_df = pd.DataFrame()
        if 'PDA' in self.msg and pd.notna(self.msg['PDA']) and 'PD01' in self.msg['PDA']:
            credit_detail_info = self.msg['PDA']['PD01']
            credit_detail_info = [credit_detail_info] if type(credit_detail_info) != list else credit_detail_info
            for i, each_loan in enumerate(credit_detail_info):
                loan_dict = {'index': i}
                for key in ['PD01A', 'PD01B', 'PD01C']:
                    if key in each_loan:
                        loan_dict.update(each_loan[key])
                if 'PD01E' in each_loan and 'PD01EH' in each_loan['PD01E']:
                    temp_df = to_dataframe(each_loan['PD01E']['PD01EH'])
                    temp_df['index'] = i
                    """青岛PD01ER03格式非标，为'2021-01'形式，需要单独处理"""
                    # 将PD01ER03中的年+月做拆分
                    temp_df['PD01ER04'] = temp_df['PD01ER03'].apply(lambda x: x.split('-')[1])
                    temp_df['PD01ER03'] = temp_df['PD01ER03'].apply(lambda x: x.split('-')[0])

                    repay_df = pd.concat([repay_df, temp_df], axis=0, ignore_index=True)
                if 'PD01F' in each_loan and 'PD01FH' in each_loan['PD01F']:
                    temp_df = to_dataframe(each_loan['PD01F']['PD01FH'])
                    temp_df['index'] = i
                    special_trade_df = pd.concat([special_trade_df, temp_df], axis=0, ignore_index=True)
                # if 'PD01G' in each_loan and 'PD01GH' in each_loan['PD01G']:
                #     temp_df = to_dataframe(each_loan['PD01G']['PD01GH'])
                #     temp_df['index'] = i
                #     special_issue_df = pd.concat([special_issue_df, temp_df], axis=0, ignore_index=True)
                if 'PD01H' in each_loan and 'PD01HH' in each_loan['PD01H']:
                    temp_df = to_dataframe(each_loan['PD01H']['PD01HH'])
                    temp_df['index'] = i
                    large_scale_df = pd.concat([large_scale_df, temp_df], axis=0, ignore_index=True)
                loan_df = loan_df.append(loan_dict, ignore_index=True)

        if 'PCA' in self.msg and pd.notna(self.msg['PCA']) and 'PD02' in self.msg['PCA']:
            credit_cont_info = self.msg['PCA']['PD02']
            credit_cont_info = [credit_cont_info] if type(credit_cont_info) != list else credit_cont_info
            for each_cont in credit_cont_info:
                if 'PD02A' in each_cont:
                    credit_cont_df = credit_cont_df.append(each_cont['PD02A'], ignore_index=True)
        if 'PCR' in self.msg and pd.notna(self.msg['PCR']) and 'PD03' in self.msg['PCR']:
            response_info = self.msg['PCR']['PD03']
            response_info = [response_info] if type(response_info) != list else response_info
            for each_resp in response_info:
                if 'PD03A' in each_resp:
                    response_df = response_df.append(each_resp['PD03A'], ignore_index=True)

        loan_df.rename(column_mapping, axis=1, inplace=True)
        credit_cont_df.rename(column_mapping, axis=1, inplace=True)
        response_df.rename(column_mapping, axis=1, inplace=True)
        credit_cont_df['account_type'] = '授信协议'
        response_df['account_type'] = '相关还款责任'
        # 新增对相关还款责任loan_type的处理
        transform_enumerate(response_df,['loan_type'], [resp_loan_type_mapping], ['99'])
        loan_df = pd.concat([loan_df, credit_cont_df, response_df], axis=0, ignore_index=True)
        loan_df['account_mark'] = loan_df['account_mark'].apply(lambda x: str(x)[:16] if pd.notna(x) else x) \
            if 'account_mark' in loan_df else None

        if loan_df.shape[0] > 0:
            loan_df['report_id'] = self.report_id
            choose_one(loan_df, ['overdue_180_principal', 'avg_overdraft_balance_6', 'loan_status_time', 'max_limit',
                                 'loan_status', 'loan_amount'])
            transform_enumerate(loan_df, ['account_org1'], [account_org_type_mapping], ['其他'])
            transform_org(loan_df, 'account_org')
            transform_count(loan_df, ['repay_period', 'surplus_repay_period', 'overdue_period'])
            transform_amount(loan_df, ['principal_amount', 'latest_loan_balance', 'latest_replay_amount',
                                       'loan_balance', 'quota_used', 'large_scale_balance', 'repay_amount',
                                       'amout_replay_amount', 'overdue_amount', 'overdue_31_principal',
                                       'overdue_61_principal', 'overdue_91_principal', 'overdue_180_principal',
                                       'loan_amount', 'avg_overdraft_balance_6', 'credit_limit', 'max_limit',
                                       'credit_share_amt'])
            transform_date(loan_df, ['loan_date', 'end_date', 'loan_status_time', 'latest_replay_date',
                                     'expiry_date', 'plan_repay_date', 'lately_replay_date', 'loan_end_date'])
            # transform_enumerate(loan_df, ['repay_frequency', 'category', 'latest_category', 'loan_type',
            #                               'loan_guarantee_type', 'loan_repay_type', 'loan_status', 'account_status',
            #                               'respon_object', 'respon_type', 'credit_purpose', 'account_type'],
            #                     [repay_frequency_mapping, category_mapping, category_mapping, indiv_loan_type_mapping,
            #                      guar_type_mapping, repay_type_mapping, loan_status_mapping_1, loan_status_mapping_2,
            #                      response_object_mapping, response_type_mapping, credit_purpose_mapping,
            #                      account_type_mapping],
            #                     ['99', '9', '9', '99', '99', '90', '09', '09', '09', '9', '09', None])
            transform_enumerate(loan_df, ['loan_type',
                                          'loan_guarantee_type', 'account_status',
                                          'respon_object', 'respon_type', 'credit_purpose', 'account_type'],
                                [indiv_loan_type_mapping,
                                 guar_type_mapping, loan_status_mapping_2,
                                 response_object_mapping, response_type_mapping, credit_purpose_mapping,
                                 account_type_mapping],
                                ['99', '99', '09', '09', '9', '09', None])
            # account_type 映射
            # transform_enumerate(loan_df, ['account_type'], [account_type_mapping], [None])
            # loan_status 映射
            loan_df['loan_status'] = loan_df.apply(lambda x: loan_status_mapping_1[x['loan_status']]
            if x['account_type'] in ['01', '02', '03'] and pd.notna(x['loan_status'])
            else loan_status_mapping_2[x['loan_status']]
            if x['account_type'] in ['04', '05'] and pd.notna(x['loan_status']) else x['loan_status'], axis=1)

            loan_df['account_status'] = loan_df['loan_status'] if 'loan_status' in loan_df else None
            if loan_df is not None and 'index' not in loan_df.columns:
                loan_df['index'] = None
            role_list = loan_df.to_dict('records')
            role_map = {}
            acc_type_map = {}
            for role in role_list:
                pcredit_loan = transform_dict(role, 'PcreditLoan')
                self.session.add(pcredit_loan)
                self.session.commit()
                if pd.notna(role['index']):
                    role_map[role['index']] = pcredit_loan.id
                    acc_type_map[role['index']] = role.get('account_type', None)
            loan_df['id'] = loan_df['index'].map(role_map)
            self.dfs['PcreditLoan'] = loan_df

            if repay_df.shape[0] > 0:
                repay_df['record_id'] = repay_df['index'].map(role_map)
                repay_df['record_type'] = repay_df['index'].map(acc_type_map)
                repay_df.rename(column_mapping, axis=1, inplace=True)
                repay_df = repay_df[pd.notna(repay_df['repayment_amt'])]
                transform_amount(repay_df, ['repayment_amt'])
                self.dfs['PcreditRepayment'] = repay_df

            if special_trade_df.shape[0] > 0:
                special_trade_df['record_id'] = special_trade_df['index'].map(role_map)
                special_trade_df['record_type'] = special_trade_df['index'].map(acc_type_map)
                special_trade_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(special_trade_df, ['special_money'])
                transform_count(special_trade_df, ['special_month'])
                transform_date(special_trade_df, ['special_date'])
                # transform_enumerate(special_trade_df, ['special_type'], [special_type_mapping], ['99'])
                self.dfs['PcreditSpecial'] = special_trade_df

            if large_scale_df.shape[0] > 0:
                large_scale_df['record_id'] = large_scale_df['index'].map(role_map)
                large_scale_df['record_type'] = large_scale_df['index'].map(acc_type_map)
                large_scale_df.rename(column_mapping, axis=1, inplace=True)
                transform_amount(large_scale_df, ['large_scale_quota', 'usedsum'])
                transform_date(large_scale_df, ['effective_date', 'end_date'])
                self.dfs['PcreditLargeScale'] = large_scale_df

    def pub_info(self):
        logger.info('开始保存征信报告公共信息')
        # 四、公共信息
        level1 = ["PND", "POT", "PCJ", "PCE", "PAP", "PHF", "PBS", "PPQ", "PAH"]
        level2 = ["PE01", "PF01", "PF02", "PF03", "PF04", "PF05", "PF06", "PF07", "PF08"]
        level3 = ["PE01A", "PF01A", "PF02A", "PF03A", "PF04A", "PF05A", "PF06A", "PF07A", "PF08A"]
        tables = ["PcreditNoncreditList", "PcreditCreditTaxRecord", "PcreditCivilJudgmentsRecord",
                  "PcreditForceExecutionRecord", "PcreditPunishmentRecord", "PcreditHouseFundRecord",
                  "PcreditThresholdRecord", "PcreditQualificationRecord", "PcreditRewardRecord"]
        for i, l1 in enumerate(level1):
            if l1 in self.msg and self.msg[l1] is not None and level2[i] in self.msg[l1]:
                public_info = self.msg[l1][level2[i]]
                public_df = pd.DataFrame()
                public_info = [public_info] if type(public_info) != list else public_info
                for pub in public_info:
                    if level3[i] in pub:
                        public_df.append(pub[level3[i]], ignore_index=True)
                if public_df.shape[0] > 0:
                    public_df['seq'] = public_df.index + 1
                    public_df.rename(column_mapping, axis=1, inplace=True)
                    transform_amount(public_df, ['amount', 'litigious_amt', 'apply_execution_object_amt',
                                                 'current_arrears_amt', 'executed_object_amt', 'month_fee_amt',
                                                 'home_monthly_income'])
                    transform_date(public_df, ['stats_date', 'register_date', 'case_end_date', 'end_date', 'apply_date',
                                               'pay_date', 'approval_date', 'effective_date', 'info_update_date',
                                               'award_date', 'expired_date', 'revoked_date', 'biz_start_date',
                                               'record_date'])
                    # transform_enumerate(public_df, ['biz_type', 'current_payment_status'],
                    #                     [biz_type_mapping, biz_status_mapping], ['09', '09'])
                    transform_enumerate(public_df, ['current_payment_status'],
                                        [biz_status_mapping], ['09'])
                    self.dfs[tables[i]] = public_df

    def query_info(self):
        logger.info('开始保存征信报告查询信息')
        # 五、查询信息
        if 'POQ' in self.msg and self.msg['POQ'] is not None:
            if 'PH01' in self.msg['POQ']:
                q_info = self.msg['POQ']['PH01']
                query_df = to_dataframe(q_info)
                query_df['no'] = query_df.index + 1
                query_df.rename(column_mapping, axis=1, inplace=True)
                transform_enumerate(query_df, ['operator1'], [account_org_type_mapping], ['其他'])
                transform_org(query_df, 'operator')
                transform_date(query_df, ['jhi_time'])
                transform_enumerate(query_df, ['reason'], [query_reason_mapping], ['99'])
                self.dfs['PcreditQueryRecord'] = query_df

    def default_info(self):
        # 异议标注信息
        pass

    def credit_parse_request(self):
        logger.info('开始保存征信报告解析请求')
        request_df = pd.DataFrame()
        request_df.loc[0, 'app_id'] = '0000000000'
        request_df['out_req_no'] = self.report_id
        request_df['provider'] = 'CENTRAL'
        request_df['credit_type'] = 'PER'
        request_df['credit_version'] = 'SECOND'
        request_df['report_id'] = self.report_id
        request_df['process_status'] = 'DONE'
        request_df['process_memo'] = '成功'
        request_df['create_time'] = self.now
        request_df['update_time'] = self.now
        self.dfs['CreditParseRequest'] = request_df
