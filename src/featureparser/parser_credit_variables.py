import json

import pandas as pd
import numpy as np

from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str
from util.mysql_reader import sql_to_df

model_variable_name_mapping = {
    "ahp_marriage_status": " 婚姻状况",
    "ahp_live_address_type": " 个人房产数",
    "ahp_all_loan_account_cnt": " 所有账户数合计",
    "ahp_unsettled_business_loan_org_cnt": " 经营性贷款在贷机构家数",
    "ahp_unsettled_consume_loan_org_cnt": " 消费性贷款在贷机构家数",
    "ahp_bus_loan_credit_guar_balance_prop": " 信用保证类经营性贷款在贷金额占比",
    "ahp_total_loan_change_rate": " 近一年贷款金额变化率",
    "ahp_org_change_cnt": " 近一年机构变化家数",
    "ahp_new_add_1y_loan_amount": " 近一年新增机构借款金额",
    "ahp_loan_approved_rate_1y": " 近一年贷款审批查询放款比例",
    "ahp_loan_overdue_2year_total_cnt": " 总计贷款两年内逾期次数",
    "ahp_credit_overdue_2year_total_cnt": " 总计贷记卡两年内逾期次数",
    "ahp_overdue_5year_total_cnt": " 总计五年内逾期次数",
    "ahp_single_loan_3year_overdue_max_month": " 单笔贷记卡近3年内出现连续逾期最大期数",
    "ahp_abnormal_loans_and_external_guarantees_cnt": " 贷款及对外担保异常笔数",
    "ahp_abnormal_credit_cards_cnt": " 非正常状态信用卡张数",
    "ahp_loan_credit_query_3month_cnt": " 近3个月审批查询机构家数",
    "ahp_total_credit_usage_rate": " 贷记卡总透支率",
    "ahp_credit_min_payed_number": " 贷记卡最低还款张数",
    "ahp_credit_quota_used_div_avg_used_rate": " 已用额度/近六个月平均使用额"
}
rules_variable_name_mapping = {
    "public_sum_count": "存在被追偿记录",
    "rhzx_business_loan_3year_overdue_cnt": "3年内经营性贷款存在本金逾期",
    "loan_category_abnormal_status": "贷款五级分类存在关注",
    "business_loan_average_3year_overdue_cnt": "3年内还款方式为等额本息分期偿还的经营性贷款连续逾期2期",
    "loan_now_overdue_money": "贷款有当前逾期",
    "loan_now_overdue_cnt": "贷款有当前逾期笔数",
    "single_loan_overdue_2year_cnt": "单笔贷款近2年内存在6次以上逾期",
    "extension_number": "存在展期",
    "loan_status_abnorm_cnt": "贷款账户状态存在异常",
    "loan_overdue_2year_total_cnt": "总计贷款2年内逾期超过10次",
    "loan_non_mort_overdue_2year_total_cnt": "总计非按揭贷款2年内逾期超过6次",
    "unsettled_business_loan_org_cnt": "有经营性贷款在贷余额的合作机构超过7家",
    "large_loan_2year_overdue_cnt": "经营性贷款2年内存在连续2期逾期",
    "unsettled_consume_loan_org_cnt": "未结清消费性贷款机构数偏多",
    "unsettled_consume_total_cnt": "未结清消费性贷款笔数过多",
    "business_loan_type_cnt": "未结清经营性贷款笔数偏多",
    "unsettled_loan_agency_number": "未结清贷款机构数偏多",
    "unsettled_consume_total_amount": "未结清消费性贷款总额过高",
    "rhzx_business_loan_3year_ago_overdue_cnt": "3年前经营性贷款存在本金逾期",
    "business_loan_average_3year_ago_overdue_cnt": "3年前还款方式为等额本息分期偿还的经营性贷款连续逾期2期",
    "loan_overdue_months_2year": "近2年贷款最大逾期期数",
    "credit_now_overdue_1k_money": "贷记卡当前严重逾期",
    "credit_now_overdue_money": "贷记卡当前逾期",
    "single_credit_overdue_2year_cnt": "单张贷记卡（信用卡）近2年内存在6次以上逾期（年费及手续费等逾期金额在1000元下的除外）",
    "credit_status_abnormal_cnt": "贷记卡账户状态存在异常",
    "credit_overdrawn_2card": "贷记卡总透支率达80%且最低额还款张数较多",
    "credit_overdue_2year_total_cnt": "总计贷记卡（信用卡）2年内逾期超过10次",
    "credit_financial_tension": "贷记卡资金紧张",
    "activated_credit_card_cnt": "已激活贷记卡张数偏多",
    "credit_min_payed_number": "贷记卡最低还款张数偏多",
    "credit_org_cnt": "未销户贷记卡发卡机构数偏多",
    "credit_overdue_5year": "总计贷记卡5年内逾期次数多",
    "credit_and_mort_loan_3year_overdue_cnt": "近2年贷记卡及房屋按揭贷款逾期次数较多",
    "credit_overdue_months_2year": "近2年贷记卡最大逾期期数",
    "single_credit_or_loan_3year_overdue_max_month": "单张贷记卡（信用卡）、单笔贷款3年内出现连续90天以上逾期记录（年费及手续费等逾期金额在1000元下的除外）",
    "loan_scured_five_a_level_abnormality_cnt": "对外担保五级分类存在“次级、可疑、损失”",
    "loan_scured_five_b_level_abnormality_cnt": "对外担保五级分类存在关注",
    "guar_loan_balance": "存在担保负债压力",
    "force_execution_cnt": "存在强制执行记录",
    "civil_judge_cnt": "存在民事判决记录",
    "loan_doubtful": "存在疑似压贷",
    "owing_tax_cnt": "存在欠税记录",
    "admin_punish_cnt": "存在行政处罚记录",
    "loan_credit_query_3month_cnt": "近三个月征信查询（贷款审批及贷记卡审批等）超过6次",
    "ecredit_unsettled_bad_cnt": "存在未结清不良类信贷",
    "ecredit_unsettled_focus_cnt": "存在未结清关注类信贷",
    "ecredit_settled_bad_cnt": "存在已结清不良类信贷",
    "ecredit_settled_focus_cnt": "存在已结清关注类信贷",
    "ecredit_unsettled_loan_org_cnt": "未结清信贷业务机构数较多"
}
variable_name_mapping = {
    "report_no": "报告编号",
    "report_time": "报告时间",
    "per_debt_amt": "在贷余额",
    "if_no_credit_record": "征信白户",
    "com_debt_amt": "在贷余额",
    "base_age": "主体年龄",
    "base_sex": "主体性别",
    "marriage_status": "是否离异",
    "per_credit_loan_balance": "贷款余额",
    "unsettled_business_loan_org_cnt": "未结清经营性贷款机构家数",
    "unsettled_consume_loan_org_cnt": "未结清消费性贷款机构家数",
    "unsettled_house_loan_cnt": "未结清房贷笔数",
    "unsettled_car_loan_cnt": "未结清车贷笔数",
    "com_credit_loan_balance": "贷款余额",
    "public_sum_count": "贷款被追偿笔数",
    "loan_status_abnorm_cnt": "贷款账户状态异常笔数",
    "loan_category_abnormal_cnt": "贷款五级分类异常笔数",
    "extension_number": "贷款展期笔数",
    "loan_now_overdue_cnt": "贷款当前逾期笔数",
    "loan_now_overdue_money": "贷款当前逾期金额",
    "business_loan_average_3year_overdue_cnt": "近3年还款方式为分期偿还的经营性贷款连续逾期期数",
    "rhzx_business_loan_3year_overdue_cnt": "近3年经营性贷款本金逾期次数",
    "single_loan_3year_overdue_max_month": "单笔贷款近3年内出现连续逾期最大期数",
    "single_loan_overdue_2year_cnt": "单笔贷款近2年内最大逾期总次数",
    "total_credit_loan_amount": "贷记卡授信总额",
    "credit_org_cnt": "贷记卡授信机构家数",
    "activated_credit_card_cnt": "已激活贷记卡张数",
    "total_credit_avg_used_6m": "贷记卡最近6个月平均使用额度",
    "total_credit_usage_rate": "贷记卡总使用率",
    "credit_min_payed_number": "贷记卡最低还款张数",
    "single_credit_3year_overdue_max_month": "单笔贷记卡近3年内出现连续逾期最大期数",
    "single_credit_overdue_2year_cnt": "单笔贷记卡近2年内总逾期次数",
    "credit_now_overdue_money": "贷记卡当前逾期金额",
    "credit_now_overdue_1k_money": "贷记卡当前严重逾期金额",
    "credit_status_abnormal_cnt": "贷记卡账户状态异常账户数",
    "credit_overdue_2year_total_cnt": "总计贷记卡2年内逾期次数",
    "single_credit_2year_overdue_max_month": "近2年贷记卡最大连续逾期期数",
    "guar_loan_cnt": "对外担保笔数",
    "guar_loan_balance": "对外担保余额",
    "loan_scured_five_b_level_abnormality_cnt": "对外担保五级分类存在“关注”笔数",
    "loan_scured_five_a_level_abnormality_cnt": "对外担保五级分类存在“次级、可疑、损失”笔数",
    "com_guar_loan_cnt": "对外担保笔数",
    "com_guar_loan_balance": "对外担保余额",
    "self_query_1month_cnt": "近1个月本人查询次数",
    "loan_credit_query_3month_cnt": "近3个月审批查询机构家数",
    "credit_query_3month_cnt": "近3个月信用卡查询机构家数",
    "loan_query_3month_cnt": "近3个月贷款审批查询机构家数",
    "query_loan_approved_3m_org_cnt": "近3个月贷款放款机构家数",
    "query_loan_approved_3m_prob": "近3个月贷款审批查询放款比例",
    "force_execution_cnt": "强制执行条数",
    "admin_punish_cnt": "行政处罚条数",
    "civil_judge_cnt": "民事判决条数",
    "owing_tax_cnt": "欠税信息条数",
    "total_loan_amount_latest_year": "近12个月放款总额",
    "total_loan_amount_last_year": "上一年放款总额",
    "live_address_type": "个人房产数",
    "guarantee_type_balance_prop": "担保方式余额占比",
    "org_change_cnt": "近一年净增机构家数",
    "loan_overdue_2year_total_cnt": "总计贷款2年内逾期次数",
    "large_loan_2year_overdue_cnt": "经营性贷款2年内最大连续逾期期数",
    "loan_balance_due_soon": "近2个月内将到期担保余额",
    "credit_review_query_cnt": "近3个月资信审查笔数",
    "guar_query_cnt": "近3个月保前审查笔数",
    "mort_settle_loan_date": "房贷按揭已归还",
    "mort_no_settle_loan_date": "房贷按揭未结清",
    "per_org_type": "个人在贷机构类型",
    "per_balance": "个人在贷机构余额",
    "per_balance_prop": "个人在贷机构占比",
    "business_loan_type_balance": "经营性贷款余额",
    "business_loan_type_cnt": "经营性贷款在贷笔数",
    "consume_loan_type_balance": "消费性贷款余额",
    "consume_loan_type_cnt": "消费性贷款在贷笔数",
    "mortgage_loan_type_balance": "住房贷款余额",
    "mortgage_loan_type_cnt": "住房贷款在贷笔数",
    "bus_mortgage_loan_type_balance": "商用住房贷款余额",
    "bus_mortgage_loan_type_cnt": "商用住房贷款在贷笔数",
    "annual_year": "年份",
    "annual_bus_loan_amount": "经营性借款金额",
    "annual_cousume_loan_amount": "消费性借款金额",
    "annual_org_cnt": "机构家数",
    "guarantee_type": "担保方式",
    "business_loan_guarantee_type_cnt": "笔数",
    "guarantee_type_balance": "余额"
}


class ParserCreditVariables:
    def __init__(self, resp, report_req_no, product_code, query_data_array):
        self.report_req_no = report_req_no
        self.product_code = product_code
        self.unique_code = None
        self.resp = resp
        self.query_data_array = query_data_array
        self.feature_list = []

    def process(self):
        self.get_public_param()
        self.parser_sub_head_info()
        self.parser_sub_credit_result()
        self.parser_sub_risk_detail()
        self.parser_sub_suggestions()
        self.parser_com_loan_pressure_info()
        self.parser_com_loan_trans_info()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def process1(self):
        self.get_public_param()
        self.parser_feature_info()
        self.parser_rules_variables()
        self.parser_model_variables()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def get_public_param(self):
        subject_info = self.resp['subject']
        for sub in subject_info:
            relation_info = sub['queryData']['relation']
            if relation_info == 'MAIN':
                self.unique_code = sub['queryData']['idno']

    def parser_sub_credit_result(self):
        pass

    def parser_sub_head_info(self):
        pass

    def parser_sub_risk_detail(self):
        pass

    def parser_sub_suggestions(self):
        pass

    def parser_com_loan_pressure_info(self):
        pass

    def parser_com_loan_trans_info(self):
        pass

    def parser_rules_variables(self):
        # 规则指标每个主体下均存在
        for sub in self.resp['subject']:
            rules_info = sub['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
            unique_code = sub['queryData']['idno']
            for key, value in rules_info.items():
                if key in rules_variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['level_1'] = 'rules_variables'
                    # temp_dict['level_2'] = key
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = rules_variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(rules_info[key], ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_model_variables(self):
        model_info = self.resp['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
        for key, value in model_info.items():
            if key in model_variable_name_mapping.keys():
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['level_1'] = 'model_variables'
                # temp_dict['level_2'] = key
                # temp_dict['level_3'] = ''
                temp_dict['variable_name'] = key
                temp_dict['variable_name_cn'] = model_variable_name_mapping[key]
                temp_dict['variable_values'] = json.dumps(model_info[key], ensure_ascii=False)
                self.feature_list.append(temp_dict)

    def parser_feature_info(self):
        """
        直接落库省联社清洗的指标数据
        """
        for query_data in self.query_data_array:
            strategy = query_data.get("extraParam")['strategy']
            if strategy == '01':
                credit_parser_req_no = query_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
                unique_code = query_data.get('idno')
                data_detail_sql = """
                    select basic_id,variable_name,variable_value from info_union_credit_data_detail 
                    where basic_id = ( select id from info_union_credit_data where credit_parse_no = %(credit_parse_no)s 
                    and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1)
                """
                data_detail_info = sql_to_df(data_detail_sql, params={"credit_parse_no": credit_parser_req_no})
                if data_detail_info.shape[0] > 0:
                    data_detail_info.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
                    all_variable_name_list = data_detail_info['variable_name'].tolist()
                    # 解析指标落库
                    for key, values in variable_name_mapping.items():
                        if key in all_variable_name_list:
                            temp_dict = dict()
                            temp_dict['report_req_no'] = self.report_req_no
                            temp_dict['product_code'] = self.product_code
                            temp_dict['unique_code'] = unique_code
                            # temp_dict['level_1'] = 'model_variables'
                            # temp_dict['level_2'] = ''
                            # temp_dict['level_3'] = ''
                            temp_dict['variable_name'] = key
                            temp_dict['variable_name_cn'] = variable_name_mapping[key]
                            variable_values = data_detail_info.loc[data_detail_info['variable_name'] == key][
                                'variable_value'].values[0]
                            temp_dict['variable_values'] = json.dumps(variable_values, ensure_ascii=False)
                            self.feature_list.append(temp_dict)
