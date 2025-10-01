import json

import pandas as pd

from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str
from util.mysql_reader import sql_insert

mapping_dict = {
    "relation_info": "关联人",
    "flow_info": "流水信息",
    "cusName": "cusName",
    "trans_summary_risk_tips": "risk_tips",
    "trans_summary_trans_detail": "trans_detail",
    "bull_form_detail": "form_detail",
    "bull_risk_tips": "risk_tips",
    "gaming_form_detail": "form_detail",
    "gaming_risk_tips": "risk_tips",
    "amusement_form_detail": "form_detail",
    "amusement_risk_tips": "risk_tips",
    "case_disputes_form_detail": "form_detail",
    "case_disputes_risk_tips": "risk_tips",
    "security_fines_form_detail": "form_detail",
    "security_fines_risk_tips": "risk_tips",
    "insurance_claims_form_detail": "form_detail",
    "insurance_claims_risk_tips": "risk_tips",
    "stock_form_detail": "form_detail",
    "stock_risk_tips": "risk_tips",
    "hospital_form_detail": "form_detail",
    "hospital_risk_tips": "risk_tips",
    "noritomo_form_detail": "form_detail",
    "noritomo_risk_tips": "risk_tips",
    "loan_form_detail": "form_detail",
    "loan_risk_tips": "risk_tips",
    "foreign_guarantee_form_detail": "form_detail",
    "foreign_guarantee_risk_tips": "risk_tips",
    "big_in_out_form_detail": "form_detail",
    "big_in_out_risk_tips": "risk_tips",
    "fast_in_out_form_detail": "form_detail",
    "fast_in_out_risk_tips": "risk_tips",
    "financing_form_detail": "form_detail",
    "financing_risk_tips": "risk_tips",
    "house_sale_form_detail": "form_detail",
    "house_sale_risk_tips": "risk_tips",
    "family_unstable_form_detail": "form_detail",
    "family_unstable_risk_tips": "risk_tips",
    "com_expense": "对公出账",
    "com_income": "对公进账",
    "per_expense": "对私出账",
    "per_income": "对私进账",
    "strong_related_risk_tips": "risk_tips",
    "strong_related_trans_detail": "trans_detail",
    "generally_related_risk_tips": "risk_tips",
    "generally_related_trans_detail": "trans_detail",
    "third_party_guarantee_risk_tips": "risk_tips",
    "third_party_guarantee_trans_detail": "trans_detail",
    "s_confidence_analyse_risk_tips": "risk_tips",
    "s_trans_scale_risk_tips": "risk_tips",
    "s_bank_trans_type_risk_tips": "risk_tips",
    "s_loan_analyse_risk_tips": "risk_tips",
    "s_daily_mean_balance_risk_tips": "risk_tips",
    "s_money_mobilize_ability_risk_tips": "risk_tips",
    "t_analysis_subjects_risk_tips": "risk_tips",
    "t_confidence_analyse_risk_tips": "risk_tips",
    "t_trans_scale_risk_tips": "risk_tips",
    "t_bank_trans_income_type_risk_tips": "risk_tips",
    "t_bank_trans_expense_type_risk_tips": "risk_tips",
    "t_loan_analyse_risk_tips": "risk_tips",
    "t_business_scale_risk_tips": "risk_tips",
    "t_business_risk_risk_tips": "risk_tips",
    "t_daily_mean_balance_risk_tips": "risk_tips",
    "t_money_mobilize_ability_risk_tips": "risk_tips",
    "t_upstream_customers_risk_tips": "risk_tips",
    "t_downstream_customers_risk_tips": "risk_tips",
    "t_strong_relation_info_risk_tips": "risk_tips",
    "t_normal_relation_info_risk_tips": "risk_tips",
    "t_guarantor_info_risk_tips": "risk_tips",
    "t_abnormal_trans_risk_risk_tips": "risk_tips",
    "t_marketing_feedback_risk_tips": "risk_tips",
    "trm_risk_tips": "risk_tips"

}
variable_name_mapping = {
    "cusName": "客户名称",
    "relation_info": "关联信息",
    "flow_info": "流水信息",
    "suggest_repay_month": "建议本金还款月",
    "suggest_repay_month_balance": "建议本金还款月_余额",
    "confidence_analyse": "可信度分析",
    "bank_summary_tips": "银行流水结论",
    "bank_summary_trend_chart": "银行流水表单信息",
    "wxzfb_summary_tips": "微信支付宝流水结论",
    "wxzfb_summary_trend_chart": "微信支付宝表单信息",
    "loan_analyse_tips": "融资分析结论",
    "loan_analyse_trend_chart": "融资表单信息",
    "bank_expense_trans_circular_chart": "出账环形图",
    "bank_expense_trans_tips": "出账结论",
    "bank_expense_distribute": "出账分账户",
    "bank_normal_trans_circular_chart": "进账环形图",
    "bank_normal_trans_tips": "进账结论",
    "bank_normal_distribute": "进账分账户",
    "operational_form_detail": "经营性流水表单详细信息",
    "operational_trend_chart": "经营性流水分月趋势图信息",
    "risk_tips_season": "经营淡旺季",
    "risk_tips_union": "经营性收入及月均",
    "normal_income_m": "经营性收入金额，单位元",
    "expense_amt_order": "上游客户",
    "income_amt_order": "下游客户",
    "trans_summary_risk_tips": "结息日均、余额日均结论",
    "trans_summary_trans_detail": "结息详情",
    "expense_section": "出账资金调动能力",
    "income_section": "进账资金调动能力",
    "trm_risk_tips": "资金调动能力结论",
    "bull_form_detail": "多头表单信息",
    "bull_risk_tips": "多头结论",
    "gaming_form_detail": "博彩表单",
    "gaming_risk_tips": "博彩结论",
    "amusement_form_detail": "娱乐表单",
    "amusement_risk_tips": "娱乐结论",
    "case_disputes_form_detail": "案件纠纷表单",
    "case_disputes_risk_tips": "案件纠纷结论",
    "security_fines_form_detail": "治安罚款表单",
    "security_fines_risk_tips": "治安罚款结论",
    "insurance_claims_form_detail": "保险理赔表单",
    "insurance_claims_risk_tips": "保险理赔结论",
    "stock_form_detail": "股票期货表单",
    "stock_risk_tips": "股票期货结论",
    "hospital_form_detail": "医疗表单",
    "hospital_risk_tips": "医疗结论",
    "noritomo_form_detail": "典当表单",
    "noritomo_risk_tips": "典当结论",
    "loan_form_detail": "贷款异常表单",
    "loan_risk_tips": "贷款异常结论",
    "foreign_guarantee_form_detail": "对外担保异常表单",
    "foreign_guarantee_risk_tips": "对外担保异常结论",
    "big_in_out_form_detail": "整进整出表单",
    "big_in_out_risk_tips": "整进整出结论",
    "fast_in_out_form_detail": "快进快出表单",
    "fast_in_out_risk_tips": "快进快出结论",
    "financing_form_detail": "理财行为表单",
    "financing_risk_tips": "理财行为结论",
    "house_sale_form_detail": "房产买卖表单",
    "house_sale_risk_tips": "房产买卖结论",
    "family_unstable_form_detail": "家庭不稳定表单",
    "family_unstable_risk_tips": "家庭不稳定结论",
    "com_expense": "对公出账",
    "com_income": "对公进账",
    "per_expense": "对私出账",
    "per_income": "对私进账",
    "strong_related_risk_tips": "强关联交易结论",
    "strong_related_trans_detail": "强关联交易详情",
    "generally_related_risk_tips": "一般关联交易结论",
    "generally_related_trans_detail": "一般关联交易详情",
    "third_party_guarantee_risk_tips": "第三方担保交易结论",
    "third_party_guarantee_trans_detail": "第三方担保交易详情",
    "ahp_income_loanable": "进账资金调动能力",
    "income_rate_0_to_1": "流水进账0-1w区间占比",
    "petty_loan_expense_cnt_6m": "近6个月小贷出账次数",
    "petty_loan_income_cnt_6m": "近6个月小贷进账次数",
    "mean_balance_12m": "近一年余额日均",
    "total_income_cnt_3m": "近3个月非银机构总进账次数",
    "consumption_income_amt_6m": "近6个月消金进账金额",
    "factoring_expense_cnt_12m": "近12个月保理出账次数",
    "private_lending_expense_cnt_6m": "近6个月民间借贷出账次数",
    "financial_leasing_min_expense_amt_12m": "近12个月融资租赁最小出账金额",
    "bank_max_income_amt_3m": "近3个月银行最大进账金额",
    "hospital_expense_cnt_12m": "近12个月医院出账次数",
    "house_sale_expense_cnt_12m": "近12个月房产买卖出账次数",
    "credible_score": "可信度评分",
    "model_score": "模型综合评分",
    "unbank_repay_type_cnt_r3m": "近3个月还款非银机构类型数",
    "court_cnt_6m": "近6个月案件纠纷交易笔数",
    "relationship_income_rate": "关联交易进账占比",
    "top5_income_rate": "前五大经营性进账交易对手进账占比",
    "ahp_income_mean_m": "月均经营性进账",
    "ahp_loan_income_amt_proportion": "近一年贷款金额/经营性进账",
    "ahp_loanable": "资金调动能力",
    "ahp_relationship_income_amt_proportion": "关联关系进账金额占比",
    "ahp_relationship_expense_amt_proportion": "关联关系出账金额占比",
    "ahp_income_amt_diff_proportion": "年经营性进账变化率",
    "ahp_top_10_change_rate": "年经营性前十大交易对手变化率",
    "ahp_large_income_period_rate": "大额进账月份占比",
    "ahp_income_amt_rate_top5": "经营性进账前五大交易对手交易金额占比",
    "ahp_expense_amt_rate_top5": "经营性出账前五大交易对手交易金额占比",
    "ahp_mean_interest_12m": "近一年结息日均",
    "ahp_others_investment_net_amt_proportion": "其它投资净值/经营性进账",
    "ahp_others_investment_amt_proportion": "其他投资总额/经营性进账",
    "ahp_investment_amt": "对外投资金额",
    "ahp_dividends": "对外投资",
    "ahp_unusual_trans_cnt_proportion": "异常交易笔数占比",
    "ahp_extreme_income_amt_proportion": "极端大额进账金额占比",
    "ahp_extreme_expense_amt_proportion": "极端大额出账金额占比",
    "ahp_not_bank_trans_cnt": "近半年非银机构交易笔数",
    "ahp_not_bank_trans_org_cnt": "近半年交易非银机构类型数",
    "ahp_bank_trans_amt": "近半年银行机构贷款金额",
    "ahp_not_bank_trans_amt": "近半年非银机构贷款金额",
    "flow_score": "模型分",
    "s_confidence_analyse_risk_tips": "建议指导-概貌-可信度分析",
    "s_trans_scale_risk_tips": "建议指导-概貌-流水规模",
    "s_bank_trans_type_risk_tips": "建议指导-概貌-账户交易类型",
    "s_loan_analyse_risk_tips": "建议指导-融资情况",
    "s_daily_mean_balance_risk_tips": "建议指导-经营情况-余额日均",
    "s_money_mobilize_ability_risk_tips": "建议指导-经营情况-资金调动",
    "t_analysis_subjects_risk_tips": "解析结果-概貌-分析对象",
    "t_confidence_analyse_risk_tips": "解析结果-概貌-可信度分析",
    "t_trans_scale_risk_tips": "解析结果-概貌-流水规模",
    "t_bank_trans_income_type_risk_tips": "解析结果-概貌-进账类型",
    "t_bank_trans_expense_type_risk_tips": "解析结果-概貌-出账类型",
    "t_loan_analyse_risk_tips": "解析结果-融资情况",
    "t_business_scale_risk_tips": "解析结果-经营情况-流水规模",
    "t_business_risk_risk_tips": "解析结果-经营情况-经营风险",
    "t_daily_mean_balance_risk_tips": "解析结果-经营情况-余额日均",
    "t_money_mobilize_ability_risk_tips": "解析结果-经营情况-资金调动",
    "t_upstream_customers_risk_tips": "解析结果-经营情况-上游客户",
    "t_downstream_customers_risk_tips": "解析结果-经营情况-下游客户",
    "t_strong_relation_info_risk_tips": "解析结果-关联信息-强关联",
    "t_normal_relation_info_risk_tips": "解析结果-关联信息-一般关联",
    "t_guarantor_info_risk_tips": "解析结果-关联关系-第三方担保",
    "t_abnormal_trans_risk_risk_tips": "解析结果-特殊交易",
    "t_marketing_feedback_risk_tips": "解析结果-营销反哺",
    "model_risk": "风险评级"
}
rules_variables_list = ['petty_loan_expense_cnt_6m', 'petty_loan_income_cnt_6m', 'mean_balance_12m',
                        'total_income_cnt_3m', 'consumption_income_amt_6m', 'factoring_expense_cnt_12m',
                        'private_lending_expense_cnt_6m', 'financial_leasing_min_expense_amt_12m',
                        'bank_max_income_amt_3m', 'hospital_expense_cnt_12m', 'house_sale_expense_cnt_12m',
                        'credible_score', 'model_score', 'unbank_repay_type_cnt_r3m', 'court_cnt_6m',
                        'relationship_income_rate', 'top5_income_rate', 'ahp_income_loanable', 'income_rate_0_to_1']
model_variables_list = ['ahp_income_mean_m', 'ahp_loan_income_amt_proportion', 'ahp_loanable',
                        'ahp_relationship_income_amt_proportion', 'ahp_relationship_expense_amt_proportion',
                        'ahp_income_amt_diff_proportion', 'ahp_top_10_change_rate', 'ahp_large_income_period_rate',
                        'ahp_income_amt_rate_top5', 'ahp_expense_amt_rate_top5', 'ahp_mean_interest_12m',
                        'ahp_others_investment_net_amt_proportion', 'ahp_others_investment_amt_proportion',
                        'ahp_investment_amt', 'ahp_dividends', 'ahp_unusual_trans_cnt_proportion',
                        'ahp_extreme_income_amt_proportion', 'ahp_extreme_expense_amt_proportion',
                        'ahp_not_bank_trans_cnt', 'ahp_not_bank_trans_org_cnt', 'ahp_bank_trans_amt',
                        'ahp_not_bank_trans_amt', 'flow_score', 'model_risk']


class ParserFlowVariables:

    def __init__(self, resp):
        self.resp = resp
        self.report_req_no = None
        self.product_code = None
        self.unique_code = None
        self.feature_list = []
        self.main_subject = None

    def process(self):
        self.get_public_param()
        self.parser_title()
        self.parser_proportion_and_repay_month()
        self.parser_suggestion_and_guide()
        self.parser_trans_report_overview()
        self.parser_confidence_analyse()
        self.parser_trans_report_fullview()
        self.parser_operational_analysis()
        self.parser_trans_u_counterparty_portrait()
        self.parser_trans_u_summary_portrait()
        self.parser_trans_risk_money()
        self.parser_bull_credit_risk()
        self.parser_abnormal_trans_risk()
        self.normal_trans_trans_detail()
        self.parser_marketing_feedback()
        self.trans_u_related_portrait()
        self.parser_rules_variables()
        self.parser_model_variables()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def get_public_param(self):
        if self.resp is not None:
            self.product_code = self.resp['product_code']
            for subject in self.resp['subject']:
                if subject['queryData']['relation'] != 'MAIN':
                    continue
                self.main_subject = subject
                self.report_req_no = subject['queryData']['preReportReqNo']
                self.unique_code = subject['queryData']['idno']
                break

    def parser_title(self):
        title_info = self.main_subject['reportDetail'][0]['variables']['表头']
        for i in ['cusName', 'flow_info', 'relation_info']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'title'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(title_info[mapping_dict[i]], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_proportion_and_repay_month(self):
        repay_month_info = self.main_subject['reportDetail'][0]['variables']['proportion_and_repay_month']
        for i in ['suggest_repay_month', 'suggest_repay_month_balance']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'proportion_and_repay_month'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(repay_month_info[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_suggestion_and_guide(self):
        suggestion_and_guide_info = self.main_subject['reportDetail'][0]['variables']['suggestion_and_guide']
        # trans_general_info
        trans_general_info = suggestion_and_guide_info['trans_general_info']
        for i in ['confidence_analyse', 'trans_scale', 'bank_trans_type']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'suggestion_and_guide_info'
            temp_dict['level_2'] = 'trans_general_info'
            temp_dict['level_3'] = i
            temp_dict['variable_name'] = "s_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["s_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(trans_general_info[i]['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # loan_analyse
        loan_analyse_info = suggestion_and_guide_info['loan_analyse']
        for i in ['loan_analyse']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'suggestion_and_guide_info'
            temp_dict['level_2'] = i
            # temp_dict['level_3'] = i
            temp_dict['variable_name'] = "s_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["s_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(loan_analyse_info['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # business_info
        business_info = suggestion_and_guide_info['business_info']
        for i in ['daily_mean_balance', 'money_mobilize_ability']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'suggestion_and_guide_info'
            temp_dict['level_2'] = 'business_info'
            temp_dict['level_3'] = i
            temp_dict['variable_name'] = "s_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["s_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(business_info[i]['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_trans_report_overview(self):
        trans_report_overview_info = self.main_subject['reportDetail'][0]['variables']['trans_report_overview']
        # trans_general_info
        trans_general_info = trans_report_overview_info['trans_general_info']
        for i in ['analysis_subjects', 'confidence_analyse', 'trans_scale', 'bank_trans_income_type',
                  'bank_trans_expense_type']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = 'trans_general_info'
            temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(trans_general_info[i]['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # loan_analyse
        loan_analyse_info = trans_report_overview_info['loan_analyse']
        for i in ['loan_analyse']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = i + '_risk'
            # temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(loan_analyse_info['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # business_info
        business_info = trans_report_overview_info['business_info']
        for i in ['business_scale', 'business_risk', 'daily_mean_balance', 'money_mobilize_ability',
                  'upstream_customers', 'downstream_customers']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = 'business_info'
            temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(business_info[i]['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # related_info
        related_info = trans_report_overview_info['related_info']
        for i in ['strong_relation_info', 'normal_relation_info', 'guarantor_info']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = 'related_info'
            temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(related_info[i]['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # abnormal_trans_risk
        abnormal_trans_risk_info = trans_report_overview_info['abnormal_trans_risk']
        for i in ['abnormal_trans_risk']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = i
            # temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(abnormal_trans_risk_info['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # marketing_feedback
        marketing_feedback_info = trans_report_overview_info['marketing_feedback']
        for i in ['marketing_feedback']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_overview'
            temp_dict['level_2'] = i
            # temp_dict['level_3'] = i
            temp_dict['variable_name'] = "t_" + i + "_risk_tips"
            temp_dict['variable_name_cn'] = variable_name_mapping["t_" + i + "_risk_tips"]
            temp_dict['variable_values'] = json.dumps(marketing_feedback_info['risk_tips'], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_confidence_analyse(self):
        confidence_analyse_info = self.main_subject['reportDetail'][0]['variables']['confidence_analyse']
        for i in ['confidence_analyse']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'confidence_analyse'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(confidence_analyse_info, ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_trans_report_fullview(self):
        trans_report_fullview_info = self.main_subject['reportDetail'][0]['variables']['trans_report_fullview']
        # bank_summary
        bank_summary = trans_report_fullview_info['bank_summary']
        for i in ['bank_summary_tips', 'bank_summary_trend_chart']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_fullview'
            temp_dict['level_2'] = 'bank_summary'
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(bank_summary[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # wxzfb_summary
        wxzfb_summary = trans_report_fullview_info['wxzfb_summary']
        for i in ['wxzfb_summary_tips', 'wxzfb_summary_trend_chart']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_fullview'
            temp_dict['level_2'] = 'wxzfb_summary'
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(wxzfb_summary[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # loan_analyse
        loan_analyse = trans_report_fullview_info['loan_analyse']
        for i in ['loan_analyse_tips', 'loan_analyse_trend_chart']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_fullview'
            temp_dict['level_2'] = 'loan_analyse'
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(loan_analyse[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        # bank_trans_type
        bank_trans_type = trans_report_fullview_info['bank_trans_type']
        bank_expense = bank_trans_type['bank_expense']
        bank_normal = bank_trans_type['bank_normal']
        for i in ['bank_expense_trans_circular_chart', 'bank_expense_trans_tips', 'bank_expense_distribute']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_fullview'
            temp_dict['level_2'] = 'bank_trans_type'
            temp_dict['level_3'] = 'bank_expense'
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(bank_expense[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

        for i in ['bank_normal_trans_circular_chart', 'bank_normal_trans_tips', 'bank_normal_distribute']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_report_fullview'
            temp_dict['level_2'] = 'bank_trans_type'
            temp_dict['level_3'] = 'bank_normal'
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(bank_normal[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_operational_analysis(self):
        operational_analysis_info = self.main_subject['reportDetail'][0]['variables']['operational_analysis']
        for i in ['operational_form_detail', 'operational_trend_chart', 'risk_tips_season', 'risk_tips_union', 'normal_income_m']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'operational_analysis'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(operational_analysis_info[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_trans_u_counterparty_portrait(self):
        counterparty_portrait_info = self.main_subject['reportDetail'][0]['variables'][
            'trans_u_counterparty_portrait']
        for i in ['expense_amt_order', 'income_amt_order']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_u_counterparty_portrait'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(counterparty_portrait_info[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_trans_u_summary_portrait(self):
        summary_portrait_info = self.main_subject['reportDetail'][0]['variables']['trans_u_summary_portrait']
        for i in ['trans_summary_risk_tips', 'trans_summary_trans_detail']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_u_summary_portrait'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(summary_portrait_info[mapping_dict[i]], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_trans_risk_money(self):
        trans_risk_money_info = self.main_subject['reportDetail'][0]['variables']['trans_risk_money']
        for i in ['expense_section', 'income_section', 'risk_tips']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_risk_money'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i if i != 'risk_tips' else 'trm_' + i
            temp_dict['variable_name_cn'] = variable_name_mapping[i] if i != 'risk_tips' else variable_name_mapping['trm_' + i]
            temp_dict['variable_values'] = json.dumps(trans_risk_money_info[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_bull_credit_risk(self):
        bull_credit_risk_info = self.main_subject['reportDetail'][0]['variables']['bull_credit_risk']
        for i in ['bull_form_detail', 'bull_risk_tips']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'bull_credit_risk'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(bull_credit_risk_info[mapping_dict[i]], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_abnormal_trans_risk(self):
        abnormal_trans_risk_info = self.main_subject['reportDetail'][0]['variables']['abnormal_trans_risk']
        for i in ['gaming', 'amusement', 'case_disputes', 'security_fines', 'insurance_claims', 'stock', 'hospital',
                  'noritomo', 'loan', 'foreign_guarantee']:
            for j in ['form_detail', 'risk_tips']:
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['level_1'] = 'abnormal_trans_risk'
                # temp_dict['level_2'] = ''
                temp_dict['level_3'] = i
                temp_dict['variable_name'] = i + '_' + j
                temp_dict['variable_name_cn'] = variable_name_mapping[i + '_' + j]
                temp_dict['variable_values'] = json.dumps(abnormal_trans_risk_info[i][j], ensure_ascii=False)
                self.feature_list.append(temp_dict)

    def normal_trans_trans_detail(self):
        normal_trans_detail_info = self.main_subject['reportDetail'][0]['variables']['normal_trans_detail']
        for i in ['fast_in_out', 'big_in_out', 'financing', 'house_sale', 'family_unstable']:
            for j in ['form_detail', 'risk_tips']:
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['level_1'] = 'normal_trans_detail'
                # temp_dict['level_2'] = ''
                temp_dict['level_3'] = i
                temp_dict['variable_name'] = i + '_' + j
                temp_dict['variable_name_cn'] = variable_name_mapping[i + '_' + j]
                temp_dict['variable_values'] = json.dumps(normal_trans_detail_info[i][j], ensure_ascii=False)
                self.feature_list.append(temp_dict)

    def parser_marketing_feedback(self):
        marketing_feedback_info = self.main_subject['reportDetail'][0]['variables']['营销反哺']
        for i in ['com_expense', 'com_income', 'per_expense', 'per_income']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'marketing_feedback'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(marketing_feedback_info[mapping_dict[i]], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def trans_u_related_portrait(self):
        # strong and generally info
        related_portrait_info = self.main_subject['reportDetail'][0]['variables']['trans_u_related_portrait']
        for i in ['strong_related', 'generally_related']:
            for j in ['risk_tips', 'trans_detail']:
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['level_1'] = 'trans_u_related_portrait'
                temp_dict['level_2'] = i + '_trans_info'
                # temp_dict['level_3'] = ''
                temp_dict['variable_name'] = i + '_' + j
                temp_dict['variable_name_cn'] = variable_name_mapping[i + '_' + j]
                temp_dict['variable_values'] = json.dumps(related_portrait_info[i + '_trans_info'][j],
                                                          ensure_ascii=False)
                self.feature_list.append(temp_dict)

        # third_party_guarantee info
        third_party_guarantee_info = self.main_subject['reportDetail'][0]['variables'][
            'third_party_guarantee_info']
        for i in ['risk_tips', 'trans_detail']:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'trans_u_related_portrait'
            temp_dict['level_2'] = 'third_party_guarantee_info'
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = 'third_party_guarantee_' + i
            temp_dict['variable_name_cn'] = variable_name_mapping['third_party_guarantee_' + i]
            temp_dict['variable_values'] = json.dumps(
                third_party_guarantee_info[mapping_dict['third_party_guarantee_' + i]], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_rules_variables(self):
        rules_variables_info = self.main_subject['strategyResult'][
            'StrategyOneResponse']['Body']['Application']['Variables']
        for i in rules_variables_list:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'rules_variables'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            if i == 'model_score':
                temp_dict['variable_values'] = json.dumps(rules_variables_info['flow_score'], ensure_ascii=False)
            else:
                temp_dict['variable_values'] = json.dumps(rules_variables_info[i], ensure_ascii=False)
            self.feature_list.append(temp_dict)

    def parser_model_variables(self):
        model_variables_info = self.main_subject['strategyResult'][
            'StrategyOneResponse']['Body']['Application']['Variables']
        for i in model_variables_list:
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['product_code'] = self.product_code
            temp_dict['unique_code'] = self.unique_code
            temp_dict['level_1'] = 'model_variables'
            # temp_dict['level_2'] = ''
            # temp_dict['level_3'] = ''
            temp_dict['variable_name'] = i
            temp_dict['variable_name_cn'] = variable_name_mapping[i]
            temp_dict['variable_values'] = json.dumps(model_variables_info[i], ensure_ascii=False) \
                if model_variables_info[i] != -999 else ""
            self.feature_list.append(temp_dict)
