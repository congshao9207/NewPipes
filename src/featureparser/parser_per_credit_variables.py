import json
from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str

mapping_dict = {
    "rules_credit_overdue_5year": "credit_overdue_5year",
    "model_credit_overdue_5year": "credit_overdue_5year",
    "rules_credit_financial_tension": "credit_financial_tension",
    "model_credit_financial_tension": "credit_financial_tension",
    "report_extension_number": "extension_number",
    "rules_extension_number": "extension_number",
    "report_total_credit_min_repay_cnt": "total_credit_min_repay_cnt",
    "rules_total_credit_min_repay_cnt": "total_credit_min_repay_cnt"
}
model_variable_name_mapping = {
    "all_house_car_loan_reg_cnt": "所有房屋、汽车贷款机构数",
    "model_credit_overdue_5year": "总计贷记卡5年内逾期次数__",
    "model_credit_financial_tension": "贷记卡资金紧张程度__",
    "unsettled_loan_number": "未结清贷款笔数",
    "unsettled_house_loan_number": "未结清房贷笔数",
    "loan_approval_year1": "贷款审批最近一年内查询次数",
}
rules_variable_name_mapping = {
    "rhzx_business_loan_3year_overdue_cnt": "3年内经营性贷款逾期笔数",
    "public_sum_count": "呆账、资产处置、保证人代偿笔数",
    "loan_fiveLevel_a_level_cnt": "贷款五级分类存在-次级、可疑、损失",
    "business_loan_average_3year_overdue_cnt": "3年内还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数",
    "credit_now_overdue_money": "贷记卡当前逾期金额",
    "consume_loan_now_overdue_money": "消费性贷款当前逾期金额",
    "bus_loan_now_overdue_money": "经营性贷款当前逾期金额",
    "single_credit_or_loan_3year_overdue_max_month": "贷记卡/贷款逾期最大连续月",
    "single_credit_overdue_2year_cnt": "单张贷记卡近2年内最大逾期次数",
    "single_bus_loan_overdue_2year_cnt": "单笔经营性贷款近2年内最大逾期次数",
    "single_consume_loan_overdue_2year_cnt": "单笔消费性贷款近2年内最大逾期次数",
    "loan_fiveLevel_b_level_cnt": "贷款五级分类存在-关注",
    "loan_scured_five_a_level_abnormality_cnt": "对外担保五级分类存在-次级、可疑、损失",
    "rules_extension_number": "展期笔数___",
    "loan_scured_five_b_level_abnormality_cnt": "对外担保五级分类存在-关注",
    "credit_status_bad_cnt": "贷记卡账户2年内出现过-呆账",
    "credit_status_legal_cnt": "贷记卡账户状态存在-司法追偿",
    "credit_status_b_level_cnt": "贷记卡账户状态存在-银行止付、冻结",
    "loan_status_bad_cnt": "贷款账户状态存在-呆账",
    "loan_status_legal_cnt": "贷款账户状态存在-司法追偿",
    "loan_status_b_level_cnt": "贷款账户状态存在-银行止付、冻结",
    "if_marriage": "婚姻状况不一致",
    "loan_credit_query_3month_cnt": "近三个月征信查询（贷款审批及贷记卡审批）次数",
    "total_credit_used_rate": "贷记卡总透支率达",
    "rules_total_credit_min_repay_cnt": "贷记卡最低还款张数___",
    "credit_overdue_2year": "总计贷记卡（信用卡）2年内逾期次数",
    "loan_consume_overdue_2year": "总计贷款2年内逾期次数",
    "unsettled_busLoan_agency_number": "有经营性贷款在贷余额的合作机构家数",
    "large_loan_2year_overdue_cnt": "经营性贷款（经营性+个人消费大于等于20万+农户+其他）2年内最大连续逾期期数",
    "enforce_record": "强制执行记录条数",
    "unsettled_consume_agency_cnt": "未结清消费性贷款机构家数",
    "unsettled_consume_total_cnt": "未结清消费性贷款笔数",
    "rules_credit_financial_tension": "贷记卡资金紧张程度___",
    "credit_activated_number": "已激活贷记卡张数",
    "credit_min_payed_number": "贷记卡最低还款张数",
    "uncancelled_credit_organization_number": "未销户贷记卡发卡机构家数",
    "unsettled_busLoan_total_cnt": "未结清经营性贷款笔数",
    "rules_credit_overdue_5year": "总计贷记卡5年内逾期次数___",
    "marriage_status": "离婚",
    "judgement_record": "民事判决记录条数",
    "loan_doubtful": "疑似压贷笔数",
    "guarantee_amont": "对外担保金额：元",
    "unsettled_loan_agency_number": "未结清贷款机构家数",
    "unsettled_consume_total_amount": "未结清消费性贷款总额：元",
    "tax_record": "欠税记录条数",
    "ad_penalty_record": "行政处罚记录条数",
    "rhzx_business_loan_3year_ago_overdue_cnt": "3年前经营性贷款逾期笔数",
    "business_loan_average_3year_ago_overdue_cnt": "3年前还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数",

}
variable_name_mapping = {
    "account_org": "信贷交易信息-贷款信息-近三年机构申请总变化机构名称",
    "average_repay_12m_after": "信贷交易信息-资金压力解析-未来12个月平均应还款",
    "average_repay_6m_before": "信贷交易信息-资金压力解析-过去6个月平均应还款",
    "bank_query_cnt": "查询信息-近一年贷款审批和贷记卡审批的查询记录银行查询笔数",
    "bank_query_loan_cnt": "查询信息-近一年贷款审批和贷记卡审批的查询记录银行查询未放款笔数",
    "busi_loan_balance_max": "信贷交易信息-贷款信息-近五年经营性贷款余额变化-最大余额",
    "busi_loan_balance_min": "信贷交易信息-贷款信息-近五年经营性贷款余额变化-最小余额",
    "busi_org_balance_1y_ago_max": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前最大余额",
    "busi_org_balance_1y_ago_min": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前最小余额",
    "busi_org_balance_2y_ago_max": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前最大余额",
    "busi_org_balance_2y_ago_min": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前最小余额",
    "busi_org_balance_3y_ago_max": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前最大余额",
    "busi_org_balance_3y_ago_min": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前最小余额",
    "busi_org_balance_now_max": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前余额",
    "busi_org_balance_now_min": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前余额",
    "busi_org_cnt_1y_ago": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前个数",
    "busi_org_cnt_2y_ago": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前个数",
    "busi_org_cnt_3y_ago": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前个数",
    "busi_org_cnt_now": "信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前个数",
    "category": "征信不良信息-严重预警信息-五级分类状态",
    "certificate_no": "证件号码",
    "communication_address": "通讯地址",
    "credit_avg_used_6m": "信贷交易信息-贷记卡信息-贷记卡信息汇总最近6个月平均使用额度",
    "credit_loan_date": "信贷交易信息-贷记卡信息-贷记卡信息汇总-开户时间",
    "credit_loan_status": "信贷交易信息-贷记卡信息-贷记卡信息汇总-账户状态",
    "credit_min_repay": "信贷交易信息-贷记卡信息-贷记卡信息汇总是否为最低还款",
    "credit_min_repay_cnt": "信贷交易信息-贷记卡信息-贷记卡信息汇总最低还款张数",
    "credit_org": "信贷交易信息-贷记卡信息-贷记卡信息汇总发卡机构",
    "credit_org_cnt": "信贷交易信息-贷记卡信息-贷记卡信息汇总发卡机构个数",
    "credit_principal_amount": "信贷交易信息-贷记卡信息-贷记卡信息汇总-授信额度",
    "credit_query_cnt": "查询信息-近一年贷款审批和贷记卡审批的查询记录贷记卡查询笔数",
    "credit_query_loan_cnt": "查询信息-近一年贷款审批和贷记卡审批的查询记录贷记卡查询未放款笔数",
    "credit_quota_used": "信贷交易信息-贷记卡信息-贷记卡信息汇总-已使用额度",
    "credit_usage_rate": "信贷交易信息-贷记卡信息-贷记卡信息汇总贷记卡使用率",
    "default_type": "征信不良信息-严重预警信息-交易违约信息",
    "each_interest_rate": "信贷交易信息-贷款信息-贷款趋势变化图-贷款利率",
    "each_loan_account_org": "信贷交易信息-贷款信息-贷款趋势变化图-贷款机构",
    "each_loan_date": "信贷交易信息-贷款信息-贷款趋势变化图-发放时间",
    "each_loan_status": "信贷交易信息-贷款信息-贷款趋势变化图-账号状态",
    "each_loan_type": "信贷交易信息-贷款信息-贷款趋势变化图-贷款类型",
    "each_principal_amount": "信贷交易信息-贷款信息-贷款趋势变化图-贷款发放额",
    "employment": "就业状况",
    "ensure_max_principal": "信贷交易信息-贷款信息-担保方式余额分布保证类最大金额",
    "ensure_principal_multi_apply": "信贷交易信息-贷款信息-担保方式余额分布保证类最大金额是我司申请金额倍数",
    "report_extension_number": "征信不良信息-严重预警信息-展期",
    "guar_acc_org": "担保信息-担保信息明细-管理机构",
    "guar_acc_org_cnt": "担保信息-担保信息明细-管理机构个数",
    "guar_end_date": "担保信息-担保信息明细-到期日期",
    "guar_latest_category": "担保信息-担保信息明细-五级分类",
    "guar_loan_balance": "担保信息-担保信息明细-担保余额",
    "guar_loan_type": "担保信息-担保信息明细-业务种类",
    "guar_principal_amount": "担保信息-担保信息明细-担保金额",
    "guar_query_cnt": "查询信息-近三个月查询记录-保前审查记录条数",
    "guar_type": "信贷交易信息-贷款信息-担保方式余额分布-担保类型",
    "guar_type_balance": "信贷交易信息-贷款信息-担保方式余额分布-目前余额",
    "guar_type_balance_prop": "信贷交易信息-贷款信息-担保方式余额分布-余额占比",
    "guar_type_cnt": "信贷交易信息-贷款信息-担保方式余额分布-目前笔数",
    "hint_account_org": "信贷交易信息-资金压力解析-未推算出的贷款-机构名",
    "hint_loan_date": "信贷交易信息-资金压力解析-未推算出的贷款-发放时间",
    "hint_principal_amount": "信贷交易信息-资金压力解析-未推算出的贷款-贷款金额",
    "house_loan_pre_settle_date": "信贷交易信息-贷款信息-房贷提前结清日期",
    "house_loan_pre_settle_org": "信贷交易信息-贷款信息-房贷提前结清机构名",
    "if_loan": "查询信息-近一年贷款审批和贷记卡审批的查询记录是否放款",
    "info_marriage_status": "婚姻状况",
    "jhi_time_1y": "查询信息-近一年贷款审批和贷记卡审批的查询记录查询日期",
    "jhi_time_3m": "查询信息-近三个月查询记录-查询日期",
    "loan_principal_0_20w_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-0-20万笔数",
    "loan_principal_0_20w_prop": "信贷交易信息-贷款信息-贷款额度区间分布-0-20万占比",
    "loan_principal_100_200w_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-100-200万笔数",
    "loan_principal_100_200w_prop": "信贷交易信息-贷款信息-贷款额度区间分布-100-200万占比",
    "loan_principal_200w_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-大于200万笔数",
    "loan_principal_200w_prop": "信贷交易信息-贷款信息-贷款额度区间分布-大于200万占比",
    "loan_principal_20_50w_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-20-50万笔数",
    "loan_principal_20_50w_prop": "信贷交易信息-贷款信息-贷款额度区间分布-20-50万占比",
    "loan_principal_50_100w_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-50-100万笔数",
    "loan_principal_50_100w_prop": "信贷交易信息-贷款信息-贷款额度区间分布-50-100万占比",
    "loan_principal_total_cnt": "信贷交易信息-贷款信息-贷款额度区间分布-总数",
    "loan_query_cnt": "查询信息-近三个月查询记录-资信审查记录条数",
    "loan_type": "信贷交易信息-贷款信息-贷款类型余额分布-贷款类型",
    "loan_type_12m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款类型-前12个月",
    "loan_type_3m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款类型-前3个月",
    "loan_type_6m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款类型-前6个月",
    "loan_type_balance": "信贷交易信息-贷款信息-贷款类型余额分布-目前余额",
    "loan_type_balance_prop": "信贷交易信息-贷款信息-贷款类型余额分布-余额占比",
    "loan_type_cnt": "信贷交易信息-贷款信息-贷款类型余额分布-目前笔数",
    "max_interest_rate_1y_ago": "信贷交易信息-贷款信息-近一年机构申请总变化最大利率",
    "max_interest_rate_2y_ago": "信贷交易信息-贷款信息-近二年机构申请总变化最大利率",
    "max_interest_rate_3y_ago": "信贷交易信息-贷款信息-近三年机构申请总变化最大利率",
    "max_principal_amount": "信贷交易信息-贷款信息-贷款趋势变化图最大贷款金额",
    "max_terms_1y_ago": "信贷交易信息-贷款信息-近一年机构申请总变化最大还款期数",
    "max_terms_2y_ago": "信贷交易信息-贷款信息-近二年机构申请总变化最大还款期数",
    "max_terms_3y_ago": "信贷交易信息-贷款信息-近三年机构申请总变化最大还款期数",
    "min_principal_amount": "信贷交易信息-贷款信息-贷款趋势变化图最小贷款金额",
    "mort_max_principal": "信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额",
    "mort_no_settle_loan_date": "个人信息-固定资产-按揭未结清",
    "mort_principal_multi_apply": "信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额",
    "mort_settle_loan_date": "个人信息-固定资产-按揭已归还",
    "multiple_principal_amount": "信贷交易信息-贷款信息-贷款趋势变化图贷款金额比值",
    "name": "姓名",
    "new_org_12m_ago": "信贷交易信息-贷款信息-贷款申请新增机构-前12个月",
    "new_org_3m_ago": "信贷交易信息-贷款信息-贷款申请新增机构-前3个月",
    "new_org_6m_ago": "信贷交易信息-贷款信息-贷款申请新增机构-前6个月",
    "operator_1y": "查询信息-近一年贷款审批和贷记卡审批的查询记录查询机构",
    "operator_3m": "查询信息-近三个月查询记录-查询机构",
    "phone": "手机号码",
    "principal_amount_12m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款金额-前12个月",
    "principal_amount_3m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款金额-前3个月",
    "principal_amount_6m_ago": "信贷交易信息-贷款信息-贷款申请新增贷款金额-前6个月",
    "reason_1y": "查询信息-近一年贷款审批和贷记卡审批的查询记录查询原因",
    "reason_3m": "查询信息-近三个月查询记录-查询原因",
    "repay_credit_10m_after": "信贷交易信息-资金压力解析-应还贷记卡10个月后",
    "repay_credit_11m_after": "信贷交易信息-资金压力解析-应还贷记卡11个月后",
    "repay_credit_12m_after": "信贷交易信息-资金压力解析-应还贷记卡12个月后",
    "repay_credit_1m_after": "信贷交易信息-资金压力解析-应还贷记卡1个月后",
    "repay_credit_1m_before": "信贷交易信息-资金压力解析-应还贷记卡1个月前",
    "repay_credit_2m_after": "信贷交易信息-资金压力解析-应还贷记卡2个月后",
    "repay_credit_2m_before": "信贷交易信息-资金压力解析-应还贷记卡2个月前",
    "repay_credit_3m_after": "信贷交易信息-资金压力解析-应还贷记卡3个月后",
    "repay_credit_3m_before": "信贷交易信息-资金压力解析-应还贷记卡3个月前",
    "repay_credit_4m_after": "信贷交易信息-资金压力解析-应还贷记卡4个月后",
    "repay_credit_4m_before": "信贷交易信息-资金压力解析-应还贷记卡4个月前",
    "repay_credit_5m_after": "信贷交易信息-资金压力解析-应还贷记卡5个月后",
    "repay_credit_5m_before": "信贷交易信息-资金压力解析-应还贷记卡5个月前",
    "repay_credit_6m_after": "信贷交易信息-资金压力解析-应还贷记卡6个月后",
    "repay_credit_6m_before": "信贷交易信息-资金压力解析-应还贷记卡6个月前",
    "repay_credit_7m_after": "信贷交易信息-资金压力解析-应还贷记卡7个月后",
    "repay_credit_8m_after": "信贷交易信息-资金压力解析-应还贷记卡8个月后",
    "repay_credit_9m_after": "信贷交易信息-资金压力解析-应还贷记卡9个月后",
    "repay_installment_10m_after": "信贷交易信息-资金压力解析-应还分期额10个月后",
    "repay_installment_11m_after": "信贷交易信息-资金压力解析-应还分期额11个月后",
    "repay_installment_12m_after": "信贷交易信息-资金压力解析-应还分期额12个月后",
    "repay_installment_1m_after": "信贷交易信息-资金压力解析-应还分期额1个月后",
    "repay_installment_1m_before": "信贷交易信息-资金压力解析-应还分期额1个月前",
    "repay_installment_2m_after": "信贷交易信息-资金压力解析-应还分期额2个月后",
    "repay_installment_2m_before": "信贷交易信息-资金压力解析-应还分期额2个月前",
    "repay_installment_3m_after": "信贷交易信息-资金压力解析-应还分期额3个月后",
    "repay_installment_3m_before": "信贷交易信息-资金压力解析-应还分期额3个月前",
    "repay_installment_4m_after": "信贷交易信息-资金压力解析-应还分期额4个月后",
    "repay_installment_4m_before": "信贷交易信息-资金压力解析-应还分期额4个月前",
    "repay_installment_5m_after": "信贷交易信息-资金压力解析-应还分期额5个月后",
    "repay_installment_5m_before": "信贷交易信息-资金压力解析-应还分期额5个月前",
    "repay_installment_6m_after": "信贷交易信息-资金压力解析-应还分期额6个月后",
    "repay_installment_6m_before": "信贷交易信息-资金压力解析-应还分期额6个月前",
    "repay_installment_7m_after": "信贷交易信息-资金压力解析-应还分期额7个月后",
    "repay_installment_8m_after": "信贷交易信息-资金压力解析-应还分期额8个月后",
    "repay_installment_9m_after": "信贷交易信息-资金压力解析-应还分期额9个月后",
    "repay_loan_10m_after": "信贷交易信息-资金压力解析-应还贷款总额10个月后",
    "repay_loan_11m_after": "信贷交易信息-资金压力解析-应还贷款总额11个月后",
    "repay_loan_12m_after": "信贷交易信息-资金压力解析-应还贷款总额12个月后",
    "repay_loan_1m_after": "信贷交易信息-资金压力解析-应还贷款总额1个月后",
    "repay_loan_1m_before": "信贷交易信息-资金压力解析-应还贷款总额1个月前",
    "repay_loan_2m_after": "信贷交易信息-资金压力解析-应还贷款总额2个月后",
    "repay_loan_2m_before": "信贷交易信息-资金压力解析-应还贷款总额2个月前",
    "repay_loan_3m_after": "信贷交易信息-资金压力解析-应还贷款总额3个月后",
    "repay_loan_3m_before": "信贷交易信息-资金压力解析-应还贷款总额3个月前",
    "repay_loan_4m_after": "信贷交易信息-资金压力解析-应还贷款总额4个月后",
    "repay_loan_4m_before": "信贷交易信息-资金压力解析-应还贷款总额4个月前",
    "repay_loan_5m_after": "信贷交易信息-资金压力解析-应还贷款总额5个月后",
    "repay_loan_5m_before": "信贷交易信息-资金压力解析-应还贷款总额5个月前",
    "repay_loan_6m_after": "信贷交易信息-资金压力解析-应还贷款总额6个月后",
    "repay_loan_6m_before": "信贷交易信息-资金压力解析-应还贷款总额6个月前",
    "repay_loan_7m_after": "信贷交易信息-资金压力解析-应还贷款总额7个月后",
    "repay_loan_8m_after": "信贷交易信息-资金压力解析-应还贷款总额8个月后",
    "repay_loan_9m_after": "信贷交易信息-资金压力解析-应还贷款总额9个月后",
    "repay_principal_10m_after": "信贷交易信息-资金压力解析-应还本金额10月后",
    "repay_principal_11m_after": "信贷交易信息-资金压力解析-应还本金额11月后",
    "repay_principal_12m_after": "信贷交易信息-资金压力解析-应还本金额12月后",
    "repay_principal_1m_after": "信贷交易信息-资金压力解析-应还本金1个月后",
    "repay_principal_1m_before": "信贷交易信息-资金压力解析-应还本金1个月前",
    "repay_principal_2m_after": "信贷交易信息-资金压力解析-应还本金2个月后",
    "repay_principal_2m_before": "信贷交易信息-资金压力解析-应还本金2个月前",
    "repay_principal_3m_after": "信贷交易信息-资金压力解析-应还本金3个月后",
    "repay_principal_3m_before": "信贷交易信息-资金压力解析-应还本金3个月前",
    "repay_principal_4m_after": "信贷交易信息-资金压力解析-应还本金4个月后",
    "repay_principal_4m_before": "信贷交易信息-资金压力解析-应还本金4个月前",
    "repay_principal_5m_after": "信贷交易信息-资金压力解析-应还本金5个月后",
    "repay_principal_5m_before": "信贷交易信息-资金压力解析-应还本金5个月前",
    "repay_principal_6m_after": "信贷交易信息-资金压力解析-应还本金额6月后",
    "repay_principal_6m_before": "信贷交易信息-资金压力解析-应还本金6个月前",
    "repay_principal_7m_after": "信贷交易信息-资金压力解析-应还本金额7月后",
    "repay_principal_8m_after": "信贷交易信息-资金压力解析-应还本金额8月后",
    "repay_principal_9m_after": "信贷交易信息-资金压力解析-应还本金额9月后",
    "report_no": "报告编号",
    "report_time": "报告时间",
    "residence_address": "户籍地址",
    "rng_principal_amount": "信贷交易信息-贷款信息-贷款趋势变化图贷款金额极差",
    "self_query_cnt": "查询信息-近三个月查询记录-本人查询记录条数",
    "settle_account_org": "信贷交易信息-资金压力解析-已结清贷款机构名",
    "settle_date": "信贷交易信息-资金压力解析-结清时间",
    "settle_loan_amount": "信贷交易信息-资金压力解析-结清贷款金额",
    "settle_loan_date": "信贷交易信息-资金压力解析-已结清贷款申请时间",
    "sex": "性别",
    "spouse_certificate_no": "配偶证件号码",
    "spouse_name": "配偶姓名",
    "spouse_phone": "配偶手机号码",
    "total_credit_cnt_1y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化1年前总张数",
    "total_credit_cnt_2y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化2年前总张数",
    "total_credit_cnt_3y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化3年前总张数",
    "total_credit_limit_1y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化1年前总额度",
    "total_credit_limit_2y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化2年前总额度",
    "total_credit_limit_3y_ago": "信贷交易信息-贷记卡信息-贷记卡额度及张数变化3年前总额度",
    "report_total_credit_min_repay_cnt": "信贷交易信息-贷记卡信息-贷记卡信息汇总最低还款张数",
    "total_guar_loan_balance": "担保信息-担保信息明细-担保余额总额",
    "total_guar_principal_amount": "担保信息-担保信息明细-担保金额总额",
    "total_principal_1y_ago": "信贷交易信息-贷款信息-近一年机构申请总变化总贷款金额",
    "total_principal_2y_ago": "信贷交易信息-贷款信息-近二年机构申请总变化总贷款金额",
    "total_principal_3y_ago": "信贷交易信息-贷款信息-近三年机构申请总变化总贷款金额",
    "total_repay_10m_after": "信贷交易信息-资金压力解析-应还总额10个月后",
    "total_repay_11m_after": "信贷交易信息-资金压力解析-应还总额11个月后",
    "total_repay_12m_after": "信贷交易信息-资金压力解析-应还总额12个月后",
    "total_repay_1m_after": "信贷交易信息-资金压力解析-应还总额1个月后",
    "total_repay_1m_before": "信贷交易信息-资金压力解析-应还总额1个月前",
    "total_repay_2m_after": "信贷交易信息-资金压力解析-应还总额2个月后",
    "total_repay_2m_before": "信贷交易信息-资金压力解析-应还总额2个月前",
    "total_repay_3m_after": "信贷交易信息-资金压力解析-应还总额3个月后",
    "total_repay_3m_before": "信贷交易信息-资金压力解析-应还总额3个月前",
    "total_repay_4m_after": "信贷交易信息-资金压力解析-应还总额4个月后",
    "total_repay_4m_before": "信贷交易信息-资金压力解析-应还总额4个月前",
    "total_repay_5m_after": "信贷交易信息-资金压力解析-应还总额5个月后",
    "total_repay_5m_before": "信贷交易信息-资金压力解析-应还总额5个月前",
    "total_repay_6m_after": "信贷交易信息-资金压力解析-应还总额6个月后",
    "total_repay_6m_before": "信贷交易信息-资金压力解析-应还总额6个月前",
    "total_repay_7m_after": "信贷交易信息-资金压力解析-应还总额7个月后",
    "total_repay_8m_after": "信贷交易信息-资金压力解析-应还总额8个月后",
    "total_repay_9m_after": "信贷交易信息-资金压力解析-应还总额9个月后",
    "work_unit": "工作单位"
}


class ParserPerCreditVariables:
    def __init__(self, resp, report_req_no, product_code, query_data_array):
        self.report_req_no = report_req_no
        self.product_code = product_code
        self.unique_code = None
        self.unique_name = None
        self.resp = resp
        self.query_data_array = query_data_array
        self.feature_list = []

    def process(self):
        self.get_public_param()
        self.parser_report_variables()
        self.parser_rules_variables()
        self.parser_model_variables()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def get_public_param(self):
        subject_info = self.resp['subject'][0]
        self.unique_code = subject_info['queryData']['idno']
        self.unique_name = subject_info['queryData']['name']

    @staticmethod
    def _transform_variable_name(variable_name):
        if variable_name in mapping_dict.keys():
            return mapping_dict[variable_name]
        else:
            return variable_name

    def parser_report_variables(self):
        report_info = self.resp['subject'][0]['reportDetail'][0]['variables']
        for key in variable_name_mapping.keys():
            if self._transform_variable_name(key) in report_info.keys():
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['unique_name'] = self.unique_name
                temp_dict['level_1'] = 'report_variables'
                # temp_dict['level_2'] = key
                # temp_dict['level_3'] = ''
                temp_dict['variable_name'] = key
                temp_dict['variable_name_cn'] = variable_name_mapping[key]
                temp_dict['variable_values'] = json.dumps(report_info[self._transform_variable_name(key)], ensure_ascii=False)
                self.feature_list.append(temp_dict)

    def parser_rules_variables(self):
        # 规则指标每个主体下均存在
        rules_info = self.resp['subject'][0]['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
        # unique_code = self.resp['queryData']['idno']
        for key in rules_variable_name_mapping.keys():
            if self._transform_variable_name(key) in rules_info.keys():
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['unique_name'] = self.unique_name
                temp_dict['level_1'] = 'rules_variables'
                # temp_dict['level_2'] = key
                # temp_dict['level_3'] = ''
                temp_dict['variable_name'] = key
                temp_dict['variable_name_cn'] = rules_variable_name_mapping[key]
                temp_dict['variable_values'] = json.dumps(rules_info[self._transform_variable_name(key)], ensure_ascii=False)
                self.feature_list.append(temp_dict)

    def parser_model_variables(self):
        model_info = self.resp['subject'][0]['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
        for key in model_variable_name_mapping.keys():
            if self._transform_variable_name(key) in model_info.keys():
                temp_dict = dict()
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['product_code'] = self.product_code
                temp_dict['unique_code'] = self.unique_code
                temp_dict['unique_name'] = self.unique_name
                temp_dict['level_1'] = 'model_variables'
                # temp_dict['level_2'] = key
                # temp_dict['level_3'] = ''
                temp_dict['variable_name'] = key
                temp_dict['variable_name_cn'] = model_variable_name_mapping[key]
                temp_dict['variable_values'] = json.dumps(model_info[self._transform_variable_name(key)], ensure_ascii=False)
                self.feature_list.append(temp_dict)
