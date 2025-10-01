import json
from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str

mapping_dict = {}

model_variable_name_mapping = {
    "ae_m12_id_nbank_allnum": "近12个月，通过身份证号查询，在非银机构申请次数",
    "ae_m12_cell_nbank_allnum": "近12个月，通过手机号查询，在非银机构申请次数",
    "ae_m12_cell_nbank_else_cons_orgnum": "近12个月，通过手机号查询，在非银-其他机构-消费贷款机构申请机构数",
    "ae_m12_id_nbank_else_cons_orgnum": "近12个月，通过身份证号查询，在非银-其他机构-消费贷款机构申请机构数",
    "ae_m12_cell_orgnum_d": "近12个月，通过手机号查询，申请总机构数（去重后）",
    "ae_m12_id_nbank_else_rel_allnum": "近12个月，通过身份证号查询，在非银-其他机构-类信用卡机构申请次数",
    "ae_m6_cell_min_monnum": "近6个月，通过手机号查询，最小月申请次数",
    "ae_d15_cell_allnum_d": "近15天，通过手机号查询，申请总次数（去重后）",
    "ae_m3_cell_nbank_else_cons_allnum": "近3个月，通过手机号查询，在非银-其他机构-消费贷款机构申请次数",
    "ae_m6_cell_bank_region_allnum": "近6个月，通过手机号查询，在银行-区域银行机构申请次数",
    "ae_m12_cell_bank_max_monnum": "近12个月，通过手机号查询，在银行机构最大月申请次数",
    "ae_m1_id_bank_region_orgnum": "近1个月，通过身份证号查询，在银行-区域银行机构申请机构数",
    "ae_m6_id_bank_weekend_orgnum": "近6个月，通过身份证号查询，在银行机构周末申请机构数",
    "score": "模型评分",
}
rules_variable_name_mapping = {
    "base_age": "年龄",
    "base_gender": "性别",
    "base_marry_state": "婚姻状况",
    "ps_name_id": "姓名与身份证号不匹配",
    "base_black": "命中行内信贷黑名单",
    "phone_check": "手机号实名认证不一致",
    "phone_on_line_state": "手机号状态非正常使用",
    "phone_on_line_days": "手机号在网时长短",
    "ae_m12_id_nbank_allnum": "近12个月身份证号非银机构申请次数过多",
    "ae_m12_cell_nbank_allnum": "近12个月手机号非银机构申请次数过多",
    "ae_m12_cell_nbank_else_cons_orgnum": "近12个月消费贷款机构申请家数过多",
    "establish_year": "企业成立时间短",
    "com_bus_shares_frost": "现有股权冻结信息",
    "com_bus_change_legal_3m": "3个月内存在法定代表人变更",
    "com_bus_status_execption": "企业登记状态异常",
    "com_bus_relent_revoke": "关联企业有吊销",
    "com_bus_exception": "现有经营异常信息",
    "com_bus_saicChanLegal_5y": "法定代表人变更次数多",
    "com_bus_saicChanInvestor_5y": "投资人变更次数多",
    "com_bus_saicChanRegister_5y": "注册资本变更次数多",
    "com_bus_exception_his": "曾有经营异常信息",
    "com_bus_shares_impawn": "现有股权出质信息,需调查反馈",
    "com_bus_mor_detail": "现有动产抵押信息,需调查反馈",
    "com_bus_liquidation": "现有清算信息,需调查反馈",
    "com_bus_shares_impawn_his": "曾有股权出质信息",
    "com_bus_mor_detail_his": "曾有动产抵押信息",
    "com_bus_case_info": "现有行政处罚信息,需调查反馈",
    "com_bus_shares_frost_his": "曾有股权冻结信息,需调查反馈",
    "com_bus_illegal_list_his": "曾有严重违法失信信息,需调查反馈",
    "court_owed_owe": "命中欠款欠费",
    "court_dishonesty": "命中法院失信名单",
    "court_limit_entry": "命中限制出入境名单",
    "court_high_cons": "命中限制高消费名单",
    "court_cri_sus": "命中犯罪及嫌疑人",
    "court_fin_loan_con": "命中金融借款合同纠纷",
    "court_loan_con": "命中借款合同纠纷",
    "court_pop_loan": "命中民间借贷纠纷",
    "court_max_execute_amt": "命中大额执行案件",
    "court_admi_vio": "命中行政违法,需调查反馈",
    "court_judge": "命中民商事裁判,需调查反馈",
    "court_trial_proc": "命中民商事审判,需调查反馈",
    "court_tax_pay": "命中纳税非正常户,需调查反馈",
    "court_tax_arrears": "命中欠税名单,需调查反馈",
    "court_pub_info": "命中法院执行名单,需调查反馈",
    "court_tax_arrears_amt_3y": "三年内欠税总金额大,需调查反馈",
    "court_pub_info_amt_3y": "三年内执行公开涉案金额大,需调查反馈",
    "court_admi_vio_amt_3y": "三年内行政违法涉案金额大,需调查反馈",
    "court_judge_amt_3y": "三年内民商事裁判涉案金额大,需调查反馈",
    "court_ent_owed_owe": "命中欠款欠费",
    "court_ent_dishonesty": "命中法院失信名单",
    "court_ent_limit_entry": "命中限制出入境名单",
    "court_ent_high_cons": "命中限制高消费名单",
    "court_ent_cri_sus": "命中犯罪及嫌疑人",
    "court_ent_fin_loan_con": "命中金融借款合同纠纷",
    "court_ent_loan_con": "命中借款合同纠纷",
    "court_ent_pop_loan": "命中民间借贷纠纷",
    "court_ent_max_execute_amt": "命中大额执行案件",
    "court_ent_admi_vio": "命中行政违法,需调查反馈",
    "court_ent_judge": "命中民商事裁判,需调查反馈",
    "court_ent_trial_proc": "命中民商事审判,需调查反馈",
    "court_ent_tax_pay": "命中纳税非正常户,需调查反馈",
    "court_ent_tax_arrears": "命中欠税名单,需调查反馈",
    "court_ent_pub_info": "命中法院执行名单,需调查反馈",
    "court_ent_tax_arrears_amt_3y": "三年内欠税总金额大,需调查反馈",
    "court_ent_pub_info_amt_3y": "三年内执行公开涉案金额大,需调查反馈",
    "court_ent_admi_vio_amt_3y": "三年内行政违法涉案金额大,需调查反馈",
    "court_ent_judge_amt_3y": "三年内民商事裁判涉案金额大,需调查反馈"
}
variable_name_mapping = {
    "basic_share_ent_name": " 企业名称",
    "basic_share_holder_name": " 股东名称",
    "basic_share_holder_type": " 股东类型",
    "basic_share_sub_conam": " 认缴出资额（万元）",
    "basic_share_funded_ratio": " 认缴出资比例",
    "basic_share_con_date": " 认缴出资日期",
    "basic_share_holder_cnt": " 新增：企业股东及出资信息条数",
    "basic_ent_name": " 企业名称",
    "basic_enc_cnt": " 新增：工商基本信息条数",
    "bus_industry_industry": " 新增：主营行业",
    "bus_industry_hint": " 新增：风险提示",
    "bus_industry_grade": " 新增：风险评级",
    "bus_industry_cnt": " 新增：行业经营风险信息条数",
    "branch_info_cnt": "分支机构信息条数",
    "branchInfo": "分支机构信息",
    "company_personal_cnt": "主要人员信息条数",
    "companyPersonInfo": "企业主要人员信息",
    "black_info_cnt": "企业黑名单信息条数",
    "basic_blackInfo": "企业黑名单信息",
    "bus_abnomal_cnt": " 经营异常信息条数",
    "bus_change_record_cnt": " 企业变更信息条数",
    "bus_invest_cnt": " 对外投资信息条数",
    "bus_abnormal_name": " 经营异常企业名称",
    "bus_abnormal_cause": " 经营异常列入原因",
    "bus_abnormal_date": " 经营异常列入日期",
    "bus_abnormal_org": " 列入作出决定机关",
    "bus_abnormal_clear_cause": " 经营异常移出原因",
    "bus_abnormal_clear_date": " 经营异常移出日期",
    "bus_abnormal_clear_org": " 移出作出决定机关",
    "bus_change_category": " 企业变更事项",
    "bus_change_date": " 企业变更日期",
    "bus_change_content_before": " 企业变更前内容",
    "bus_change_content_after": " 企业变更后内容",
    "bus_change_num": " 企业变更次数",
    "bus_invest_name": " 对外投资企业名称",
    "bus_invest_status": " 对外投资企业状态",
    "bus_invest_date": " 对外投资企业成立日期",
    "bus_invest_proportion": " 对外投资出资比例",
    "bus_invest_sub_conam": " 新增：对外投资认缴出资额（万元）",
    "fin_mort_cnt": " 抵押信息条数",
    "fin_impawn_cnt": " 出质信息条数",
    "fin_alt_cnt": " 变更信息条数",
    "fin_mort_name": " 抵押人名称",
    "fin_mab_guar_amt": " 主债权",
    "fin_mort_cert_code": " 新增：抵押人主体身份代码",
    "fin_mort_object_name": " 新增：抵押物名称",
    "fin_mort_object_detail": " 新增：抵押物描述",
    "fin_mort_interest": " 新增：抵押利息情况",
    "fin_mort_date_from": " 新增：履约起始日期",
    "fin_mort_date_to": " 新增：履约截止日期",
    "fin_impawn_name": " 质权人",
    "fin_impawn_role": " 质权人类别",
    "fin_impawn_am": " 质押金额",
    "fin_impawn_state": " 出质执行状态",
    "fin_impawn_filing_date": " 新增：质押备案日期",
    "fin_impawn_approval_org": " 新增：质押审批部门",
    "fin_impawn_approval_date": " 新增：质押批准日期",
    "fin_impawn_date_to": " 新增: 质押截至日期",
    "black_froz_name": " 股权冻结相关企业名称",
    "black_froz_status": "股权冻结执行状态",
    "black_froz_amt": " 股权冻结金额",
    "black_froz_auth": " 股权冻结机关",
    "black_froz_invalid_reason": " 股权冻结原因",
    "black_froz_date_from": " 新增：冻结起始日期",
    "black_froz_date_to": " 新增：冻结截止日期",
    "black_froz_prop": " 新增：股权冻结比例",
    "black_froz_cancel_auth": " 新增：股权冻结解冻机关",
    "black_froz_cancel_date": " 新增：股权冻结解冻日期",
    "black_froz_mark": " 新增：股权冻结标志",
    "black_froz_cancel_detail": " 新增：股权冻结解冻说明",
    "black_froz_num": " 股权冻结信息条数",
    "fin_alt_item": " 股权变更事项",
    "fin_alt_date": " 股权变更日期",
    "fin_alt_be": " 股权变更前内容",
    "fin_alt_af": " 股权变更后内容",
    "fin_alt_num": " 股权变更次数",
    "black_list_cnt": " 执行限制信息条数",
    "black_crim_cnt": " 新增：罪犯及嫌疑人信息条数",
    "black_overt_cnt": " 民商事审判流程信息条数",
    "black_judge_cnt": " 裁判文书信息条数",
    "black_exec_cnt": " 执行公开信息条数",
    "black_illegal_cnt": " 行政违法信息条数",
    "black_abnormal_cnt": " 新增：纳税非正常户信息条数",
    "black_tax_cnt": " 新增：欠税信息条数",
    "black_arrears_cnt": " 新增：欠款欠费信息条数",
    "black_list_case_no": " 执行限制执行案号",
    "black_list_detail": " 执行限制执行内容",
    "black_list_title": " 新增：执行限制标题",
    "black_list_date": " 新增：执行限制立案时间",
    "black_list_org": " 新增：执行限制执行法院",
    "black_list_amt": " 新增：执行限制执行金额（元）",
    "black_list_status": " 新增：执行限制执行状态",
    "black_list_balance": " 新增：执行限制未履行金额",
    "black_crim_case_no": " 新增：罪犯及嫌疑人案号",
    "black_crim_reason": " 新增：罪犯及嫌疑人违法事由",
    "black_crim_title": " 新增：罪犯及嫌疑人标题",
    "black_crim_date": " 新增：罪犯及嫌疑人立案时间",
    "black_crim_org": " 新增：罪犯及嫌疑人侦察",
    "black_crim_amt": " 新增：罪犯及嫌疑人涉案金额",
    "black_crim_result": " 新增：罪犯及嫌疑人处理结果",
    "black_overt_reason": " 民商事审判涉案事由",
    "black_overt_type": " 民商事审判日期类别",
    "black_overt_authority": " 民商事审判审理机关",
    "black_overt_case_no": " 民商事审判执行案号",
    "black_overt_status": " 民商事审判诉讼地位",
    "black_overt_date": " 民商事审判立案时间",
    "black_overt_title": " 新增：民商事审判流程标题",
    "black_overt_detail": " 新增：民商事审判公告内容",
    "black_judge_reason": " 裁判文书涉案事由",
    "black_judge_authority": " 裁判文书审理机关",
    "black_judge_case_no": " 裁判文书执行案号",
    "black_judge_time": " 裁判文书立案时间",
    "black_judge_title": " 新增：裁判文书标题",
    "black_judge_status": " 新增：裁判文书诉讼地位",
    "black_judge_type": " 新增：裁判文书文书类型",
    "black_judge_amt": " 新增：裁判文书涉案金额",
    "black_judge_result": " 新增：裁判文书审理结果",
    "black_judge_process": " 新增：裁判文书审理程序",
    "black_judge_case_type": " 新增：裁判文书案件类型",
    "black_judge_plaintiff": " 新增：裁判文书原告当事人",
    "black_judge_defendant": " 新增：裁判文书被告当事人",
    "black_judge_otherparty": " 新增：裁判文书其他当事人",
    "black_exec_authority": " 执行公开执行法院",
    "black_exec_case_no": " 执行公开执行案号",
    "black_exec_date": " 执行公开立案时间",
    "black_exec_content": " 执行公开执行内容",
    "black_exec_title": " 新增：执行公开标题",
    "black_exec_amt": " 新增：执行公开执行标的",
    "black_exec_status": " 新增：执行公开执行状态",
    "black_exec_end_date": " 新增：执行公开终本日期",
    "black_exec_balance": " 新增：执行公开未履行金额",
    "black_illegal_reason": " 行政违法违法事由",
    "black_illegal_datetime": " 行政违法立案时间",
    "black_illegal_case_no": " 行政违法案号",
    "black_illegal_title": " 新增：行政违法标题",
    "black_illegal_org": " 新增：行政违法执法",
    "black_illegal_amt": " 新增：行政违法金额（元）",
    "black_illegal_result": " 新增：行政违法行政执法结果",
    "black_illegal_date_type": " 新增：行政违法日期类别",
    "black_abnormal_title": " 新增：纳税非正常户标题",
    "black_abnormal_date": " 新增：纳税非正常户认定日期",
    "black_abnormal_name": " 新增：纳税非正常户纳税人名称",
    "black_abnormal_code": " 新增：纳税非正常户纳税人识别号",
    "black_abnormal_tax_org": " 新增：纳税非正常户主管税务机关",
    "black_tax_title": " 新增：欠税记录标题",
    "black_tax_date": " 新增：欠税记录立案日期",
    "black_tax_org": " 新增：欠税记录主管税务机关",
    "black_tax_amt": " 新增：欠税记录欠税金额",
    "black_tax_type": " 新增：欠税记录所欠税种",
    "black_tax_time": " 新增：欠税记录欠税属期",
    "black_arrears_title": " 新增：欠款欠费标题",
    "black_arrears_date": " 新增：欠款欠费具体日期",
    "black_arrears_status": " 新增：欠款欠费身份",
    "black_arrears_reason": " 新增：欠款欠费欠款原因",
    "black_arrears_amt": " 新增：欠款欠费拖欠金额",
    "owner_age": " 主体年龄",
    "owner_resistence": " 主体籍贯",
    "owner_marriage_status": " 主体婚姻状况",
    "owner_education": " 主体学历水平",
    "owner_major_job_year": " 主体从业年限",
    "marray_info": "婚姻登记信息",
    "inhabitantFlag": "xx是否为青岛市常住人口:是|否",
    "operatorInfo": "运营商信息",
    "educationInfo": "学籍信息",
    "socialInsuranceInfo": "社保缴纳信息",
    "accumulationFundInfo": "公积金信息",
    "registInfoNum": "机动车登记信息条数",
    "detail": "机动车信息详情",
    "homesteadNum": "建设用地使用权、宅基地使用权登记条数",
    "homesteadDetail": "不动产信息详情",
    "landRightNum": "房地产权登记条数",
    "landRightDetail": "房地产权详情",
    "seaAreaUseRightNum": "海域使用权登记条数",
    "seaAreaUseRightDetail": "海域使用权详情",
    "forestOwnershipNum": "林权登记条数",
    "forestOwnershipDetail": "林权登记详情",
    "mortgageNum": "抵押登记条数",
    "mortgageDetail": "抵押登记详情",
    "seizureNum": "查封登记条数",
    "seizureDetail": "查封登记详情",
    "objectionNum": "异议登记条数",
    "objectionDetail": "异议登记详情",
    "owner_blackInfo": "反欺诈信息",
    "vehicleRegistInfo": "机动车信息",
    "realEstateRegistInfo": "不动产信息"

}


class ParserBusVariables:

    def __init__(self, resp, report_req_no, product_code):
        self.resp = resp
        self.report_req_no = report_req_no
        self.product_code = product_code
        self.unique_code = None
        self.feature_list = []

    def process(self):
        self.get_public_param()
        self.parser_basic_info()
        self.parser_basic_info()
        self.parser_bus_info()
        self.parser_black_info()
        self.parser_owner_info()
        self.parser_rules_variables()
        self.parser_model_variables()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def get_public_param(self):
        subject_info = self.resp['subject']
        for sub in subject_info:
            relation_info = sub['queryData']['relation']
            if relation_info == 'MAIN':
                self.unique_code = sub['queryData']['idno']

    def parser_basic_info(self):
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            basic_info = sub['reportDetail']['basic']
            for key, values in basic_info.items():
                if key in variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['unique_name'] = unique_name
                    temp_dict['level_1'] = 'basic'
                    # temp_dict['level_2'] = ''
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(values, ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_bus_info(self):
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            bus_info = sub['reportDetail']['bus']
            for key, values in bus_info.items():
                if key in variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['unique_name'] = unique_name
                    temp_dict['level_1'] = 'bus'
                    # temp_dict['level_2'] = ''
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(values, ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_black_info(self):
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            black_info = sub['reportDetail']['black']
            for key, values in black_info.items():
                if key in variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['unique_name'] = unique_name
                    temp_dict['level_1'] = 'black'
                    # temp_dict['level_2'] = ''
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(values, ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_owner_info(self):
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            owner_info = sub['reportDetail']['owner']
            for key, values in owner_info.items():
                if key in variable_name_mapping.keys():
                    if key in ['vehicleRegistInfo', 'realEstateRegistInfo']:
                        for key_1, values_1 in owner_info[key].items():
                            temp_dict = dict()
                            temp_dict['report_req_no'] = self.report_req_no
                            temp_dict['product_code'] = self.product_code
                            temp_dict['unique_code'] = unique_code
                            temp_dict['unique_name'] = unique_name
                            temp_dict['level_1'] = 'owner'
                            temp_dict['level_2'] = key
                            # temp_dict['level_3'] = ''
                            temp_dict['variable_name'] = key_1
                            temp_dict['variable_name_cn'] = variable_name_mapping[key_1]
                            temp_dict['variable_values'] = json.dumps(values_1, ensure_ascii=False)
                            self.feature_list.append(temp_dict)
                    else:
                        temp_dict = dict()
                        temp_dict['report_req_no'] = self.report_req_no
                        temp_dict['product_code'] = self.product_code
                        temp_dict['unique_code'] = unique_code
                        temp_dict['unique_name'] = unique_name
                        temp_dict['level_1'] = 'owner'
                        # temp_dict['level_2'] = key
                        # temp_dict['level_3'] = ''
                        temp_dict['variable_name'] = key
                        temp_dict['variable_name_cn'] = variable_name_mapping[key]
                        temp_dict['variable_values'] = json.dumps(values, ensure_ascii=False)
                        self.feature_list.append(temp_dict)

    def parser_rules_variables(self):
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            rules_info = sub['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
            for key, values in rules_info.items():
                if key in rules_variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['unique_name'] = unique_name
                    temp_dict['level_1'] = 'rules_variables'
                    # temp_dict['level_2'] = ''
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = rules_variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(rules_info[key], ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_model_variables(self):
        # 模型分 model
        model_info = self.resp['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
        temp_dict = dict()
        temp_dict['report_req_no'] = self.report_req_no
        temp_dict['product_code'] = self.product_code
        temp_dict['unique_code'] = self.unique_code
        temp_dict['level_1'] = 'model_variables'
        # temp_dict['level_2'] = key
        # temp_dict['level_3'] = ''
        temp_dict['variable_name'] = 'score'
        temp_dict['variable_name_cn'] = model_variable_name_mapping['score']
        temp_dict['variable_values'] = json.dumps(model_info['score'], ensure_ascii=False)
        self.feature_list.append(temp_dict)

        # 模型变量
        for sub in self.resp['subject']:
            unique_code = sub['queryData']['idno']
            unique_name = sub['queryData']['name']
            model_info = sub['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
            for key, values in model_info.items():
                if key in model_variable_name_mapping.keys():
                    if key == 'score':
                        continue
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = unique_code
                    temp_dict['unique_name'] = unique_name
                    temp_dict['level_1'] = 'model_variables'
                    # temp_dict['level_2'] = ''
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = model_variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(model_info[key], ensure_ascii=False)
                    self.feature_list.append(temp_dict)
