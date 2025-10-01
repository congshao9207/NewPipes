import json
from portrait.transflow.single_account_portrait.trans_flow import transform_feature_class_str

mapping_dict = {
    "info_e_name": "e_name",
    "ti_e_name": "e_name",
    "info_soci_credit_code": "soci_credit_code",
    "ti_soci_credit_code": "soci_credit_code",
    "rr_category": "category",
    "se_category": "category",
    "rr_overdue": "overdue",
    "se_overdue": "overdue",
    "rule_on_loan_cnt": "on_loan_cnt"
}
model_variable_name_mapping = {

}
rules_variable_name_mapping = {
    "care_industry": "所属行业风险较高",
    "keep_year": "成立年限",
    "abnorm_status": "存续状态",
    "rule_on_loan_cnt": "当前未结清信贷交易的机构数",
    "on_loan_prop": "销贷比",
    "app_cnt_recent": "历史3年申请总笔数",
    "asset_dispose_amt": "资产管理公司处置的债务:万元",
    "advance_amt": "垫款:万元",
    "overdue_prin": "逾期本金(:万元",
    "overdue_interest": "逾期利息:万元",
    "history_prin_overdue": "存在历史本金逾期记录",
    "bad_loan_cnt": "在贷业务五级分类",
    "care_loan_cnt": "在贷业务五级分类",
    "postpone_cnt": "存在展期业务",
    "bad_rr_cnt": "对外担保业务五级分类",
    "care_rr_cnt": "对外担保业务五级分类",
    "bad_done_cnt": "已结清业务五级分类",
    "care_done_cnt": "已结清业务五级分类",
    "risk_case_cnt": "黑名单信息条数"
}
variable_name_mapping = {
    "abnorm_debt_prop": "非正常负债余额占全部的比",
    "add_issuance": "新增发放额",
    "bad_debt": "不良类负债",
    "care_debt": "关注类负债",
    "debt_cnt": "负债业务笔数",
    "first_loan_year": "首次信贷交易的年份",
    "first_rr_year": "首次对外担保的年份",
    "future_12_month": "未来12个月月份yyyy-mm",
    "history_debt_month": "月份跨度（13个）",
    "history_loan_cnt": "发生信贷交易的机构数",
    "inst_cnt": "在贷机构个数",
    "last_6_month": "过去6个月月份yyyy-mm",
    "loan_due_amt": "借贷到期金额",
    "loan_f_due_amt": "借贷到期金额",
    "loan_flow": "余额",
    "loan_total_bal": "在贷总余额",
    "norm_debt": "正常类负债",
    "on_loan_cnt": "当前有未结清信贷交易的机构数",
    "open_due_amt": "敞口到期金额",
    "open_f_due_amt": "敞口到期金额",
    "open_total_bal": "敞口余额",
    "rr_total_bal": "对外担保总余额",
    "total_debt": "全部负债",
    "address": "办公/经营地址",
    "capital": "注册资本",
    "credict_code": "中证码",
    "info_e_name": "企业名称",
    "id_code": "身份标识号码",
    "industry": "所属行业",
    "launch_year": "成立年份",
    "related_name": "名称",
    "relation": "关系",
    "remark": "备注",
    "info_soci_credict_code": "社会信用代码",
    "status": "存续状态",
    "update_date": "更新日期",
    "arrears": "累计欠费",
    "handle_amt": "处理金额",
    "handle_date": "处理日期",
    "handle_inst": "处理机构",
    "handle_no": "记录编号",
    "handle_reason": "处理缘由",
    "handle_remark": "备注",
    "handle_status": "状态",
    "handle_type": "事件类型",
    "pay_status": "缴费状态",
    "recent_pay_date": "最近一次缴费日期",
    "staff_size": "职工人数",
    "ti_e_name": "企业名称",
    "ori_report_date": "征信报告时间",
    "ori_report_no": "征信报告编号",
    "ti_soci_credict_code": "社会信用代码",
    "bus_type": "业务种类",
    "rr_category": "五级分类",
    "due_date": "到期日",
    "guar_type": "担保方式",
    "inst_name": "机构名",
    "left_m_cnt": "剩余还款月数",
    "rr_overdued": "是否发生过逾期",
    "r_amt": "还款责任金额",
    "r_bal": "余额",
    "r_type": "责任类型",
    "start_date": "开立日期",
    "se_category": "五级分类",
    "coop_cnt": "放款次数",
    "finish_coop_date": "结束合作时间",
    "first_coop_date": "首次合作时间",
    "grant_max": "历史最高金额",
    "grant_min": "历史最低金额",
    "grant_total": "该年度总发放额",
    "inst": "授信机构",
    "last_repay_form": "最后一次还款形式",
    "se_overdued": "是否发生过逾期",
    "bus_pie_debt_bal": "负债余额",
    "bus_pie_debt_prop": "负债余额占比",
    "bus_pie_type": "业务类型",
    "inst_pie_debt_prop": "在贷余额+敞口余额  占比",
    "inst_pie_loan_bal": "在贷余额",
    "inst_pie_name": "机构名",
    "inst_pie_open_bal": "敞口余额",
    "table_bus_sup": "业务种类上级",
    "table_bus_type": "业务种类",
    "table_category": "五级分类",
    "table_cur_bal": "借款余额",
    "table_due_date": "到期日",
    "table_grant_amt": "发放额、借款额度",
    "table_grant_type": "发放形式",
    "table_guar_type": "担保方式",
    "table_inst_name": "借贷合作机构",
    "table_start_date": "开立日期"
}


class ParserComCreditVariables:
    def __init__(self, resp, report_req_no, product_code, query_data_array):
        self.report_req_no = report_req_no
        self.product_code = product_code
        self.unique_code = None
        self.resp = resp
        self.query_data_array = query_data_array
        self.feature_list = []

    def process(self):
        self.get_public_param()
        self.parser_report_variables()
        self.parser_rules_variables()
        transform_feature_class_str(self.feature_list, 'ReportFeatureDetail')

    def get_public_param(self):
        subject_info = self.resp['subject'][0]
        self.unique_code = subject_info['queryData']['idno']

    @staticmethod
    def _transform_variable_name(variable_name):
        if variable_name in mapping_dict.keys():
            return mapping_dict[variable_name]
        else:
            return variable_name

    def parser_report_variables(self):
        report_info = self.resp['subject'][0]['reportDetail'][0]['variables']
        for key_level in report_info.keys():
            for key, value in report_info[key_level].items():
                if key in variable_name_mapping.keys():
                    temp_dict = dict()
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_dict['product_code'] = self.product_code
                    temp_dict['unique_code'] = self.unique_code
                    temp_dict['level_1'] = 'report_variables'
                    temp_dict['level_2'] = key_level
                    # temp_dict['level_3'] = ''
                    temp_dict['variable_name'] = key
                    temp_dict['variable_name_cn'] = variable_name_mapping[key]
                    temp_dict['variable_values'] = json.dumps(value[self._transform_variable_name(key)], ensure_ascii=False)
                    self.feature_list.append(temp_dict)

    def parser_rules_variables(self):
        # 规则指标每个主体下均存在
        rules_info = self.resp['subject'][0]['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']
        unique_code = self.resp['queryData']['idno']
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
                temp_dict['variable_values'] = json.dumps(rules_info[self._transform_variable_name(key)], ensure_ascii=False)
                self.feature_list.append(temp_dict)
