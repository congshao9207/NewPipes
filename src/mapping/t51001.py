# @Time : 2020/6/19 1:52 PM
# @Author : lixiaobo
# @File : t51001.py.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
# from mapping.p08001_m.app_amt_predication import ApplyAmtPrediction
# from mapping.p08001_m.get_variable_in_db import GetVariableInDB
# from mapping.p08001_m.get_variable_in_flow import GetVariableInFlow
# from mapping.p08001_m.flow_strategy_processor import FlowStrategyProcessor
# from mapping.p08001_m.loan_amt import LoanAmt
from mapping.tranformer import Transformer
from mapping.p08001_m.get_variable_ahp_model import GetVariableAhpModel
from mapping.p08001_m.get_variable_rules import GetVariableRules

logger = LoggerUtil().logger(__name__)


class T51001(Transformer):
    """
    流水报告决策入参及变量清洗调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ahp_income_mean_m': 0,
            'ahp_loan_income_amt_proportion': -999,
            'ahp_loanable': 0,
            'ahp_relationship_income_amt_proportion': -999,
            'ahp_relationship_expense_amt_proportion': 0,
            'ahp_income_amt_diff_proportion': -999,
            'ahp_top_10_change_rate': -999,
            'ahp_large_income_period_rate': -999,
            'ahp_income_amt_rate_top5': 0,
            'ahp_expense_amt_rate_top5': 0,
            'ahp_mean_interest_12m': -999,
            'ahp_others_investment_net_amt_proportion': -999,
            'ahp_others_investment_amt_proportion': -999,
            'ahp_investment_amt': 0,
            'ahp_dividends': 0,
            'ahp_unusual_trans_cnt_proportion': 0,
            'ahp_extreme_income_amt_proportion': 0,
            'ahp_extreme_expense_amt_proportion': 0,
            'ahp_not_bank_trans_cnt': 0,
            'ahp_not_bank_trans_org_cnt': 0,
            'ahp_bank_trans_amt': 0,
            'ahp_not_bank_trans_amt': 0,

            'ahp_income_loanable': 0,
            'income_rate_0_to_1': 0,
            'petty_loan_expense_cnt_6m': 0,
            'petty_loan_income_cnt_6m': 0,
            'mean_balance_12m': 0,
            'total_income_cnt_3m': 0,
            'consumption_income_amt_6m': 0,
            'factoring_expense_cnt_12m': 0,
            'private_lending_expense_cnt_6m': 0,
            'financial_leasing_min_expense_amt_12m': 0,
            'bank_max_income_amt_3m': 0,
            'hospital_expense_cnt_12m': 0,
            'house_sale_expense_cnt_12m': 0,
            'credible_score': 0,
            'model_score': 0,
            'unbank_repay_type_cnt_r3m': 0,
            'court_cnt_6m': 0,
            'relationship_income_rate': 0,
            'top5_income_rate': 0
        }

    def transform(self):
        """
        input_param 为所有关联关系的入参
        [
            {
                "applyAmo":66600,
                "authorStatus":"AUTHORIZED",
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11843,
                "idno":"31011519910503253X",
                "name":"韩骁頔",
                "parentId":0,
                "phone":"13611647802",
                "relation":"CONTROLLER",
                "userType":"PERSONAL",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COM_CT_PERSONAL"
            },
            {
                "applyAmo":66600,
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11844,
                "idno":"91440300MA5EEJUR92",
                "name":"磁石供应链商业保理（深圳）有限公司",
                "parentId":0,
                "phone":"021-1234567",
                "relation":"MAIN",
                "userType":"COMPANY",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COMPANY"
            }
        ]
        """
        # logger.info("input_param:%s", self.cached_data.get("input_param"))

        handle_list = [
            # GetVariableInFlow(),
            # GetVariableInDB(),
            # ApplyAmtPrediction(),
            # FlowStrategyProcessor(),
            # LoanAmt(),
            GetVariableRules(),
            GetVariableAhpModel()
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
