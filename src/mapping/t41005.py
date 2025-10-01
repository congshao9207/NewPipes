from mapping.p07005.com_data_prepared_processor import ComDataPreparedProcessor
from mapping.p07005.com_strategy_input_process import ComStrategyInputProcessor
from mapping.p07005.per_data_prepared_processor import PerDataPreparedProcessor

from mapping.p07005.per_strategy_input_process import PerStrategyInputProcessor
from mapping.tranformer import Transformer


class T41005(Transformer):

    def __init__(self) -> None:
        super().__init__()
        # 初始化融合征信规则变量默认值
        self.variables = {
            "public_sum_count": 0,
            "rhzx_business_loan_3year_overdue_cnt": 0,
            "loan_category_abnormal_status": 0,
            "business_loan_average_3year_overdue_cnt": 0,
            "loan_now_overdue_money": 0.0,
            "single_loan_overdue_2year_cnt": 0,
            "extension_number": 0,
            "loan_status_abnorm_cnt": 0,
            "loan_overdue_2year_total_cnt": 0,
            "unsettled_business_loan_org_cnt": 0,
            "large_loan_2year_overdue_cnt": 0,
            "unsettled_consume_loan_org_cnt": 0,
            "unsettled_consume_total_cnt": 0,
            "business_loan_type_cnt": 0,
            "unsettled_loan_agency_number": 0,
            "unsettled_consume_total_amount": 0.0,
            "rhzx_business_loan_3year_ago_overdue_cnt": 0,
            "business_loan_average_3year_ago_overdue_cnt": 0,
            "credit_now_overdue_1k_money": 0.0,
            "credit_now_overdue_money": 0.0,
            "single_credit_overdue_2year_cnt": 0,
            "credit_status_abnormal_cnt": 0,
            "credit_overdrawn_2card": 0,
            "credit_overdue_2year_total_cnt": 0,
            "credit_financial_tension": 0,
            "activated_credit_card_cnt": 0,
            "credit_min_payed_number": 0,
            "credit_org_cnt": 0,
            "credit_overdue_5year": 0,
            "single_credit_or_loan_3year_overdue_max_month": 0,
            "loan_scured_five_a_level_abnormality_cnt": 0,
            "loan_scured_five_b_level_abnormality_cnt": 0,
            "guar_loan_balance": 0.0,
            "force_execution_cnt": 0,
            "civil_judge_cnt": 0,
            "loan_doubtful": 0,
            "owing_tax_cnt": 0,
            "admin_punish_cnt": 0,
            "loan_credit_query_3month_cnt": 0,
            "ecredit_unsettled_bad_cnt": 0,
            "ecredit_unsettled_focus_cnt": 0,
            "ecredit_settled_bad_cnt": 0,
            "ecredit_settled_focus_cnt": 0,
            "ecredit_unsettled_loan_org_cnt": 0,
            "loan_now_overdue_cnt": 0,
            "credit_and_mort_loan_3year_overdue_cnt": 0,
            "credit_overdue_months_2year": 0,
            "loan_overdue_months_2year": 0
        }

    def transform(self):

        if self.user_type == "PERSONAL":
            handle_list = [
                # PerDataPreparedProcessor(),
                PerStrategyInputProcessor()
            ]
        else:
            handle_list = [
                # ComDataPreparedProcessor(),
                ComStrategyInputProcessor()
            ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
