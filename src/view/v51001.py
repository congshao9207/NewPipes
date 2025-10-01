# @Time : 2020/6/19 2:13 PM 
# @Author : lixiaobo
# @File : 51001.py.py 
# @Software: PyCharm
from mapping.tranformer import Transformer
from view.p08001_v.json_s_counterparty_portrait import JsonSingleCounterpartyPortrait
from view.p08001_v.json_s_loan_portrait import JsonSingleLoanPortrait
from view.p08001_v.json_s_marketing import JsonSingleMarketing
from view.p08001_v.json_s_portrait import JsonSinglePortrait
from view.p08001_v.json_s_related_guarantor import JsonSingleGuarantor
from view.p08001_v.json_s_related_portrait import JsonSingleRelatedPortrait
from view.p08001_v.json_s_remark_portrait import JsonSingleRemarkPortrait
from view.p08001_v.json_s_remark_trans_detail import JsonSingleRemarkTransDetail
from view.p08001_v.json_s_summary_portrait import JsonSingleSummaryPortrait
from view.p08001_v.json_s_title import JsonSingleTitle
from view.p08001_v.json_s_unusual_trans import JsonSingleUnusualTrans
from view.p08001_v.json_u_counterparty_portrait import JsonUnionCounterpartyPortrait
from view.p08001_v.json_u_loan_portrait import JsonUnionLoanPortrait
from view.p08001_v.json_u_marketing import JsonUnionMarketing
from view.p08001_v.json_u_portrait import JsonUnionPortrait
# from view.p08001_v.json_u_related_guarantor import JsonUnionGuarantor
from view.p08001_v.json_u_related_portrait import JsonUnionRelatedPortrait
from view.p08001_v.json_u_remark_portrait import JsonUnionRemarkPortrait
from view.p08001_v.json_u_remark_trans_detail import JsonUnionRemarkTransDetail
from view.p08001_v.json_u_summary_portrait import JsonUnionSummaryPortrait
from view.p08001_v.json_u_title import JsonUnionTitle
from view.p08001_v.json_u_unusual_trans import JsonUnionUnusualTrans
from view.p08001_v.json_u_fund_procurement import JsonUFundProcurement
from view.p08001_v.json_u_normal_income_portrait import JsonUnionNormalIncomePortrait
from view.p08001_v.json_u_funds_summary_portrait import JsonUnionFundsSummaryPortrait
from view.p08001_v.json_u_confidence_analyse import JsonUnionConfidenceAnalyse


class V51001(Transformer):
    """
    流水报告变量清洗
    """

    def __init__(self) -> None:
        super().__init__()

    def transform(self):
        view_handle_list = [
        ]

        if self.cached_data["single"]:
            view_handle_list.append(JsonSingleTitle())
            view_handle_list.append(JsonSinglePortrait())
            view_handle_list.append(JsonSingleSummaryPortrait())
            view_handle_list.append(JsonSingleRemarkPortrait())
            view_handle_list.append(JsonSingleRemarkTransDetail())
            view_handle_list.append(JsonSingleCounterpartyPortrait())
            view_handle_list.append(JsonSingleRelatedPortrait())
            view_handle_list.append(JsonSingleGuarantor())
            view_handle_list.append(JsonSingleLoanPortrait())
            view_handle_list.append(JsonSingleUnusualTrans())
            view_handle_list.append(JsonSingleMarketing())

        else:
            view_handle_list.append(JsonUnionTitle())
            view_handle_list.append(JsonUnionPortrait())
            view_handle_list.append(JsonUFundProcurement())
            view_handle_list.append(JsonUnionSummaryPortrait())
            view_handle_list.append(JsonUnionRemarkPortrait())
            view_handle_list.append(JsonUnionRemarkTransDetail())
            view_handle_list.append(JsonUnionCounterpartyPortrait())
            view_handle_list.append(JsonUnionRelatedPortrait())
            # view_handle_list.append(JsonUnionGuarantor())
            view_handle_list.append(JsonUnionLoanPortrait())
            view_handle_list.append(JsonUnionUnusualTrans())
            view_handle_list.append(JsonUnionMarketing())
            view_handle_list.append(JsonUnionNormalIncomePortrait())
            view_handle_list.append(JsonUnionFundsSummaryPortrait())
            view_handle_list.append(JsonUnionConfidenceAnalyse())

        for view in view_handle_list:
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()
