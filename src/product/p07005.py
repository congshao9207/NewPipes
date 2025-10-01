import traceback

import requests
from flask import request

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each_person, invoke_union, invoke_each_company, invoke_each
from mapping.mapper import translate_for_strategy
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
import json
import pandas as pd

from product.p_config import product_codes_dict
from product.p_utils import _relation_risk_subject, _build_request, score_to_int
from strategy_config import obtain_strategy_url
from util.type_converter import format_var
from view.grouped_mapper_detail import view_variables_scheduler
from jsonpath import jsonpath

from mapping.p07005 import ahp_credit_model
from featureparser.parser_credit_variables import ParserCreditVariables

logger = LoggerUtil().logger(__name__)


class P07005(Generate):
    def shake_hand_process(self):
        raise NotImplementedError()

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            query_data_array = strategy_param.get('queryData')
            subject = []
            # 遍历query_data_array调用strategy
            logger.info("loop invoke begin: %s", req_no)
            index = -1
            count = len(query_data_array)
            # 判断风险拦截级别
            risk_level = "A"
            risk_level_list = []
            for data in query_data_array:
                index = index + 1
                logger.info("loop %s index %d//%d begin", req_no, index, count)
                resp,risk_level_list = self._strategy_hand(json_data, data, product_code, req_no, risk_level_list)
                logger.info("loop %s index %d//%d end", req_no, index, count)
                subject.append(resp)
                # 最后返回报告详情
            logger.info("view_variables_scheduler begin :%s", req_no)
            common_detail = view_variables_scheduler(product_code, json_data,
                                                     None, None, None, None, None, None, invoke_union)
            logger.info("view_variables_scheduler end :%s", req_no)
            # 调用征信模型指标清洗
            p = ahp_credit_model.AhpCreditModel(strategy_param)
            p.process()
            cache_array = p.variables

            if "D" in risk_level_list:
                risk_level = "D"
            elif "C" in risk_level_list:
                risk_level = "C"
            elif "B" in risk_level_list:
                risk_level = "B"
            cache_array['risk_level'] = risk_level
            # 封装第二次调用参数
            variables = self._create_strategy_second_request(cache_array)

            logger.info("final invoke_strategy begin :%s ", req_no)
            strategy_resp = self.invoke_strategy(variables, product_code, req_no)
            logger.info("final invoke_strategy end :%s ", req_no)
            score_to_int(strategy_resp)

            # 封装最终返回json
            resp_end = self._create_strategy_resp(strategy_resp, variables, common_detail, subject)
            format_var(None, None, -1, resp_end)

            # 处理解析落库
            """青岛确定不采用融合征信"""
            # resp = resp_end.copy()
            # report_req_no = strategy_param.get('reportReqNo')
            # pcv = ParserCreditVariables(resp, report_req_no, product_code, query_data_array)
            # pcv.process1()

            logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
            self.response = resp_end
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _strategy_hand(self, json_data, data, product_code, req_no, risk_level_list):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        resp = {}
        # 判断主体是否有征信，没有征信不做任何处理
        credit_parse_req_no = data.get("extraParam")["passthroughMsg"]['creditParseReqNo']
        if pd.notnull(credit_parse_req_no):
            codes = product_codes_dict[product_code]
            base_type = data.get('baseType')
            biz_types = codes.copy()
            variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                                  user_type, base_type, self.df_client, data)

            origin_input = data.get('strategyInputVariables')
            if origin_input is None:
                origin_input = {}
            origin_input['out_strategyBranch'] = ','.join(filter(lambda e: e != "00000", codes))
            # 合并新的转换变量
            origin_input.update(variables)

            strategy_resp = self.invoke_strategy(origin_input, product_code, req_no)

            warn_level_list = jsonpath(strategy_resp, '$.StrategyOneResponse.Body.Application.Categories')[0]
            if len(warn_level_list) > 0:
                risk_level_list = risk_level_list + jsonpath(warn_level_list,'$..warn_level')
            self._calc_view_variables(base_type, json_data, data, id_card_no, out_decision_code, phone, product_code,
                                      resp, strategy_resp, user_name, user_type, variables)
        else:
            resp['queryData'] = data

        return resp,risk_level_list

    @staticmethod
    def invoke_strategy(variables, product_code, req_no):
        strategy_request = _build_request(req_no, product_code, variables)
        logger.info("strategy_request:%s", strategy_request)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        logger.debug("strategy_response%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        return strategy_resp

    @staticmethod
    def _calc_view_variables(base_type, json_data, data, id_card_no, out_decision_code, phone, product_code,
                             resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        """
        data['strategyInputVariables'] = variables
        if user_type == "PERSONAL":
            detail = view_variables_scheduler(product_code, json_data, user_name, id_card_no, phone, user_type,
                                              base_type,
                                              data, invoke_each)
        else:
            detail = view_variables_scheduler(product_code, json_data, user_name, id_card_no, phone, user_type,
                                              base_type,
                                              data, invoke_each_company)
        resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['queryData'] = data

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject):
        resp = {
            'strategyInputVariables': variables,
            'strategyResult': strategy_resp,
            'commonDetail': common_detail,
            'subject': subject
        }
        return resp

    @staticmethod
    def _create_strategy_second_request(cache_array):
        """
        :param cache_array:
        :return:
        """
        # df = pd.DataFrame(cache_array)
        # 取前10行数据
        # df_person = df.query('userType=="PERSONAL" and strategy=="01"') \
        #     .sort_values(by=["fundratio"], ascending=False)
        #
        # # 删除重复的数据
        # df_person.drop_duplicates(subset=["name", "idno"], inplace=True)
        #
        # logger.info("-------df_person\n%s", df_person)

        # 拼接入参variables
        variables = cache_array
        # variables['score_p1'] = 900
        # variables['score_p2'] = 900
        # for index, row in df_person.iterrows():
        #     if row["phantomRelation"]:
        #         continue
        #     base_type = row['baseType']
        #     score = row['score']
        #     if base_type == 'U_PERSONAL' or base_type == 'U_COM_CT_PERSONAL':
        #         variables['score_p1'] = score
        #     elif base_type == 'U_PER_SP_PERSONAL' or base_type == 'U_COM_CT_SP_PERSONAL':
        #         variables['score_p2'] = score
        variables['segment_name'] = 'model'
        return variables
