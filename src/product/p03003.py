# @Time : 2020/10/14 9:42 AM
# @Author : lixiaobo
# @File : p03002.py
# @Software: PyCharm
import json
import traceback

import pandas as pd
import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each, invoke_union
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from service.base_type_service_v2 import BaseTypeServiceV2
from strategy_config import obtain_strategy_url
from util.type_converter import format_var
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE

logger = LoggerUtil().logger(__name__)


class P03003(Generate):
    """
    新版一级联合报告处理逻辑
    """

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            app_id = json_data.get('appId')
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            industry = json_data.get("industry")
            base_type_service = BaseTypeServiceV2(query_data_array)

            #判断是否有配偶
            has_spouse = 0
            for data in query_data_array:
                relation = data.get("relation")
                if "SPOUSE" == relation:
                    has_spouse = 1
                    break

            response_array = []
            for data in query_data_array:
                response_array.append(self._shake_hand_response(base_type_service, data, product_code, req_no,has_spouse))
            resp = {
                'appId': app_id,
                'productCode': product_code,
                'reqNo': req_no,
                'industry': industry,
                'queryData': response_array
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

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

    def _shake_hand_response(self, base_type_service, data, product_code, req_no,has_spouse):
        """
        和决策交互，封装response
        :param data:
        :param product_code:
        :param req_no:
        :return:
        """
        user_type = data.get('userType')
        strategy = data.get("extraParam").get("strategy")
        # 获取base_type
        base_type = base_type_service.parse_base_type(data)
        variables = {"base_type": base_type, "product_code": product_code,
                     'out_strategyBranch': '00000', "user_type": user_type,
                     "strategy": strategy,"has_spouse":has_spouse}
        # 决策要求一直要加上00000，用户基础信息。q
        logger.info("variables:%s", variables)

        resp_json = self.invoke_strategy(variables, product_code, req_no)
        biz_types, categories = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)

        resp = {}
        resp.update(data)
        resp['baseType'] = base_type
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories

        return resp
