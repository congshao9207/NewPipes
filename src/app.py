import importlib
import json
import time
import traceback

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from py_eureka_client import eureka_client
from werkzeug.exceptions import HTTPException

from config import EUREKA_SERVER, version_info, GEARS_DB
from config_controller import base_type_api
from exceptions import APIException, ServerException
from fileparser.Parser import Parser
from logger.logger_util import LoggerUtil
from product.generate import Generate
from util.defensor_client import DefensorClient
from util.mysql_reader import sql_to_df
from config import download_minio

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)
app.register_blueprint(base_type_api)
start_time = time.localtime()

# logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(logging.WARNING)
logger.info("init eureka client...")
logger.info("EUREKA_SERVER:%s", EUREKA_SERVER)
try:
    eureka_client.init(eureka_server=EUREKA_SERVER,
                       app_name="PIPES",
                       instance_port=8010)
    logger.info("eureka client started. center: %s", EUREKA_SERVER)
except Exception as e:
    logger.error("init eureka client error:%s", e)


@app.route("/biz-types", methods=['POST'])
def shake_hand():
    """
    根据productCode调用对应的handler处理业务
    :return:
    """
    json_data = request.get_json()
    product_code = json_data.get('productCode')
    handler = _get_product_handler(product_code)
    df_client = DefensorClient(request.headers)
    handler.df_client = df_client
    handler.sql_db = sql_db()

    resp = handler.shake_hand(json_data)
    logger.info("shake_hand------end-------")
    return jsonify(resp)


@app.route("/strategy", methods=['POST'])
def strategy():
    logger.info("strategy begin...")
    json_data = request.get_json()
    # logger.info("strategy param:%s", json_data)
    strategy_param = json_data.get('strategyParam')
    product_code = strategy_param.get('productCode')
    handler = _get_product_handler(product_code)
    df_client = DefensorClient(request.headers)
    handler.df_client = df_client
    handler.sql_db = sql_db()

    resp = handler.call_strategy(json_data)
    return jsonify(resp)


@app.route("/parse", methods=['POST'])
def parse():
    """
    流水解析，验真请求
    """
    file = request.files.get("file")
    function_code = request.args.get("parseCode")
    if function_code is None:
        function_code = request.form.get("parseCode")
    data = request.args.get("param")
    if data is None:
        data = request.form.get("param")

    if function_code is None:
        return "缺少 parseCode字段"
    elif data is None:
        return "缺少 param字段"
    elif file is None:
        return "缺少 file字段"

    handler = _get_handler("fileparser", "Parser", function_code)
    handler.init_param(json.loads(data), file)
    handler.sql_db = sql_db()
    resp = handler.process()

    return jsonify(resp)


@app.route("/health", methods=['GET'])
def health_check():
    """
    检查当前应用的健康情况
    :return:
    """
    return 'pipes is running'


@app.route("/info", methods=['GET'])
def info():
    return 'pipes is running'


# 获取系统基本参数信息，用于系统监控
@app.route("/sys-basic-info", methods=['GET'])
def sys_basic_info():
    return jsonify({
        "SysName": "Pipes",
        "Version": version_info,
        "StartTime": time.strftime("%Y-%m-%d %H:%M:%S", start_time)
    })


@app.route("/acc_speculate", methods=['GET'])
def acc_speculate():
    """
    利率推算及还款计划推算
    :return:
    """
    try:
        resCode = 0
        resMsg = '成功'
        report_id = request.args.get("reportId")
        if report_id is None:
            resMsg = 'report id is None'
            resCode = 1
            return jsonify({'resCode': resCode, 'resMsg': resMsg})
        from credit_speculate.main_processor import CreditMain
        CreditMain(report_id, loan_df=None).processor()
    except Exception as ex:

        logger.error("exception " + str(ex))
        resCode, resMsg = 1, '失败'
        logger.error("acc_speculate exception:" + traceback.format_exc())

    return jsonify({'resCode': resCode, 'resMsg': resMsg})


@app.route("/per_credit_report", methods=['POST'])
def per_credit_report():
    """
    个人征信xml报文解析
    :return:
    """
    try:
        param_mapping = {
            "report_no": "reportNo",  # 报告编号
            "report_time": "reportDate",  # 报告时间
            "name": "userName",  # 被查询者姓名
            "certificate_type": "userIdType",  # 被查询者证件类型
            "certificate_no": "userIdNo",  # 被查询者证件号码
            "queryer": "queryOrg",  # 查询机构代码
            "query_reason": "queryReason"  # 查询原因代码
        }
        resCode, resMsg, status, base_info, generalize_info = '0', '成功', True, None, None
        user_name = request.json["userName"]
        id_card_no = request.json["userId"]
        credit_parse_no = request.json["reportId"]
        logger.info("per credit report analyze begin, user_name:%s, id_card_no:%s, credit_parse_no:%s", user_name, id_card_no, credit_parse_no)
        """20240807 不判断条数，直接解析"""
        # 查询征信指标详情表，若条数大于1，则说明该征信报告已存在，不再解析
        # exist_sql = """
        #     select basic_id, variable_value as cnt from info_union_credit_data_detail where variable_name = 'per_credit_xml' and basic_id = (
        #          select id from info_union_credit_data where id_card_no = %(id_card_no)s and
        #          user_name = %(user_name)s and credit_parse_no = %(credit_parse_no)s)
        # """
        # exist_count_df = sql_to_df(exist_sql, params={"id_card_no": id_card_no,
        #                                               "user_name": user_name,
        #                                               "credit_parse_no": credit_parse_no})
        # if exist_count_df.shape[0] > 1:
        #     return jsonify({'resCode': resCode, 'resMsg': resMsg, 'parseSuccess': status, 'persistenceSuccess': status,
        #                     'type': '1', 'pcreditBaseInfoVO': base_info})

        # 获取征信xml文件路径
        file_path_sql = """
            select basic_id, variable_value from info_union_credit_data_detail where variable_name = 'per_credit_xml' and basic_id = (
                 select id from info_union_credit_data where id_card_no = %(id_card_no)s and
                 user_name = %(user_name)s and credit_parse_no = %(credit_parse_no)s)
        """
        file_path_df = sql_to_df(file_path_sql, params={"id_card_no": id_card_no,
                                                        "user_name": user_name,
                                                        "credit_parse_no": credit_parse_no})
        logger.info("file_path_df:%s", file_path_df)
        if file_path_df.shape[0] == 0:
            resCode, resMsg, status, base_info, generalize_info = '1', '失败', False, None, None
            return jsonify({'resCode': resCode, 'resMsg': resMsg, 'parseSuccess': status, 'persistenceSuccess': status,
                            'type': '1', 'pcreditBaseInfoVO': base_info})
        file_path = file_path_df.iloc[0]['variable_value']
        logger.info("file_path:%s", file_path)
        # basic_id = file_path_df.iloc[0]['basic_id']
        # 下载征信xml文件
        if file_path == '白户':
            file = None
        else:
            # 处理文件路径，保留pcredit/xxx.xml
            file_path = '/'.join(file_path.split('/')[-2:])
            file = download_minio(file_path)
        report_id = request.json["reportId"]
        if report_id is None:
            report_id = request.form.get("reportId")
        if report_id is None or file is None:
            resMsg = 'report id is None or file is None'
            resCode = '1'
            return jsonify({'resCode': resCode, 'resMsg': resMsg})
        logger.info("per credit report analyze begin, report_id:%s", report_id)
        from creditreport.person.pcredit_report import PCreditReport
        dfs = PCreditReport(file, report_id).save_data()
        if 'CreditBaseInfo' in dfs and dfs['CreditBaseInfo'].shape[0] > 0:
            dfs['CreditBaseInfo'].rename(param_mapping, axis=1, inplace=True)
            base_info = dfs['CreditBaseInfo'].to_dict('records')[0]
            base_info.update({'reportTitle': '个人信用报告', 'reportVersion': '（授信机构版）', 'objectionMark': ''})
            logger.info(base_info)
        if 'PcreditLoan' in dfs and dfs['PcreditLoan'].shape[0] > 0:
            from credit_speculate.main_processor import CreditMain
            loan_df = dfs['PcreditLoan']
            CreditMain(report_id, loan_df).processor()
        logger.info("per credit report analyze end, report_id:%s", report_id)

        # 清洗指标
        # 20240621 用个人版征信，不用清洗指标的逻辑
        """
        from creditreport.cleaning_variables.cleaning_per_variables import CleaningPerVariables
        CleaningPerVariables(report_id, basic_id).process()
        """
    except Exception as ex:
        resCode, resMsg, status, base_info, generalize_info = '1', '失败', False, None, None
        logger.error("exception " + str(ex))
        logger.error("ent credit report analyze exception:" + traceback.format_exc())
    return jsonify({'resCode': resCode, 'resMsg': resMsg, 'parseSuccess': status, 'persistenceSuccess': status,
                    'type': '1', 'pcreditBaseInfoVO': base_info})


@app.route("/ent_credit_report", methods=['POST'])
def ent_credit_report():
    """
    企业征信xml报文解析
    :return:
    """
    try:
        param_mapping = {
            "report_no": "reportNo",
            "report_time": "reportTime",
            "query_org": "queryOrg",
            "query_reason": "queryReason",
            "ent_name": "entName",
            "credit_code": "creditCode",
            "unify_credit_code": "unifyCreditCode",
            "org_code": "orgCode",
            "tax_payer_id_c": "taxPayerIdC",
            "tax_payer_id_l": "taxPayerIdL",
            "org_credit_code": "orgCreditCode",
            "registered_capital": "registeredCapital",
            "economy_type": "economyType",
            "org_type": "orgType",
            "ent_scale": "entScale",
            "industry": "industry",
            "launch_year": "launchYear",
            "reg_cert_duedate": "regCertDuedate",
            "reg_site": "regSite",
            "office_site": "officeSite",
            "life_status": "lifeStatus"
        }
        resCode, resMsg, status, base_info, generalize_info = '0', '成功', True, None, None
        '''
        user_name = request.json["userName"]
        id_card_no = request.json["userId"]
        credit_parse_no = request.json["reportId"]
        logger.info("ent credit report analyze begin, user_name:%s, id_card_no:%s, credit_parse_no:%s", user_name, id_card_no, credit_parse_no)
        # 获取企业征信xml文件路径
        file_path_sql = """
                    select basic_id, variable_value from info_union_credit_data_detail where variable_name = 'ent_credit_xml' and basic_id = (
                         select id from info_union_credit_data where id_card_no = %(id_card_no)s and
                         user_name = %(user_name)s and credit_parse_no = %(credit_parse_no)s)
                """
        file_path_df = sql_to_df(file_path_sql, params={"id_card_no": id_card_no,
                                                        "user_name": user_name,
                                                        "credit_parse_no": credit_parse_no})
        logger.info("file_path_df:%s", file_path_df)
        if file_path_df.shape[0] == 0:
            resCode, resMsg, status, base_info, generalize_info = '1', '失败', False, None, None
            return jsonify({'resCode': resCode, 'resMsg': resMsg, 'parseSuccess': status, 'persistenceSuccess': status,
                            'type': '1', 'pcreditBaseInfoVO': base_info})
        file_path = file_path_df.iloc[0]['variable_value']
        logger.info("file_path:%s", file_path)
        # 下载征信xml文件
        if file_path == '白户':
            file = None
        else:
            # 处理文件路径，保留pcredit/xxx.xml
            file_path = '/'.join(file_path.split('/')[-2:])
            file = download_minio(file_path)
        '''
        file = request.files.get("file")
        report_id = request.args.get("reportId")
        if report_id is None:
            report_id = request.form.get("reportId")
        if report_id is None or file is None:
            resMsg = 'report id is None or file is None'
            resCode = '1'
            return jsonify({'resCode': resCode, 'resMsg': resMsg})
        from creditreport.company.ecredit_report import ECreditReport
        dfs = ECreditReport(file, report_id).save_data()
        if 'EcreditBaseInfo' in dfs and dfs['EcreditBaseInfo'].shape[0] > 0:
            dfs['EcreditBaseInfo'].rename(param_mapping, axis=1, inplace=True)
            base_info = dfs['EcreditBaseInfo'].to_dict('records')[0]
        if 'EcreditGeneralizeInfo' in dfs and dfs['EcreditGeneralizeInfo'].shape[0] > 0:
            dfs['EcreditGeneralizeInfo'].rename(param_mapping, axis=1, inplace=True)
            generalize_info = dfs['EcreditGeneralizeInfo'].to_dict('records')[0]
    except Exception as ex:
        resCode, resMsg, status, base_info, generalize_info = '1', '失败', False, None, None
        logger.error("exception " + str(ex))
        logger.error("ent credit report analyze exception:" + traceback.format_exc())
    return jsonify({'resCode': resCode, 'resMsg': resMsg, 'parseSuccess': status, 'persistenceSuccess': status,
                    'type': '2', 'ecreditBaseInfo': base_info, 'ecreditGeneralizeInfo': generalize_info})


@app.route("/feign_test", methods=['GET'])
def feign_test():
    logger.info("1_feign_test")
    report_req_no = request.args.get("reportReqNo")
    flag = request.args.get("flag")
    if flag and flag == '1':
        raise Exception("异常啦")
    logger.info("begin_feign_test,reportReqNo:" + report_req_no)
    time.sleep(60 * 60)
    logger.info("end_feign_test,reportReqNo:" + report_req_no)
    jsonify({'resCode': 0, 'resMsg': "Finished"})


@app.errorhandler(Exception)
def flask_global_exception_handler(e):
    # 判断异常是不是APIException
    if isinstance(e, APIException):
        return e
    # 判断异常是不是HTTPException
    if isinstance(e, HTTPException):
        error = APIException()
        error.code = e.code
        error.description = e.description
        return error
    # 异常肯定是Exception
    from flask import current_app
    # 如果是调试模式,则返回e的具体异常信息。否则返回json格式的ServerException对象！
    if current_app.config["DEBUG"]:
        return e
    return ServerException()


def _get_product_handler(product_code) -> Generate:
    model = None
    try:
        model = importlib.import_module("product.p" + str(product_code))
    except ModuleNotFoundError as err:
        try:
            model = importlib.import_module("product.P" + str(product_code))
        except ModuleNotFoundError as err:
            logger.error(str(err))
            return Generate()
    try:
        api_class = getattr(model, "P" + str(product_code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Generate()


def _get_handler(folder, prefix, code) -> Parser:
    try:
        model = importlib.import_module(folder + "." + prefix + str(code))
        api_class = getattr(model, prefix + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Parser()


def sql_db():
    db_url = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB
    app.config['SQLALCHEMY_DATABASE_URI'] = db_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ECHO'] = False
    db = SQLAlchemy(app)
    return db


if __name__ == '__main__':
    logger.info('starting pipes...')
    app.run(host='0.0.0.0')
    logger.info('pipes started.')
