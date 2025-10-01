import json
import traceback

import pandas as pd
from numpy import int64
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None and pd.notna(obj):
        return obj.strftime('%Y-%m-%d')
    else:
        return ''


def exception(describe):
    def robust(actual_do):
        def add_robust(*args, **keyargs):
            try:
                return actual_do(*args, **keyargs)
            except Exception as e:
                logger.error(describe)
                logger.error(traceback.format_exc())

        return add_robust

    return robust


def replace_nan(values):
    v_list = [x if pd.notna(x) else 0 for x in values]
    result = []
    for v in v_list:
        if isinstance(v, int64):
            result.append(int(str(v)))
        else:
            result.append(v)

    return result


def get_query_data(msg, query_user_type, query_strategy):
    logger.info("full_msg :%s", json.dumps(msg))

    query_data_list = jsonpath(msg, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        strategy = query_data.get("extraParam")['strategy']
        education = query_data.get("extraParam")['education']
        mar_status = query_data.get('extraParam')['marryState']
        priority = query_data.get('extraParam')['priority']
        phone = query_data.get("phone")
        if pd.notna(query_user_type) and user_type == query_user_type and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno, 'phone': phone,
                         'education': education, 'marry_state': mar_status, 'priority':priority}
            resp.append(resp_dict)
        if pd.isna(query_user_type) and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno}
            resp.append(resp_dict)
    return resp


def get_all_related_company(msg):
    query_data_list = jsonpath(msg, '$..queryData[*]')
    per_type = dict()
    resp = dict()
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        base_type = query_data.get("baseType")
        strategy = query_data.get("extraParam")['strategy']
        industry = query_data.get("extraParam")['industry']
        if user_type == 'PERSONAL' and strategy == '01':
            resp[idno] = {'name': [name], 'idno': [idno], 'industry': [industry]}
            if base_type == 'U_PERSONAL':
                per_type['main'] = idno
            elif 'SP' in base_type:
                per_type['spouse'] = idno
            elif 'CT' in base_type:
                per_type['controller'] = idno
            # else:
            #     per_type[base_type] = idno
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        base_type = query_data.get("baseType")
        strategy = query_data.get("extraParam")['strategy']
        industry = query_data.get("extraParam")['industry']
        temp_code = None
        if user_type == 'COMPANY' and strategy == '01':
            if 'SP' in base_type:
                temp_code = per_type.get('spouse')
            if 'CT' in base_type and temp_code is None:
                temp_code = per_type.get('controller')
            if temp_code is None:
                temp_code = per_type.get('main')
            if temp_code is not None:
                resp[temp_code]['name'].append(name)
                resp[temp_code]['idno'].append(idno)
                resp[temp_code]['industry'].append(industry)
    return resp


# def get_industry_risk_level(industry_code):
#     '''
#     行业国标由2011版升级至2017版，此版本行业风险作废
#     '''
#     if industry_code in ['H623', 'H622', 'H621', 'L727', 'C183', 'C366', 'P821', 'L726', 'H629', 'C231', 'F523',
#                          'P829', 'F513', 'O795', 'O794']:
#         return "D"
#     elif industry_code in ['F525', 'F514', 'E492', 'C223', 'E501', 'N781', 'A014', 'F518', 'C219',
#                            'F528', 'E470', 'H612', 'C339', 'L729', 'L724', 'H611', 'A021', 'Q839',
#                            'O801', 'F515', 'E502', 'A015', 'A041', 'C175', 'C182', 'C201']:
#         return "C"
#     elif industry_code in ['G543', 'C203', 'F529', 'O799', 'E489', 'L721', 'F511', 'C331', 'F524',
#                            'F517', 'H619', 'F522', 'E499', 'F527', 'C211', 'F526', 'F516', 'L711',
#                            'C292', 'F519', 'G582', 'C352', 'C336', 'E481', 'C335', 'C326', 'C338',
#                            'C342', 'C348', 'C382', 'C419', 'E503', 'G581', 'G599', 'K702', 'L712',
#                            'O811']:
#         return "B"
#     elif industry_code in ['F521', 'F512', 'C135']:
#         return "A"
#     else:
#         return "暂无风险评级"


# def get_industry_risk_tips(industry_code):
#     '''
#     行业国标由2011版升级至2017版，此版本行业风险作废
#     '''
#     resp_list = []
#     if industry_code in ['E501', 'E5010']:
#         resp_list.append("1、如果企业规模小、仍运用传统的、主要以体力为支出的运作模式，抗风险能 力较弱。")
#         resp_list.append("2、请关注该行业的挂靠和转包、分包现象，以及是否有行业等级资质。")
#         resp_list.append("3、请关注隐形负债，该行业垫资多，且多为民营企业，企业发展资金主要依靠自身积累，融资渠道也有限，民间借贷普遍存在。")
#         resp_list.append("4、如果应收账款超年营业额70%以上，请关注应收账款质量，存在坏账可能。")
#         resp_list.append("5、企业主实际经营年限不足3年，风险较高。如果有实力较好的从事相关行业的经营者给与支持或者合作，可适当降低风险。")
#     elif industry_code in ['A041']:
#         resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害风险。")
#         resp_list.append("2、该行业生产周期长，季节性明显，建议将贷款到期日设置在销售旺季。")
#         resp_list.append("3、请关注饲料供应是否有稳定的来源及稳定的价格。")
#         resp_list.append("4、请关注苗种的来源和质量是否稳定。")
#         resp_list.append("5、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
#     elif industry_code in ['F528', 'F5281']:
#         resp_list.append("1、请关注客户所代理的品牌在当地或者在行业内是否有一定的知名度。")
#         resp_list.append("2、请关注客户线下店面所在地的专业市场是否人气不足，整体不景气。")
#         resp_list.append("3、如果销售的五金产品种类少及规模小，请谨慎授信。")
#         resp_list.append("4、3年内经营场地搬迁2次以上，固定资产保障性较弱的客群，请谨慎授信。")
#     elif industry_code in ['E48']:
#         resp_list.append("1、如果经营主体资质不足，一般采用挂靠、转包、分包形式施工的，应收账款回款周期较慢。")
#         resp_list.append("2、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
#         resp_list.append("3、请关注隐形负债，该行业普遍存在应收款质押、设备融资租赁、民间负债多等现象。")
#         resp_list.append("4、请关注法律风险，该行业易产生合同纠纷、劳务纠纷、借款纠纷。")
#         resp_list.append("5、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
#         resp_list.append("6、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
#     elif industry_code in ['H62']:
#         resp_list.append("1、请关注客户的资质完备情况，餐饮行业基本资质证件含《营业执照》、《食品经营许可证》、《卫生安全许可证》、《消防安全许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、如果营业执照非本人，通过租赁场地经营的，该类客户经营稳定性差。")
#         resp_list.append("3、请关注客户从业年限和经营店面开业年限，同一店面经营时间3年以上违约风险会显著降低。")
#         resp_list.append("4、请关注店铺实际口碑情况，可关注大众点评等网评差评内容。")
#         resp_list.append("5、请关注该行业的核心员工稳定度（管理人员、店长、主厨等）。")
#         resp_list.append("6、餐饮行业不建议将装修投资记录资产，并请谨慎评估原始投资资金来源。")
#     elif industry_code in ['F5212']:
#         resp_list.append("1、请关注客户的资质完备情况，商超行业基本资质证件含《营业执照》、《食品经营许可证》、《消防安全许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、请关注公司名下行政处罚是否涉及食品安全问题及货款纠纷等。")
#         resp_list.append("3、请关注客户经营地段是否在人口流动集聚的社区及地域。")
#         resp_list.append("4、请关注经营超市的品牌是否在经营当地具备一定认可度，尤其关注客户自有品牌的市场认可度。")
#         resp_list.append("5、请关注单体商超的实际股份构成，以及企业主其他多元化对外投资情况。")
#         resp_list.append("6、如果企业主实际经营商超行业不足5年，请谨慎授信。")
#     elif industry_code in ['G543', 'G5430']:
#         resp_list.append("1、请关注客户的资质完备情况，物流运输行业基本资质证件含《营业执照》、《道路运输经营许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、请关注客户是否购买车辆保险且是否足额，防范交通事故赔偿风险。")
#         resp_list.append("3、请核实客户公司名下车辆所有权及融资情况，关注是否有已报废的车辆继续营运的情况。")
#         resp_list.append("4、请关注客户近期或者历史有无未结清的交通案件，如果案件涉及金额较大或情节较严重的，请谨慎授信。")
#         resp_list.append("5、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
#         resp_list.append("6、请关注客户在经营当地固定资产的保障情况，该行业的资产投入较集中在经营性资产上（车辆及应收）。")
#     elif industry_code in ['C2438', 'F5245']:
#         resp_list.append("1、请关注品牌在经营当地的知名度和市场接受度。")
#         resp_list.append("2、请关注隐形负债，该行业内拆借及民间借贷情况普遍存在。")
#         resp_list.append("3、请关注多头信贷，该行业普遍存在暗股的情况。")
#         resp_list.append("4、该行业存货价值较高、变现能力强，且存货体积较小，存挪比较方便，对于发生风险后的实际处置力有一定隐患，建议有增信措施。")
#         resp_list.append("5、请关注国际金价波动对企业经营的影响。")
#         resp_list.append("6、如果客户在经营当地无固定资产，请谨慎授信。")
#         resp_list.append("7、如果客户为福建籍，请谨慎授信。")
#     elif industry_code in ['F5261']:
#         resp_list.append("1、请关注隐形负债，该行业车辆融资租赁以及金融机构特殊授信产品普遍存在。")
#         resp_list.append("2、请核实库存车辆的所有权属，建议收集车辆行驶证等资产证明材料。")
#         resp_list.append("3、请关注实时行业政策，该行业受国家消费、能耗、技术等方面政策影响较大。例如：取消新能源汽车的补贴。")
#     elif industry_code in ['F5123']:
#         resp_list.append("1、请关注水果市场变化，该行业受需求供给状况、行业政策、气候变化的影响较大。")
#         resp_list.append("2、请关注客户的存货周转率是否过低，注意与同行业同类型产品比较。")
#         resp_list.append("3、如果客户进货渠道单一，请谨慎授信。")
#         resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
#     elif industry_code in ['E4813']:
#         resp_list.append("1、请关注客户的资质完备情况，没有建筑施工资质的客户往往采用挂靠的形式开展业务，可以核实客户的挂靠协议或项目合同。")
#         resp_list.append("2、请关注法律风险，该行业易产生交通事故、买卖合同纠纷、劳务纠纷、借款纠纷。")
#         resp_list.append("3、请关注隐形负债，该行业内垫资、拆借及民间借贷情况普遍存在。")
#         resp_list.append("4、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
#         resp_list.append("5、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
#     elif industry_code in ['P82']:
#         resp_list.append("1、请关注客户的资质完备情况，教育行业办学资质证件含《办学许可证》、《营业执照》等。")
#         resp_list.append("2、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂。")
#         resp_list.append("3、请关注该行业的声誉以及信誉风险，可通过员工素质、知识水平以及管理规范等方面了解。")
#         resp_list.append("4、请关注客户的机构品牌的市场认可度，如品牌为自创品牌或市场受众较小，请谨慎授信。")
#     elif industry_code in ['F516']:
#         resp_list.append("1、请关注客户经营产品品牌的市场认可度，如产品品牌较为小众，请谨慎授信。")
#         resp_list.append("2、请关注产品市场价格波动风险。")
#         resp_list.append("3、请核实应收账款质量。一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
#         resp_list.append("4、该行业客户资产存货和应收占比高，经营风险相对较大，如果客户固定资产少，保障性弱，请谨慎授信。")
#         resp_list.append("5、该行业的日均与资金调动能力一般高于其他行业，如果客户资金调动能力较差，请谨慎授信。")
#     elif industry_code in ['H61']:
#         resp_list.append("1、请关注客户的资质完备情况，酒店行业基本资质证件含《营业执照》、《消防证》、《特种行业许可证》、《卫生许可证齐全》，且证件地址需与酒店地址一致。")
#         resp_list.append("2、请关注酒店物业的剩余租期是否在贷款期限内，警惕到期不能续租的情况。")
#         resp_list.append("3、请关注酒店经营的年限，以及经营者酒店行业的从业年限，如果酒店经营1年以内，或经营者酒店行业从业2年以内的，请谨慎授信。")
#         resp_list.append("4、请关注酒店投资时间以及金额，酒店价值随投资年限增长而下降。")
#         resp_list.append("5、该行业一般会有多人合伙投资占股情况，请核实酒店的实际控制人、真实股份构成、分红方式以及企业主其他多元化对外投资情况。")
#         resp_list.append("6、如果客户经营非连锁品牌的低端宾馆或一般旅馆，请谨慎授信。")
#         resp_list.append("7、如果客户不参与名下所有占股酒店的实际经营，请谨慎授信。")
#     elif industry_code in ['F5124']:
#         resp_list.append("1、请关注客户的资质完备情况，肉类批发行业基本资质证件含《营业执照》、《食品流通许可证齐全》。")
#         resp_list.append("2、请关注产品市场价格波动风险。")
#         resp_list.append("3、请关注应收账款质量和账期长短，该行业普遍存在应收账款坏账风险和账期不稳定情况。")
#         resp_list.append("4、请关注客户的存货周转率与同行业同类型产品相比是否过低。")
#         resp_list.append(
#             "5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。牛羊肉、鸡鸭肉冻品旺季一般为10-3月；水产虾蟹生鲜的销售旺季一般为4-7月、9-12月，以及端午、中秋、国庆等节假日；海鲜冻品的销售旺季一般在休渔期。")
#     elif industry_code in ['F5274']:
#         resp_list.append("1、请关注企业主手机门店是否有品牌授权以及是否在授权期限内。")
#         resp_list.append("2、请关注客户销售手机的品牌，国内目前华为、vivo、OPPO、小米、苹果5个品牌的市场占有率达90%，如果客户主营产品非主流品牌，建议谨慎授信。")
#         resp_list.append("3、请关注是否有存货积压的风险。")
#         resp_list.append("4、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂，甚至过度举债经营。")
#         resp_list.append("5、请关注门店地段的人口密集程度。")
#     elif industry_code in ['F512', 'F5127']:
#         resp_list.append("1、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
#         resp_list.append("2、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
#         resp_list.append("3、请关注客户的存货周转率与同行业同类型产品是否过低。")
#         resp_list.append("4、如果客户经营产品为饮料、啤酒等，产品保质期要求较严格的，请关注固定资产保障。")
#         resp_list.append("5、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
#         resp_list.append("6、该行业淡旺季明显，建议将贷款到期日设置在销售旺季")
#     elif industry_code in ['F5283']:
#         resp_list.append("1、请关注库存积压情况。")
#         resp_list.append("2、请关注线下零售门店所在专业市场的成熟度及客流量。")
#         resp_list.append("3、请关注零售品牌的市场认可度和销售渠道。")
#         resp_list.append("4、请关注仓库的防火措施是否到位。")
#         resp_list.append("5、该行业属于夕阳行业，请谨慎授信。")
#     elif industry_code in ['F5137']:
#         resp_list.append("1、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
#         resp_list.append("2、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
#         resp_list.append("3、请关注线下零售门店所在专业市场的客流量。")
#         resp_list.append("4、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
#         resp_list.append("5、请关注库存积压情况。")
#         resp_list.append("6、如果客户经营的产品种类多，销量低，请谨慎授信。")
#     elif industry_code in ['F513', 'F5132']:
#         resp_list.append("1、请关注应收账款质量和账期长短，该行业普遍存在应收账款金额较大、账期长、坏账率高的情况。")
#         resp_list.append("2、请关注该行业客户是否有选款不慎，压货过多的情况。")
#         resp_list.append("3、请关注隐形负债，该行业库存、应收资金占用较大，厂房、设备投入较多，人工工资等运营成本较高，设备融资租赁、民间借贷等情况普遍存在。")
#         resp_list.append("4、请关注资产保障性和体外担保设置的合理性，建议有增信措施。")
#     return resp_list


def get_industry_risk_level(industry_code):
    '''
    国标行业升级至2017版，行业风险等级映射
    由于新版本中有部分行业映射成1、2、4级，所以将原本加工成三级行业入参改为四级行业入参，分别进行对照
    :param industry_code: 行业代码
    :return: 风险等级
    '''
    industry_code_3rd = industry_code[:4]
    if industry_code in ["L7291", "O8051"]:
        return "D"
    elif industry_code_3rd in ["C183", "C231", "C367", "F513", "F523", "H621", "H622", "H623", "H629", "L726", "O804", "P831", "P839"]:
        return "D"
    elif "E47" in industry_code:
        return "C"
    elif industry_code_3rd in ["A014", "A015", "A021", "A041", "C141", "C149", "C175", "C182", "C201", "C212", "C219",
                               "C223", "C304", "C339", "E491", "E492", "E501", "E502", "F514", "F515", "F518",
                               "F525", "F528", "H611", "H612", "L725", "L729", "N781", "O811", "Q849"]:
        return "C"
    elif "A03" in industry_code:
        return "B"
    elif industry_code_3rd in ["A011", "A012", "A013", "A016", "A017",
                               "C142", "C143", "C144", "C145", "C146", "C203", "C213", "C214", "C211", "C221", "C222",
                               "C232", "C233", "C292", "C301", "C303", "C305", "C306", "C307", "C308", "C309", "C302",
                               "C325", "C332", "C333", "C334", "C337", "C331", "C335", "C336", "C338", "C342", "C348",
                               "C352", "C382", "C419", "E482", "E483", "E484", "E485", "E481", "E489", "E499", "E503",
                               "F511", "F516", "F517", "F519", "F522", "F524", "F526", "F527", "F529", "G541", "G542",
                               "G544", "G543", "G591", "G582", "G599", "H619", "K702", "L711", "L712", "L723", "L724",
                               "M752", "L727", "L721", "O801", "O802", "O803", "O805", "O807", "O808", "O809", "O821",
                               "P832", "P833", "P834", "P835"]:
        return "B"
    elif "I" in industry_code:
        return "A"
    elif industry_code_3rd in ["C135", "F512", "F521"]:
        return "A"
    else:
        return "暂无风险评级"


def get_industry_risk_tips(industry_code):
    '''
    国标行业升级至2017版，行业风险话术映射
    :param industry_code: 行业代码
    :return: resp_list：话术列表，默认为空列表
    '''
    resp_list = []
    if "E501" in industry_code:
            # 建筑装饰行业
            resp_list.append("1、如果企业规模小、仍运用传统的、主要以体力为支出的运作模式，抗风险能 力较弱。")
            resp_list.append("2、请关注该行业的挂靠和转包、分包现象，以及是否有行业等级资质。")
            resp_list.append("3、请关注隐形负债，该行业垫资多，且多为民营企业，企业发展资金主要依靠自身积累，融资渠道也有限，民间借贷普遍存在。")
            resp_list.append("4、如果应收账款超年营业额70%以上，请关注应收账款质量，存在坏账可能。")
            resp_list.append("5、企业主实际经营年限不足3年，风险较高。如果有实力较好的从事相关行业的经营者给与支持或者合作，可适当降低风险。")
    elif "A041" in industry_code:
            # 水产养殖行业
            resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害风险。")
            resp_list.append("2、该行业生产周期长，季节性明显，建议将贷款到期日设置在销售旺季。")
            resp_list.append("3、请关注饲料供应是否有稳定的来源及稳定的价格。")
            resp_list.append("4、请关注苗种的来源和质量是否稳定。")
            resp_list.append("5、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
    elif industry_code in ["F528", "F5281"]:
            # 五金行业 精确匹配
            resp_list.append("1、请关注客户所代理的品牌在当地或者在行业内是否有一定的知名度。")
            resp_list.append("2、请关注客户线下店面所在地的专业市场是否人气不足，整体不景气。")
            resp_list.append("3、如果销售的五金产品种类少及规模小，请谨慎授信。")
            resp_list.append("4、3年内经营场地搬迁2次以上，固定资产保障性较弱的客群，请谨慎授信。")
    elif "E48" in industry_code:
            # 土木工程建筑行业
            resp_list.append("1、如果经营主体资质不足，一般采用挂靠、转包、分包形式施工的，应收账款回款周期较慢。")
            resp_list.append("2、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
            resp_list.append("3、请关注隐形负债，该行业普遍存在应收款质押、设备融资租赁、民间负债多等现象。")
            resp_list.append("4、请关注法律风险，该行业易产生合同纠纷、劳务纠纷、借款纠纷。")
            resp_list.append("5、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
            resp_list.append("6、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
    elif "H62" in industry_code:
            # 餐饮行业
            resp_list.append("1、请关注客户的资质完备情况，餐饮行业基本资质证件含《营业执照》、《食品经营许可证》、《卫生安全许可证》、《消防安全许可证》或根据各省市实际情况判定。")
            resp_list.append("2、如果营业执照非本人，通过租赁场地经营的，该类客户经营稳定性差。")
            resp_list.append("3、请关注客户从业年限和经营店面开业年限，同一店面经营时间3年以上违约风险会显著降低。")
            resp_list.append("4、请关注店铺实际口碑情况，可关注大众点评等网评差评内容。")
            resp_list.append("5、请关注该行业的核心员工稳定度（管理人员、店长、主厨等）。")
            resp_list.append("6、餐饮行业不建议将装修投资记录资产，并请谨慎评估原始投资资金来源。")
    elif "F521" in industry_code:
            # 综合零售
            resp_list.append("1、请关注客户的资质完备情况，商超行业基本资质证件含《营业执照》、《食品经营许可证》、《消防安全许可证》或根据各省市实际情况判定。")
            resp_list.append("2、请关注公司名下行政处罚是否涉及食品安全问题及货款纠纷等。")
            resp_list.append("3、请关注客户经营地段是否在人口流动集聚的社区及地域。")
            resp_list.append("4、请关注经营超市的品牌是否在经营当地具备一定认可度，尤其关注客户自有品牌的市场认可度。")
            resp_list.append("5、请关注单体商超的实际股份构成，以及企业主其他多元化对外投资情况。")
            resp_list.append("6、如果企业主实际经营商超行业不足5年，请谨慎授信。")
    elif "G54" in industry_code:
            # 道路运输业
            resp_list.append("1、请关注客户的资质完备情况，物流运输行业基本资质证件含《营业执照》、《道路运输经营许可证》或根据各省市实际情况判定。")
            resp_list.append("2、请关注客户是否购买车辆保险且是否足额，防范交通事故赔偿风险。")
            resp_list.append("3、请核实客户公司名下车辆所有权及融资情况，关注是否有已报废的车辆继续营运的情况。")
            resp_list.append("4、请关注客户近期或者历史有无未结清的交通案件，如果案件涉及金额较大或情节较严重的，请谨慎授信。")
            resp_list.append("5、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
            resp_list.append("6、请关注客户在经营当地固定资产的保障情况，该行业的资产投入较集中在经营性资产上（车辆及应收）。")
    elif industry_code in ["C2438", "F5245"]:
            # 黄金珠宝行业
            resp_list.append("1、请关注品牌在经营当地的知名度和市场接受度。")
            resp_list.append("2、请关注隐形负债，该行业内拆借及民间借贷情况普遍存在。")
            resp_list.append("3、请关注多头信贷，该行业普遍存在暗股的情况。")
            resp_list.append("4、该行业存货价值较高、变现能力强，且存货体积较小，存挪比较方便，对于发生风险后的实际处置力有一定隐患，建议有增信措施。")
            resp_list.append("5、请关注国际金价波动对企业经营的影响。")
            resp_list.append("6、如果客户在经营当地无固定资产，请谨慎授信。")
            resp_list.append("7、如果客户为福建籍，请谨慎授信。")
    elif industry_code in ["F5261", "F5262"]:
            # 汽车零售行业
            resp_list.append("1、请关注隐形负债，该行业车辆融资租赁以及金融机构特殊授信产品普遍存在。")
            resp_list.append("2、请核实库存车辆的所有权属，建议收集车辆行驶证等资产证明材料。")
            resp_list.append("3、请关注实时行业政策，该行业受国家消费、能耗、技术等方面政策影响较大。例如：取消新能源汽车的补贴。")
    elif industry_code == "F5123":
            # 果品、蔬菜批发
            resp_list.append("1、请关注水果市场变化，该行业受需求供给状况、行业政策、气候变化的影响较大。")
            resp_list.append("2、请关注客户的存货周转率是否过低，注意与同行业同类型产品比较。")
            resp_list.append("3、如果客户进货渠道单一，请谨慎授信。")
            resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
    elif industry_code == "E4813":
            # 市政道路工程
            resp_list.append("1、请关注客户的资质完备情况，没有建筑施工资质的客户往往采用挂靠的形式开展业务，可以核实客户的挂靠协议或项目合同。")
            resp_list.append("2、请关注法律风险，该行业易产生交通事故、买卖合同纠纷、劳务纠纷、借款纠纷。")
            resp_list.append("3、请关注隐形负债，该行业内垫资、拆借及民间借贷情况普遍存在。")
            resp_list.append("4、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
            resp_list.append("5、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
    elif "P83" in industry_code:
            # 教育行业
            resp_list.append("1、请关注客户的资质完备情况，教育行业办学资质证件含《办学许可证》、《营业执照》等。")
            resp_list.append("2、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂。")
            resp_list.append("3、请关注该行业的声誉以及信誉风险，可通过员工素质、知识水平以及管理规范等方面了解。")
            resp_list.append("4、请关注客户的机构品牌的市场认可度，如品牌为自创品牌或市场受众较小，请谨慎授信。")
    elif "F516" in industry_code:
            # 矿产品、建材及化工产品批发
            resp_list.append("1、请关注客户经营产品品牌的市场认可度，如产品品牌较为小众，请谨慎授信。")
            resp_list.append("2、请关注产品市场价格波动风险。")
            resp_list.append("3、请核实应收账款质量。一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
            resp_list.append("4、该行业客户资产存货和应收占比高，经营风险相对较大，如果客户固定资产少，保障性弱，请谨慎授信。")
            resp_list.append("5、该行业的日均与资金调动能力一般高于其他行业，如果客户资金调动能力较差，请谨慎授信。")
    elif "H61" in industry_code:
            # 住宿业
            resp_list.append("1、请关注客户的资质完备情况，酒店行业基本资质证件含《营业执照》、《消防证》、《特种行业许可证》、《卫生许可证齐全》，且证件地址需与酒店地址一致。")
            resp_list.append("2、请关注酒店物业的剩余租期是否在贷款期限内，警惕到期不能续租的情况。")
            resp_list.append("3、请关注酒店经营的年限，以及经营者酒店行业的从业年限，如果酒店经营1年以内，或经营者酒店行业从业2年以内的，请谨慎授信。")
            resp_list.append("4、请关注酒店投资时间以及金额，酒店价值随投资年限增长而下降。")
            resp_list.append("5、该行业一般会有多人合伙投资占股情况，请核实酒店的实际控制人、真实股份构成、分红方式以及企业主其他多元化对外投资情况。")
            resp_list.append("6、如果客户经营非连锁品牌的低端宾馆或一般旅馆，请谨慎授信。")
            resp_list.append("7、如果客户不参与名下所有占股酒店的实际经营，请谨慎授信。")
    elif industry_code == "F5124":
            # 肉类批发行业
            resp_list.append("1、请关注客户的资质完备情况，肉类批发行业基本资质证件含《营业执照》、《食品流通许可证齐全》。")
            resp_list.append("2、请关注产品市场价格波动风险。")
            resp_list.append("3、请关注应收账款质量和账期长短，该行业普遍存在应收账款坏账风险和账期不稳定情况。")
            resp_list.append("4、请关注客户的存货周转率与同行业同类型产品相比是否过低。")
            resp_list.append("5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。牛羊肉、鸡鸭肉冻品旺季一般为10-3月；水产虾蟹生鲜的销售旺季一般为4-7月、9-12月，以及端午、中秋、国庆等节假日；海鲜冻品的销售旺季一般在休渔期。")
    elif industry_code == "F5274":
            # 通信设备零售
            resp_list.append("1、请关注企业主手机门店是否有品牌授权以及是否在授权期限内。")
            resp_list.append("2、请关注客户销售手机的品牌，国内目前华为、vivo、OPPO、小米、苹果5个品牌的市场占有率达90%，如果客户主营产品非主流品牌，建议谨慎授信。")
            resp_list.append("3、请关注是否有存货积压的风险。")
            resp_list.append("4、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂，甚至过度举债经营。")
            resp_list.append("5、请关注门店地段的人口密集程度。")
    elif industry_code in ["F512", "F5127", "F5226"]:
            # 酒水、饮品批发零售行业 精确匹配
            resp_list.append("1、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
            resp_list.append("2、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
            resp_list.append("3、请关注客户的存货周转率与同行业同类型产品是否过低。")
            resp_list.append("4、如果客户经营产品为饮料、啤酒等，产品保质期要求较严格的，请关注固定资产保障。")
            resp_list.append("5、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
            resp_list.append("6、该行业淡旺季明显，建议将贷款到期日设置在销售旺季")
    elif "C21" in industry_code or industry_code == "F5283":
            # 家具制造及零售行业
            resp_list.append("1、请关注库存积压情况。")
            resp_list.append("2、请关注线下零售门店所在专业市场的成熟度及客流量。")
            resp_list.append("3、请关注零售品牌的市场认可度和销售渠道。")
            resp_list.append("4、请关注仓库的防火措施是否到位。")
            resp_list.append("5、该行业属于夕阳行业，请谨慎授信。")
    elif industry_code in ["F5138", "F5272"]:
            # 家电批发零售行业
            resp_list.append("1、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
            resp_list.append("2、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
            resp_list.append("3、请关注线下零售门店所在专业市场的客流量。")
            resp_list.append("4、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
            resp_list.append("5、请关注库存积压情况。")
            resp_list.append("6、如果客户经营的产品种类多，销量低，请谨慎授信。")
    elif industry_code in ["F513", "F5132", "F523", "F5232"]:
            # 服装批发零售行业 精确匹配
            resp_list.append("1、请关注应收账款质量和账期长短，该行业普遍存在应收账款金额较大、账期长、坏账率高的情况。")
            resp_list.append("2、请关注该行业客户是否有选款不慎，压货过多的情况。")
            resp_list.append("3、请关注隐形负债，该行业库存、应收资金占用较大，厂房、设备投入较多，人工工资等运营成本较高，设备融资租赁、民间借贷等情况普遍存在。")
            resp_list.append("4、请关注资产保障性和体外担保设置的合理性，建议有增信措施。")
    elif "L711" in industry_code:
            # 机械设备租赁
            resp_list.append("1、请关注客户的经营年限，如行业新进入者（少于3年）初始投资大，可能存在民间借款。")
            resp_list.append("2、请关注该客户是否有行业相关资质，若无资质，请关注是否有牢靠的社会关系，反之可能存在业务不够连续性，收入不稳定性。")
            resp_list.append("3、请关注经营性租赁的现金净流入是否可以支付融资租赁款项。")
            resp_list.append("4、请关注应收账款回收周期及应收账款是否集中。")
            resp_list.append("5、请关注客户的业务分布，是否与房地产行业合作，该房地产是否命中三道红线。")
            resp_list.append("6、请关注设备的出租率，评估客户的实际经营情况。")
            resp_list.append("7、客户资产多数集中在机械设备，关注固定资产保障度，对于固定资产保障度弱的客户，又无强担保，谨慎授信。")
            resp_list.append("8、该行业是重资产运作，请关注设备所有权归属。")
    elif "E47" in industry_code:
            # 房屋建筑业
            resp_list.append("1、请关注客户合作的房地产企业，是否命中三道红线（恒大、华夏幸福、绿地及中南建设等高危房产公司），关注其应收账款是否能正常回收。")
            resp_list.append("2、请关注客户主营公司的行业等级资质，若无，是否通过正规途径挂靠其他有资质的公司，或者转包等形式进行施工。")
            resp_list.append("3、请关注应收账款是否过于集中，账龄是否过长，死账是否过多。")
            resp_list.append("4、请关注客户的隐形负债，是否存在较多的民间借款，关注利率是否过高。")
            resp_list.append("5、请关注客户的固定资产中是否存在较多的工抵房，判断其变现能力。")
            resp_list.append("6、该行业客户一般高资产高负债，如果客户固定资产保障度较弱，请谨慎授信。")
    elif "E49" in industry_code:
            # 建筑安装业
            resp_list.append("1、请关注该企业是否有相关安装资质；若有资质，是否有被其他无资质公司挂靠，需核实该公司的实际营业额。")
            resp_list.append("2、请关注应收账款质量和账期长短，及回收情况是否稳定。")
            resp_list.append("3、请关注客户合作的对象是民营还是国企，关系是否稳定。")
            resp_list.append("4、该行业环境复杂多变，容易出现财务风险。")
            resp_list.append("5、请关注该行业在建项目与储备项目占比，尽量选择学校、医院及商业体等项目，与高危房产公司合作的谨慎进入。")
            resp_list.append("6、请关注是否为员工购买足额保险，该行业作业易出严重事故，伤残、死亡等高额索赔易造成重大损失。")
    elif "L72" in industry_code:
            # 商业服务业
            resp_list.append("1、该行业属于轻资产经营，环境复杂，流动性较大，存在一定的不稳定性。")
            resp_list.append("2、请关注企业或者客户个人名下是否有不动产积累，若无，保障性欠佳，请引入强担保谨慎授信。")
            resp_list.append("3、该行业容易被包装，请更加谨慎的核实客户所提供的资料。")
            resp_list.append("4、该行业需要较强的人脉关系，行业经验、客户资源、请关注客户的经营能力。")
            resp_list.append("5、该行业多人合作形式较多，请关注其股份之间的稳定性。")
            resp_list.append("6、该类行业一般不扩张的情况下不需要大量资金，请核实实际资金用途。")
            resp_list.append("7、国家政策对该行业的细分相关行业有较大影响。")
    elif "C33" in industry_code:
            # 金属制品业
            resp_list.append("1、该行业的原材料随着市场波动较大，且基本上为现金购买，资金压力较大。")
            resp_list.append("2、如金属工具制造行业，其厂房、设备投资较大，融资租赁和民间负债都较频繁。")
            resp_list.append("3、如金属工具制造行业基本上都是先接订单再生产，请关注客户是否有稳定的客源，是外贸还是内销。")
            resp_list.append("4、如金属门窗制造，其应收账款占比高，且账期较长。")
            resp_list.append("5、请关注客户产品的市场竞争力，是否有议价能力。")
            resp_list.append("6、如铝合金门窗制造，关注是否有环评报告。")
    elif "C23" in industry_code or "C22" in industry_code:
            # 印刷、造纸、纸制品业
            resp_list.append("1、请关注该行业的相关资质证明（如环评资质，ISO质量管理体系等等）。")
            resp_list.append("2、该行业属于高能耗高污染行业，存在较大的政策性风险。")
            resp_list.append("3、该行业设备投资较大，且容易更新换代，资金压力大，设备融资租赁频繁。")
            resp_list.append("4、该行业三角债关系明显，现金流短缺。")
            resp_list.append("5、中大型规模企业垄断现象普遍，微小型规模企业容易挤压。")
    elif "C30" in industry_code:
            # 非金属矿物制品业
            resp_list.append("1、请关注该企业是否有相关生产资质及环评资质等。")
            resp_list.append("2、请关注该企业销售渠道是否稳定，应收款回收是否及时。")
            resp_list.append("3、请关注国家行业政策及当地新规，细分行业及原料是否属于夕阳行业。")
            resp_list.append("4、请关注客户用电情况是否稳定，该行业设备开机后一般不停机。")
            resp_list.append("5、关注客户是否存在对外投资，特别是房屋建筑相关行业对外投资。")
            resp_list.append("6、请关注该企业是都与命中三道红线的房企合作，尽量规避。")
    elif "O80" in industry_code:
            # 居民服务业
            resp_list.append("1、该行业初始投资大，资金压力大，如开业后客源不能保障，短时间内可能会倒闭。")
            resp_list.append("2、请关注实际经营中是都有现在不符合法律法规的业务。")
            resp_list.append("3、该行业属于人员密集型行业，人员固定成本高，且流动性大。")
            resp_list.append("4、在近几年疫情影响下，该行业的抗风险能力较差。")
            resp_list.append("5、该行业对于门面位置要求较高，如位置不好，再加上经营能力差，店面容易倒闭。")
            resp_list.append("6、该行业各项固定成本高，资金压力大。")
    elif "C14" in industry_code:
            # 食品制造业
            resp_list.append("1、请关注该企业是否有相关齐全的证件，如食品生产许可证 、食品流通许可证、卫生许可证、工商营业执照、组织机构代码证、税务登记证。")
            resp_list.append("2、该行业容易出现食品质量问题，导致舆情，影响企业形象及合作稳定性。")
            resp_list.append("3、请关注该企业的管理制度是否严谨，执行是否到位，如只是执行浮于表面、意识淡薄，则风险较高。")
            resp_list.append("4、请关注该企业的食品物流风险，如在运输过程中，有损坏、处理时间长、运输环境未达标等等都会让企业亏损。")
            resp_list.append("5、请关注该企业的食品研发风险，产品老化是风险，新产品研发也存在风险。")
    elif "K702" in industry_code:
            # 物业管理
            resp_list.append("1、请关注该企业主或者职业经理人是否有足够的经验，是否有稳定的企业管理团队，制度是否完善。")
            resp_list.append("2、关注客户物业经营权的期限是否到期。")
            resp_list.append("3、请关注该物业公司管辖内，是否消防设施完善，有无隐患风险。")
            resp_list.append("4、请关注该户具体从事的细分行业，小区居民物业，二房东或经营性物业。")
            resp_list.append("5、若是二房东、经营性物业请关注满租率、产权，是否涉及租金贷，p2p非法集资。")
            resp_list.append("6、请关注该户户籍，从业历史，一般此行业福建蒲城较多，福建籍二房东此行业民间借贷严重。")
            resp_list.append("7、请关注实际控制权人，该行业暗股情况严重。")
            resp_list.append("8、该行业前期投资大，有隐形负债，关注是否存在多头授信风险。")
            resp_list.append("9、请关注圈子客户互保、合伙人之间担保等，避免系统性风险。")
    elif "A01" in industry_code:
            # 农业
            resp_list.append("1、请关注该户农业保险购买保额与实际经营体量的区别，是否全额保险，合理评估真实经营体量。")
            resp_list.append("2、请关注该户是否全额购买农业保险对冲风险。")
            resp_list.append("3、请关注该户农业产品销售渠道，是否有稳定的销售渠道，如供超级市场、大型食品中心、企事业单位食堂等。")
            resp_list.append("4、请关注所处地区交通是否便利及自然灾害发生频率，如台风、洪水及泥石流等情况。")
            resp_list.append("5、请关注农业产品生长周期及出货时间等，合理设置贷款到期日。")
            resp_list.append("6、请关注客户的种植经验，种植经验3年以内的，请谨慎授信。")
            resp_list.append("7、该行业融资能力偏弱，建议加强担保设置。")
    elif "A03" in industry_code:
            # 畜牧业
            resp_list.append("1、请关注该户从业年限，该行业需要多年经验，家族是否有从业史。")
            resp_list.append("2、请关注环境评估报告及保险购买保额与实际体量作比较，一般环境评估报告最大产能应大于该户实际产能。")
            resp_list.append("3、请关注所处地区交通是否便利及自然灾害发生频率，如台风、洪水及泥石流等情况。")
            resp_list.append("4、请关注禽畜生长周期及出货时间等，合理设置贷款到期日。")
            resp_list.append("5、请关注该行业疾病防治情况，养殖场地规模，是否科学喂养及疾病防治，关注禽流感、非洲猪瘟及牛炭疽等传染病防治。")
            resp_list.append("6、请关注客户近两年的效益情况，是否发生过大规模损失。")
            resp_list.append("7、请关注近期政策上对价格的调控。")
    elif "I" in industry_code:
            # 信息传输、软件和信息技术服务业
            resp_list.append("1、请关注客户教育经历及从业年限，该细分行业需要有相应的知识储备，是否有相关的专业技能和专业背景。")
            resp_list.append("2、请关注该户资金用途、经营模式及合作方，正常不需要资金，除非接工程有账期或需要垫资。")
            resp_list.append("3、请关注该户人工支出占比，社保公积金缴纳情况，一般来说社保及公积金支出越多，该公司含金量越高。")
            resp_list.append("4、请关注客户的产品的优势，及下游的稳定性。")
            resp_list.append("5、请关注细分行业发展前景，品牌的市场认可度。")
    return resp_list