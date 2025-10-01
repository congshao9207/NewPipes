# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : owner.py.py 
# @Software: PyCharm
import datetime
import json
import re
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from util.common_util import get_query_data, get_all_related_company, logger


def translate_marry_state(marry_state):
    if marry_state == 'MARRIED':
        return "已婚"
    elif marry_state == 'UNMARRIED':
        return "未婚"
    elif marry_state == 'MARRIAGE':
        return "初婚"
    elif marry_state == 'REMARRIAGE':
        return "再婚"
    elif marry_state == 'REMARRY':
        return "复婚"
    elif marry_state == 'WIDOWED':
        return "丧偶"
    elif marry_state == 'DIVORCE':
        return "离婚"
    elif marry_state == 'NO_DESC':
        return "未说明的婚姻状况"
    elif marry_state == 'SINGLE':
        return "单身"
    elif marry_state == 'UNKNOWN':
        return "未知"
    else:
        return "未说明的婚姻状况"


# 在网状态枚举
online_state_map = {
    'OUT_SERVICE': '在网但不可用',
    'NOT_ENABLED': '未启用',
    'NORMAL': '正常',
    'ARREARAGE_SHUTDOWN': '欠费停机',
    'OTHER_SHUTDOWN': '其他停机',
    'DISABLED': '已销号',
    'SHUTDOWN': '关机',
    'BUSY': '忙'
}

# 公积金账户状态
basic_account_status = {
    '01': '正常',
    '02': '封存',
    '03': '合并销户',
    '04': '外部转出销户',
    '05': '提取销户',
    '06': '冻结',
    '99': '其他'
}
# 公积金借款状态
loan_account_status = {
    '1': '未审',
    '2': '已审',
    '3': '正常',
    '8': '注销',
    '9': '发放'
}


class Owner(GroupedTransformer):
    """
    企业主分析_owner
    """

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "owner"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "owner_info_cnt": 1,
            "owner_tax_cnt": 0,
            "owner_list_cnt": 0,
            "owner_app_cnt": 0,
            "owner_age": 0,  # 主体年龄
            "owner_resistence": "",  # 主体籍贯
            "owner_marriage_status": "",  # 主体婚姻状况
            "owner_education": "",  # 主体学历水平
            "owner_criminal_score_level": "",
            "owner_list_name": [],
            "owner_list_type": [],
            "owner_list_case_no": [],
            "owner_list_detail": [],
            "owner_job_year": 0,
            "owner_major_job_year": 0,  # 主体从业年限
            "owner_tax_name": [],
            "owner_tax_amt": [],
            "owner_tax_type": [],
            "owner_tax_date": [],
            "owner_app_traffic": 0,
            "owner_app_hotel": 0,
            "owner_app_finance": 0,
            "owner_app_invest": 0,
            "owner_app_ent": 0,
            "owner_app_live": 0,
            "owner_app_read": 0,
            "owner_app_game": 0,
            "owner_app_life": 0,
            "owner_app_social": 0,
            "owner_app_edu": 0,
            "owner_app_shop": 0,
            "owner_app_work": 0,
            "owner_app_loan": 0,
            "owner_app_loan_car": 0,
            "owner_app_loan_installment": 0,
            "owner_app_loan_credit": 0,
            "owner_app_loan_cash": 0,
            "owner_app_loan_house": 0,
            "owner_app_loan_p2p": 0,
            "owner_app_loan_platform": 0,
            # 20230922 新增主体关联信息
            # 主体关联信息
            "marray_info": "",  # 婚姻登记信息
            "inhabitantFlag": "",  # xx是否为青岛市常住人口:是|否
            "operatorInfo": {
                # 运营商信息
                # 可以后端直接返回需要显示的内容字符串，也可以返回关键字段值，由前端组装展示
                # 现在按照返回关键字段值，前端展示方案设计
                # 如果使用第一种方案，operatorInfo字段值类型为字符串
                "certification": "",  # 实名认证结果
                "onlineDuration": "",  # 在网时长
                "onlineStatus": ""  # 在网状态
            },
            "educationInfo": {
                # 学籍信息
                "education": "",  # 学历
                "school": ""  # 学校
            },
            "socialInsuranceInfo": {
                # 社保缴纳信息
                "status": "",  # 状态
                "paymentBaseRange": "",  # 缴纳基数范围
                "paymentRange": "",  # 缴纳年月范围
                "paymentMonth": ""  # 累计缴纳月份
            },
            "accumulationFundInfo": {
                # 公积金信息
                "status": "",  # 状态
                "paymentBase": "",  # 缴纳基数
                "depositoryUnit": "",  # 缴存单位
                "paymentRatio": "",  # 缴存比例
                "loanAmt": "",  # 借款金额
                "balance": "",  # 本金余额
                "loanStatus": ""  # 借款状态
            },
            "vehicleRegistInfo": {
                # 机动车登记信息
                "registInfoNum": 0,  # 登记信息条数
                "detail": [
                    # 登记信息详情, 显示内容未提供
                    # {
                    #     "carType": "",  # 车辆类型
                    #     "regDate": "",  # 出场登记日期
                    #     "status": "",  # 车辆状态
                    #     "brand": "",  # 号牌种类
                    #     "style": ""  # 车辆型号
                    # }
                ]
            },
            "realEstateRegistInfo": {
                # 不动产登记信息
                "homesteadNum": 0,  # 建设用地使用权、宅基地使用权登记条数
                "homesteadDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "area": "",  # 面积
                    #     "startDate": "",  # 使用权开始时间
                    #     "endDate": ""  # 使用权结束时间
                    # }
                ],
                "landRightNum": 0,  # 房地产权登记条数
                "landRightDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "purpose": "",  # 用途
                    #     "outArea": "",  # 建筑面积
                    #     "houseProperty": "",  # 房屋性质
                    #     "registDate": "",  # 登记日期
                    #     "countyName": "",  # 区县名称
                    #     "ownerName": "",  # 权利人名称
                    #     "coOwnership": "",  # 共有情况
                    #     "term": ""  # 使用期限
                    # }
                ],
                "seaAreaUseRightNum": 0,  # 海域使用权登记条数
                "seaAreaUseRightDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "projectName": "",  # 项目名称
                    #     "seaAreaAddress": "",  # 海域面积
                    #     "seaTypeA": "",  # 用海类型A
                    #     "seaTypeB": "",  # 用海类型B
                    #     "islandName": "",  # 海岛名称
                    #     "islandAddress": "",  # 海岛位置
                    #     "useRightArea": "",  # 使用权面积
                    #     "startDate": "",  # 使用权开始时间
                    #     "endDate": ""  # 使用权结束时间
                    # }
                ],
                "forestOwnershipNum": 0,  # 林权登记条数
                "forestOwnershipDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "area": "",  # 使用权面积
                    #     "startDate": "",  # 使用权开始时间
                    #     "endDate": ""  # 使用权结束时间
                    # }
                ],
                "mortgageNum": 0,  # 抵押登记条数
                "mortgageDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "mortgagee": "",  # 抵押权人
                    #     "mortgageMode": "",  # 抵押方式
                    #     "mortgageRegistDate": "",  # 抵押登记时间
                    #     "startDate": "",  # 债务履行开始时间
                    #     "endDate": "",  # 债务履行结束时间
                    #     "guaranteeAmt": "",  # 担保金额
                    #     "noTransferFlag": "",  # 抵押期间禁止转让标志
                    #     "guaranteeScope": ""  # 担保范围
                    # }
                ],
                "seizureNum": 0,  # 查封登记条数
                "seizureDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "seizureUnit": "",  # 查封单位
                    #     "startDate": "",  # 查封起始时间
                    #     "endDate": "",  # 查封结束时间
                    #     "registDate": ""  # 查封登记时间
                    # }
                ],
                "objectionNum": 0,  # 异议登记条数
                "objectionDetail": [
                    # {
                    #     "noMoveUnitNo": "",  # 不动产单元号
                    #     "address": "",  # 坐落
                    #     "registDate": "",  # 登记时间
                    #     "reason": ""  # 异议原因
                    # }
                ]
            },
            "blackInfo": {
                # 反欺诈及黑名单信息 个人企业均有该字段
                "bankBlackFlag": False,  # 是否命中本行黑名单 true|false
                "bigDataBlackFlag": "",  # 是否命中大数据局黑名单 true|false
                "brScore": "",  # 百融反欺诈指数
                "tencentScore": "",  # 腾讯欺诈分值
                "ltLevel": ""  # 联通金融反欺诈恶意等级
            },
            # 行内信息
            "bank_info": ""  # 行内信息，pipes输出字符串，前端直接展示
        }
        self.person_list = None
        self.company_list = None
        self.per_type = None

    # 获取对应主体及主体的关联企业对应的欠税信息
    def _info_court_id(self, idno):
        input_info = self.per_type.get(idno)
        if input_info is not None:
            unique_idno = input_info['idno']
            unique_idno_str = '"' + '","'.join(unique_idno) + '"'
            sql = """
                select id 
                from info_court 
                where unique_id_no in (%s) 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                """ % unique_idno_str
            df = sql_to_df(sql=sql)
            if df is not None and df.shape[0] > 0:
                id_list = df['id'].to_list()
                court_id_list = ','.join([str(x) for x in id_list])
                sql = """
                    select *
                    from info_court_tax_arrears
                    where court_id in (%s)
                """ % court_id_list
                df = sql_to_df(sql=sql)
                if df.shape[0] > 0:
                    self.variables['owner_tax_cnt'] = df.shape[0]
                    df.sort_values(by='taxes_time', ascending=False, inplace=True)
                    self.variables['owner_tax_name'] = df['name'].to_list()
                    self.variables['owner_tax_amt'] = df['taxes'].to_list()
                    self.variables['owner_tax_type'] = df['taxes_type'].to_list()
                    self.variables['owner_tax_date'] = df['taxes_time'].apply(
                        lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()

    # 获取个人基本信息
    def _indiv_base_info(self, idno):
        idno = str(idno)
        now = datetime.datetime.now()
        self.variables['owner_age'] = now.year - int(idno[6:10]) + \
                                      (now.month - int(idno[10:12]) + (now.day - int(idno[12:14])) // 100) // 100
        self.variables['owner_resistence'] = idno[:6]
        # for index in self.person_list:
        #     temp_id = index.get('id_card_no')
        #     if str(temp_id) == idno:
        #         self.variables['owner_marriage_status'] = translate_marry_state(index.get('marry_state'))
        #         self.variables['owner_education'] = index.get('education')
        #         break

        # 通过接口获取单人婚姻状态
        marriage_sql = """
            select id_card_no, marriage_status from info_marriage_status 
            where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        marriage_df = sql_to_df(sql=marriage_sql, params={'unique_id_no': idno})
        if marriage_df.shape[0] != 0:
            if pd.notna(marriage_df['marriage_status'][0]):
                self.variables['owner_marriage_status'] = marriage_df['marriage_status'][0]

        # 通过接口获取学历信息
        education_sql = """
            select id_card_no, education_level from info_education 
            where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        education_df = sql_to_df(sql=education_sql, params={'unique_id_no': idno})
        if education_df.shape[0] != 0:
            if pd.notna(education_df['education_level'][0]):
                self.variables['owner_education'] = education_df['education_level'][0]

    # 20230925 新增一键查询内容
    def _one_click_query_info(self, idno, name):
        idno = str(idno)
        """婚姻登记信息"""
        marriage_sql = """
                    select id_card_no, marriage_status from info_marriage_status 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                """
        marriage_df = sql_to_df(sql=marriage_sql, params={'unique_id_no': idno})
        if marriage_df.shape[0] != 0:
            if pd.notna(marriage_df['marriage_status'][0]):
                self.variables['marray_info'] = f"{name}婚姻状况为：{marriage_df['marriage_status'][0]}"

        """常住人口登记信息"""
        inhabitant_sql = """
                    select id_card_no, inhabitant_flag from info_inhabitant 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                """
        inhabitant_df = sql_to_df(sql=inhabitant_sql, params={'unique_id_no': idno})
        if inhabitant_df.shape[0] > 0:
            inhabitant_flag = inhabitant_df['inhabitant_flag'][0]
            if pd.notna(inhabitant_flag) and inhabitant_flag == 1:
                self.variables['inhabitantFlag'] = f"{name}是否为青岛市常住人口：是"
            else:
                self.variables['inhabitantFlag'] = f"{name}是否为青岛市常住人口：否"

        """运营商信息"""
        # 实名认证
        certification_sql = """
                    select id_card_no, cause from info_certification 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        certification_df = sql_to_df(sql=certification_sql, params={'unique_id_no': idno})
        if certification_df.shape[0] > 0:
            cause = certification_df['cause'].values[0]
            if pd.notna(cause):
                self.variables['operatorInfo']['certification'] = cause
        # 在网时长
        online_duration_sql = """
                    select id_card_no, on_line_days from info_on_line_duration 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        online_duration_df = sql_to_df(sql=online_duration_sql, params={'unique_id_no': idno})
        if online_duration_df.shape[0] > 0:
            on_line_days = online_duration_df['on_line_days'].values[0]
            if pd.notna(on_line_days) and on_line_days != '':
                if on_line_days == 10:
                    on_line_days = '0-3个月'
                elif on_line_days == 11:
                    on_line_days = '3-6个月'
                elif on_line_days == 12:
                    on_line_days = '6-12个月'
                elif on_line_days == 13:
                    on_line_days = '12-24个月'
                elif on_line_days == 14:
                    on_line_days = '24个月以上'
                else:
                    on_line_days = '在网时长数据不符合逾期'
                self.variables['operatorInfo']['onlineDuration'] = on_line_days
        # 在网状态
        online_status_sql = """
                    select id_card_no, mobile_state from info_on_line_state 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        online_status_df = sql_to_df(sql=online_status_sql, params={'unique_id_no': idno})
        if online_status_df.shape[0] > 0:
            mobile_state = online_status_df['mobile_state'].values[0]
            mobile_state_translate = online_state_map.get(mobile_state)
            self.variables['operatorInfo']['onlineStatus'] = mobile_state_translate if pd.notna(mobile_state_translate) else "在网状态结果不符合预期"

        """学籍信息"""
        education_sql = """
                    select id_card_no, education_level,school_name from info_education 
                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                """
        education_df = sql_to_df(sql=education_sql, params={'unique_id_no': idno})
        if education_df.shape[0] > 0:
            if pd.notna(education_df['education_level'][0]):
                self.variables['educationInfo']['education'] = education_df['education_level'][0]
            if pd.notna(education_df['school_name'][0]):
                self.variables['educationInfo']['school'] = education_df['school_name'][0]

        """社保缴纳信息"""
        self.social_insurance_info(idno)

        """公积金信息"""
        self.accumulation_fund_info(idno)

        """机动车登记信息"""
        self.vehicle_register_info(idno)

        """不动产登记信息"""
        self.real_estate_register_info(idno)

        """反欺诈及黑名单信息"""
        self.black_info(idno)

    def black_info(self, idno):
        """
        反欺诈及黑名单信息
        :param idno:
        :return:
        """
        # 行内黑名单 取version最大的
        black_sql = """
                select id_card_no from info_black_list where 
                version = (select max(version) from info_black_list)
                and id_card_no = %(unique_id_no)s
        """
        black_df = sql_to_df(sql=black_sql, params={'unique_id_no': idno})
        if black_df.shape[0] > 0:
            self.variables['blackInfo']['bankBlackFlag'] = True
        # 腾讯反欺诈
        anti_sql = """
                select id, anti_fraud_score from info_anti_fraud 
                where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                and channel_api_no = '12006' order by id limit 1
        """
        anti_df = sql_to_df(sql=anti_sql, params={'unique_id_no': idno})
        if anti_df.shape[0] > 0:
            self.variables['blackInfo']['tencentScore'] = anti_df['anti_fraud_score'].values[0]
        # 百融欺诈分
        br_sql = """
                select id, anti_fraud_score from info_anti_fraud 
                where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                and channel_api_no = '12007' order by id desc limit 1
        """
        br_df = sql_to_df(sql=br_sql, params={'unique_id_no': idno})
        if br_df.shape[0] > 0:
            self.variables['blackInfo']['brScore'] = br_df['anti_fraud_score'].values[0]
        # 联通金融反欺诈恶意等级 接口已弃用

    def real_estate_register_info(self, idno):
        """
        不动产登记信息
        :param idno:
        :return:
        """
        # 宅基地使用权
        homestead_sql = """
                select  no_move_unit_no,address,area,start_date,end_date from info_no_move_homestead
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        homestead_df = sql_to_df(sql=homestead_sql, params={'unique_id_no': idno})
        if homestead_df.shape[0] > 0:
            homestead_df.fillna('', inplace=True)
            for i in ['start_date', 'end_date']:
                homestead_df[i] = homestead_df[i].astype(str)
            homestead_df.rename(columns={'no_move_unit_no': 'noMoveUnitNo', 'start_date': 'startDate', 'end_date': 'endDate'}, inplace=True)
            self.variables['realEstateRegistInfo']['homesteadNum'] = homestead_df.shape[0]
            self.variables['realEstateRegistInfo']['homesteadDetail'] = homestead_df.to_dict(orient='records')

        # 房地产权
        land_right_sql = """
                select no_move_unit_no, address, purpose, out_area, house_property, regist_date,
                county_name, owner_name, co_ownership, term from info_no_move_land_right 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        land_right_df = sql_to_df(sql=land_right_sql, params={'unique_id_no': idno})
        if land_right_df.shape[0] > 0:
            land_right_df.fillna('', inplace=True)
            for i in ['regist_date']:
                land_right_df[i] = land_right_df[i].astype(str)
            land_right_df.rename(columns={'no_move_unit_no': 'noMoveUnitNo', 'out_area': 'outArea', 'house_property': 'houseProperty',
                                          'regist_date': 'registDate', 'county_name': 'countyName', 'owner_name': 'ownerName',
                                          'co_ownership': 'coOwnership'}, inplace=True)
            self.variables['realEstateRegistInfo']['landRightNum'] = land_right_df.shape[0]
            self.variables['realEstateRegistInfo']['landRightDetail'] = land_right_df.to_dict(orient='records')

        # 海域使用权
        sea_sql = """
                select no_move_unit_no, project_name, sea_area_address, sea_type_a, sea_type_b,
                island_name, island_address, use_right_area, start_date, end_date from info_sea_area_use_right 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        sea_df = sql_to_df(sql=sea_sql, params={'unique_id_no': idno})
        if sea_df.shape[0] > 0:
            sea_df.fillna('', inplace=True)
            for i in ['start_date', 'end_date']:
                sea_df[i] = sea_df[i].astype(str)
            rename_col = ['noMoveUnitNo', 'projectName', 'seaAreaAddress', 'seaTypeA', 'seaTypeB',
                          'islandName', 'islandAddress', 'useRightArea', 'startDate', 'endDate']
            sea_df.columns = rename_col
            self.variables['realEstateRegistInfo']['seaAreaUseRightNum'] = sea_df.shape[0]
            self.variables['realEstateRegistInfo']['seaAreaUseRightDetail'] = sea_df.to_dict(orient='records')

        # 林权登记
        forest_sql = """
                select no_move_unit_no, address, area, start_date, end_date from info_forest_ownership 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        forest_df = sql_to_df(sql=forest_sql, params={'unique_id_no': idno})
        if forest_df.shape[0] > 0:
            forest_df.fillna('', inplace=True)
            for i in ['start_date', 'end_date']:
                forest_df[i] = forest_df[i].astype(str)
            forest_df.rename(columns={'no_move_unit_no': 'noMoveUnitNo', 'start_date': 'startDate', 'end_date': 'endDate'}, inplace=True)
            self.variables['realEstateRegistInfo']['forestOwnershipNum'] = forest_df.shape[0]
            self.variables['realEstateRegistInfo']['forestOwnershipDetail'] = forest_df.to_dict(orient='records')

        # 抵押登记
        mortgage_sql = """
                select no_move_unit_no, address, mortgagee, mortgage_mode, mortgage_regist_date, start_date, end_date,
                guarantee_amt, no_transfer_flag, guarantee_scope from info_mortgage 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        mortgage_df = sql_to_df(sql=mortgage_sql, params={'unique_id_no': idno})
        if mortgage_df.shape[0] > 0:
            mortgage_df.fillna('', inplace=True)
            for i in ['start_date', 'end_date', 'mortgage_regist_date']:
                mortgage_df[i] = mortgage_df[i].astype(str)
            rename_col = ['noMoveUnitNo', 'address', 'mortgagee', 'mortgageMode', 'mortgageRegistDate',
                          'startDate', 'endDate', 'guaranteeAmt', 'noTransferFlag', 'guaranteeScope']
            mortgage_df.columns = rename_col
            self.variables['realEstateRegistInfo']['mortgageNum'] = mortgage_df.shape[0]
            self.variables['realEstateRegistInfo']['mortgageDetail'] = mortgage_df.to_dict(orient='records')

        # 查封登记
        seizure_sql = """
                select no_move_unit_no, address, seizure_unit, start_date, end_date, regist_date from info_seizure 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        seizure_df = sql_to_df(sql=seizure_sql, params={'unique_id_no': idno})
        if seizure_df.shape[0] > 0:
            seizure_df.fillna('', inplace=True)
            for i in ['start_date', 'end_date', 'regist_date']:
                seizure_df[i] = seizure_df[i].astype(str)
            rename_col = ['noMoveUnitNo', 'address', 'seizureUnit', 'startDate', 'endDate', 'registDate']
            seizure_df.columns = rename_col
            self.variables['realEstateRegistInfo']['seizureNum'] = seizure_df.shape[0]
            self.variables['realEstateRegistInfo']['seizureDetail'] = seizure_df.to_dict(orient='records')

        # 异议登记
        objection_sql = """
                select no_move_unit_no, address, regist_date, reason from info_objection 
                where basic_id = (select id from info_no_move_basic where id_card_no = %(unique_id_no)s and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1)
        """
        objection_df = sql_to_df(sql=objection_sql, params={'unique_id_no': idno})
        if objection_df.shape[0] > 0:
            objection_df.fillna('', inplace=True)
            for i in ['regist_date']:
                objection_df[i] = objection_df[i].astype(str)
            objection_df.rename(columns={'no_move_unit_no': 'noMoveUnitNo', 'regist_date': 'registDate'}, inplace=True)
            self.variables['realEstateRegistInfo']['objectionNum'] = objection_df.shape[0]
            self.variables['realEstateRegistInfo']['objectionDetail'] = objection_df.to_dict(orient='records')

    def vehicle_register_info(self, idno):
        """
        机动车登记信息
        :param idno:
        :return:
        """
        car_basic_sql = """
                select id, record_num from info_car_basic 
                where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        car_basic_df = sql_to_df(sql=car_basic_sql, params={'unique_id_no': idno})
        if car_basic_df.shape[0] > 0:
            self.variables['vehicleRegistInfo']['registInfoNum'] = car_basic_df['record_num'].values[0]
            basic_id = car_basic_df['id'].values[0]
            car_record_sql = """
                    select car_type,reg_date,status,brand,style from info_car_record
                    where basic_id = %(id)s
            """
            car_record_df = sql_to_df(sql=car_record_sql, params={'id': str(basic_id)})
            if car_record_df.shape[0] > 0:
                # reg_date字段仅保留日期
                car_record_df['reg_date'] = car_record_df['reg_date'].map(lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
                car_record_df.rename(columns={'car_type': 'carType', 'reg_date': 'regDate'}, inplace=True)
                self.variables['vehicleRegistInfo']['detail'] = car_record_df.to_dict(orient='records')

    def accumulation_fund_info(self, idno):
        """
        公积金信息
        :param idno:
        :return:
        """
        # 公积金信息
        fund_sql = """
               select account_status, pad_base, company_name, person_pad_ratio from info_accumulation_fund 
                where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) 
        """
        fund_df = sql_to_df(sql=fund_sql, params={'unique_id_no': idno})
        if fund_df.shape[0] > 0:
            # 处理公积金账户状态
            fund_df['status'] = fund_df['account_status'].apply(lambda x: basic_account_status.get(x) if pd.notna(basic_account_status.get(x)) else '未知状态')
            fund_df.rename(columns={'pad_base': 'paymentBase', 'company_name': 'depositoryUnit', 'person_pad_ratio': 'paymentRatio'}, inplace=True)
            self.variables['accumulationFundInfo'].update(fund_df.to_dict(orient='records')[0])
        # 公积金贷款信息
        fund_loan_sql = """
                       select status, loan_amount, remaining_principal from info_accumulation_fund_loan
                        where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) 
                """
        fund_loan_df = sql_to_df(sql=fund_loan_sql, params={'unique_id_no': idno})
        if fund_loan_df.shape[0] > 0:
            # 处理贷款账户状态
            fund_loan_df['loanStatus'] = fund_loan_df['status'].apply(lambda x: loan_account_status.get(x) if pd.notna(loan_account_status.get(x)) else '未知公积金借款状态')
            fund_loan_df.rename(columns={'loan_amount': 'loanAmt', 'remaining_principal': 'balance'}, inplace=True)
            self.variables['accumulationFundInfo'].update(fund_loan_df.to_dict(orient='records')[0])

    def social_insurance_info(self, idno):
        """
        社保缴纳信息
        :param idno:
        :return:
        """
        social_insurance_sql = """
                           select id_card_no, retire_flag, worker_payment_status, resident_payment_status from info_social_insurance 
                            where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                            order by id desc limit 1
                """
        social_insurance_df = sql_to_df(sql=social_insurance_sql, params={'unique_id_no': idno})
        if social_insurance_df.shape[0] > 0:
            # 状态判断，如果未退休，取职工参保缴费状态，若已退休，取居民参保缴费状态
            retire_flag = social_insurance_df['retire_flag'].values[0]
            if retire_flag == '在职':
                self.variables['socialInsuranceInfo']['status'] = social_insurance_df['worker_payment_status'].values[0] if pd.notna(social_insurance_df['worker_payment_status'].values[0]) else ''
            else:
                self.variables['socialInsuranceInfo']['status'] = social_insurance_df['resident_payment_status'].values[0] if pd.notna(social_insurance_df['resident_payment_status'].values[0]) else ''
        # 缴纳年月范围、累计缴纳月份
        social_insurance_basic_info_sql = """
                                   select begin_payment_date, end_payment_date, payment_month_num from info_social_insurance_payment_basic
                                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                                    order by id desc limit 1
                                """
        social_insurance_basic_info_df = sql_to_df(sql=social_insurance_basic_info_sql, params={'unique_id_no': idno})
        if social_insurance_basic_info_df.shape[0] > 0:
            self.variables['socialInsuranceInfo']['paymentMonth'] = social_insurance_basic_info_df['payment_month_num'].values[0]
            payment_range = (social_insurance_basic_info_df['begin_payment_date'].map(str) + '~' +
                             social_insurance_basic_info_df['begin_payment_date'].map(str))[0]
            self.variables['socialInsuranceInfo']['paymentRange'] = payment_range
        social_insurance_record_sql = """
                                   select max_payment_amount, min_payment_amount from info_social_insurance_payment_record where basic_id = 
                                   (select id from info_social_insurance_payment_basic
                                    where id_card_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                                    order by id desc limit 1) order by id desc limit 1
                        """
        social_insurance_record_df = sql_to_df(sql=social_insurance_record_sql, params={'unique_id_no': idno})
        if social_insurance_record_df.shape[0] > 0:
            payment_base_range = (social_insurance_record_df['min_payment_amount'].map(str) + '~' +
                                  social_insurance_record_df['max_payment_amount'].map(str))[0]
            self.variables['socialInsuranceInfo']['paymentBaseRange'] = payment_base_range

    # 获取个人公安重点评分
    def _info_criminal_score(self, idno):
        sql = """
            select score 
            from info_criminal_case
            where id_card_no = %(unique_id_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df = sql_to_df(sql=sql, params={'unique_id_no': idno})
        if df.shape[0] == 0:
            return
        score = df['score'].to_list()[0]
        try:
            score = float(score)
        except ValueError:
            score = None
        if score is not None:
            if score > 60:
                level = "A"
            elif score == 60:
                level = "B"
            elif score > 20:
                level = "C"
            else:
                level = "D"
            self.variables['owner_criminal_score_level'] = level

    # 不良记录条数和详情
    def _info_bad_behavior_record(self, idno):
        court_sql = """
            select id 
            from info_court 
            where unique_id_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        court_df = sql_to_df(sql=court_sql, params={'unique_id_no': str(idno)})
        if court_df is not None and court_df.shape[0] > 0:
            court_id = court_df['id'].to_list()[0]
            behavior_sql = """
                select name, '罪犯及嫌疑人' as type, case_no as case_no, criminal_reason as detail 
                from info_court_criminal_suspect where court_id = %(court_id)s
                union all 
                select name, '失信老赖' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_deadbeat where court_id = %(court_id)s and execute_status != '已结案'
                union all 
                select name, '限制高消费' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_limit_hignspending where court_id = %(court_id)s
                union all 
                select name, '限制出入境' as type, execute_no as case_no, execute_content as detail 
                from info_court_limited_entry_exit where court_id = %(court_id)s
            """
            df = sql_to_df(sql=behavior_sql, params={'court_id': court_id})
            if df is not None and df.shape[0] > 0:
                self.variables['owner_list_cnt'] = df.shape[0]
                self.variables['owner_list_name'] = df['name'].to_list()
                self.variables['owner_list_type'] = df['type'].to_list()
                self.variables['owner_list_case_no'] = df['case_no'].to_list()
                self.variables['owner_list_detail'] = df['detail'].apply(lambda x: re.sub(r'\s', '', x)).to_list()

    # 获取每个主体的从业年限和主营业年限
    def _info_operation_period(self, idno):
        main_indu = self.full_msg['strategyParam'].get('industry')
        if main_indu is not None:
            temp_idno = self.per_type.get(idno)
            if temp_idno is not None:
                code_str = '"' + '","'.join(temp_idno['idno']) + '"'
                sql = """
                    select * 
                    from info_com_bus_face 
                    where basic_id in (
                        select id 
                        from info_com_bus_basic 
                        where credit_code in (%s) 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    )
                """ % code_str
                df = sql_to_df(sql=sql)
                df = df[pd.notna(df['es_date'])]
                if df.shape[0] == 0:
                    return
                df['industry_phy_code'] = df['industry_phy_code'].fillna('').astype(str)
                df['industry_code'] = df['industry_code'].fillna('').astype(str)
                # 加一个判断，不返回industry_phy_code和industry_code的处理
                if pd.notna(df['industry_phy_code']) and pd.notna(df['industry_code']):
                    df['industry'] = df['industry_phy_code'] + df['industry_code']
                min_es_date = df['es_date'].min()
                self.variables['owner_job_year'] = datetime.datetime.now().year - min_es_date.year
                main_df = df[df['industry'] == main_indu]
                if main_df.shape[0] == 0:
                    return
                temp_min_es_date = main_df['es_date'].min()
                self.variables['owner_major_job_year'] = datetime.datetime.now().year - temp_min_es_date.year

    # 获取对应客户的极光数据
    def _info_jg_v5(self, mobile):
        jg_mapping = {
            "owner_app_traffic": ["APP_HOBY_BUS", "APP_HOBY_TICKET", "APP_HOBY_TRAIN", "APP_HOBY_FLIGHT",
                                  "APP_HOBY_TAXI", "APP_HOBY_SPECIAL_DRIVE", "APP_HOBY_HIGH_BUS",
                                  "APP_HOBY_OTHER_DRIVE", "APP_HOBY_RENT_CAR"],
            "owner_app_hotel": ["APP_HOBY_YOUNG_HOTEL", "APP_HOBY_HOME_HOTEL", "APP_HOBY_CONVERT_HOTEL"],
            "owner_app_finance": ["APP_HOBY_BANK_UNIN", "APP_HOBY_THIRD_PAY", "APP_HOBY_INTERNET_BANK",
                                  "APP_HOBY_FOREIGN_BANK", "APP_HOBY_MIDDLE_BANK", "APP_HOBY_CREDIT_CARD",
                                  "APP_HOBY_CITY_BANK", "APP_HOBY_STATE_BANK"],
            "owner_app_invest": ["APP_HOBY_FUTURES", "APP_HOBY_VIRTUAL_CURRENCY", "APP_HOBY_FOREX",
                                 "APP_HOBY_NOBLE_METAL", "APP_HOBY_FUND", "APP_HOBY_STOCK", "APP_HOBY_ZONGHELICAI"],
            "owner_app_ent": ["APP_HOBY_SPORT_LOTTERY", "APP_HOBY_WELFARE_LOTTERY", "APP_HOBY_DOUBLE_BALL",
                              "APP_HOBY_LOTTERY", "APP_HOBY_FOOTBALL_LOTTERY"],
            "owner_app_live": ["APP_HOBY_SUMMARY_LIVE", "APP_HOBY_SHORT_VIDEO", "APP_HOBY_SOCIAL_LIVE",
                               "APP_HOBY_SUMMARY_VIDEO", "APP_HOBY_SPORTS_VIDEO", "APP_HOBY_GAME_LIVE",
                               "APP_HOBY_SELF_PHOTO", "APP_HOBY_TV_LIVE", "APP_HOBY_CULTURE_LIVE", "APP_HOBY_SHOW_LIVE",
                               "APP_HOBY_SPORTS_LIVE"],
            "owner_app_read": ["APP_HOBY_READ_LISTEN", "APP_HOBY_SUNMMARY_NEWS", "APP_HOBY_WOMEN_HEL_BOOK",
                               "APP_HOBY_ARMY_NEWS", "APP_HOBY_CARTON_BOOK", "APP_HOBY_PHY_NEWS",
                               "APP_HOBY_FAMOUSE_BOOK", "APP_HOBY_FINCAL_NEWS", "APP_HOBY_FUN_NEWS", "APP_HOBY_EDU_MED",
                               "APP_HOBY_KONGFU", "APP_HOBY_TECH_NEWS", "APP_HOBY_LOOK_FOR_MED",
                               "APP_HOBY_ENCOURAGE_BOOK", "APP_HOBY_CAR_INFO_NEWS", "APP_HOBY_HUMERIOUS"],
            "owner_app_game": ["APP_HOBY_CARDS_GAME", "APP_HOBY_SPEED_GAME", "APP_HOBY_ROLE_GAME", "APP_HOBY_NET_GAME",
                               "APP_HOBY_RELAX_GAME", "APP_HOBY_KONGFU_GAME", "APP_HOBY_GAME_VIDEO",
                               "APP_HOBY_TALE_GAME", "APP_HOBY_DIAMONDS_GAME", "APP_HOBY_TRAGEDY_GAME"],
            "owner_app_life": ["APP_HOBY_OUTDOOR", "APP_HOBY_MOVIE", "APP_HOBY_CARTON", "APP_HOBY_BEAUTIFUL",
                               "APP_HOBY_LOSE_WEIGHT", "APP_HOBY_PHY_BOOK", "APP_HOBY_FRESH_SHOPPING", "APP_HOBY_WIFI",
                               "APP_HOBY_CAR_PRO", "APP_HOBY_LIFE_PAY", "APP_HOBY_PET_MARKET", "APP_HOBY_OUT_FOOD",
                               "APP_HOBY_FOOD", "APP_HOBY_PALM_MARKET", "APP_HOBY_WOMEN_HEAL", "APP_HOBY_RECORD",
                               "APP_HOBY_CONCEIVE", "APP_HOBY_SHARE", "APP_HOBY_COOK_BOOK", "APP_HOBY_BUY_RENT_HOUSE",
                               "APP_HOBY_CHINESE_MEDICINE", "APP_HOBY_JOB", "APP_HOBY_HOME_SERVICE", "APP_HOBY_KRAYOK",
                               "APP_HOBY_FAST_SEND"],
            "owner_app_social": ["APP_HOBY_PEOPLE_RESOUSE", "APP_HOBY_MAMA_SOCIAL", "APP_HOBY_HOT_SOCIAL",
                                 "APP_HOBY_MARRY_SOCIAL", "APP_HOBY_CAMPUS_SOCIAL", "APP_HOBY_LOVERS_SOCIAL",
                                 "APP_HOBY_ECY", "APP_HOBY_STRANGER_SOCIAL", "APP_HOBY_ANONYMOUS_SOCIAL",
                                 "APP_HOBY_CITY_SOCIAL", "APP_HOBY_FANS"],
            "owner_app_edu": ["APP_HOBY_FIN", "APP_HOBY_MIDDLE", "APP_HOBY_IT", "APP_HOBY_PRIMARY", "APP_HOBY_BABY",
                              "APP_HOBY_ONLINE_STUDY", "APP_HOBY_FOREIGN", "APP_HOBY_DRIVE", "APP_HOBY_SERVANTS",
                              "APP_HOBY_CHILD_EDU", "APP_HOBY_UNIVERSITY"],
            "owner_app_shop": ["APP_HOBY_CAR_SHOPPING", "APP_HOBY_SECONDHAND_SHOPPING", "APP_HOBY_ZONGHE_SHOPPING",
                               "APP_HOBY_PAYBACK", "APP_HOBY_DISCOUNT_MARKET", "APP_HOBY_BABY_SHOPPING",
                               "APP_HOBY_WOMEN_SHOPPING", "APP_HOBY_REBATE_SHOPPING", "APP_HOBY_GROUP_BUY",
                               "APP_HOBY_GLOBAL_SHOPPING", "APP_HOBY_SHOPPING_GUIDE", "APP_HOBY_SEX_SHOPPING"],
            "owner_app_work": ["APP_HOBY_SMOTE_OFFICE"],
            "owner_app_loan": ["APP_HOBY_CAR_LOAN", "APP_HOBY_DIVIDE_LOAN", "APP_HOBY_CREDIT_CARD_LOAN",
                               "APP_HOBY_CASH_LOAN", "APP_HOBY_HOUSE_LOAN", "APP_HOBY_P2P", "APP_HOBY_LOAN_PLATFORM"],
            "owner_app_loan_car": ["APP_HOBY_CAR_LOAN"],
            "owner_app_loan_installment": ["APP_HOBY_DIVIDE_LOAN"],
            "owner_app_loan_credit": ["APP_HOBY_CREDIT_CARD_LOAN"],
            "owner_app_loan_cash": ["APP_HOBY_CASH_LOAN"],
            "owner_app_loan_house": ["APP_HOBY_HOUSE_LOAN"],
            "owner_app_loan_p2p": ["APP_HOBY_P2P"],
            "owner_app_loan_platform": ["APP_HOBY_LOAN_PLATFORM"]
        }
        mobile = str(mobile)
        sql = """
            select * 
            from info_audience_tag_item
            where audience_tag_id = (
                select id 
                from info_audience_tag 
                where mobile = %(mobile)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                order by id desc limit 1
            )
        """
        jg_df = sql_to_df(sql=sql, params={'mobile': mobile})
        if jg_df.shape[0] == 0:
            return
        cnt = 0
        for k, v in jg_mapping.items():
            temp_df = jg_df[jg_df['field_name'].isin(v)]
            if temp_df.shape[0] == 0:
                continue
            cnt += 1
            temp_df['field_value'] = temp_df['field_value'].apply(
                lambda x: re.search(r'(?<=score=).*?(?=,)', x).group())
            temp_df['field_value'] = temp_df['field_value'].fillna(0).apply(float)
            self.variables[k] = round(temp_df['field_value'].mean() * 100, 1)
        self.variables['owner_app_cnt'] = 1 if cnt > 0 else 0

    def transform(self):
        logger.info("owner_unique_debug start")
        try:
            logger.info("full_msg :%s", json.dumps(self.full_msg))
        except:
            logger.info("full_msg exception")
            logger.info(self.full_msg)
        self.person_list = get_query_data(self.full_msg, 'PERSONAL', '01')
        self.company_list = get_query_data(self.full_msg, 'COMPANY', '01')
        self.per_type = get_all_related_company(self.full_msg)
        id_no = self.id_card_no
        base_type = self.base_type
        name = self.user_name
        phone = self.phone
        if "PERSONAL" in base_type:
            self._indiv_base_info(id_no)
            # self._info_court_id(id_no)
            # self._info_criminal_score(id_no)
            # self._info_bad_behavior_record(id_no)
            self._info_operation_period(id_no)
            # self._info_jg_v5(phone)
            self._one_click_query_info(id_no, name)
