import datetime

import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from util.common_util import get_industry_risk_level, get_industry_risk_tips, get_query_data


class BasicUnique(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "basic"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'basic_share_ent_name': [],  # 企业名称
            'basic_share_holder_name': [],  # 股东名称
            'basic_share_holder_type': [],  # 股东类型
            'basic_share_sub_conam': [],  # 认缴出资额（万元）
            'basic_share_ratio': [],
            'basic_share_quantity': [],
            'basic_share_funded_ratio': [],  # 认缴出资比例
            'basic_share_con_date': [],  # 认缴出资日期
            'basic_share_con_form': [],
            'basic_share_holder_cnt': 0,  # 新增：企业股东及出资信息条数
            'basic_ex_ent_name': '',
            'basic_ex_industry_phyname': '',
            'basic_ex_ent_status': '',
            'basic_ex_reg_cap': '',
            'basic_ex_open_date_range': '',
            'basic_ent_name': '',  # 企业名称
            'basic_fr_name': '',
            'basic_es_date': '',
            'basic_appr_date': '',
            'basic_industry_phyname': '',
            'basic_address': '',
            'basic_operate_scope': '',
            'basic_ent_type': '',
            'basic_credit_code': '',
            'basic_reg_cap': '',
            'basic_ent_status': '',
            'basic_open_date_range': '',
            'basic_enc_cnt': 0,  # 新增：工商基本信息条数
            'bus_industry_industry': '',  # 新增：主营行业
            'bus_industry_hint': '',  # 新增：风险提示
            'bus_industry_grade': '',  # 新增：风险评级
            'bus_industry_cnt': 0,  # 新增：行业经营风险信息条数
            # 20230922 新增
            # 企业分支机构信息
            # 信息综览-分支机构信息条数
            'branch_info_cnt': 0,
            'branchInfo': [
                # 分支机构信息
                # {
                #     'companyName': '',  # 企业名称
                #     'branchName': '',  # 分支机构名称
                #     'branchRegNo': '',  # 分支机构注册号
                #     'branchAddress': '',  # 分支机构地址
                #     'branchCharge': '',  # 分支结构负责人
                #     'establishDate': ''  # 成立日期
                # }
            ],
            # 企业主要人员信息
            # 信息综览-主要人员信息条数
            'company_personal_cnt': 0,
            'companyPersonInfo': [
                # 企业主要人员信息
                # {
                #     'companyName': '',  # 企业名称
                #     'name': '',  # 名称
                #     'sex': '',  # 性别 男|女
                #     'duties': '',  # 职务
                #     'dutyDateRange': ''  # 任职起始日期
                # }
            ],
            # 企业黑名单信息
            # 信息综览-企业黑名单信息条数
            'black_info_cnt': 0,
            'blackInfo': [
                # {
                #     'black_company_name': '',  # 企业名称
                #     'black_info': ''  # 企业命中黑名单信息，string类型
                # }
            ]
        }

    def _get_industry_name(self, industry):
        sql = '''
            select cn_name from tree_dic where opt_type="STD_GB_4754-2017" and en_name=%(industry)s order by id desc limit 1
        '''
        df = sql_to_df(sql=sql, params={"industry": industry})
        if not df.empty:
            return df.loc[0, 'cn_name']
        else:
            return ''

    def _info_com_bus_basic(self):
        sql = '''SELECT id, ent_name FROM info_com_bus_basic WHERE ent_name=%(user_name)s'''
        if pd.notna(self.id_card_no):
            sql += ' and credit_code = %(id_card_no)s'
        sql += ''' and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1 '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def _info_com_bus_shareholder(self, user_name, id_card_no):
        """
        info_com_bus_shareholder 工商核查-股东出资信息
        info_com_bus_basic 企业工商-基础信息表
        """
        sql = '''
                SELECT a.ent_name,b.share_holder_name,b.share_holder_type,b.sub_conam,b.funded_ratio,b.con_date,b.con_form
                FROM info_com_bus_shareholder b, info_com_bus_basic a
                where a.id = (
                    SELECT id FROM info_com_bus_basic where ent_name = %(user_name)s 
                    and credit_code = %(id_card_no)s 
                    AND unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1
                )
                and b.basic_id = a.id
                and b.funded_ratio is not null
           '''
        df = sql_to_df(sql=sql,
                       params={"user_name": user_name,
                               "id_card_no": id_card_no})
        df['con_date'] = df['con_date'].apply(
            lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
        return df

    def _info_com_bus_face(self, id):
        """
        info_com_bus_face 工商核查-照面信息
        :param id:
        :return:
        """
        sql = '''
            select * from info_com_bus_face where basic_id = %(id)s
        '''
        df = sql_to_df(sql=sql,
                       params={"id": id})
        return df

    def clean_variables_face(self, basic_df):
        id = basic_df.loc[0, 'id']
        face_df = self._info_com_bus_face(int(id))
        if face_df.empty:
            return
        self.variables['basic_ex_industry_phyname'] = face_df.loc[0, 'industry_name']
        self.variables['basic_ex_ent_status'] = face_df.loc[0, 'ent_status']
        self.variables['basic_ex_reg_cap'] = face_df.loc[0, 'reg_cap']
        open_from = "" if pd.isna(face_df.loc[0, 'open_from']) else datetime.datetime.strftime(
            face_df.loc[0, 'open_from'], "%Y-%m-%d, %H:%M:%S")
        open_to = "" if pd.isna(face_df.loc[0, 'open_to']) else datetime.datetime.strftime(face_df.loc[0, 'open_to'],
                                                                                           "%Y-%m-%d, %H:%M:%S")
        self.variables['basic_ex_open_date_range'] = datetime.datetime.now().year - int(open_from[:4]) \
            if open_from != '' else 0
        if self.origin_data.get("extraParam").get("strategy") == '02':
            return
        self.variables['basic_fr_name'] = face_df.loc[0, 'fr_name']
        self.variables['basic_es_date'] = "" if pd.isna(face_df.loc[0, 'es_date']) else datetime.datetime.strftime(
            face_df.loc[0, 'es_date'], "%Y-%m-%d")
        self.variables['basic_appr_date'] = "" if pd.isna(face_df.loc[0, 'appr_date']) else datetime.datetime.strftime(
            face_df.loc[0, 'appr_date'], "%Y-%m-%d")
        self.variables['basic_industry_phyname'] = face_df.loc[0, 'industry_name']
        self.variables['basic_address'] = face_df.loc[0, 'address']
        self.variables['basic_operate_scope'] = face_df.loc[0, 'operate_scope']
        self.variables['basic_ent_type'] = face_df.loc[0, 'ent_type']
        self.variables['basic_credit_code'] = self.id_card_no
        self.variables['basic_reg_cap'] = face_df.loc[0, 'reg_cap']
        self.variables['basic_ent_status'] = face_df.loc[0, 'ent_status']
        open_from = "" if pd.isna(face_df.loc[0, 'open_from']) else datetime.datetime.strftime(
            face_df.loc[0, 'open_from'], "%Y-%m-%d")
        open_to = "" if pd.isna(face_df.loc[0, 'open_to']) else datetime.datetime.strftime(
            face_df.loc[0, 'open_to'], "%Y-%m-%d")
        self.variables['basic_open_date_range'] = open_from + "至" + open_to
        self.variables['basic_enc_cnt'] = 1

    # 分支机构信息
    def branch_info(self, basic_df):
        """
        分支机构信息
        :param basic_df:
        :return:
        """
        id = basic_df.loc[0, 'id']
        ent_name = basic_df['ent_name'].values[0]
        branch_sql = """
            select br_name, brreg_no, braddr, brprincipal, es_date from info_com_bus_filiation where basic_id = %(id)s
        """
        branch_df = sql_to_df(sql=branch_sql, params={"id": str(id)})
        if branch_df.shape[0] > 0:
            self.variables['branch_info_cnt'] = branch_df.shape[0]
            branch_df['companyName'] = ent_name
            rename_col = ['branchName', 'branchRegNo', 'branchAddress', 'branchCharge', 'establishDate', 'companyName']
            branch_df.columns = rename_col
            self.variables['branchInfo'] = branch_df.to_dict(orient='records')

    def company_personal_info(self, basic_df):
        """
        企业主要人员信息
        :param basic_df:
        :return:
        """
        id = basic_df.loc[0, 'id']
        ent_name = basic_df['ent_name'].values[0]
        company_personal_sql = """
            select person_id, sex, position, offh_from from info_com_bus_senior where basic_id = %(id)s
        """
        company_personal_df = sql_to_df(sql=company_personal_sql, params={"id": str(id)})
        if company_personal_df.shape[0] > 0:
            self.variables['company_personal_cnt'] = company_personal_df.shape[0]
            company_personal_df['companyName'] = ent_name
            rename_col = ['name', 'sex', 'duties', 'dutyDateRange', 'companyName']
            company_personal_df.columns = rename_col
            self.variables['companyPersonInfo'] = company_personal_df.to_dict(orient='records')

    def black_info(self):
        """
        企业黑名单信息
        :param basic_df:
        :return:
        """
        # 行内黑名单 取version最大的
        black_sql = """
                        select id_card_no from info_black_list where 
                        version = (select max(version) from info_black_list)
                        and id_card_no = %(unique_id_no)s
                """
        black_df = sql_to_df(sql=black_sql, params={'unique_id_no': self.id_card_no})
        if black_df.shape[0] > 0:
            temp_dict = {}
            temp_dict['black_company_name'] = self.user_name
            temp_dict['black_info'] = '命中行内黑名单'
            self.variables['black_info_cnt'] = 1
            self.variables['blackInfo'] = [temp_dict]

    def clean_variables_shareholder(self):
        resp = get_query_data(self.full_msg, 'COMPANY', '01')
        df = None
        for i in resp:
            user_name = i.get("name")
            id_card_no = i.get("id_card_no")
            priority = i.get("priority")
            df_shareholder = self._info_com_bus_shareholder(user_name, id_card_no)
            df_shareholder['priority'] = priority
            if not df_shareholder.empty and df is not None:
                df = pd.concat([df, df_shareholder])
            if df is None and not df_shareholder.empty:
                df = df_shareholder
        if df is None:
            return
        df = df.sort_values(by=['priority', 'ent_name', 'funded_ratio'], ascending=False)
        self.variables['basic_share_ent_name'] = df['ent_name'].to_list()
        self.variables['basic_share_holder_name'] = df['share_holder_name'].to_list()
        self.variables['basic_share_holder_type'] = df['share_holder_type'].to_list()
        self.variables['basic_share_sub_conam'] = df['sub_conam'].to_list()
        self.variables['basic_share_funded_ratio'] = df['funded_ratio'].to_list()
        self.variables['basic_share_con_date'] = df['con_date'].to_list()
        self.variables['basic_share_con_form'] = df['con_form'].to_list()
        self.variables['basic_share_holder_cnt'] = df.shape[0]

    def transform(self):
        self.variables['basic_ex_ent_name'] = self.user_name
        self.variables['basic_ent_name'] = self.user_name
        # industry = self.full_msg.get('strategyParam').get('industry') if self.full_msg.get(
        #     'strategyParam') is not None else ''
        # if industry is not None:
        #     self.variables['bus_industry_industry'] = self._get_industry_name(industry)
        #     self.variables['bus_industry_grade'] = get_industry_risk_level(industry)
        #     self.variables['bus_industry_hint'] = get_industry_risk_tips(industry)
        #     self.variables['bus_industry_cnt'] = len(self.variables['bus_industry_hint'])
        basic_df = self._info_com_bus_basic()
        if basic_df.empty:
            return
        # 修改行业取值为从数据库取值
        basic_id = basic_df.loc[0, 'id']
        df = self._info_com_bus_face(int(basic_id))
        if not df.empty:
            df['industry_phy_code_concat'] = df['industry_phy_code'].map(lambda x:str(x) if pd.notna(x) else '') + \
                                             df['industry_code'].map(lambda x:str(x) if pd.notna(x) else '')
            self.variables['bus_industry_ent_name'] = df.loc[0, 'ent_name']
            self.variables['bus_industry_industry'] = df.loc[0, 'industry_name']
            # df['industry_code_1'] = df.apply(lambda x: (x['industry_phy_code'] + x['industry_code'])[:4] if len(
            #     x['industry_phy_code'] + x['industry_code']) >= 4 else x['industry_phy_code'] + x['industry_code'],
            #                                axis=1)
            if pd.notna(df.loc[0, 'industry_phy_code_concat']):
                self.variables['bus_industry_grade'] = get_industry_risk_level(df.loc[0, 'industry_phy_code_concat'])
                self.variables['bus_industry_hint'] = get_industry_risk_tips(df.loc[0, 'industry_phy_code_concat'])
                self.variables['bus_industry_cnt'] = len(self.variables['bus_industry_hint'])
        self.clean_variables_face(basic_df)
        self.clean_variables_shareholder()
        """分支机构信息"""
        self.branch_info(basic_df)
        """主要人员信息"""
        self.company_personal_info(basic_df)
        """行内黑名单"""
        self.black_info()
