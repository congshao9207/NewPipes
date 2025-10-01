import jsonpath
import pandas as pd
from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.common_util import get_industry_risk_level, get_industry_risk_tips
from util.mysql_reader import sql_to_df


def trans_black_froz_time(begin, end):
    if pd.isna(begin) and pd.isna(end):
        return "/"
    elif pd.isna(begin) and pd.notna(end):
        return "/" + "至" + end.strftime('%Y-%m-%d')
    elif pd.notna(begin) and pd.isna(end):
        return begin.strftime('%Y-%m-%d') + "至" + "/"
    else:
        return begin.strftime('%Y-%m-%d') + "至" + end.strftime('%Y-%m-%d')


class BusUnique(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "bus"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "bus_industry_ent_name": "",
            "bus_industry_industry": "",
            "bus_industry_grade": "",
            "bus_industry_hint": [],
            'bus_industry_cnt': 0,
            'bus_abnomal_cnt': 0,
            'bus_change_record_cnt': 0,
            'bus_invest_cnt': 0,
            'bus_abnormal_name': [],
            'bus_abnormal_cause': [],
            'bus_abnormal_date': [],
            'bus_abnormal_org': [],
            'bus_abnormal_clear_cause': [],
            'bus_abnormal_clear_date': [],
            'bus_abnormal_clear_org': [],
            'bus_change_name': [],
            'bus_change_category': [],
            'bus_change_date': [],
            'bus_change_content_before': [],
            'bus_change_content_after': [],
            'bus_invest_name': [],
            'bus_invest_code': [],
            'bus_invest_legal_rep': [],
            'bus_invest_regist': [],
            'bus_invest_type': [],
            'bus_invest_capital': [],
            'bus_invest_status': [],
            'bus_invest_date': [],
            'bus_invest_com_cnt': [],
            'bus_invest_proportion': [],
            'bus_invest_form': [],
            'bus_invest_sub_conam': [],  # 新增：对外投资认缴出资额（万元）
            'black_froz_name': [],  # 股权冻结相关企业名称
            'black_froz_role': [],
            'black_froz_status': [],  # 股权冻结执行状态
            'black_froz_execute_no': [],
            'black_froz_amt': [],  # 股权冻结金额
            'black_froz_inv': [],
            'black_froz_auth': [],  # 股权冻结机关
            'black_froz_public_date': [],
            'black_froz_time': [],
            'black_froz_thaw_date': [],
            'black_froz_invalid_date': [],
            'black_froz_invalid_reason': [],  # 股权冻结原因
            'black_froz_date_from': [],  # 新增：冻结起始日期
            'black_froz_date_to': [],  # 新增：冻结截止日期
            'black_froz_prop': [],  # 新增：股权冻结比例
            'black_froz_cancel_auth': [],  # 新增：股权冻结解冻机关
            'black_froz_cancel_date': [],  # 新增：股权冻结解冻日期
            'black_froz_mark': [],  # 新增：股权冻结标志
            'black_froz_cancel_detail': [],  # 新增：股权冻结解冻说明
            'black_froz_num': 0,  # 股权冻结信息条数
            'black_froz_cnt': 0,
            'fin_mort_cnt': 0,
            'fin_impawn_cnt': 0,
            'fin_alt_cnt': 0,
            'fin_multi_cnt': 0,
            'fin_mort_name': [],
            'fin_mort_to_name': [],
            'fin_mort_reg_no': [],
            'fin_mort_reg_date': [],
            'fin_mort_status': [],
            'fin_mort_reg_org': [],
            'fin_mab_guar_amt': [],
            'fin_mab_guar_type': [],
            'fin_pef_date_range': [],
            'fin_gua_name': [],
            'fin_gua_own': [],
            'fin_gua_des': [],
            'fin_cancle_date': [],
            'fin_impawn_name': [],
            'fin_impawn_role': [],
            'fin_impawn_equity_no': [],
            'fin_impawn_pled_gor': [],
            'fin_impawn_am': [],
            'fin_impawn_org': [],
            'fin_impawn_state': [],
            'fin_impawn_equple_date': [],
            'fin_impawn_pub_date': [],
            'fin_impawn_filing_date': [],  # 新增：质押备案日期
            'fin_impawn_approval_org': [],  # 新增：质押审批部门
            'fin_impawn_approval_date': [],  # 新增：质押批准日期
            'fin_impawn_date_to': [],  # 新增: 质押截至日期
            'fin_alt_name': [],
            'fin_alt_item': [],
            'fin_alt_date': [],
            'fin_alt_be': [],
            'fin_alt_af': [],
            'fin_mort_cert_code': [],  # 新增：抵押人主体身份代码
            'fin_mort_object_name': [],  # 新增：抵押物名称
            'fin_mort_object_detail': [],  # 新增：抵押物描述
            'fin_mort_interest': [],  # 新增：抵押利息情况
            'fin_mort_date_from': [],  # 新增：履约起始日期
            'fin_mort_date_to': []  # 新增：履约截止日期
        }

    def _info_com_bus_face(self):
        sql = '''
            select ent_name,industry_phy_code,industry_code,industry_name from info_com_bus_face where basic_id = (
                select id from info_com_bus_basic where ent_name = %(ent_name)s and credit_code = %(credit_code)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"ent_name": self.user_name,
                               "credit_code": self.id_card_no})
        return df

    def _load_info_com_bus_basic_id(self):
        if self.id_card_no != 0:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and credit_code = %(credit_code)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1;
            """
            df = sql_to_df(sql=sql,
                           params={"ent_name": self.user_name,
                                   "credit_code": self.id_card_no})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['id'].iloc[0])
        else:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": self.user_name})
            if df is not None and len(df) > 0:
                return int(df['id'].iloc[0])
        return None

    # 读取 info_com_bus_exception 企业工商-经营异常数据
    def _load_info_com_bus_exception_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.result_in,a.date_in,a.org_name_in,a.result_out,a.date_out,a.org_name_out
               FROM info_com_bus_exception as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s;
        """
        info_com_bus_exception_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_exception_df is not None and len(info_com_bus_exception_df) > 0:
            return info_com_bus_exception_df
        return None

    # 读取 info_com_bus_alter 工商核查-企业变更信息数据
    def _load_info_com_bus_alter_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.alt_item,a.alt_date,a.alt_be,a.alt_af
               FROM info_com_bus_alter as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s and alt_be != alt_af ;
        """
        info_com_bus_alter_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_alter_df is not None and len(info_com_bus_alter_df) > 0:
            return info_com_bus_alter_df
        return None

    # 读取 info_com_bus_entinvitem 工商核查-企业对外投资信息数据
    def _load_info_com_bus_entinvitem_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.credit_code,a.fr_name,a.reg_no,a.ent_type,a.reg_cap,a.ent_status,a.es_date,a.pinv_amount,a.funded_ratio,a.con_form,a.sub_conam
               FROM info_com_bus_entinvitem as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s;
        """
        info_com_bus_entinvitem_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_entinvitem_df is not None and len(info_com_bus_entinvitem_df) > 0:
            return info_com_bus_entinvitem_df
        return None

    # 读取股权冻结数据
    def _info_com_bus_shares_frost(self, ids):
        """info_com_bus_shares_frost 企业工商-股权冻结信息"""
        sql = '''
                   select * from info_com_bus_shares_frost where basic_id = %(ids)s order by froz_ent,froz_public_date DESC
              '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    # 读取 info_com_bus_mort_basic 企业工商-动产抵押-基本信息数据
    def _load_info_com_bus_mort_basic_df(self, id):
        sql = """
               SELECT a.*,b.*
               FROM info_com_bus_mort_basic as a left join info_mortgage as b on a.basic_id = b.basic_id 
               WHERE a.basic_id = %(id)s and a.jhi_role = '抵押人';
        """
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 读取 info_com_bus_mort_registe 企业工商-动产抵押-登记信息数据
    def _load_info_com_bus_mort_registe_df(self, id):
        sql = '''
            select mort_id,mort_reg_no,mab_guar_amt,mab_guar_type,pef_per_from,pef_per_to,mortgagor as mort_gager,
             reg_date,status as mort_status,reg_org,mortgagot_id as mort_cer_no,interest,principal_credit from info_com_bus_mort_registe where mort_id = %(id)s 
        '''
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 读取 info_com_bus_mort_collateral 数据
    def _load_info_com_bus_mort_collateral_df(self, id):
        sql = '''
            select mort_id,mort_reg_no,gua_name,gua_own,gua_des from info_com_bus_mort_collateral where mort_id = %(id)s;
        '''
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 读取 info_com_bus_mort_cancel 数据
    def _load_info_com_bus_mort_cancel_df(self, id):
        sql = '''
            select mort_id,can_date,mort_reg_no from info_com_bus_mort_cancel where mort_id = %(id)s;
        '''
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 读取 info_com_bus_mort_holder 数据
    def _load_info_com_bus_mort_holder_df(self, id):
        sql = '''
            select mort_id,mort_reg_no,mort_org from info_com_bus_mort_holder where mort_id = %(id)s;
        '''
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 读取 info_com_bus_shares_impawn 企业工商-股权出质信息数据
    def _load_info_com_bus_shares_impawn_df(self, id):
        sql = """
               SELECT *
               FROM info_com_bus_shares_impawn
               WHERE basic_id = %(id)s and imp_exe_state = '有效';
        """
        df = sql_to_df(sql=sql, params={"id": id})
        return df

    # 动产抵押信息
    def _fin_mort(self, df=None):
        if df is not None and len(df) > 0:
            # df = df.drop_duplicates().sort_values(by=['mort_gager', 'reg_date'], ascending=False)
            # self.variables['fin_mort_cnt'] += len(df)
            self.variables['fin_mort_name'] += df['mort_gager'].to_list()
            self.variables['fin_mort_cert_code'] += df['mort_cer_no'].tolist()
            self.variables['fin_mort_reg_no'] += df['mort_reg_no'].iloc[:, 0].to_list()
            self.variables['fin_mort_reg_date'] += df['reg_date'].map(
                lambda x: x.strftime('%Y-%m-%d') if x != "-" and pd.notna(x) else "-").to_list()
            self.variables['fin_mort_status'] += df['mort_status'].to_list()
            self.variables['fin_mort_reg_org'] += df['reg_org'].to_list()
            self.variables['fin_mort_object_name'] += df['gua_name'].tolist()
            self.variables['fin_mort_object_detail'] += df['gua_des'].tolist()
            self.variables['fin_mort_interest'] += df['interest'].tolist()
            self.variables['fin_mort_date_from'] += df['pef_per_from'].map(
                lambda x: x.strftime('%Y-%m-%d') if x != "-" and pd.notna(x) else "-").to_list()
            self.variables['fin_mort_date_to'] += df['pef_per_to'].map(
                lambda x: x.strftime('%Y-%m-%d') if x != "-" and pd.notna(x) else "-").to_list()
            self.variables['fin_mab_guar_amt'] += df['principal_credit'].tolist()

    # 股权出质
    def _fin_impawn(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['pl_edge_ent', 'imp_pub_date'], ascending=False)
            self.variables['fin_impawn_cnt'] += len(df)
            # self.variables['fin_impawn_name'] += df['pl_edge_ent'].to_list()
            self.variables['fin_impawn_name'] += df['imp_org'].tolist()
            # self.variables['fin_impawn_role'] += df['jhi_role'].to_list()
            self.variables['fin_impawn_role'] += df['imp_org_type'].tolist()
            self.variables['fin_impawn_equity_no'] += df['imp_equity_no'].to_list()
            self.variables['fin_impawn_pled_gor'] += df['imp_pled_gor'].to_list()
            self.variables['fin_impawn_am'] += df['imp_am'].to_list()
            self.variables['fin_impawn_org'] += df['imp_org'].to_list()
            self.variables['fin_impawn_state'] += df['imp_exe_state'].to_list()
            self.variables['fin_impawn_equple_date'] += df['imp_equple_date'].map(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['fin_impawn_pub_date'] += df['imp_pub_date'].map(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['fin_impawn_filing_date'] += df['imp_equple_date'].map(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['fin_impawn_approval_org'] += df['imp_approval_dep'].tolist()
            self.variables['fin_impawn_approval_date'] += df['imp_san_date'].map(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).tolist()
            self.variables['fin_impawn_date_to'] += df['imp_to'].map(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).tolist()

    # 计算 fin_alt 相关字段
    # 股权变动信息
    def _fin_alt(self, df=None):
        if df is None:
            return

        # target_fileds = '股权和公证书|股权转让信息|' \
        #                 '投资人\(股权\)变更|' \
        #                 '投资人（股权内部转让）备案|' \
        #                 '投资人（股权）变更|投资人（股权）备案'
        target_fileds = '股东变更|股权和公证书|股权转让信息|' \
                        '负责人变更|投资人\(股权\)变更|投资人信息变更|' \
                        '投资人及出资信息|投资人变更|投资人（股权内部转让）备案|' \
                        '投资人（股权）变更|投资人（股权）备案|投资总额变更|' \
                        '投资人\(包括出资额、出资方式、出资日期、投资人名称等\)|股东\(投资人\)'
        df_temp = df[df['alt_item'].str.contains(target_fileds)]
        if df_temp.shape[0] == 0:
            return
        df_temp = df_temp.sort_values(by=['ent_name', 'alt_date'], ascending=False)
        df_temp['alt_date'] = df_temp['alt_date'].apply(
            lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
        self.variables['fin_alt_cnt'] = df_temp.shape[0]
        self.variables['fin_alt_name'] = df_temp['ent_name'].to_list()
        self.variables['fin_alt_item'] = df_temp['alt_item'].to_list()
        self.variables['fin_alt_date'] = df_temp['alt_date'].to_list()
        self.variables['fin_alt_be'] = df_temp['alt_be'].to_list()
        self.variables['fin_alt_af'] = df_temp['alt_af'].to_list()

    # 计算股权冻结指标
    def _bus_frost(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['black_froz_cnt'] = df.shape[0]
            self.variables['black_froz_num'] = df.shape[0]
            df['black_froz_time'] = df.apply(lambda x: trans_black_froz_time(x['froz_from'], x['froz_to']), axis=1)
            df['froz_public_date'] = df['froz_public_date'].apply(lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            df['thaw_date'] = df['thaw_date'].apply(lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            df['invalid_time'] = df['invalid_time'].apply(lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            self.variables['black_froz_name'] += df['froz_ent'].to_list()
            self.variables['black_froz_role'] += df['jhi_role'].to_list()
            self.variables['black_froz_status'] += df['judicial_froz_state'].to_list()
            self.variables['black_froz_execute_no'] += df['froz_doc_no'].to_list()
            self.variables['black_froz_amt'] += df['judicial_fro_am'].to_list()
            self.variables['black_froz_inv'] += df['judicial_inv'].to_list()
            self.variables['black_froz_auth'] += df['froz_auth'].to_list()
            self.variables['black_froz_public_date'] += df['froz_public_date'].to_list()
            self.variables['black_froz_time'] += df['black_froz_time'].to_list()
            self.variables['black_froz_thaw_date'] += df['thaw_date'].to_list()
            self.variables['black_froz_invalid_date'] += df['invalid_time'].to_list()
            self.variables['black_froz_invalid_reason'] += df['invalid_reason'].to_list()
            # 股权冻结起止日期、股权冻结比例、股权冻结解冻机关、股权冻结解冻日期、股权冻结标志、股权冻结解冻说明
            self.variables['black_froz_date_from'] += df['froz_from'].tolist()
            self.variables['black_froz_date_to'] += df['froz_to'].tolist()
            self.variables['black_froz_prop'] += df['share_froz_prop'].tolist()
            self.variables['black_froz_cancel_auth'] += df['thaw_aut'].tolist()
            self.variables['black_froz_cancel_date'] += df['thaw_date'].tolist()
            self.variables['black_froz_mark'] += df['froz_sign'].tolist()
            self.variables['black_froz_cancel_detail'] += df['invalid_reason'].tolist()

    # 计算 bus_abnormal 相关字段
    # 经营异常
    def _bus_abnormal(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['ent_name', 'date_in'])
            self.variables['bus_abnomal_cnt'] += len(df)
            self.variables['bus_abnormal_name'] += df['ent_name'].to_list()
            self.variables['bus_abnormal_cause'] += df['result_in'].to_list()
            self.variables['bus_abnormal_date'] += df['date_in'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_abnormal_org'] += df['org_name_in'].to_list()
            self.variables['bus_abnormal_clear_cause'] += df['result_out'].to_list()
            self.variables['bus_abnormal_clear_date'] += df['date_out'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_abnormal_clear_org'] += df['org_name_out'].to_list()

    # 计算 bus_change 相关字段
    # 企业变更，不包含股权变动
    def _bus_change(self, df=None):
        if df is not None and len(df) > 0:
            target_fileds = '股东变更|股权和公证书|股权转让信息|' \
                            '负责人变更|投资人\(股权\)变更|投资人信息变更|' \
                            '投资人及出资信息|投资人变更|投资人（股权内部转让）备案|' \
                            '投资人（股权）变更|投资人（股权）备案|投资总额变更|' \
                            '投资人\(包括出资额、出资方式、出资日期、投资人名称等\)|股东\(投资人\)'
            # target_fileds = '股东变更|' \
            #                 '负责人变更|投资人变更|投资人信息变更|' \
            #                 '投资人及出资信息|投资人变更|' \
            #                 '投资总额变更'
            df = df[(~df['alt_item'].str.contains(target_fileds)) & pd.notna(df.alt_item)]
            if len(df) > 0:
                df = df.drop_duplicates().sort_values(by=['alt_date'], ascending=False)
                self.variables['bus_change_record_cnt'] += len(df)
                self.variables['bus_change_name'] += df['ent_name'].to_list()
                self.variables['bus_change_category'] += df['alt_item'].to_list()
                self.variables['bus_change_date'] += df['alt_date'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
                self.variables['bus_change_content_before'] += df['alt_be'].to_list()
                self.variables['bus_change_content_after'] += df['alt_af'].to_list()

    # 计算 bus_invest 相关字段
    # 对外投资
    def _bus_invest(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['ent_name', 'credit_code', 'es_date']).sort_values(by=['credit_code'])
            self.variables['bus_invest_cnt'] += len(df)
            self.variables['bus_invest_name'] += df['ent_name'].to_list()
            self.variables['bus_invest_code'] += df['credit_code'].to_list()
            self.variables['bus_invest_legal_rep'] += df['fr_name'].to_list()
            self.variables['bus_invest_regist'] += df['reg_no'].to_list()
            self.variables['bus_invest_type'] += df['ent_type'].to_list()
            self.variables['bus_invest_capital'] += df['reg_cap'].to_list()
            self.variables['bus_invest_status'] += df['ent_status'].to_list()
            self.variables['bus_invest_date'] += df['es_date'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_invest_com_cnt'] += df['pinv_amount'].to_list()
            self.variables['bus_invest_proportion'] += df['funded_ratio'].to_list()
            self.variables['bus_invest_form'] += df['con_form'].to_list()
            # 认缴出资额（元）
            self.variables['bus_invest_sub_conam'] += df['sub_conam'].tolist()

    def _get_industry_name(self, industry):
        sql = '''
            select cn_name from tree_dic where opt_type="STD_GB_4754-2017" and en_name=%(industry)s order by id desc limit 1
        '''
        df = sql_to_df(sql=sql, params={"industry": industry})
        if not df.empty:
            return df.loc[0, 'cn_name']
        else:
            return ''

    def clean_variables(self):
        df = self._info_com_bus_face()
        if not df.empty:
            self.variables['bus_industry_ent_name'] = df.loc[0, 'ent_name']
            self.variables['bus_industry_industry'] = df.loc[0, 'industry_name']
            # df['industry_code_1'] = df.apply(lambda x: (x['industry_phy_code'] + x['industry_code'])[:4] if len(
            #     x['industry_phy_code'] + x['industry_code']) >= 4 else x['industry_phy_code'] + x['industry_code'],
            #                                axis=1)
            if pd.notna(df.loc[0, 'industry_phy_code']):
                self.variables['bus_industry_grade'] = get_industry_risk_level(df.loc[0, 'industry_phy_code'])
                self.variables['bus_industry_hint'] = get_industry_risk_tips(df.loc[0, 'industry_phy_code'])

    def transform(self):
        strategy = self.origin_data.get("extraParam")['strategy']
        # if 'COMPANY' in self.base_type and strategy == "01":
        #     self.clean_variables()

        industry = self.full_msg.get('strategyParam').get('industry') if self.full_msg.get(
            'strategyParam') is not None else ''
        if "PERSONAL" not in self.base_type.upper() and strategy == '01':
            com_id = self._load_info_com_bus_basic_id()

            df = self._load_info_com_bus_exception_df(com_id)
            self._bus_abnormal(df)

            df = self._load_info_com_bus_alter_df(com_id)
            # 企业变更，不包含股权变更
            self._bus_change(df)
            # 股权变动
            self._fin_alt(df)

            df = self._load_info_com_bus_entinvitem_df(com_id)
            self._bus_invest(df)

            df = self._info_com_bus_shares_frost(com_id)
            if not df.empty and df is not None:
                self._bus_frost(df)

            # 股权出质
            df = self._load_info_com_bus_shares_impawn_df(com_id)
            self._fin_impawn(df)

            # 动产抵押
            # df1 = self._load_info_com_bus_mort_basic_df(com_id)
            # if not df1.empty:
                # df1 = df1.drop_duplicates().sort_values(by=['mort_gager', 'reg_date'], ascending=False)
                # df1['reg_date'] = df1['reg_date'].map(
                #     lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()

            df2 = self._load_info_com_bus_mort_registe_df(com_id)
            df3 = self._load_info_com_bus_mort_collateral_df(com_id)
            df4 = self._load_info_com_bus_mort_cancel_df(com_id)
            df5 = self._load_info_com_bus_mort_holder_df(com_id)

            # df_temp1 = pd.merge(df1, df2, how="left", left_on="id", right_on="mort_id")
            # df_temp2 = pd.merge(df1, df3, how="left", left_on="id", right_on="mort_id")
            # df_temp2 = df_temp2.drop(columns=['mort_gager', 'reg_date', 'mort_status', 'reg_org'])
            # df_temp3 = pd.merge(df1, df4, how="left", left_on="id", right_on="mort_id")
            # df_temp3 = df_temp3.drop(columns=['mort_gager', 'reg_date', 'mort_status', 'reg_org'])
            # df_temp4 = pd.merge(df1, df5, how="left", left_on="id", right_on="mort_id")
            # df_temp4 = df_temp4.drop(columns=['mort_gager', 'reg_date', 'mort_status', 'reg_org'])
            # df_final = pd.merge(df_temp1, df_temp2, how="outer", on=["mort_id", "mort_reg_no_x"])
            # df_final = pd.merge(df_final, df_temp3, how="outer", on=["mort_id", "mort_reg_no_x"])
            # df_final = pd.merge(df_final, df_temp4, how="outer", on=["mort_id", "mort_reg_no_x"])
            dfs = [df2, df3, df4, df5]
            df_final = pd.concat(dfs, axis=1, join='inner')
            if not df_final.empty:
                df_final = df_final.fillna("-")
                df_final = df_final.sort_values(by=['mort_gager', 'reg_date'], ascending=False)
                self.variables['fin_mort_cnt'] += len(df_final)
                self._fin_mort(df_final)

        # if industry is not None:
        #     self.variables['bus_industry_industry'] = self._get_industry_name(industry)
        #     # self.variables['bus_industry_grade'] = get_industry_risk_level(industry)
        #     # self.variables['bus_industry_hint'] = get_industry_risk_tips(industry)
        #     self.variables['bus_industry_cnt'] = len(self.variables['bus_industry_hint'])
