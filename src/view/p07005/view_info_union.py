from mapping.grouped_tranformer import GroupedTransformer, invoke_union
import pandas as pd
import numpy as np
import json
from util.mysql_reader import sql_to_df
# from util.common_util import get_query_data
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


# 判断是否存在指标及指标返回值是否为空
def get_value(df, vari_name):
    temp_df = df.loc[df['variable_name'] == vari_name]
    if temp_df.shape[0] > 0:
        if pd.isna(temp_df['variable_value'].values[0]):
            return None
        else:
            return temp_df['variable_value'].values[0]
    else:
        return None

def get_query_data(msg, query_user_type, query_strategy):
    logger.info("full_msg :%s", json.dumps(msg))

    query_data_list = jsonpath(msg, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        strategy = query_data.get("extraParam")['strategy']
        creditParseReqNo = query_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
        # education = query_data.get("extraParam")['education']
        # mar_status = query_data.get('extraParam')['marryState']
        # priority = query_data.get('extraParam')['priority']
        phone = query_data.get("phone")
        if pd.notna(query_user_type) and user_type == query_user_type and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno, 'phone': phone, 'creditParseReqNo': creditParseReqNo}
            resp.append(resp_dict)
        if pd.isna(query_user_type) and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno,'creditParseReqNo':creditParseReqNo}
            resp.append(resp_dict)
    return resp


class ViewInfoUnion(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_union

    def __init__(self) -> None:
        super().__init__()
        self.per_df = None
        self.com_df = None
        self.variables = {
            "loan_pressure_info": {
                "donut_chart": [
                    {
                        "user_name": "",  # 主体名称
                        "id_card_no": "",  # 主体证件号
                        "unsettled_loan_balance": 0.0,  # 在贷余额
                        "unsettled_loan_balance_rate": 0.0  # 在贷余额占比
                    }
                ],
                "risk_tips": ""  # 专家经验
            },  # 资金压力解析
            "loan_trans_info": {
                "business_loan_type_balance": {
                    "business_loan_type_balance": 0.0,  # 经营性贷款余额
                    "business_loan_type_cnt": 0,  # 经营性贷款在贷笔数
                    "consume_loan_type_balance": 0.0,  # 消费性贷款余额
                    "consume_loan_type_cnt": 0,  # 消费性贷款在贷笔数
                    "mortgage_loan_type_balance": 0.0,  # 住房贷款余额
                    "mortgage_loan_type_cnt": 0,  # 住房贷款在贷笔数
                    "bus_mortgage_loan_type_balance": 0.0,  # 商用住房贷款余额
                    "bus_mortgage_loan_type_cnt": 0,  # 商用住房贷款在贷笔数
                    "risk_tips": ""  # 专家经验
                },  # 贷款类型余额分布
                "business_loan_change": {
                    "trend_chart": [
                        {
                            "annual_year": "",  # 年份
                            "annual_bus_loan_amount": 0.0,  # 经营性借款金额
                            "annual_cousume_loan_amount": 0.0,  # 消费性借款金额
                            "annual_org_cnt": 0  # 机构家数
                        }
                    ],  # 趋势图
                    "risk_tips": ""  # 专家经验
                },  # 近3年贷款放款金额、机构数变化
                "overdue_cnt_info": {
                    "trend_chart": [
                        {
                            "overdue_year": "",  # 本息逾期年份
                            "overdue_cnt": 0  # 本息逾期次数
                        }
                    ],  # 趋势图
                    "risk_tips": ""  # 专家经验
                },  # 近3年本息逾期次数变化
                "normal_loan": {
                    "donut_chart": [
                        {
                            "guarantee_type": "",  # 担保方式
                            "business_loan_guarantee_type_cnt": 0,  # 笔数
                            "guarantee_type_balance": 0.0,  # 余额
                            "guarantee_type_balance_prop": 0.00  # 余额占比
                        }
                    ],  # 环形图
                    "risk_tips": ""  # 专家经验
                }  # 经营性贷款分析
            }  # 贷款交易信息
        }  # 风险详情

    def transform(self):
        self.process()

    def process(self):
        # if self.per_df is not None:
        self.loan_pressure_info_view()
        self.business_loan_type_balance_view()
        self.business_loan_change_view()
        self.overdue_cnt_info_view()
        self.normal_loan_view()

    # 单一征信主体数据查询
    def query_credit_sql(self,creditParseReqNo):
        sql = '''select basic_id,variable_name,variable_value from info_union_credit_data_detail where basic_id = (
                        select id from info_union_credit_data where credit_parse_no = %(credit_parse_no)s 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1) '''
        # creditParseReqNo = self.origin_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
        df = sql_to_df(sql=sql, params={"credit_parse_no": creditParseReqNo})
        if df.shape[0] > 0:
            df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            return df
        else:
            return None

    # 联合征信主体数据查询
    def query_union_credit_sql(self, creditParseReqNo_list):
        sql = '''select basic_id,variable_name,variable_value from info_union_credit_data_detail where basic_id in (
                        select id from info_union_credit_data where credit_parse_no in %(credit_parse_no)s 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at))'''
        # creditParseReqNo = self.origin_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
        df = sql_to_df(sql=sql, params={"credit_parse_no": creditParseReqNo_list})
        if df.shape[0] > 0:
            df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            return df
        else:
            return None

    # 分别查询企业和个人主体征信数据
    def query_credit_info(self):
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        if len(resp) > 0:
            per_df_list = []
            for i in resp:
                user_name = i.get('name')
                id_card_no = i.get('id_card_no')
                credit_df = self.query_credit_sql(user_name, id_card_no)
                if credit_df is not None:
                    per_df_list.append(credit_df)
            if len(per_df_list) > 0:
                per_df = pd.concat(per_df_list)
                self.per_df = per_df

    # 资金压力解析
    def loan_pressure_info_view(self):
        user_l = []
        id_l = []
        unsettled_loan_balance_l = []
        risk_tips = []
        risk_tip1 = []
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        if len(resp) > 0:
            for i in resp:
                user_name = i.get('name')
                id_card_no = i.get('id_card_no')
                creditParseReqNo = i.get('creditParseReqNo')
                credit_df = self.query_credit_sql(creditParseReqNo)
                if credit_df is not None:
                    # 个人在贷机构余额 per_balance
                    per_balance = get_value(credit_df, 'per_balance')
                    if per_balance and len(per_balance) > 0:
                        per_balance = json.loads(per_balance)
                        if len(per_balance) > 0:
                            temp_ = pd.DataFrame()
                            temp_['per_balance'] = per_balance
                            unsettled_loan_balance = round(temp_['per_balance'].astype(float).sum() / 10000, 2)
                            user_l.append(user_name)
                            id_l.append(id_card_no)
                            unsettled_loan_balance_l.append(unsettled_loan_balance)
                            risk_tip1.append(f"{user_name}余额{unsettled_loan_balance:.2f}万元")
                    else:
                        user_l.append(user_name)
                        id_l.append(id_card_no)
                        unsettled_loan_balance_l.append(0.0)
                        risk_tip1.append(f"{user_name}余额0万元")
        # 企业主体
        resp = get_query_data(self.full_msg, 'COMPANY', '01')
        if len(resp) > 0:
            for i in resp:
                user_name = i.get('name')
                id_card_no = i.get('id_card_no')
                creditParseReqNo = i.get('creditParseReqNo')
                credit_df = self.query_credit_sql(creditParseReqNo)
                if credit_df is not None:
                    # 在贷余额 wjqxdye
                    temp_df = credit_df.loc[credit_df.variable_name == 'wjqxdye']
                    if temp_df.shape[0] > 0:
                        user_l.append(user_name)
                        id_l.append(id_card_no)
                        unsettled_loan_balance = round(temp_df['variable_value'].astype(float).sum() / 10000, 2)
                        unsettled_loan_balance_l.append(unsettled_loan_balance)
                        risk_tip1.append(f"{user_name}余额{unsettled_loan_balance:.2f}万元")
        # 组装
        pressure_df = pd.DataFrame.from_dict(
            {'user_name': user_l, 'id_card_no': id_l, 'unsettled_loan_balance': unsettled_loan_balance_l})
        pressure_df['unsettled_loan_balance_rate'] = pressure_df['unsettled_loan_balance'] / pressure_df[
            'unsettled_loan_balance'].sum() if pressure_df['unsettled_loan_balance'].sum() > 0 else 0.0
        # 专家经验
        if len(risk_tip1) > 0:
            risk_tips.append('，'.join(risk_tip1))
        if pressure_df.shape[0] > 0:
            temp_df1 = pressure_df.loc[pressure_df.unsettled_loan_balance_rate > 0.5]
            if temp_df1.shape[0] > 0:
                user_list = temp_df1.user_name.tolist()
                risk_tips.append(
                    f"在贷余额集中于{'/'.join(user_list)}，{'/'.join(user_list)}为该借款人融资主体，需关注该主体贷款到期后的续贷情况")
            temp_df2 = pressure_df.loc[pressure_df.unsettled_loan_balance_rate < 0.5]
            if temp_df2.shape[0] == pressure_df.shape[0]:
                risk_tips.append(f"该借款人在贷余额分布均匀")
        self.variables['loan_pressure_info']['donut_chart'] = pressure_df.to_dict('records')
        if len(risk_tips) > 0:
            self.variables['loan_pressure_info']['risk_tips'] = ';'.join(risk_tips)

    # 贷款交易信息-贷款类型余额分布
    def business_loan_type_balance_view(self):
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        creditParseReqNo_list = []
        if len(resp) > 0:
            for i in resp:
                creditParseReqNo_list.append(i.get('creditParseReqNo'))
            risk_tips = []
            temp_dict = dict()
            df = self.query_union_credit_sql(creditParseReqNo_list)
            if df is not None:
                # 判断省联社指标是否返回
                temp_df = df.loc[df.variable_name == 'business_loan_type_balance']
                if temp_df.shape[0] > 0:
                    # 总贷款余额
                    type_list = ['business_loan_type_balance', 'consume_loan_type_balance', 'mortgage_loan_type_balance',
                                 'bus_mortgage_loan_type_balance']
                    total_loan_balance = \
                        df.loc[df.variable_name.isin(type_list)].variable_value.astype(float).sum() / 10000
                    # 经营性贷款余额、经营性贷款在贷笔数
                    business_balance = df.loc[df['variable_name'] == 'business_loan_type_balance'].variable_value.astype(float).sum()
                    temp_dict['business_loan_type_balance'] = round(business_balance / 10000, 2)
                    business_cnt = int(df.loc[df['variable_name'] == 'business_loan_type_cnt'].variable_value.astype(float).sum())
                    temp_dict['business_loan_type_cnt'] = business_cnt
                    # 消费性贷款余额、消费性贷款在贷笔数
                    consume_balance = df.loc[df['variable_name'] == 'consume_loan_type_balance'].variable_value.astype(float).sum()
                    temp_dict['consume_loan_type_balance'] = round(consume_balance / 10000, 2)
                    consume_cnt = int(df.loc[df['variable_name'] == 'consume_loan_type_cnt'].variable_value.astype(float).sum())
                    temp_dict['consume_loan_type_cnt'] = consume_cnt
                    # 住房贷款余额、住房贷款在贷笔数
                    mortgage_balance = df.loc[df['variable_name'] == 'mortgage_loan_type_balance'].variable_value.astype(float).sum()
                    temp_dict['mortgage_loan_type_balance'] = round(mortgage_balance / 10000, 2)
                    mortgage_cnt = int(df.loc[df['variable_name'] == 'mortgage_loan_type_cnt'].variable_value.astype(float).sum())
                    temp_dict['mortgage_loan_type_cnt'] = mortgage_cnt
                    # 商用住房贷款余额、商用住房贷款在贷笔数
                    bus_mortgage_balance = df.loc[df['variable_name'] == 'bus_mortgage_loan_type_balance'].variable_value.astype(float).sum()
                    temp_dict['bus_mortgage_loan_type_balance'] = round(bus_mortgage_balance / 10000, 2)
                    bus_mortgage_cnt = int(df.loc[df['variable_name'] == 'bus_mortgage_loan_type_cnt'].variable_value.astype(float).sum())
                    temp_dict['bus_mortgage_loan_type_cnt'] = bus_mortgage_cnt
                    if total_loan_balance > 0 and consume_balance / total_loan_balance > 0.5:
                        risk_tips.append(f"消费性贷款较多，资金调动能力弱，资金紧张程度高")
                    if business_cnt > 10:
                        risk_tips.append(f"经营性贷款笔数较多，建议谨慎授信")
                    temp_dict['risk_tips'] = ';'.join(risk_tips)

                    self.variables['loan_trans_info']['business_loan_type_balance'].update(temp_dict)

    # 近3年贷款放款金额、机构数变化
    def business_loan_change_view(self):
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        creditParseReqNo_list = []
        if len(resp) > 0:
            for i in resp:
                creditParseReqNo_list.append(i.get('creditParseReqNo'))
            risk_tips = []
            df = self.query_union_credit_sql(creditParseReqNo_list)
            if df is not None:
                temp_df = df.loc[df.variable_name == 'annual_year']
                if temp_df.shape[0] > 0:
                    df1 = pd.DataFrame()
                    # 根据主体循环
                    for i in df.basic_id.unique().tolist():
                        temp_df1 = pd.DataFrame()
                        # 年份
                        annual_year = df.loc[(df.basic_id == i) & (df.variable_name == 'annual_year')].variable_value.values[0]
                        # 解析json字符串
                        annual_year = json.loads(annual_year)
                        temp_df1['annual_year'] = annual_year
                        # 经营性借款金额
                        annual_bus_loan_amount = \
                            df.loc[(df.basic_id == i) & (df.variable_name == 'annual_bus_loan_amount')].variable_value.values[0]
                        # 解析json字符串
                        annual_bus_loan_amount = json.loads(annual_bus_loan_amount)
                        temp_df1['annual_bus_loan_amount'] = annual_bus_loan_amount
                        # 消费性借款金额
                        annual_cousume_loan_amount = \
                            df.loc[(df.basic_id == i) & (df.variable_name == 'annual_cousume_loan_amount')].variable_value.values[0]
                        # 解析json字符串
                        annual_cousume_loan_amount = json.loads(annual_cousume_loan_amount)
                        temp_df1['annual_cousume_loan_amount'] = annual_cousume_loan_amount
                        # 机构家数
                        annual_org_cnt = \
                            df.loc[(df.basic_id == i) & (df.variable_name == 'annual_org_cnt')].variable_value.values[0]
                        # 解析json字符串
                        annual_org_cnt = json.loads(annual_org_cnt)
                        temp_df1['annual_org_cnt'] = annual_org_cnt
                        df1 = df1.append(temp_df1)
                    # 转换类型
                    col_list = ['annual_bus_loan_amount', 'annual_cousume_loan_amount', 'annual_org_cnt']
                    for col in col_list:
                        df1[col] = df1[col].apply(lambda x:float(x) if x != '' else 0)
                    # 金额转为万元，保留两位小数
                    df1['annual_bus_loan_amount'] = (df1['annual_bus_loan_amount'] / 10000).round(2)
                    df1['annual_cousume_loan_amount'] = (df1['annual_cousume_loan_amount'] / 10000).round(2)
                    df1 = df1.groupby('annual_year')[
                        'annual_bus_loan_amount', 'annual_cousume_loan_amount', 'annual_org_cnt'].sum()
                    df1['total_loan_amount'] = df1['annual_bus_loan_amount'] + df1['annual_cousume_loan_amount']
                    df1.reset_index(inplace=True)
                    # 贷款总额是否持续增长
                    if df1.total_loan_amount.is_monotonic_decreasing:
                        risk_tips.append(
                            f"贷款金额持续上升，关注经营规模变化：（1）若快速扩张，关注经营风险；（2）若规模缩小或者不变，核实是否存在应收账款回收难、库存积压、经营亏损或者对外投资情况")
                    # 近一年比上一年
                    if df1.shape[0] > 1 and df1.total_loan_amount.values[-2] > 0:
                        if df1.total_loan_amount.values[-1] / df1.total_loan_amount.values[-2] > 1.5:
                            risk_tips.append(
                                f"近1年贷款放款金额较上一年有大幅上涨，关注经营规模是否有快速扩张或者有对外投资情况")
                    self.variables['loan_trans_info']['business_loan_change']['trend_chart'] = df1.to_dict('records')
                    self.variables['loan_trans_info']['business_loan_change']['risk_tips'] = ';'.join(risk_tips)

    # 近3年本息逾期次数变化
    def overdue_cnt_info_view(self):
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        creditParseReqNo_list = []
        if len(resp) > 0:
            for i in resp:
                creditParseReqNo_list.append(i.get('creditParseReqNo'))
            df = self.query_union_credit_sql(creditParseReqNo_list)
            if df is not None:
                overdue_df = pd.DataFrame()
                risk_tips = []
                month_list = ["近36-24个月", "近24-12个月", "近12-6个月", "近6个月"]
                rel_dict = {
                    "sixmoverduenum": "近6个月",
                    "oneyoverduenum": "近12-6个月",
                    "towyoverduenum": "近24-12个月",
                    "threeyoverduenum": "近36-24个月"
                }
                for i in df.basic_id.unique().tolist():
                    df_i = df.loc[df.basic_id == i]
                    type_list, overdue_cnt_list = [], []
                    for rel in rel_dict:
                        type_list.append(rel_dict[rel])
                        overdue_cnt_list.append(df_i.loc[df_i.variable_name == rel].variable_value.values[0])
                    temp_df = pd.DataFrame.from_dict({'overdue_year': type_list, 'overdue_cnt': overdue_cnt_list})
                    overdue_df = overdue_df.append(temp_df)
                # 转换类型
                overdue_df['overdue_cnt'] = overdue_df['overdue_cnt'].apply(lambda x:int(x) if x != '' else 0)
                overdue_df = overdue_df.groupby('overdue_year')['overdue_cnt'].sum()
                # 按指定月份排序
                overdue_df = overdue_df.loc[month_list].reset_index()
                # 专家经验
                risk_df1 = overdue_df.head(3)
                if risk_df1.overdue_cnt.is_monotonic_increasing:
                    risk_tips.append(f"该客户近三年逾期情况持续变差，建议谨慎授信")
                # 近6个月逾期次数，近12个月逾期次数
                overdue_cnt_6m = overdue_df.overdue_cnt.values[-1]
                overdue_cnt_12m = overdue_df.overdue_cnt.values[-2]
                if overdue_cnt_12m and overdue_cnt_6m / overdue_cnt_12m > 0.5:
                    risk_tips.append(f"该客户近半年逾期情况存在变差的情况，建议谨慎授信")
                self.variables['loan_trans_info']['overdue_cnt_info']['trend_chart'] = overdue_df.to_dict('records')
                self.variables['loan_trans_info']['overdue_cnt_info']['risk_tips'] = ';'.join(risk_tips)

    # 经营性贷款分析
    def normal_loan_view(self):
        # 个人主体
        resp = get_query_data(self.full_msg, 'PERSONAL', '01')
        creditParseReqNo_list = []
        if len(resp) > 0:
            for i in resp:
                creditParseReqNo_list.append(i.get('creditParseReqNo'))
            df = self.query_union_credit_sql(creditParseReqNo_list)
            risk_tips = []
            if df is not None:
                temp_df = df.loc[df.variable_name == 'guarantee_type']
                if temp_df.shape[0] > 0:
                    df1 = pd.DataFrame()
                    # 根据主体循环
                    for i in df.basic_id.unique().tolist():
                        temp_df1 = pd.DataFrame()
                        # 担保方式
                        type = df.loc[(df.basic_id == i) & (df.variable_name == 'guarantee_type')].variable_value.values[0]
                        type = json.loads(type)
                        temp_df1['type'] = type
                        # 笔数
                        cnt = df.loc[(df.basic_id == i) &
                                     (df.variable_name == 'business_loan_guarantee_type_cnt')].variable_value.values[0]
                        cnt = json.loads(cnt)
                        # 新增兼容，若笔数list和类型list长度不一致，补全
                        if len(cnt) != len(type):
                            diff = len(type) - len(cnt)
                            cnt.extend(['0' for i in range(0, diff)])
                        temp_df1['cnt'] = cnt
                        # 余额
                        balance = df.loc[(df.basic_id == i) &
                                         (df.variable_name == 'guarantee_type_balance')].variable_value.values[0]
                        balance = json.loads(balance)
                        # 新增兼容，若余额list和类型list长度不一致，补全
                        if len(balance) != len(type):
                            diff = len(type) - len(balance)
                            balance.extend(['0.0' for i in range(0, diff)])
                        temp_df1['balance'] = balance
                        # 余额占比
                        # prop = df.loc[(df.id == i) & (df.variable_name == 'guarantee_type_balance_prop')].variable_value.values[0]
                        # prop = json.loads(prop)
                        # temp_df1['prop'] = prop
                        df1 = df1.append(temp_df1)
                    # 数据类型转换
                    df1['balance'] = df1['balance'].apply(lambda x:float(x) if x != '' else 0)
                    df1['cnt'] = df1['cnt'].apply(lambda x:int(x) if x != '' else 0)
                    # 余额转为万元，保留2位小数
                    df1['balance'] = (df1['balance'] / 10000).round(2)
                    df1 = df1.groupby('type')['balance', 'cnt'].sum()
                    df1.reset_index(inplace=True)
                    df1['porp'] = df1.balance / df1.balance.sum()
                    # 总经营性贷款
                    total_balance = df1.balance.sum()
                    if total_balance > 0:
                        # 信用保证类余额占比
                        credit_guar_balance = df1.loc[df1.type == '信用保证类'].balance.sum()
                        guar_balance_porp = credit_guar_balance / total_balance
                        if guar_balance_porp > 0.5:
                            risk_tips.append(
                                f"信用保证类贷款余额占比{guar_balance_porp:.2%}，主要贷款余额集中在信用保证类，在金融机构的融资保障能力尚可，但存在被金融机构抽贷的风险")
                        # 抵押组合类余额占比
                        mort_balance = df1.loc[df1.type == '抵押组合类'].balance.sum()
                        mort_balance_porp = mort_balance / total_balance
                        if mort_balance_porp > 0.5:
                            risk_tips.append(
                                f"抵押组合类贷款余额占比{mort_balance_porp:.2%}，主要贷款余额集中在抵押组合类，续贷率存在一定保障，需关注抵押物所有权非借款主体的贷款到期后续贷情况")
                        # 质押
                        pledge_cnt = df1.loc[df1.type == '质押'].cnt.sum()
                        if pledge_cnt > 0:
                            risk_tips.append(f"存在质押贷款，请核实质押物")

                    df1.columns = ['guarantee_type', 'guarantee_type_balance', 'business_loan_guarantee_type_cnt',
                                   'guarantee_type_balance_prop']
                    self.variables['loan_trans_info']['normal_loan']['donut_chart'] = df1.to_dict('records')
                    self.variables['loan_trans_info']['normal_loan']['risk_tips'] = ';'.join(risk_tips)
