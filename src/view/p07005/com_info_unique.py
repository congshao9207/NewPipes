from mapping.grouped_tranformer import GroupedTransformer, invoke_each_company
import pandas as pd
import numpy as np
import json
import time

from util.mysql_reader import sql_to_df
import datetime


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


# 时间戳转换
def transform_timetamp(strValue):
    # 时间戳转换，精确到秒
    intValue = int(strValue[0:10])
    timeValue = time.localtime(intValue)
    tempDate = time.strftime("%Y-%m-%d %H:%M:%S", timeValue)
    return tempDate


# 按揭房产时间戳转换
def transform_house_timetamp(strValue):
    # 时间戳转换，精确到秒
    intValue = int(strValue[0:10])
    timeValue = time.localtime(intValue)
    tempDate = time.strftime("%Y-%m-%d", timeValue)
    return tempDate


class ComInfoUnique(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each_company

    def __init__(self) -> None:
        super().__init__()
        self.df = None
        # 在贷余额默认0
        self.unsettled_loan_balance = 0.0
        self.variables = {
            "marriage_status": 1,  # 公共变量
            "head_info": {
                "user_name": "",  # 姓名
                "idno": "",  # 证件号
                "unsettled_loan_balance": 0,  # 在贷余额
                "report_no": "",  # 征信报告编号
                "report_time": "",  # 征信报告时间
                "if_no_credit_record": 0  # 是否白户，1是0否
            },  # 报告头变量
            "credit_result": {
                "base_info": "",  # 基本信息
                "loan_liability_info": "",  # 贷款负债情况
                "loan_fulfillment_info": "",  # 贷款履约情况
                "credit_card_liability_info": "",  # 贷记卡负债情况
                "credit_card_fulfillment_info": "",  # 贷记卡履约情况
                "related_repayment_info": "",  # 相关还款责任信息
                "query_records_info": "",  # 查询记录信息
                "public_info": ""  # 公共信息记录
            },  # 征信解析结果
            "suggestions": {
                "drawdown_risk": "",  # 抽贷及压贷风险
                "house_analysis": "",  # 房产解析
                "guarantee_analysis": "",  # 担保方式解析
                "loan_institutions": "",  # 融资机构变化
                "loan_fulfillment_risks": "",  # 贷款履约风险
                "credit_card_risks": "",  # 贷记卡风险
                "related_repayment_risks": "",  # 相关还款责任风险
                "query_records_risks": "",  # 查询记录风险
                "legal_risks": ""  # 法律风险
            },  # 风控要点与建议
            "risk_detail": {
                "loan_pressure_info": [
                    {
                        "per_org_type": "",  # 个人在贷机构类型
                        "per_balance": 0.0,  # 个人在贷机构余额
                        "per_balance_prop": 0.00  # 个人在贷机构占比
                    }
                ],  # 资金压力解析
                "credit_card_info": {
                    "id": "",  # 序号
                    "card_org": "",  # 发卡机构
                    "user_status": "",  # 用户状态
                    "credit_loan_amount": 0.0,  # 授信额度
                    "credit_used": 0.0,  # 已使用额度
                    "credit_avg_used_6m": 0.0,  # 最近6个月平均使用额度
                    "credit_usage_rate": 0.00,  # 使用率
                    "credit_min_payed_number": 0,  # 最低还款张数
                    "risk_tips": ""
                }  # 贷记卡信息
            }  # 风险详情
        }  # 所有展示变量

    def transform(self):
        strategy = self.origin_data.get("extraParam")['strategy']
        if 'COMPANY' in self.user_type and strategy == "01":
            self.query_credit_info()
            if self.df is not None:
                self.get_head_info()
                self.get_credit_result()
                # self.loan_pressure_info_view()

    # 企业征信数据查询
    def query_credit_info(self):
        sql = '''select basic_id,variable_name,variable_value from info_union_credit_data_detail where basic_id = (
                                        select id from info_union_credit_data where credit_parse_no = %(credit_parse_no)s 
                                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1) '''
        creditParseReqNo = self.origin_data.get('extraParam')['passthroughMsg']['creditParseReqNo']
        df = sql_to_df(sql=sql, params={"credit_parse_no": creditParseReqNo})
        if df.shape[0] > 0:
            df.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
            self.df = df

    # 报告头变量
    def get_head_info(self):
        df = self.df.copy()
        # 姓名
        self.variables['head_info']["user_name"] = self.user_name
        # 证件号
        self.variables['head_info']["idno"] = self.id_card_no
        # 在贷余额 wjqxdye
        if get_value(df, 'wjqxdye'):
            self.unsettled_loan_balance = round(float(get_value(df, 'wjqxdye')) / 10000, 2)
        self.variables['head_info']['unsettled_loan_balance'] = self.unsettled_loan_balance
        # 判断报告编号和报告时间指标是否返回
        if get_value(df, 'report_no'):
            # 报告编号
            self.variables['head_info']["report_no"] = get_value(df, 'report_no')
            # 报告时间
            self.variables['head_info']["report_time"] = transform_timetamp(get_value(df, 'report_time'))
        # 是否白户
        if get_value(df, 'if_no_credit_record'):
            # 是否白户
            self.variables['head_info']['if_no_credit_record'] = int(get_value(df, 'if_no_credit_record'))

    # 征信解析结果
    def get_credit_result(self):
        df = self.df.copy()
        """贷款负债情况"""
        # 在贷余额
        unsettled_loan_balance = self.unsettled_loan_balance
        if unsettled_loan_balance > 0:
            loan_liability_info_tips = f"{self.user_name}贷款在贷余额{unsettled_loan_balance:.2f}万元"
            self.variables['credit_result']['loan_liability_info'] = loan_liability_info_tips
        else:
            self.variables['credit_result'][
                'loan_liability_info'] = f"{self.user_name}贷款在贷余额{unsettled_loan_balance:.0f}万元"

        """贷款履约情况"""
        loan_fulfillment_info_tips = []
        loan_fulfillment_info_tip1 = []
        loan_fulfillment_info_tip2 = []
        # 已结清不良类信贷笔数
        settle_loan_bl_cnt = get_value(df, 'yjqblxdbs')
        if settle_loan_bl_cnt and int(settle_loan_bl_cnt) > 0:
            loan_fulfillment_info_tip1.append(f"{int(settle_loan_bl_cnt)}笔不良类贷款")
        # 已结清关注类信贷笔数
        settle_loan_gz_cnt = get_value(df, 'yjqgzlxdbs')
        if settle_loan_gz_cnt and int(settle_loan_gz_cnt) > 0:
            loan_fulfillment_info_tip1.append(f"{int(settle_loan_gz_cnt)}笔关注类贷款")
        if len(loan_fulfillment_info_tip1) > 0:
            loan_fulfillment_info_tips.append(f"已结清信贷中存在{'、'.join(loan_fulfillment_info_tip1)}")
        # 未结清不良类信贷部署
        unsettled_loan_bl_cnt = get_value(df, 'wjqblxdbs')
        if unsettled_loan_bl_cnt and int(unsettled_loan_bl_cnt) > 0:
            loan_fulfillment_info_tip2.append(f"{int(unsettled_loan_bl_cnt)}笔不良类贷款")
        # 未结清关注类信贷笔数
        unsettled_loan_gz_cnt = get_value(df, 'wjqgzlxdbs')
        if unsettled_loan_gz_cnt and int(unsettled_loan_gz_cnt) > 0:
            loan_fulfillment_info_tip2.append(f"{int(unsettled_loan_gz_cnt)}笔关注类贷款")
        if len(loan_fulfillment_info_tip2) > 0:
            loan_fulfillment_info_tips.append(f"未结清信贷中存在{'、'.join(loan_fulfillment_info_tip2)}")
        if len(loan_fulfillment_info_tips) > 0:
            self.variables['credit_result'][
                'loan_fulfillment_info'] = f"{self.user_name}{'，'.join(loan_fulfillment_info_tips)}"

        """相关还款责任信息"""
        related_repayment_info_tips = []
        # 对外担保笔数
        com_guar_loan_cnt = get_value(df, 'dwdbbs')
        if com_guar_loan_cnt:
            related_repayment_info_tips.append(f"对外担保{int(com_guar_loan_cnt)}笔")
        # 对外担保余额
        com_guar_loan_balance = get_value(df, 'dwdbye')
        if com_guar_loan_balance:
            related_repayment_info_tips.append(f"对外担保余额{float(com_guar_loan_balance) / 10000:.2f}万元")
        # 对外担保非正常类余额
        com_guar_loan_abnormal_balance = get_value(df, 'dwdbfzclye')
        if com_guar_loan_abnormal_balance:
            related_repayment_info_tips.append(f"对外担保非正常类余额{float(com_guar_loan_abnormal_balance)/10000:.2f}万元")
        if len(related_repayment_info_tips) > 0:
            self.variables['credit_result']['related_repayment_info'] = \
                f"{self.user_name}，{'，'.join(related_repayment_info_tips)}"

    # 风控要点与建议
    def get_suggestions(self):
        df = self.df.copy()
        """抽贷及压贷风险"""
        # 近12个月放款总额
        total_loan_amount_latest_year = get_value(df, 'total_loan_amount_latest_year')
        # 上一年放款总额
        total_loan_amount_last_year = get_value(df, 'total_loan_amount_last_year')
        if total_loan_amount_latest_year and total_loan_amount_last_year:
            if float(total_loan_amount_latest_year) < float(total_loan_amount_last_year):
                diff = (float(total_loan_amount_last_year) - float(total_loan_amount_latest_year)) / 10000
                self.variables['suggestions']['drawdown_risk'] = \
                    f"{self.user_name}，近12个月贷款净增加总额为负{diff:.2f}万，融资能力变弱，疑似机构抽贷或者压贷"

        """房产解析"""
        house_analysis_tips = []
        # 个人房产数
        live_address_type = get_value(df, 'live_address_type')
        if live_address_type and int(live_address_type) > 0:
            house_analysis_tips.append(f"{self.user_name}可能存在固定资产，资产保障性较好")
        # 房贷按揭未结清 json形式的list
        mort_no_settle_loan_date = get_value(df, 'mort_no_settle_loan_date')
        if mort_no_settle_loan_date and len(mort_no_settle_loan_date) > 0:
            # 解析json字符串
            mort_no_settle_loan_date = sorted(json.loads(mort_no_settle_loan_date))
            if len(mort_no_settle_loan_date) > 0:
                mort_no_settle_loan_date = [transform_house_timetamp(str(i)) for i in mort_no_settle_loan_date]
                house_analysis_tips.append(f"{self.user_name}存在{'、'.join(mort_no_settle_loan_date)}开始的房贷未结清")
        self.variables['suggestions']['house_analysis'] = ';'.join(house_analysis_tips)

        """担保方式解析"""
        # 担保方式余额占比
        guarantee_type_balance_prop = get_value(df, 'guarantee_type_balance_prop')
        if guarantee_type_balance_prop and len(guarantee_type_balance_prop) > 0:
            guarantee_type_balance_prop = json.loads(guarantee_type_balance_prop)
            if len(guarantee_type_balance_prop) > 0:
                # 新增判定
                guarantee_type = get_value(df, 'guarantee_type')
                guarantee_type = json.loads(guarantee_type)
                # 新增兼容，若担保余额占比列长度与担保类型长度不一致，将担保余额占比列默认填充‘0.0’
                if len(guarantee_type_balance_prop) != len(guarantee_type):
                    diff = len(guarantee_type) - len(guarantee_type_balance_prop)
                    guarantee_type_balance_prop.extend('0.0' for i in range(0, diff))
                guarantee_analysis_tips = []
                if '抵押组合类' in guarantee_type:
                    diya = float(guarantee_type_balance_prop[guarantee_type.index('抵押组合类')])
                    if diya > 0.5:
                        guarantee_analysis_tips.append(
                            f"{self.user_name}抵押组合类贷款余额占比{diya:.2%}，主要贷款余额集中在抵押组合类，续贷率存在一定保障，需关注抵押物所有权非借款主体的贷款到期后续贷情况")
                if '信用保证类' in guarantee_type:
                    xinyong = float(guarantee_type_balance_prop[guarantee_type.index('信用保证类')])
                    if xinyong > 0.5:
                        guarantee_analysis_tips.append(
                            f"{self.user_name}信用保证类贷款余额占比{xinyong:.2%}，主要贷款余额集中在信用保证类，在金融机构的融资保障能力尚可，但存在被金融机构抽贷的风险")
                if '质押' in guarantee_type:
                    zhiya = float(guarantee_type_balance_prop[guarantee_type.index('质押')])
                    if zhiya > 0:
                        guarantee_analysis_tips.append(f"{self.user_name}存在质押贷款，请核实质押物")
                if '其他' in guarantee_type:
                    others = float(guarantee_type_balance_prop[guarantee_type.index('其他')])
                if len(guarantee_analysis_tips) > 0:
                    self.variables['suggestions']['guarantee_analysis'] = ';'.join(guarantee_analysis_tips)

        """融资机构变化"""
        loan_institutions_tips = []
        # 近一年净增机构家数
        org_change_cnt = get_value(df, 'org_change_cnt')
        # 近一年贷款总额 total_loan_amount_latest_year
        # 上一年贷款总额 total_loan_amount_last_year
        if total_loan_amount_latest_year and total_loan_amount_last_year and float(total_loan_amount_last_year) != 0:
            div = float(total_loan_amount_latest_year) / float(total_loan_amount_last_year)
            if org_change_cnt and int(org_change_cnt) >= 3 and 0.9 <= div <= 1.1:
                loan_institutions_tips.append(
                    f"{self.user_name}近12个月净增加机构较多且贷款总额无明显变化，负债结构趋向融资小额化")
            elif div >= 1.5:
                loan_institutions_tips.append(
                    f"张三近12个月贷款总额较上一年增加{div:.2%}，需核实净增加贷款的资金用途")
            if len(loan_institutions_tips) > 0:
                self.variables['suggestions']['loan_institutions'] = ';'.join(loan_institutions_tips)

        """贷款履约风险"""
        loan_fulfillment_risks_tips = []
        # 经营性贷款2年内最大连续逾期期数
        large_loan_2year_overdue_cnt = get_value(df, 'large_loan_2year_overdue_cnt')
        # 总计贷款2年内逾期次数
        loan_overdue_2year_total_cnt = get_value(df, 'loan_overdue_2year_total_cnt')
        if large_loan_2year_overdue_cnt and loan_overdue_2year_total_cnt and int(
                large_loan_2year_overdue_cnt) < 2 and int(loan_overdue_2year_total_cnt) <= 6:
            loan_fulfillment_risks_tips.append(f"{self.user_name}近2年有信贷交易记录且履约信用良好")
        if large_loan_2year_overdue_cnt and int(large_loan_2year_overdue_cnt) >= 2:
            loan_fulfillment_risks_tips.append(
                f"{self.user_name}经营性贷款2年内存在连续{int(large_loan_2year_overdue_cnt)}期逾期，履约意识较差")
        if len(loan_fulfillment_risks_tips) > 0:
            self.variables['suggestions']['loan_fulfillment_risks'] = ';'.join(loan_fulfillment_risks_tips)

        """贷记卡风险"""
        # "credit_card_risks": "",  #
        credit_card_risks_tips = []
        # 贷记卡账户状态异常账户数
        credit_status_abnormal_cnt = get_value(df, 'credit_status_abnormal_cnt')
        if credit_status_abnormal_cnt and int(credit_status_abnormal_cnt) > 0:
            credit_card_risks_tips.append(f"{self.user_name}贷记卡账户状态存在非正常，建议谨慎授信")
        # 使用率
        credit_usage_rate = get_value(df, 'total_credit_usage_rate')
        if credit_usage_rate and float(credit_usage_rate) >= 0.8:
            credit_card_risks_tips.append(f"{self.user_name}贷记卡透支率较高，资金调动能力较大")
        # 最低还款张数
        credit_min_payed_number = get_value(df, 'credit_min_payed_number')
        if credit_min_payed_number and int(credit_min_payed_number) > 2:
            credit_card_risks_tips.append(f"{self.user_name}贷记卡最低还款张数较多，资金紧张程度较高")
        if len(credit_card_risks_tips) > 0:
            self.variables['suggestions']['credit_card_risks'] = ';'.join(credit_card_risks_tips)

        """相关还款责任风险"""
        related_repayment_risks_tips = []
        related_repayment_risks_tip1 = []
        # 对外担保五级分类存在“关注”笔数
        loan_scured_five_b_level_abnormality_cnt = get_value(df, 'loan_scured_five_b_level_abnormality_cnt')
        if loan_scured_five_b_level_abnormality_cnt and int(loan_scured_five_b_level_abnormality_cnt) > 0:
            related_repayment_risks_tip1.append('关注')
        # 对外担保五级分类存在“次级、可疑、损失”笔数
        loan_scured_five_a_level_abnormality_cnt = get_value(df, 'loan_scured_five_a_level_abnormality_cnt')
        if loan_scured_five_a_level_abnormality_cnt and int(loan_scured_five_a_level_abnormality_cnt) > 0:
            related_repayment_risks_tips.append('不良')
        if len(related_repayment_risks_tip1) > 0:
            related_repayment_risks_tips.append(
                {f"{self.user_name}对外担保五级分类存在{'/'.join(related_repayment_risks_tip1)}，建议谨慎授信"})
        # 对外担保金额
        guar_loan_amount = get_value(df, 'foreignassuremoney')
        if guar_loan_amount and float(guar_loan_amount) >= 500 * 10000:
            related_repayment_risks_tips.append(f"{self.user_name}对外担保金额过大，建议谨慎授信")
        elif guar_loan_amount and 100 * 10000 <= float(guar_loan_amount) < 500 * 10000:
            # 对外担保笔数
            guar_loan_cnt = get_value(df, 'foreignassurenum')
            if guar_loan_cnt and int(guar_loan_cnt) >= 3:
                related_repayment_risks_tips.append(f"{self.user_name}对外担保金额过大且笔数较多，建议谨慎授信")
        # 近2个月内将到期担保余额
        loan_balance_due_soon = get_value(df, 'loan_balance_due_soon')
        if loan_balance_due_soon and float(loan_balance_due_soon) > 0:
            related_repayment_risks_tips.append(f"{self.user_name}近期存在对外担保将到期，有代偿风险")
        if len(related_repayment_risks_tips) > 0:
            self.variables['suggestions']['related_repayment_risks'] = ';'.join(related_repayment_risks_tips)

        """查询记录风险"""
        query_records_risks_tips = []
        # 近3个月资信审查笔数
        credit_review_query_cnt = get_value(df, 'credit_review_query_cnt')
        if credit_review_query_cnt and int(credit_review_query_cnt) > 0:
            query_records_risks_tips.append(
                f"{self.user_name}近3个月有{int(credit_review_query_cnt)}条资信审查记录，关联企业名下可能存在未核实的负债")
        # 近3个月保前审查笔数
        guar_query_cnt = get_value(df, 'guar_query_cnt')
        if guar_query_cnt and int(guar_query_cnt) > 0:
            query_records_risks_tips.append(
                f"{self.user_name}近3个月有{int(guar_query_cnt)}条保前审查记录，可能存在担保增信类的负债，该类负债融资成本普遍较高")
        # 近3个月审批查询机构家数
        loan_credit_query_3month_cnt = get_value(df, 'loan_credit_query_3month_cnt')
        if loan_credit_query_3month_cnt and int(loan_credit_query_3month_cnt) > 5:
            query_records_risks_tips.append(f"{self.user_name}近3个月审批查询机构数较多，资金紧张程度较高")
        # 近3个月贷款放款机构家数
        query_loan_approved_3m_org_cnt = get_value(df, 'query_loan_approved_3m_org_cnt')
        if loan_credit_query_3month_cnt and int(loan_credit_query_3month_cnt) > 3 and \
                query_loan_approved_3m_org_cnt and int(query_loan_approved_3m_org_cnt) == 0:
            query_records_risks_tips.append(
                f"{self.user_name}近3个月贷款审批查询机构数较多，且均未放款，资金紧张程度高，融资能力较差，风险较高")
        # 近一个月内个人查询次数
        onemonthquerynum = get_value(df, 'onemonthquerynum')
        if onemonthquerynum and int(onemonthquerynum) > 3:
            query_records_risks_tips.append(f"{self.user_name}近1个月本人查询次数较多，隐形负债风险较大")
        if len(query_records_risks_tips) > 0:
            self.variables['suggestions']['query_records_risks'] = ';'.join(query_records_risks_tips)

        """法律风险"""
        legal_risks_tips = []
        # 欠税信息条数
        owing_tax_cnt = get_value(df, 'qsnum')
        if owing_tax_cnt and int(owing_tax_cnt) > 0:
            legal_risks_tips.append(f"{self.user_name}存在欠税记录{int(owing_tax_cnt)}条，存在信用履约风险")
        # 民事判决条数
        civil_judge_cnt = get_value(df, 'mspjnum')
        if civil_judge_cnt and int(civil_judge_cnt) > 0:
            legal_risks_tips.append(f"{self.user_name}存在民事判决记录{int(civil_judge_cnt)}条，建议谨慎授信")
        # 强制执行条数
        force_execution_cnt = get_value(df, 'qzzxnum')
        if force_execution_cnt and int(force_execution_cnt) > 0:
            legal_risks_tips.append(f"{self.user_name}存在强制执行记录{int(force_execution_cnt)}条，建议谨慎授信")
        if len(legal_risks_tips) > 0:
            self.variables['suggestions']['legal_risks'] = ';'.join(legal_risks_tips)

    # 资金压力解析
    def loan_pressure_info_view(self):
        df = self.df.copy()
        # 个人在贷机构类型 per_org_type
        per_org_type = get_value(df, 'per_org_type')
        if per_org_type and len(per_org_type) > 0:
            per_org_type = json.loads(per_org_type)
            if len(per_org_type) > 0:
                # 个人在贷机构余额
                per_balance = json.loads(get_value(df, 'per_balance'))
                # 个人在贷机构占比
                per_balance_prop = json.loads(get_value(df, 'per_balance_prop'))
                temp_list = []
                for i in range(0, len(per_org_type)):
                    temp_dict = dict()
                    temp_dict['per_org_type'] = per_org_type[i]
                    temp_dict['per_balance'] = per_balance[i]
                    temp_dict['per_balance_prop'] = per_balance_prop[i]
                    temp_list.append(temp_dict)
                self.variables['risk_detail']['loan_pressure_info'] = temp_list
