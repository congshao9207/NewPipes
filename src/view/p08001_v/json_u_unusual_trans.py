from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df
from pandas.tseries.offsets import *


class JsonUnionUnusualTrans(TransFlow):

    def process(self):
        self.read_u_unusual_in_u_flow()

    def read_u_unusual_in_u_flow(self):
        sql = """
            select trans_date, concat(trans_date,' ',trans_time) as trans_time,
            bank,account_no,opponent_name,trans_amt,trans_use,remark,unusual_trans_type,usual_trans_type
            from trans_u_flow_portrait where report_req_no = %(report_req_no)s
        """
        flow_df = sql_to_df(sql=sql,
                            params={"report_req_no": self.reqno})

        # 异常的几个表格
        gaming = []
        amusement = []
        case_disputes = []
        security_fines = []
        insurance_claims = []
        stock = []
        hospital = []
        loan = []
        foreign_guarantee = []
        noritomo = []
        risk_gaming = ''
        risk_amusement = ''
        risk_case_disputes = ''
        risk_security_fines = ''
        risk_insurance_claims = ''
        risk_stock = ''
        risk_hospital = ''
        risk_loan = ''
        risk_foreign_guarantee = ''
        risk_noritomo = ''

        fast_in_out = []
        big_in_out = []
        family_unstable = []
        risk_fast_in_out = ''
        risk_big_in_out = ''
        risk_family_unstable = ''
        overview_abnormal_tips = ''

        financing = []
        house_sale = []
        risk_financing = ''
        risk_house_sale = ''

        if not flow_df.empty:
            # 获取一年前的日期
            year_ago = pd.to_datetime(flow_df['trans_time']).max() - DateOffset(months=12)
            # 筛选近一年有异常交易数据
            df = flow_df.loc[(pd.to_datetime(flow_df.trans_date) >= year_ago) &
                             (pd.notna(flow_df.unusual_trans_type))]
            if not df.empty:
                df['trans_time'] = df['trans_time'].astype(str)
                df['trans_amt'] = df.trans_amt.apply(lambda x: '%.2f' % x)
                df['remark'] = df[['remark', 'trans_use']].fillna("").apply(
                    lambda x: ",".join([x['remark'], x['trans_use']])
                    if len(x['trans_use']) > 0 and len(x['remark']) > 0
                    else "".join([x['remark'], x['trans_use']]), axis=1)
                df.drop(columns=['trans_use', 'trans_date'], inplace=True)

                # 博彩-投机风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('博彩')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    temp_df['temp_time'] = pd.to_datetime(temp_df.trans_time)
                    temp_df['month'] = temp_df.temp_time.dt.month
                    # 按月汇总交易金额
                    temp_month_df = temp_df.loc[temp_df.trans_amt < 0].groupby('month').agg({'trans_amt': 'sum'})
                    # 月交易金额大于1万次数
                    cnt = temp_month_df.loc[temp_month_df.trans_amt < -10000].shape[0]
                    trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
                    total_cnt = temp_df.loc[temp_df.trans_amt < 0].shape[0]
                    month_cnt = temp_df.month.unique().tolist()
                    temp_df.drop(['temp_time', 'month'], axis=1, inplace=True)
                    if trans_amt >= 1:
                        gaming = temp_df.to_dict('records')
                    if (temp_df.shape[0] > 8) or (len(month_cnt) > 5) or \
                            (temp_month_df.shape[0] > 0 and (cnt / temp_month_df.shape[0]) > (2 / 3)):
                        risk_gaming = f"博彩购买总金额{trans_amt:.2f}万，购买总次数{total_cnt}次，博彩交易频次较高，警示申请人投机风险"
                        overview_abnormal_tips += risk_gaming + ';'

                # 娱乐-不良嗜好风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('娱乐')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    temp_df['temp_time'] = pd.to_datetime(temp_df['trans_time'])
                    # 专家经验
                    # 晚上9点-凌晨4点，均算夜间
                    # 20220913修改：夜间定义为00:01-04:00
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
                    if trans_amt >= 1:
                        amusement = temp_df.drop('temp_time', axis=1).to_dict('records')
                    if temp_df.shape[0] > 8 and trans_amt > 5:
                        risk_amusement = f"娱乐场所消费金额{trans_amt:.2f}万，娱乐场所的消费金额较大，警示申请人不良嗜好风险"
                    # 筛选含交易时间数据
                    temp_df = temp_df.loc[~temp_df.temp_time.astype(str).str.contains('00:00:00')]
                    # 筛选夜间
                    temp_df = temp_df.loc[(temp_df.temp_time.dt.hour < 4) | (temp_df.temp_time.dt.hour > 0)]
                    trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
                    temp_df.drop('temp_time', axis=1, inplace=True)
                    if temp_df.shape[0] > 8 and trans_amt > 5:
                        # 若同时命中两条，只展示本条
                        risk_amusement = f"娱乐场所的夜间消费金额{trans_amt:.2f}万，在娱乐场所的夜间消费金额较大，警示申请人不良嗜好导致的家庭稳定性风险"
                    if risk_amusement != "":
                        overview_abnormal_tips += risk_amusement + ';'

                # 案件纠纷-履约风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('案件纠纷')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
                    if trans_amt >= 5:
                        case_disputes = temp_df.to_dict('records')
                    if trans_amt > 20:
                        risk_case_disputes = f"作为被告，案件纠纷总支出金额{trans_amt:.2f}万，涉案金额较大，警示申请人履约风险"
                        overview_abnormal_tips += risk_case_disputes + ';'

                # 治安罚款-治安管理风险
                # 表单
                # 20220913治安罚款修改为超过1000才展示
                temp_df = df.loc[df.unusual_trans_type.str.contains('治安罚款')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt_sum = temp_df.trans_amt.abs().sum()
                    if trans_amt_sum > 1000:
                        security_fines = temp_df.to_dict('records')
                        # 专家经验
                        trans_amt = trans_amt_sum / 10000
                        risk_security_fines = f"有治安罚款记录，罚款总金额{trans_amt:.2f}万元，预警申请人治安管理风险"
                        overview_abnormal_tips += risk_security_fines + ';'

                # 保险理赔-理赔风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('保险理赔')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    expense_amt = temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].abs().sum() / 10000
                    income_amt = temp_df.loc[temp_df.trans_amt > 0]['trans_amt'].abs().sum() / 10000
                    if expense_amt >= 5 or income_amt >= 5:
                        insurance_claims = temp_df.to_dict('records')
                    if income_amt > 20:
                        risk_insurance_claims = f"总进账理赔金额{income_amt:.2f}万，有大额理赔进账记录，关注大额理赔事件对申请人造成损失的风险;"
                    if expense_amt > 10:
                        risk_insurance_claims += f"总出账理赔金额{expense_amt:.2f}万，有大额理赔出账记录，关注大额理赔影响申请人现金流的风险，进而影响申请人经营的风险;"
                    if risk_insurance_claims != "":
                        overview_abnormal_tips += risk_insurance_claims

                # 股票期货-投资风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('股票期货')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
                    if trans_amt > 0:
                        self.variables['suggestion_and_guide']['trans_general_info']['bank_trans_type'][
                            'risk_tips'] += "客户有一定的投资理财意识，可适当进行行内理财基金等产品的营销;"
                    if trans_amt >= 20:
                        stock = temp_df.to_dict('records')
                    if trans_amt > 100:
                        risk_stock = f"股票期货出账金额{trans_amt:.2f}万，有股票期货交易记录，警示申请人投资风险"
                        overview_abnormal_tips += risk_stock + ';'

                # 医疗-健康风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('医院')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.trans_amt.sum()) / 10000
                    if trans_amt >= 1:
                        hospital = temp_df.to_dict('records')
                    if trans_amt > 3:
                        risk_hospital = f"医疗消费总金额{trans_amt:.2f}万，有大额医院消费记录，关注申请人或家人健康风险"
                        overview_abnormal_tips += risk_hospital + ';'

                # 贷款异常-逾期风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('贷款异常')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    loan = temp_df.to_dict('records')
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.trans_amt.sum()) / 10000
                    risk_loan = f"有担保公司代偿记录，代偿总金额{trans_amt:.2f}万，警示申请人贷款逾期风险"
                    overview_abnormal_tips += risk_loan + ';'

                # 对外担保异常-代偿风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('对外担保异常')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    foreign_guarantee = temp_df.to_dict('records')
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.trans_amt.sum()) / 10000
                    risk_foreign_guarantee = f"有替他人代偿记录，代偿总金额{trans_amt:.2f}万，警示代偿影响申请人现金流的风险"
                    overview_abnormal_tips += risk_foreign_guarantee

                # 典当-隐形负债风险
                # 表单
                temp_df = df.loc[df.unusual_trans_type.str.contains('典当')]
                if not temp_df.empty:
                    temp_df.drop('unusual_trans_type', inplace=True, axis=1)
                    noritomo = temp_df.to_dict('records')
                    # 专家经验
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    trans_amt = abs(temp_df.trans_amt.sum()) / 10000
                    risk_noritomo = f"有典当机构交易记录，与典当机构交易总金额{trans_amt:.2f}万，警示申请人资金紧张的风险"
                    overview_abnormal_tips += risk_noritomo + ';'

            # 筛选近一年中性交易数据
            usual_trans_df = flow_df.loc[
                (pd.to_datetime(flow_df.trans_date) >= year_ago) & (pd.notna(flow_df.usual_trans_type))]
            if not usual_trans_df.empty:
                usual_trans_df['trans_time'] = usual_trans_df['trans_time'].astype(str)
                usual_trans_df['trans_amt'] = usual_trans_df.trans_amt.apply(lambda x: '%.2f' % x)
                usual_trans_df['remark'] = usual_trans_df[['remark', 'trans_use']].fillna("").apply(
                    lambda x: ",".join([x['remark'], x['trans_use']])
                    if len(x['trans_use']) > 0 and len(x['remark']) > 0
                    else "".join([x['remark'], x['trans_use']]), axis=1)
                usual_trans_df.drop(columns=['trans_use', 'trans_date'], inplace=True)
                # 快进快出
                temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains('快进快出')]
                if not temp_df.empty:
                    temp_df.sort_values(by=['opponent_name', 'trans_time'], inplace=True)
                    temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                    fast_in_out = temp_df.to_dict('records')

                # 整进整出
                temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains('整进整出')]
                if not temp_df.empty:
                    temp_df.sort_values(by=['opponent_name', 'trans_time'], inplace=True)
                    temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                    big_in_out = temp_df.to_dict('records')

                # 家庭不稳定
                temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains('家庭不稳定')]
                if not temp_df.empty and temp_df['trans_amt'].astype(float).abs().sum() > 500:
                    temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                    family_unstable = temp_df.to_dict('records')

                # 理财行为
                # 20220919 调整为 进账大于5万或出账大于5万，展示表格
                temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains('理财行为')]
                if not temp_df.empty:
                    temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
                    financing_income = temp_df.loc[temp_df.trans_amt > 0].trans_amt.sum() / 10000
                    financing_expense = temp_df.loc[temp_df.trans_amt < 0].trans_amt.abs().sum() / 10000
                    if financing_income > 5 or financing_expense > 5:
                        temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                        financing = temp_df.to_dict('records')

                # 房产买卖
                temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains('房产买卖')]
                if not temp_df.empty:
                    temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                    house_sale = temp_df.to_dict('records')

        self.variables['abnormal_trans_risk'] = {
            "gaming": {'form_detail': gaming, 'risk_tips': risk_gaming},
            "amusement": {'form_detail': amusement, 'risk_tips': risk_amusement},
            "case_disputes": {'form_detail': case_disputes, 'risk_tips': risk_case_disputes},
            "security_fines": {'form_detail': security_fines, 'risk_tips': risk_security_fines},
            "insurance_claims": {'form_detail': insurance_claims, 'risk_tips': risk_insurance_claims},
            "stock": {'form_detail': stock, 'risk_tips': risk_stock},
            "hospital": {'form_detail': hospital, 'risk_tips': risk_hospital},
            "loan": {'form_detail': loan, 'risk_tips': risk_loan},
            "foreign_guarantee": {'form_detail': foreign_guarantee, 'risk_tips': risk_foreign_guarantee},
            "noritomo": {'form_detail': noritomo, 'risk_tips': risk_noritomo}
        }
        self.variables['normal_trans_detail'] = {
            "fast_in_out": {"form_detail": fast_in_out, "risk_tips": risk_fast_in_out},
            "big_in_out": {"form_detail": big_in_out, "risk_tips": risk_big_in_out},
            "family_unstable": {"form_detail": family_unstable, "risk_tips": risk_family_unstable},
            "financing": {"form_detail": financing, "risk_tips": risk_financing},
            "house_sale": {"form_detail": house_sale, "risk_tips": risk_house_sale}
        }
        self.variables['trans_report_overview']['abnormal_trans_risk']['risk_tips'] = overview_abnormal_tips
