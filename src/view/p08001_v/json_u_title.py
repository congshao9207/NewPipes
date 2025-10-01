# import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from view.p08001_v.trans_report_util import convert_relationship
from pandas.tseries import offsets
import datetime
import pandas as pd

RELATED_LIST = ['U_PERSONAL',
                'U_PER_LG_COMPANY',
                'U_PER_SH_H_COMPANY',
                'U_PER_SH_M_COMPANY',
                'U_PER_SP_PERSONAL',
                'U_PER_SP_LG_COMPANY',
                'U_PER_SP_SH_H_COMPANY',
                'U_PER_SP_SH_M_COMPANY',
                'U_PER_CT_COMPANY',
                'U_PER_SP_CT_COMPANY',
                'U_PER_PARENTS_PERSONAL',
                'U_PER_CHILDREN_PERSONAL',
                'U_PER_PARTNER_PERSONAL',
                'U_PER_GUARANTOR_PERSONAL',
                'U_PER_GUARANTOR_COMPANY',
                'U_PER_CHAIRMAN_COMPANY',
                'U_PER_SUPERVISOR_COMPANY',
                'U_PER_OTHER',
                'U_PER_COMPANY_SH_PERSONAL',
                'U_PER_COMPANY_FIN_PERSONAL',
                'U_COMPANY',
                'U_COMPANY_SH_PERSONAL',
                'U_COM_LEGAL_PERSONAL',
                'U_COMPANY_SH_H_COMPANY',
                'U_COMPANY_SH_M_COMPANY',
                'U_COM_CT_PERSONAL',
                'U_COM_CT_CT_COMPANY',
                'U_COM_CT_LG_COMPANY',
                'U_COM_CT_SH_H_COMPANY',
                'U_COM_CT_SH_M_COMPANY',
                'U_COM_CT_SP_PERSONAL',
                'U_COM_GUARANTOR_PERSONAL',
                'U_COM_GUARANTOR_COMPANY',
                'U_COM_CT_SP_CT_COMPANY',
                'U_COM_CT_SP_LG_COMPANY',
                'U_COM_CT_SP_SH_H_COMPANY',
                'U_COM_CT_SP_SH_M_COMPANY',
                'U_COM_OTHER',
                'U_COM_FIN_PERSONAL']
RELATED_LIST_SIMPLE = [
    'U_PERSONAL',
    'U_PER_REL_PERSONAL',
    'U_PER_REL_NA_PERSONAL',
    'U_PER_REL_COMPANY',
    'U_PER_GUARANTOR_PERSONAL',
    'U_PER_GUARANTOR_COMPANY',
    'U_PER_OTHER',
    'U_COMPANY',
    'U_COM_REL_PERSONAL',
    'U_COM_REL_NA_PERSONAL',
    'U_COM_REL_COMPANY'
    'U_COM_GUARANTOR_PERSONAL',
    'U_COM_GUARANTOR_COMPANY',
    'U_COM_OTHER']
RELATION_RANK = dict(zip(RELATED_LIST, range(1, len(RELATED_LIST) + 1)))
RELATION_RANK_SIMPLE = dict(zip(RELATED_LIST_SIMPLE, range(1, len(RELATED_LIST_SIMPLE) + 1)))


class JsonUnionTitle(TransFlow):

    def process(self):
        self.variables['suggestion_and_guide'] = {
            'trans_general_info': {
                'confidence_analyse': {'risk_tips': ''},
                'trans_scale': {'risk_tips': ''},
                'bank_trans_type': {'risk_tips': ''}
            },
            'loan_analyse': {'risk_tips': ''},
            'business_info': {
                'daily_mean_balance': {'risk_tips': ''},
                'money_mobilize_ability': {'risk_tips': ''}
            }
        }
        self.variables['trans_report_overview'] = {
            'trans_general_info': {
                'analysis_subjects': {'risk_tips': ''},
                'confidence_analyse': {'risk_tips': ''},
                'trans_scale': {'risk_tips': ''},
                'bank_trans_income_type': {'risk_tips': ''},
                'bank_trans_expense_type': {'risk_tips': ''}
            },
            'loan_analyse': {'risk_tips': ''},
            'business_info': {
                'business_scale': {'risk_tips': ''},
                'business_risk': {'risk_tips': ''},
                'daily_mean_balance': {'risk_tips': ''},
                'money_mobilize_ability': {'risk_tips': ''},
                'upstream_customers': {'risk_tips': ''},
                'downstream_customers': {'risk_tips': ''}
            },
            "related_info": {
                "strong_relation_info": {"risk_tips": ""},
                "normal_relation_info": {"risk_tips": ""},
                "guarantor_info": {"risk_tips": ""}
            },
            'abnormal_trans_risk': {'risk_tips': ''},
            'marketing_feedback': {'risk_tips': ''}
        }
        self.create_u_title()

    def create_u_title(self):

        sql1 = """
            SELECT ap.related_name AS relatedName, acc.id as account_id, ap.id_type, 
            ap.relationship AS relation,
            ac.bank AS bankName,ac.account_no AS bankAccount,
            acc.start_time, acc.end_time, ta.trans_flow_src_type, ap.id_card_no
            FROM trans_apply ap
            left join trans_account ac
            on ap.account_id = ac.id
            left join trans_account acc
            on ac.account_no = acc.account_no and ac.bank = acc.bank and ac.risk_subject_id = acc.risk_subject_id
            left join trans_parse_task ta
            on acc.id = ta.account_id
            where ap.report_req_no =  %(report_req_no)s
        """

        account_df = sql_to_df(sql=sql1,
                               params={"report_req_no": self.reqno})
        account_df['trans_flow_src_type'] = account_df['trans_flow_src_type'].fillna(1)
        # 仅展示近一年的流水数据
        year_ago = pd.to_datetime((account_df['end_time'].max() - offsets.DateOffset(months=12)).date())
        account_df.loc[(account_df['start_time'] < year_ago) & (account_df['end_time'] < year_ago) &
                       pd.notna(account_df['account_id']), ['start_time', 'end_time', 'account_id']] = None
        account_df = account_df[(account_df['start_time'] >= year_ago) | (account_df['end_time'] >= year_ago) |
                                pd.isna((account_df['account_id']))]
        account_df.loc[account_df['start_time'] < year_ago, 'start_time'] = year_ago
        # 新增报告开始和结束日期
        self.variables['report_start_date'] = \
            account_df.loc[pd.notna(account_df['start_time'])]['start_time'].min().strftime('%Y/%m/%d')
        self.variables['report_end_date'] = \
            account_df.loc[pd.notna(account_df['end_time'])]['end_time'].max().strftime('%Y/%m/%d')
        # 关联人信息
        relation_df = account_df.drop_duplicates(['relatedName', 'relation'])
        relation_df.rename({'relatedName': 'name'}, axis=1, inplace=True)
        # 流水基本信息
        account_df = account_df[pd.notna(account_df.account_id)]
        unique_acc = \
            account_df.drop_duplicates(subset=['relatedName', 'relation', 'bankName', 'bankAccount']
                                       )[['relatedName', 'relation', 'bankName', 'bankAccount', 'id_card_no']]
        if unique_acc.shape[0] == 0:
            unique_acc['startEndDate'] = None
        # 每个类别账户数
        each_acc_cnt = [0, 0, 0]
        start_end_date_list = []
        for row in unique_acc.itertuples():
            ind = getattr(row, 'Index')
            rel_name = getattr(row, 'relatedName')
            rel = getattr(row, 'relation')
            bank_name = getattr(row, 'bankName')
            bank_acc = getattr(row, 'bankAccount')
            temp_df = account_df[(account_df.relatedName == rel_name) & (account_df.relation == rel) &
                                 (account_df.bankName == bank_name) & (account_df.bankAccount == bank_acc)]
            temp_df.sort_values(by=['start_time', 'end_time'], inplace=True, ascending=True)
            temp_df['start_time'] = temp_df['start_time'].apply(lambda x: x.date())
            temp_df['end_time'] = temp_df['end_time'].apply(lambda x: x.date())
            temp_start = pd.to_datetime(temp_df['start_time']).tolist()
            temp_end = pd.to_datetime(temp_df['end_time']).tolist()
            start_end_date_list.append(self.union_date(temp_start, temp_end))
            unique_src_type = temp_df['trans_flow_src_type'].unique().tolist()
            if 1 in unique_src_type:
                unique_acc.loc[ind, 'bankNameDetail'] = f'{bank_name}（银行流水）'
                each_acc_cnt[0] += 1
            elif 2 in unique_src_type:
                unique_acc.loc[ind, 'bankNameDetail'] = f'{bank_name}（支付宝流水）'
                each_acc_cnt[1] += 1
            else:
                unique_acc.loc[ind, 'bankNameDetail'] = f'{bank_name}（微信流水）'
                each_acc_cnt[2] += 1
        unique_acc['startEndDate'] = start_end_date_list
        account_df = unique_acc
        # 根据产品编号判断使用哪一种关联关系列表
        product_code = self.origin_data['strategyInputVariables']['product_code']
        relation_rank = RELATION_RANK_SIMPLE if product_code == '08003' else RELATION_RANK
        account_df['rank'] = account_df.relation.apply(lambda x: relation_rank[x])
        account_df.sort_values(by='rank', axis=0, inplace=True)
        account_df.drop('rank', 1, inplace=True)
        account_df['relation'] = account_df['relation'].apply(lambda x: convert_relationship(x, product_code))

        cashier = pd.DataFrame(data=None,
                               columns=['name', 'bank', 'account'])
        file_df = pd.DataFrame()
        for account in self.cached_data.get('input_param'):
            if str(account).__contains__('\'ifCashier\': \'是\''):
                cashier.loc[len(cashier)] = [account.get('name'),
                                             account.get('extraParam').get('accounts')[0]['bankName'],
                                             account.get('extraParam').get('accounts')[0]['bankAccount']]
            temp_name = account.get('name')
            temp_idno = account.get('idno')
            temp_file_df = pd.DataFrame(account.get('extraParam').get('fileInfo'))
            if temp_file_df.shape[0] > 0:
                temp_file_df['ownerName'] = temp_file_df['ownerName'].apply(
                    lambda x: "未取得" if pd.isna(x) or x == "" else f"{x}(一致)" if x == temp_name else f"{x}(不一致)")
                temp_file_df = temp_file_df.groupby(by=['bankName', 'bankAccount'], as_index=False).agg({
                    'fileName': pd.Series.tolist, 'contentId': pd.Series.tolist,
                    'uploadDate': pd.Series.tolist, 'ownerName': pd.Series.tolist})
                temp_file_df.rename({"ownerName": "userName"}, axis=1, inplace=True)
                # temp_file_df['relatedName'] = temp_name
                temp_file_df['id_card_no'] = temp_idno
                file_df = pd.concat([file_df, temp_file_df], axis=0, ignore_index=True)

        if not cashier.empty:
            cashier['account_detail'] = '(出纳)'
            account_df = pd.merge(account_df, cashier,
                                  how='left',
                                  left_on=['relatedName', 'bankName', 'bankAccount'],
                                  right_on=['name', 'bank', 'account']).fillna("")
            account_df['bankAccount'] = account_df['bankAccount'] + account_df['account_detail']
        # 删除强关联关系中担保人展示
        # 不删除担保人的展示
        # drop_list = account_df[account_df.relation == '担保人'].index.tolist()
        # account_df.drop(drop_list, 0, inplace=True)
        account_df = pd.merge(account_df, file_df, how='left', on=['id_card_no', 'bankName', 'bankAccount'])
        account_df.reset_index(drop=True, inplace=True)
        for col in ['bankAccount', 'relatedName', 'bankNameDetail']:
            account_df[col] = account_df[col].apply(lambda x: str(x).strip())
        account_df['bankName'] = account_df['bankNameDetail']
        account_list = account_df[['relatedName', 'relation', 'bankName', 'bankAccount', 'startEndDate',
                                   'fileName', 'userName', 'contentId', 'uploadDate']].to_dict(orient='records')

        # 20220914新增，同一关联对象多个关联关系并列展示
        relation_df['rank'] = relation_df.relation.apply(lambda x: relation_rank[x])
        relation_df['relation'] = relation_df['relation'].apply(lambda x: convert_relationship(x, product_code))
        temp_df = relation_df.groupby(by='id_card_no', as_index=False).agg({
            'rank': 'min', 'relation': lambda x: '，'.join(x)})
        temp_df.sort_values(by='rank', axis=0, inplace=True)
        relation_df.drop(['rank', 'relation'], 1, inplace=True)
        relation_df = relation_df.drop_duplicates(subset=['id_card_no'], keep='last')
        relation_df = pd.merge(temp_df, relation_df, how='left', on='id_card_no')
        now = datetime.datetime.now()
        detail_list = []
        for row in relation_df.itertuples():
            ind = getattr(row, 'Index')
            rel_name = getattr(row, 'name')
            rel_code = getattr(row, 'id_card_no')
            id_type = getattr(row, 'id_type')
            if id_type == 'ID_CARD_NO':
                relation_df.loc[ind, 'cusType'] = 'PERSON'
                if product_code == '08003':
                    continue
                age = now.year - int(rel_code[6:10])
                if pd.to_datetime(rel_code[6: 14]) + offsets.DateOffset(years=age) > pd.to_datetime(now.date()):
                    age -= 1
                detail_list.append({'basic_name': rel_name, 'basic_age': age,
                                    'basic_sex': '男' if int(rel_code[-2]) % 2 == 1 else '女',
                                    'basic_indiv_brt_place': rel_code[:6], 'basic_id': rel_code})
            else:
                relation_df.loc[ind, 'cusType'] = 'COMPANY'
                if product_code == '08003':
                    continue
                detail_list.append(self.company_detail(rel_name, rel_code))
        relation_df['detail'] = detail_list if product_code == '08001' else None
        relation_df = relation_df[['name', 'relation', 'cusType', 'detail']]
        # 20220925 新增行业信息
        self.variables['表头'] = {'cusName': self.cusName, 'appAmt': self.appAmt, 'industryName': self.industry_name,
                                '流水信息': account_list, '关联人': relation_df.to_dict(orient='records')}
        # json_str = "{\"cusName\":\"" + self.cusName \
        #            + "\",\"appAmt\":" + str(self.appAmt) \
        #            + ",\"industryName\":\"" + str(self.industry_name) \
        #            + "\",\"流水信息\":" + account_list \
        #            + ",\"关联人\":" + relation_df.to_json(orient='records') \
        #            + "}"
        #
        # self.variables["表头"] = json.loads(json_str)
        overview_sub_tips = "客户强关联关系下识别到"
        for i, acc_cnt in enumerate(each_acc_cnt):
            if acc_cnt == 0:
                continue
            overview_sub_tips += f"{acc_cnt}个"
            overview_sub_tips += "银行账户流水，" if i == 0 else "支付宝账户流水，" if i == 1 else "微信账户流水，"
        self.variables['trans_report_overview']['trans_general_info']['analysis_subjects']['risk_tips'] = \
            overview_sub_tips[:-1] + ";"

    @staticmethod
    def union_date(start, end):
        res = []
        for i, v in enumerate(start):
            if i == 0:
                res.append([v, end[i]])
            else:
                if v <= res[-1][-1]:
                    if end[i] > res[-1][-1]:
                        res[-1][-1] = end[i]
                else:
                    res.append([v, end[i]])
        res = [f"{format(x[0], '%Y/%m/%d')}—{format(x[-1], '%Y/%m/%d')}" for x in res]
        return res

    @staticmethod
    def company_detail(name, idno):
        sql = "select * from %s where basic_id = (SELECT id FROM info_com_bus_basic WHERE ent_name='%s'"
        if pd.notna(idno):
            sql += " and credit_code = '%s'"
        sql += " and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1)"
        face_df = sql_to_df(sql=sql % ("info_com_bus_face", name, idno))
        detail = {}
        if face_df.shape[0] == 0:
            return detail
        share_df = sql_to_df(sql=sql % ("info_com_bus_shareholder", name, idno))
        shareholder_str = ''
        if share_df.shape[0] > 0:
            shareholder_str = '，'.join(
                share_df[(pd.notna(share_df['share_holder_name'])) &
                         (pd.notna(share_df['funded_ratio']))
                         ].apply(lambda x: f"{str(x['share_holder_name'])}（{x['funded_ratio']:.0%}）", axis=1))
        detail['basic_name'] = name
        detail['basic_fr_name'] = face_df.loc[0, 'fr_name']
        detail['basic_es_date'] = "" if pd.isna(face_df.loc[0, 'es_date']) else \
            format(face_df.loc[0, 'es_date'], '%Y-%m-%d')
        detail['basic_appr_date'] = "" if pd.isna(face_df.loc[0, 'appr_date']) else \
            format(face_df.loc[0, 'appr_date'], '%Y-%m-%d')
        detail['basic_industry_phyname'] = face_df.loc[0, 'industry_phyname']
        detail['basic_address'] = face_df.loc[0, 'address']
        detail['basic_opera_range'] = face_df.loc[0, 'operate_scope']
        detail['basic_shareholder'] = shareholder_str
        detail['basic_ent_type'] = face_df.loc[0, 'ent_type']
        detail['basic_credit_code'] = idno
        detail['basic_reg_cap'] = face_df.loc[0, 'reg_cap']
        detail['basic_ent_status'] = face_df.loc[0, 'ent_status']
        open_from = "*" if pd.isna(face_df.loc[0, 'open_from']) else format(face_df.loc[0, 'open_from'], "%Y-%m-%d")
        open_to = "*" if pd.isna(face_df.loc[0, 'open_to']) else format(face_df.loc[0, 'open_to'], "%Y-%m-%d")
        detail['basic_open_date_range'] = open_from + "至" + open_to
        return detail
