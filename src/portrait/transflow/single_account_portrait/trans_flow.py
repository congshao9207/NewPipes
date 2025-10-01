import datetime

import pandas as pd
# TODO eval 动态导入，不能删除下面的导入。
from app import logger, sql_db
from pandas.tseries import offsets
from portrait.transflow.single_account_portrait.models import *
from util.mysql_reader import sql_to_df
import re
import json


def months_ago(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date.date()
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day).date()


def months_ago_datetime(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day)


# def transform_class_str(params, class_name):
#     func_str = class_name + '('
#     for k, v in params.items():
#         if v is not None and v != '':
#             func_str += k + "='" + str(v) + "',"
#     func_str = func_str[:-1]
#     func_str += ')'
#     value = eval(func_str)
#     return value


def transform_class_str(params, class_name):
    f = eval(class_name + "()")
    col_list = [x for x in dir(f) if not x.startswith("_") and x not in ['id', 'metadata', 'registry']]
    start = f"insert into {f.__tablename__}({','.join(col_list)}) values "

    def sql_values(col_val):
        vals = []
        for col in col_list:
            if col in col_val and pd.notna(col_val[col]):
                vals.append(re.sub(r"(?<![\da-zA-Z]):", '-', f"'{col_val[col]}'"))
            else:
                vals.append('null')
        return f"({','.join(vals)})"
    insert_list = [start + ','.join([sql_values(params[j]) for j in range(i, min(i + 1000, len(params)))])
                   for i in range(0, len(params), 1000)]
    db = sql_db()
    try:
        for ins in insert_list:
            db.session.execute(ins)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.info(f"库表{f.__tablename__}写入数据失败，失败原因{e}")
    return insert_list


def transform_feature_class_str(params, class_name):
    f = eval(class_name + "()")
    col_list = [x for x in dir(f) if not x.startswith("_") and x not in ['id', 'metadata', 'registry']]
    start = f"insert into {f.__tablename__}({','.join(col_list)}) values "

    def sql_values(col_val):
        vals = []
        for col in col_list:
            if col in col_val and pd.notna(col_val[col]):
                vals.append(f"'{col_val[col]}'")
            elif col in ['create_time', 'update_time']:
                vals.append(f"'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                vals.append('null')
        return f"({','.join(vals)})"

    insert_list = [start + ','.join([sql_values(params[j]) for j in range(i, min(i + 1000, len(params)))])
                   for i in range(0, len(params), 1000)]
    db = sql_db()
    try:
        for ins in insert_list:
            db.session.execute(ins)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.info(f"库表{f.__tablename__}写入数据失败，失败原因{e}")
    return insert_list


class TransFlowBasic:

    def __init__(self, portrait):
        super().__init__()
        self.trans_flow_df = None
        self.account_id = None
        # # 限制上传时间在3个月内的流水会生成画像表,后续可配置
        # self.month_interval = 3
        self.object_k = 0
        self.object_nums = len(portrait.query_data_array)
        self.object_k_k = 0
        self.user_name = portrait.user_name
        self.query_data_array = portrait.query_data_array
        self.report_req_no = portrait.public_param.get('reportReqNo')
        self.app_no = portrait.public_param.get('outApplyNo')
        self.trans_flow_portrait_df = None
        self.trans_flow_portrait_df_2_years = None
        self.trans_u_flow_df = None
        self.trans_u_flow_portrait_df = None
        self.trans_u_flow_portrait_df_2_years = None
        self.db = portrait.sql_db
        self.bank_name = None
        self.user_type = None

    def process(self):
        data = self.query_data_array[self.object_k]
        bank_account = None
        self.user_name = data.get('name')
        id_card_no = data.get('riskSubjectId')
        self.user_type = data.get('userType')
        if data.__contains__('extraParam') and data['extraParam'].__contains__('accounts') and \
                data['extraParam']['accounts'][self.object_k_k].__contains__('bankAccount'):
            bank_account = data['extraParam']['accounts'][self.object_k_k]['bankAccount']
            self.bank_name = data['extraParam']['accounts'][self.object_k_k]['bankName']

        # 若为担保人，跳过
        # 流水报告2.0 担保人不跳过
        # if data.get('relation') == 'GUARANTOR':
        #     return

        # 若关联人不存在银行卡号,则必然没有上传过流水,跳过此关联人
        # 此处修改 根据姓名，身份证号查询所有的账户
        if bank_account is None or self.bank_name is None:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        sql = """select * from trans_flow where (repeated != 1 or repeated is null) and account_id in 
                (select id from trans_account where risk_subject_id = '%s' and bank = '%s' and account_no = '%s')""" % \
              (id_card_no, self.bank_name, bank_account)

        # sql = """select * from trans_flow where account_id in (select id from trans_account where account_name = '%s'
        #            and id_card_no = '%s')""" % \
        #       (self.user_name, id_card_no)
        df = sql_to_df(sql)

        # 若数据库里面不存在该银行卡的流水信息,则跳过此关联人
        if len(df) == 0:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return

        # 最新流水上传时间必须在限制时间之内,暂定为3个月内,若在限定之间之外,则不重新生成画像表
        # limit_time = pd.to_datetime(months_ago(datetime.datetime.now(), self.month_interval))
        # if df['create_time'].max() < limit_time:
        #     self.account_id = None
        #     self.trans_flow_df = None
        #     self.trans_flow_portrait_df = None
        #     self.trans_flow_portrait_df_2_years = None
        #     return
        # 上述关系均没有跳过此关联人则正常走余下的流程
        df['bank'] = self.bank_name
        df['account_no'] = bank_account
        self.trans_flow_df = self._time_interval(df, 2)
        self.account_id = self.trans_flow_df.sort_values(by='trans_time').tail(1)['account_id'].tolist()[0]

        # 新增文件类型字段
        # map映射流水文件类型
        sql = """select account_id ,trans_flow_src_type from trans_parse_task 
        where account_id is not null group by account_id,trans_flow_src_type"""
        temp_df = sql_to_df(sql)
        # 填充空值类型为1，普通流水文件
        temp_df.set_index('account_id', drop=True, inplace=True)
        temp_df.fillna(1)
        # temp_dict = temp_df.to_dict()['trans_flow_src_type']

        self.trans_flow_df['trans_flow_src_type'] = self.trans_flow_df.account_id.map(temp_df.trans_flow_src_type)
        # 将没有映射到的文件，统一为普通流水文件，赋值1
        self.trans_flow_df.trans_flow_src_type.fillna(1, inplace=True)

    def trans_single_portrait(self, df):
        # sql = """select * from trans_flow_portrait where account_id = '%s' and report_req_no = '%s'""" \
        #       % (self.account_id, self.report_req_no)
        # df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_flow_portrait_df = self._time_interval(df, 1)
        self.trans_flow_portrait_df_2_years = self._time_interval(df, 2)

    def u_process(self, df):
        # sql = """select a.*,b.bank,b.account_no from trans_flow_portrait a left join trans_account b on
        #     a.account_id=b.id where a.report_req_no = '%s'""" % self.report_req_no
        # df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_u_flow_df = df

    def trans_union_portrait(self):
        sql = """select * from trans_u_flow_portrait where report_req_no = '%s'""" % self.report_req_no
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_u_flow_portrait_df = self._time_interval(df, 1)
        self.trans_u_flow_portrait_df_2_years = self._time_interval(df, 2)

    def _time_interval(self, df, year=1):
        flow_df = df.copy()
        if 'trans_date' in flow_df.columns:
            filter_col = 'trans_date'
        else:
            filter_col = 'trans_time'
        flow_df[filter_col] = pd.to_datetime(flow_df[filter_col])
        max_date = flow_df[filter_col].max()
        min_date = flow_df[filter_col].min()

        if year != 1:
            if max_date.month == 12:
                years_before_first = datetime.datetime(max_date.year - year + 1, 1, 1)
            else:
                years_before_first = datetime.datetime(max_date.year - year, max_date.month + 1, 1)
        else:
            years_before_first = datetime.datetime(max_date.year - year, max_date.month, 1)
        # 要取报告头的时间为准这个数据
        min_date = max(min_date, years_before_first)
        flow_df = flow_df[(flow_df[filter_col] >= min_date) &
                          (flow_df[filter_col] <= max_date)]
        return flow_df

    def select_date(self):
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
                               params={"report_req_no": self.report_req_no})
        account_df['trans_flow_src_type'] = account_df['trans_flow_src_type'].fillna(1)
        year_ago = pd.to_datetime((account_df['end_time'].max() - offsets.DateOffset(months=12)).date())
        account_df.loc[(account_df['start_time'] < year_ago) & (account_df['end_time'] < year_ago) &
                       pd.notna(account_df['account_id']), ['start_time', 'end_time', 'account_id']] = None
        account_df = account_df[(account_df['start_time'] >= year_ago) | (account_df['end_time'] >= year_ago) |
                                pd.isna((account_df['account_id']))]
        account_df.loc[account_df['start_time'] < year_ago, 'start_time'] = year_ago

        return account_df.loc[pd.notna(account_df['start_time'])]['start_time'].min()