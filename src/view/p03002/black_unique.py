import datetime

import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union, invoke_each
from util.common_util import get_query_data
from util.mysql_reader import sql_to_df
import time


def trans_black_froz_time(begin, end):
    if pd.isna(begin) and pd.isna(end):
        return '/'
    elif pd.isna(begin) and pd.notna(end):
        return '/' + '至' + end.strftime('%Y-%m-%d')
    elif pd.notna(begin) and pd.isna(end):
        return begin.strftime('%Y-%m-%d') + '至' + '/'
    else:
        return begin.strftime('%Y-%m-%d') + '至' + end.strftime('%Y-%m-%d')


def get_diff_days(x):
    date = datetime.datetime.now() - pd.to_datetime(x)
    diff = date.days
    if diff < 0:
        return -diff
    return diff


class Black(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return 'black'

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'black_list_cnt': 0,
            'black_overt_cnt': 0,
            'black_judge_cnt': 0,
            'black_exec_cnt': 0,
            'black_illegal_cnt': 0,
            'black_legel_cnt': 0,
            'black_froz_cnt': 0,
            'black_list_name': [],
            'black_list_tyle': [],
            'black_list_case_no': [],
            'black_list_detail': [],
            'black_list_title': [],  # 新增：执行限制标题
            'black_list_date': [],  # 新增：执行限制立案时间
            'black_list_org': [],  # 新增：执行限制执行法院
            'black_list_amt': [],  # 新增：执行限制执行金额（元）
            'black_list_status': [],  # 新增：执行限制执行状态
            'black_list_balance': [],  # 新增：执行限制未履行金额
            'black_overt_name': [],
            'black_overt_reason': [],
            'black_overt_type': [],
            'black_overt_authority': [],
            'black_overt_case_no': [],
            'black_overt_status': [],
            'black_overt_date': [],
            'black_overt_title': [],  # 新增：民商事审判流程标题
            'black_overt_detail': [],  # 新增：民商事审判公告内容
            'black_judge_name': [],
            'black_judge_reason': [],
            'black_judge_authority': [],
            'black_judge_case_no': [],
            'black_judge_time': [],
            'black_judge_url': [],
            'black_judge_title': [],  # 新增：裁判文书标题
            'black_judge_status': [],  # 新增：裁判文书诉讼地位
            'black_judge_type': [],  # 新增：裁判文书文书类型
            'black_judge_amt': [],  # 新增：裁判文书涉案金额
            'black_judge_result': [],  # 新增：裁判文书审理结果
            'black_judge_process': [],  # 新增：裁判文书审理程序
            'black_judge_case_type': [],  # 新增：裁判文书案件类型
            'black_judge_plaintiff': [],  # 新增：裁判文书原告当事人
            'black_judge_defendant': [],  # 新增：裁判文书被告当事人
            'black_judge_otherparty': [],  # 新增：裁判文书其他当事人
            'black_exec_name': [],
            'black_exec_authority': [],
            'black_exec_case_no': [],
            'black_exec_date': [],
            'black_exec_content': [],
            'black_exec_type': [],
            'black_exec_title': [],  # 新增：执行公开标题
            'black_exec_amt': [],  # 新增：执行公开执行标的
            'black_exec_status': [],  # 新增：执行公开执行状态
            'black_exec_end_date': [],  # 新增：执行公开终本日期
            'black_exec_balance': [],  # 新增：执行公开未履行金额
            'black_illegal_name': [],
            'black_illegal_reason': [],
            'black_illegal_datetime': [],
            'black_illegal_case_no': [],
            'black_illegal_title': [],  # 新增：行政违法标题
            'black_illegal_org': [],  # 新增：行政违法执法 / 复议 / 审判机关
            'black_illegal_amt': [],  # 新增：行政违法金额（元）
            'black_illegal_result': [],  # 新增：行政违法行政执法结果
            'black_illegal_date_type': [],  # 新增：行政违法日期类别
            'black_legal_name': [],
            'black_legal_cause': [],
            'black_legal_date': [],
            'black_legal_org': [],
            'black_legal_clear_cause': [],
            'black_legal_clear_date': [],
            'black_legal_clear_org': [],
            'black_froz_name': [],
            'black_froz_role': [],
            'black_froz_status': [],
            'black_froz_execute_no': [],
            'black_froz_amt': [],
            'black_froz_inv': [],
            'black_froz_auth': [],
            'black_froz_public_date': [],
            'black_froz_time': [],
            'black_froz_thaw_date': [],
            'black_froz_invalid_date': [],
            'black_froz_invalid_reason': [],
            'black_punish_cnt': 0,
            'black_punish_name': [],
            'black_punish_reason': [],
            'black_punish_datetime': [],
            'black_punish_case_no': [],
            'black_crim_case_no': [],  # 新增：罪犯及嫌疑人案号
            'black_crim_reason': [],  # 新增：罪犯及嫌疑人违法事由
            'black_crim_title': [],  # 新增：罪犯及嫌疑人标题
            'black_crim_date': [],  # 新增：罪犯及嫌疑人立案时间
            'black_crim_org': [],  # 新增：罪犯及嫌疑人侦察 / 批捕 / 审判机关
            'black_crim_amt': [],  # 新增：罪犯及嫌疑人涉案金额
            'black_crim_result': [],  # 新增：罪犯及嫌疑人处理结果
            'black_abnormal_title': [],  # 新增：纳税非正常户标题
            'black_abnormal_date': [],  # 新增：纳税非正常户认定日期
            'black_abnormal_name': [],  # 新增：纳税非正常户纳税人名称
            'black_abnormal_code': [],  # 新增：纳税非正常户纳税人识别号
            'black_abnormal_tax_org': [],  # 新增：纳税非正常户主管税务机关
            'black_tax_title': [],  # 新增：欠税记录标题
            'black_tax_date': [],  # 新增：欠税记录立案日期
            'black_tax_org': [],  # 新增：欠税记录主管税务机关
            'black_tax_amt': [],  # 新增：欠税记录欠税金额
            'black_tax_type': [],  # 新增：欠税记录所欠税种
            'black_tax_time': [],  # 新增：欠税记录欠税属期
            'black_arrears_title': [],  # 新增：欠款欠费标题
            'black_arrears_date': [],  # 新增：欠款欠费具体日期
            'black_arrears_status': [],  # 新增：欠款欠费身份
            'black_arrears_reason': [],  # 新增：欠款欠费欠款原因
            'black_arrears_amt': []  # 新增：欠款欠费拖欠金额
        }

    def _info_court(self, user_name, id_card_no):
        '''
        法院核查基本信息表
        :param user_name:
        :param id_card_no:
        :return:
        '''
        sql = '''
            select id from info_court where unique_name = %(user_name)s
        '''
        if pd.notna(id_card_no):
            sql += 'and unique_id_no = %(id_card_no)s'
        sql += 'AND unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 '
        df = sql_to_df(sql=sql,
                       params={'user_name': user_name,
                               'id_card_no': id_card_no})
        return df

    def _info_court_criminal_suspect(self, id):
        '''
        法院核查罪犯及嫌疑人名单
        :param id_list:
        :return:
        '''
        sql = '''
            select * from info_court_criminal_suspect where court_id in %(id)s
        '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_deadbeat(self, id):
        '''
        法院核查失信老赖名单
        :param ids:
        :return:
        '''
        sql = '''
                   select * from info_court_deadbeat where court_id in %(id)s
               '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_limit_hignspending(self, id):
        '''
        法院核查限制高消费名单
        :param ids:
        :return:
        '''
        sql = '''
                   select * from info_court_limit_hignspending where court_id in %(id)s
               '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_limited_entry_exit(self, id):
        '''
        法院核查限制出入境名单
        :param ids:
        :return:
        '''
        sql = '''
                   select * from info_court_limited_entry_exit where court_id in %(id)s
               '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_trial_process(self, id):
        '''
        法院核查民商事审判流程
        :param ids:
        :return:
        '''
        sql = '''
                   select * from info_court_trial_process where court_id in %(id)s
               '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        if not df.empty:
            df['diff_day'] = df.apply(lambda x: get_diff_days(x['filing_time']), axis=1)
        return df

    def _info_court_judicative_pape(self, id):
        '''
        法院核查民商事裁判文书
        :param ids:
        :return:
        '''
        sql = '''
                          select * from info_court_judicative_pape where court_id in %(id)s
                      '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        if not df.empty:
            df['diff_day'] = df.apply(lambda x: get_diff_days(x['filing_time']), axis=1)
        return df

    def _info_court_excute_public(self, id):
        '''
        法院核查执行公开信息
        :param ids:
        :return:
        '''
        sql = '''
                          select * from info_court_excute_public where court_id in %(id)s
                      '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_administrative_violation(self, id):
        '''
        法院核查行政违法记录
        :param ids:
        :return:
        '''
        sql = '''
                             select * from info_court_administrative_violation where court_id in %(id)s
                         '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_court_taxable_abnormal_user(self, id):
        """
        法院核查纳税非正常户
        :param ids:
        :return:
        """
        sql = '''
            select * from info_court_taxable_abnormal_user where court_id in %(id)s
        '''
        df = sql_to_df(sql=sql, params={'id': id})
        return df

    def _info_court_tax_arrears(self, id):
        """
        法院核查欠税名单
        :param ids:
        :return:
        """
        sql = '''
            select * from info_court_tax_arrears where court_id in %(id)s
        '''
        df = sql_to_df(sql=sql, params={'id': id})
        return df

    def _info_court_arrearage(self, id):
        """
        法院核查欠款欠费名单
        :param ids:
        :return:
        """
        sql = '''
            select * from info_court_arrearage where court_id in %(id)s
        '''
        df = sql_to_df(sql=sql, params={'id': id})
        return df

    def _info_com_bus_basic(self, user_name, id_card_no):
        '''企业工商-基础信息表'''
        sql = '''
            select id,ent_name from info_com_bus_basic where ent_name = %(user_name)s order by id desc limit 1
        '''
        if pd.notna(id_card_no):
            sql += ' and credit_code = %(id_card_no)s'
        sql += 'AND unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 '
        df = sql_to_df(sql=sql,
                       params={'user_name': user_name,
                               'id_card_no': id_card_no})
        return df

    def _info_com_bus_illegal(self, id):
        """info_com_bus_illegal 企业工商-严重违法失信信息"""
        sql = '''
                   select * from info_com_bus_illegal where basic_id in %(id)s and illegal_date_in is not NULL
              '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_com_bus_shares_frost(self, id):
        """info_com_bus_shares_frost 企业工商-股权冻结信息"""
        sql = '''
                   select * from info_com_bus_shares_frost where basic_id in %(id)s order by froz_ent,froz_public_date DESC
              '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def _info_com_bus_case(self, id):
        '''info_com_bus_case 企业工商-行政处罚信息'''
        sql = '''
           SELECT distinct a.ent_name,b.pendec_no,b.illegact_type_name,b.pen_deciss_date FROM info_com_bus_basic a,info_com_bus_case b
           where a.id = b.basic_id
           and b.basic_id in %(id)s
        '''
        df = sql_to_df(sql=sql,
                       params={'id': id})
        return df

    def clean_variables_court(self):
        # resp = get_query_data(self.full_msg, None, '01')
        id = []
        # user_name = resp.get('name')
        user_name = self.user_name
        # id_card_no = resp.get('id_card_no')
        id_card_no = self.id_card_no
        court_df = self._info_court(user_name, id_card_no)
        if not court_df.empty:
            id.append(int(court_df.loc[0, 'id']))

        if len(id) == 0:
            return
        suspect_df = self._info_court_criminal_suspect(id)  # 罪犯及嫌疑人
        deadbeat_df = self._info_court_deadbeat(id)  # 失信老赖
        hign_df = self._info_court_limit_hignspending(id)  # 限制高消费
        entry_df = self._info_court_limited_entry_exit(id)  # 限制出入境
        process_df = self._info_court_trial_process(id)  # 法院核查民商事审判流程
        pape_df = self._info_court_judicative_pape(id)  # 法院核查民商事裁判文书
        public_df = self._info_court_excute_public(id)  # 法院核查执行公开信息
        violation_df = self._info_court_administrative_violation(id)  # 法院核查行政违法记录
        taxable_abnormal_df = self._info_court_taxable_abnormal_user(id)  # 法院核查纳税非正常户
        tax_arrears_df = self._info_court_tax_arrears(id)  # 法院核查欠税名单
        arrearage_df = self._info_court_arrearage(id)  # 法院核查欠款欠费名单

        # black_list_cnt
        # 限制执行信息
        black_list_cnt = 0
        black_list_name = []
        black_list_tyle = []
        black_list_case_no = []
        black_list_detail = []
        black_list_title = []
        black_list_date = []
        black_list_org = []
        black_list_amt = []
        black_list_status = []
        black_list_balance = []
        if not deadbeat_df.empty:
            black_list_cnt = black_list_cnt + deadbeat_df.shape[0]
            for row in deadbeat_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('失信老赖')
                black_list_case_no.append(getattr(row, 'execute_case_no'))
                black_list_detail.append(getattr(row, 'execute_content'))
                black_list_title.append(getattr(row, 'title'))
                black_list_date.append(getattr(row, 'filing_time'))
                black_list_org.append(getattr(row, 'execute_court'))
                black_list_amt.append(getattr(row, 'execution_amt'))
                black_list_status.append(getattr(row, 'execute_status'))
                black_list_balance.append(getattr(row, 'non_performance_amt'))
        if not hign_df.empty:
            black_list_cnt = black_list_cnt + hign_df.shape[0]
            for row in hign_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('限制高消费')
                black_list_case_no.append(getattr(row, 'execute_case_no'))
                black_list_detail.append(getattr(row, 'execute_content'))
                black_list_title.append(getattr(row, 'title'))
                black_list_date.append(getattr(row, 'filing_time'))
                black_list_org.append(getattr(row, 'execute_court'))
                black_list_amt.append(getattr(row, 'execution_amt'))
                black_list_status.append(getattr(row, 'execute_status'))
                black_list_balance.append('')
        if not entry_df.empty:
            black_list_cnt = black_list_cnt + entry_df.shape[0]
            for row in entry_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('限制出入境')
                black_list_case_no.append(getattr(row, 'execute_no'))
                black_list_detail.append(getattr(row, 'execute_content'))
                black_list_title.append(getattr(row, 'title'))
                black_list_date.append(getattr(row, 'filing_time'))
                black_list_org.append(getattr(row, 'execute_court'))
                black_list_amt.append(getattr(row, 'execution_amt'))
                black_list_status.append(getattr(row, 'execute_status'))
                black_list_balance.append('')
        self.variables['black_list_cnt'] = black_list_cnt
        self.variables['black_list_name'] = black_list_name
        self.variables['black_list_tyle'] = black_list_tyle
        self.variables['black_list_case_no'] = black_list_case_no
        self.variables['black_list_detail'] = black_list_detail
        self.variables['black_list_title'] = black_list_title
        self.variables['black_list_date'] = black_list_date
        self.variables['black_list_org'] = black_list_org
        self.variables['black_list_amt'] = black_list_amt
        self.variables['black_list_status'] = black_list_status
        self.variables['black_list_balance'] = black_list_balance

        # 罪犯及嫌疑人名单
        if not suspect_df.empty:
            for row in suspect_df.itertuples():
                self.variables['black_crim_case_no'] = suspect_df['case_no'].tolist()
                self.variables['black_crim_reason'] = suspect_df['criminal_reason'].tolist()
                self.variables['black_crim_title'] = suspect_df['title'].tolist()
                self.variables['black_crim_date'] = suspect_df['filing_time'].tolist()
                self.variables['black_crim_org'] = suspect_df['trial_authority'].tolist()
                self.variables['black_crim_amt'] = suspect_df['involved_amt'].tolist()
                self.variables['black_crim_result'] = suspect_df['trial_result']

        # black_overt_cnt
        # 民商事审判流程
        if not process_df.empty:
            process_df1 = process_df[pd.notna(process_df.case_no)].drop_duplicates()
            if not process_df1.empty:
                self.variables['black_overt_cnt'] = process_df1.shape[0]
                process_df2 = process_df1.sort_values(by='filing_time', ascending=False)
                process_df2['specific_date'] = process_df2['specific_date'].apply(
                    lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_overt_name'] = process_df2['name'].to_list()
                self.variables['black_overt_reason'] = process_df2['case_reason'].to_list()
                # 日期类别
                self.variables['black_overt_type'] = process_df2['date_type'].to_list()
                self.variables['black_overt_authority'] = process_df2['trial_authority'].to_list()
                self.variables['black_overt_case_no'] = process_df2['case_no'].to_list()
                # 诉讼地位
                self.variables['black_overt_status'] = process_df2['legal_status'].to_list()
                self.variables['black_overt_date'] = process_df2['filing_time'].to_list()
                self.variables['black_overt_title'] = process_df2['title'].tolist()
                self.variables['black_overt_detail'] = process_df2['announcement_content'].tolist()

        # black_judge_cnt
        # 裁判文书
        if not pape_df.empty:
            pape_df1 = pape_df[~pape_df.legal_status.str.contains('原告|申请执行人|第三人')]
            if not pape_df1.empty:
                self.variables['black_judge_cnt'] = pape_df1.shape[0]
                pape_df2 = pape_df1.sort_values(by='closed_time', ascending=False)
                pape_df2['closed_time'] = pape_df2['closed_time'].apply(
                    lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_judge_name'] = pape_df2['name'].to_list()
                self.variables['black_judge_reason'] = pape_df2['case_reason'].to_list()
                self.variables['black_judge_authority'] = pape_df2['trial_authority'].to_list()
                self.variables['black_judge_case_no'] = pape_df2['case_no'].to_list()
                self.variables['black_judge_time'] = pape_df2['closed_time'].to_list()
                self.variables['black_judge_url'] = pape_df2['url'].to_list()
                self.variables['black_judge_title'] = pape_df2['title'].tolist()
                self.variables['black_judge_status'] = pape_df2['legal_status'].tolist()
                self.variables['black_judge_type'] = pape_df2['document_type'].tolist()
                self.variables['black_judge_amt'] = pape_df2['case_amount'].tolist()
                self.variables['black_judge_result'] = pape_df2['trial_results'].tolist()
                self.variables['black_judge_process'] = pape_df2['trial_procedure'].tolist()
                self.variables['black_judge_case_type'] = pape_df2['case_type']
                self.variables['black_judge_plaintiff'] = pape_df2['plaintiff'].tolist()
                self.variables['black_judge_defendant'] = pape_df2['defendant'].tolist()
                self.variables['black_judge_otherparty'] = pape_df2['other_party'].tolist()

        # black_exec_cnt
        # 执行公开信息
        if not public_df.empty:
            # public_df1 = public_df[~public_df.execute_status.str.contains('已结案')].drop_duplicates(subset=['execute_case_no'])
            public_df1 = public_df.drop_duplicates(subset=['execute_case_no'])
            if not public_df1.empty:
                self.variables['black_exec_cnt'] = public_df1.shape[0]
                public_df2 = public_df1.sort_values(by='filing_time', ascending=False)
                public_df2['filing_time'] = public_df2['filing_time'].apply(
                    lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_exec_name'] = public_df2['name'].to_list()
                self.variables['black_exec_authority'] = public_df2['execute_court'].to_list()
                self.variables['black_exec_case_no'] = public_df2['execute_case_no'].to_list()
                self.variables['black_exec_date'] = public_df2['filing_time'].to_list()
                self.variables['black_exec_content'] = public_df2['execute_content'].to_list()
                self.variables['black_exec_type'] = public_df2['execute_status'].to_list()
                self.variables['black_exec_title'] = public_df2['title'].tolist()
                self.variables['black_exec_amt'] = public_df2['execution_amt'].tolist()
                self.variables['black_exec_status'] = public_df2['execute_status'].tolist()
                self.variables['black_exec_end_date'] = public_df2['final_date'].tolist()
                self.variables['black_exec_balance'] = public_df2['non_performance_amt'].tolist()

        # black_illegal_cnt
        # 行政违法信息
        if not violation_df.empty:
            violation_df1 = violation_df[(violation_df.case_no != '') | (violation_df.illegalreason != '')][
                ['name', 'illegalreason', 'specific_date', 'case_no', 'filing_time', 'title', 'trial_authority',
                 'involved_amt', 'execution_result', 'date_type']].drop_duplicates()
            if not violation_df1.empty:
                violation_df1 = violation_df1.sort_values(by='filing_time', ascending=False)
                violation_df1['filing_time'] = violation_df1['filing_time'].apply(
                    lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_illegal_cnt'] = violation_df1.shape[0]
                self.variables['black_illegal_name'] = violation_df1['name'].to_list()
                self.variables['black_illegal_reason'] = violation_df1['illegalreason'].to_list()
                self.variables['black_illegal_datetime'] = violation_df1['filing_time'].to_list()
                self.variables['black_illegal_case_no'] = violation_df1['case_no'].to_list()
                self.variables['black_illegal_title'] = violation_df1['title'].tolist()
                self.variables['black_illegal_org'] = violation_df1['trial_authority'].tolist()
                self.variables['black_illegal_amt'] = violation_df1['involved_amt'].tolist()
                self.variables['black_illegal_result'] = violation_df1['execution_result'].tolist()
                self.variables['black_illegal_date_type'] = violation_df1['date_type'].tolist()

        # 法院核查纳税非正常户
        if not taxable_abnormal_df.empty:
            self.variables['black_abnormal_title'] = taxable_abnormal_df['title'].tolist()
            self.variables['black_abnormal_date'] = taxable_abnormal_df['confirm_date'].tolist()
            self.variables['black_abnormal_name'] = taxable_abnormal_df['name'].tolist()
            self.variables['black_abnormal_code'] = taxable_abnormal_df['id_no'].tolist()
            self.variables['black_abnormal_tax_org'] = taxable_abnormal_df['tax_authority'].tolist()

        # 法院核查欠税名单
        if not tax_arrears_df.empty:
            self.variables['black_tax_title'] = tax_arrears_df['title'].tolist()
            self.variables['black_tax_date'] = tax_arrears_df['taxes_time'].tolist()
            self.variables['black_tax_org'] = tax_arrears_df['tax_authority'].tolist()
            self.variables['black_tax_amt'] = tax_arrears_df['taxes'].tolist()
            self.variables['black_tax_type'] = tax_arrears_df['taxes_type'].tolist()
            self.variables['black_tax_time'] = tax_arrears_df['tax_period'].tolist()

        # 欠款欠费
        if not arrearage_df.empty:
            self.variables['black_arrears_title'] = arrearage_df['title'].tolist()
            self.variables['black_arrears_date'] = arrearage_df['default_date'].tolist()
            self.variables['black_arrears_status'] = arrearage_df['pc_type'].tolist()
            self.variables['black_arrears_reason'] = arrearage_df['default_reason'].tolist()
            self.variables['black_arrears_amt'] = arrearage_df['default_amount'].tolist()

    def clean_variables_bus(self):
        resp = get_query_data(self.full_msg, 'COMPANY', '01')
        ids = []
        basic_dict = {'id': [], 'ent_name': []}
        for i in resp:
            user_name = i.get('name')
            id_card_no = i.get('id_card_no')
            court_df = self._info_com_bus_basic(user_name, id_card_no)
            if not court_df.empty:
                ids.append(int(court_df.loc[0, 'id']))
                basic_dict['id'].append(court_df.loc[0, 'id'])
                basic_dict['ent_name'].append(court_df.loc[0, 'ent_name'])

        if len(ids) == 0:
            return

        illegal_df = self._info_com_bus_illegal(ids)
        # frost_df = self._info_com_bus_shares_frost(ids)
        case_df = self._info_com_bus_case(ids)

        basic_df = pd.DataFrame(basic_dict)
        if not illegal_df.empty:
            self.variables['black_legel_cnt'] = illegal_df.shape[0]
            merge_df = pd.merge(illegal_df, basic_df, left_on='basic_id', right_on='id', how='left')
            merge_df = merge_df.sort_values(by=['ent_name', 'illegal_date_in'], ascending=False)
            merge_df['illegal_date_in'] = merge_df['illegal_date_in'].apply(
                lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
            merge_df['illegal_date_out'] = merge_df['illegal_date_out'].apply(
                lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
            self.variables['black_legal_name'] = merge_df['ent_name'].to_list()
            self.variables['black_legal_cause'] = merge_df['illegal_result_in'].to_list()
            self.variables['black_legal_date'] = merge_df['illegal_date_in'].to_list()
            self.variables['black_legal_org'] = merge_df['illegal_org_name_in'].to_list()
            self.variables['black_legal_clear_cause'] = merge_df['illegal_rresult_out'].to_list()
            self.variables['black_legal_clear_date'] = merge_df['illegal_date_out'].to_list()
            self.variables['black_legal_clear_org'] = merge_df['illegal_org_name_out'].to_list()

        # if not frost_df.empty:
        #     self.variables['black_froz_cnt'] = frost_df.shape[0]
        #     frost_df['black_froz_time'] = frost_df.apply(lambda x:trans_black_froz_time(x['froz_from'],x['froz_to']), axis=1)
        #     frost_df['froz_public_date'] = frost_df['froz_public_date'].apply(
        #         lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
        #     frost_df['thaw_date'] = frost_df['thaw_date'].apply(
        #         lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
        #     frost_df['invalid_time'] = frost_df['invalid_time'].apply(
        #         lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d'))
        #     self.variables['black_froz_name'] = frost_df['froz_ent'].to_list()
        #     self.variables['black_froz_role'] = frost_df['jhi_role'].to_list()
        #     self.variables['black_froz_status'] = frost_df['judicial_froz_state'].to_list()
        #     self.variables['black_froz_execute_no'] = frost_df['froz_doc_no'].to_list()
        #     self.variables['black_froz_amt'] = frost_df['judicial_fro_am'].to_list()
        #     self.variables['black_froz_inv'] = frost_df['judicial_inv'].to_list()
        #     self.variables['black_froz_auth'] = frost_df['froz_auth'].to_list()
        #     self.variables['black_froz_public_date'] = frost_df['froz_public_date'].to_list()
        #     self.variables['black_froz_time'] = frost_df['black_froz_time'].to_list()
        #     self.variables['black_froz_thaw_date'] = frost_df['thaw_date'].to_list()
        #     self.variables['black_froz_invalid_date'] = frost_df['invalid_time'].to_list()
        #     self.variables['black_froz_invalid_reason'] = frost_df['invalid_reason'].to_list()

        if not case_df.empty:
            self.variables['black_punish_cnt'] = case_df.shape[0]
            self.variables['black_punish_name'] = case_df['ent_name'].to_list()
            self.variables['black_punish_reason'] = case_df['illegact_type_name'].to_list()
            self.variables['black_punish_datetime'] = case_df['pen_deciss_date'].apply(
                lambda x: '' if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['black_punish_case_no'] = case_df['pendec_no'].to_list()

    def transform(self):
        self.clean_variables_court()
        # self.clean_variables_bus()
