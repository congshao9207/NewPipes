import json

import pandas as pd

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonUnionPortrait(TransFlow):

    def process(self):
        self.read_u_pt()

    def read_u_pt(self):
        sql = """
            select *
            from trans_u_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})
        df.drop(columns = ['id','apply_no','create_time','update_time'],inplace = True)
        df['analyse_start_time'] = df['analyse_start_time'].astype(str)
        df['analyse_end_time'] = df['analyse_end_time'].astype(str)

        sql1 = '''
            select report_req_no,min(trans_date) as loan_anal_start_time
            from trans_u_flow_portrait
            where report_req_no = %(report_req_no)s
            '''
        df1 = sql_to_df(sql=sql1,
                       params={"report_req_no": self.reqno})

        df1['loan_anal_start_time'] = df1['loan_anal_start_time'].astype(str)

        df = pd.merge(df,df1,how='left', on='report_req_no' ).drop(columns = 'report_req_no')

        self.variables["trans_u_portrait"] = {}

        value = json.loads(df.to_json(orient='records'))
        if value is not None and len(value) > 0:
            value = json.loads(df.to_json(orient='records'))
            self.variables["trans_u_portrait"] = value[0]
