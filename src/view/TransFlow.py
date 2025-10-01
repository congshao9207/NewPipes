from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df


class TransFlow(ModuleProcessor):

    def __init__(self):
        super().__init__()
        # self.db = self._db()
        self.account_id = None
        self.cusName = None
        self.bankName = None
        self.bankAccount = None
        self.idno = None
        self.reqno = None
        self.appAmt = None
        self.previous_out_apply_no = None
        self.industry_name = ''
        self.guarantor_list = []
        self.variables = {}

    def init(self, variables, user_name, id_card_no, origin_data, cached_data):
        super().init(variables, user_name, id_card_no, origin_data, cached_data)
        guarantor_temp = []
        for i in self.cached_data.get("input_param"):
            if i["relation"] == "MAIN":
                self.cusName = i["name"]
                if len(i["extraParam"]["accounts"]) > 0:
                    self.bankName = i["extraParam"]["accounts"][0]["bankName"]
                    self.bankAccount = i["extraParam"]["accounts"][0]["bankAccount"]
                self.idno = i["idno"]
                self.reqno = i["preReportReqNo"]
                self.appAmt = i["applyAmo"]
                self.industry_name = \
                    i['extraParam']['industryName'] if i['extraParam']['industryName'] not in ['', None] else ''

                sql = """
                    select account_id
                    from trans_apply ap
                    left join 
                    trans_account ac
                    on ap.account_id = ac.id
                    where ap.report_req_no = %(report_req_no)s 
                    and ap.id_card_no = %(id_card_no)s 
                    and ac.account_no = %(account_no)s
                """
                account_df = sql_to_df(sql=sql,
                                       params={"report_req_no": self.reqno,
                                               "id_card_no": self.idno,
                                               "account_no": self.bankAccount})
                if not account_df.empty:
                    self.account_id = int(account_df.values[0][0])

            if i["relation"] in ["GUARANTOR","GUAR_PER","GUAR_ENT"]:
                guarantor_temp.append(i["name"])

        self.previous_out_apply_no = self.cached_data.get("previous_out_apply_no")

        sql = '''
            select distinct related_name as name
            from trans_apply
            where report_req_no = %(report_req_no)s
            and relationship not in ('U_PER_GUARANTOR_PERSONAL','U_PER_GUARANTOR_COMPANY','U_COM_GUARANTOR_PERSONAL','U_COM_GUARANTOR_COMPANY')
        '''
        relation_list = sql_to_df(sql=sql,
                                  params={"report_req_no": self.reqno})['name'].tolist()

        for name in guarantor_temp:
            if name not in relation_list:
                self.guarantor_list.append(name)

    def flow_account_clean(self, account_no):
        account_no_lenth = len(account_no)
        if account_no_lenth >= 7:
            return "***" + account_no[-4:]
        if account_no_lenth >= 11:
            return account_no[:4] + "***" + account_no[-4:]
            # 7位以内的未脱敏处理， 一般不会出现此类银行卡号
        return account_no
