from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def get_value(df, vari_name):
    if df is not None and len(df) > 0:
        temp_df = df.loc[df.variable_name == vari_name]
        if temp_df.shape[0] > 0:
            vari_value = temp_df.variable_value.values[0]
            if len(vari_value) > 0:
                return float(vari_value)
            else:
                return -999.0
        else:
            return -999.0
    else:
        return -999.0


class T15001(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.df = None
        self.variables = {
            'ae_m12_id_nbank_allnum': 0,
            'ae_m12_cell_nbank_allnum': 0,
            'ae_m12_cell_nbank_else_cons_orgnum': 0,
            'ae_m12_id_nbank_else_cons_orgnum': -999,
            'ae_m12_cell_orgnum_d': -999,
            'ae_m12_id_nbank_else_rel_allnum': -999,
            'ae_m6_cell_min_monnum': -999,
            'ae_d15_cell_allnum_d': -999,
            'ae_m3_cell_nbank_else_cons_allnum': -999,
            'ae_m6_cell_bank_region_allnum': -999,
            'ae_m12_cell_bank_max_monnum': -999,
            'ae_m1_id_bank_region_orgnum': -999,
            'ae_m6_id_bank_weekend_orgnum': -999,
            'ae_m12_id_nbank_tot_mons': -999
        }

    def transform(self):
        self.query_data()
        # self.get_variable_info()
        self.get_variable_info_2()

    def query_data(self):
        info_certification = """
                    SELECT * FROM info_loan_intention_data where basic_id in  
                    (select id from info_common_basic where user_name = %(user_name)s AND id_card_no = %(id_card_no)s
                    ORDER BY id  DESC LIMIT 1)
                """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        self.df = df

    def get_variable_info_2(self):
        df = self.df.copy()
        self.variables['ae_m12_id_nbank_allnum'] = get_value(df, 'als_m12_id_nbank_allnum')
        self.variables['ae_m12_cell_nbank_allnum'] = get_value(df, 'als_m12_cell_nbank_allnum')
        self.variables['ae_m12_cell_nbank_else_cons_orgnum'] = get_value(df, 'als_m12_cell_nbank_cons_orgnum')
        self.variables['ae_m12_id_nbank_else_cons_orgnum'] = get_value(df, 'als_m12_id_nbank_cons_orgnum')
        # als_m12_cell_bank_orgnum	按手机号查询，近12个月在银行机构申请机构数 + als_m12_cell_nbank_orgnum	按手机号查询，近12个月在非银机构申请机构数
        als_m12_cell_bank_orgnum = get_value(df, 'als_m12_cell_bank_orgnum')
        als_m12_cell_nbank_orgnum = get_value(df, 'als_m12_cell_nbank_orgnum')
        if als_m12_cell_bank_orgnum != -999.0 and als_m12_cell_nbank_orgnum != -999.0:
            ae_m12_cell_orgnum_d = als_m12_cell_bank_orgnum + als_m12_cell_nbank_orgnum
        elif als_m12_cell_bank_orgnum == -999.0 and als_m12_cell_nbank_orgnum != -999.0:
            ae_m12_cell_orgnum_d = als_m12_cell_nbank_orgnum
        elif als_m12_cell_bank_orgnum != -999.0 and als_m12_cell_nbank_orgnum == -999.0:
            ae_m12_cell_orgnum_d = als_m12_cell_bank_orgnum
        else:
            ae_m12_cell_orgnum_d = -999.0
        self.variables['ae_m12_cell_orgnum_d'] = ae_m12_cell_orgnum_d
        self.variables['ae_m12_id_nbank_else_rel_allnum'] = get_value(df, 'als_m12_id_rel_allnum')
        self.variables['ae_m6_cell_min_monnum'] = get_value(df, 'als_m6_cell_min_monnum')
        # als_d15_cell_bank_allnum	按手机号查询，近15天在银行机构申请次数 + als_d15_cell_nbank_allnum	按手机号查询，近15天在非银机构申请次数
        als_d15_cell_bank_allnum = get_value(df, 'als_d15_cell_bank_allnum')
        als_d15_cell_nbank_allnum = get_value(df, 'als_d15_cell_nbank_allnum')
        if als_d15_cell_bank_allnum != -999.0 and als_d15_cell_nbank_allnum != -999.0:
            ae_d15_cell_allnum_d = als_d15_cell_bank_allnum + als_d15_cell_nbank_allnum
        elif als_d15_cell_bank_allnum == -999.0 and als_d15_cell_nbank_allnum != -999.0:
            ae_d15_cell_allnum_d = als_d15_cell_nbank_allnum
        elif als_d15_cell_bank_allnum != -999.0 and als_d15_cell_nbank_allnum == -999.0:
            ae_d15_cell_allnum_d = als_d15_cell_bank_allnum
        else:
            ae_d15_cell_allnum_d = -999.0
        self.variables['ae_d15_cell_allnum_d'] = ae_d15_cell_allnum_d
        self.variables['ae_m3_cell_nbank_else_cons_allnum'] = get_value(df, 'als_m3_cell_nbank_cons_allnum')
        self.variables['ae_m6_cell_bank_region_allnum'] = get_value(df, 'als_m6_cell_bank_allnum')
        self.variables['ae_m12_cell_bank_max_monnum'] = get_value(df, 'als_m12_cell_bank_max_monnum')
        self.variables['ae_m1_id_bank_region_orgnum'] = get_value(df, 'als_m1_id_bank_orgnum')
        self.variables['ae_m6_id_bank_weekend_orgnum'] = get_value(df, 'als_m6_id_bank_week_orgnum')
        self.variables['ae_m12_id_nbank_tot_mons'] = get_value(df, 'als_m12_id_nbank_tot_mons')

    # 20230417 百融3.0借贷意向验证数据，修改为用2.0数据
    def get_variable_info(self):
        df = self.df.copy()
        self.variables['ae_m12_id_nbank_allnum'] = get_value(df, 'ae_m12_id_nbank_allnum')
        self.variables['ae_m12_cell_nbank_allnum'] = get_value(df, 'ae_m12_cell_nbank_allnum')
        self.variables['ae_m12_cell_nbank_else_cons_orgnum'] = get_value(df, 'ae_m12_cell_nbank_else_cons_orgnum')
        self.variables['ae_m12_id_nbank_else_cons_orgnum'] = get_value(df, 'ae_m12_id_nbank_else_cons_orgnum')
        self.variables['ae_m12_cell_orgnum_d'] = get_value(df, 'ae_m12_cell_orgnum_d')
        self.variables['ae_m12_id_nbank_else_rel_allnum'] = get_value(df, 'ae_m12_id_nbank_else_rel_allnum')
        self.variables['ae_m6_cell_min_monnum'] = get_value(df, 'ae_m6_cell_min_monnum')
        self.variables['ae_d15_cell_allnum_d'] = get_value(df, 'ae_d15_cell_allnum_d')
        self.variables['ae_m3_cell_nbank_else_cons_allnum'] = get_value(df, 'ae_m3_cell_nbank_else_cons_allnum')
        self.variables['ae_m6_cell_bank_region_allnum'] = get_value(df, 'ae_m6_cell_bank_region_allnum')
        self.variables['ae_m12_cell_bank_max_monnum'] = get_value(df, 'ae_m12_cell_bank_max_monnum')
        self.variables['ae_m1_id_bank_region_orgnum'] = get_value(df, 'ae_m1_id_bank_region_orgnum')
        self.variables['ae_m6_id_bank_weekend_orgnum'] = get_value(df, 'ae_m6_id_bank_weekend_orgnum')
