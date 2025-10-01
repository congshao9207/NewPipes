from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def transform_on_line_days(obj):
    if obj == 10:
        return 89
    elif obj == 11:
        return 180
    elif obj == 12:
        return 360
    elif obj == 13:
        return 720
    elif obj == 14:
        return 721
    else:
        return 360


class T05001(Transformer):
    """
    手机状态
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_check': 1,  # 手机号实名认证不一致
            'phone_on_line_state': "",  # 手机号状态非正常使用
            'phone_on_line_days': 0,  # 手机号在网时长短
            'ps_name_id': 0  # 姓名与身份证号不匹配

        }

    # 手机号三要素验证
    def _info_certification_df(self):
        info_certification = """
            SELECT phone, result FROM info_certification 
            WHERE certification_type = 'ID_NAME_MOBILE' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone=%(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "phone": self.phone,
                               "id_card_no": self.id_card_no})
        return df

    # 在网时长
    def _info_on_line_duration_df(self):
        info_certification = """
            SELECT phone, on_line_days FROM info_on_line_duration 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone=%(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "phone": self.phone,
                               "id_card_no": self.id_card_no})
        return df

    # 手机号状态
    def _info_on_line_state_df(self):
        info_certification = """
            SELECT phone, mobile_state FROM info_on_line_state 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone=%(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "phone": self.phone,
                               "id_card_no": self.id_card_no})
        return df

    def _phone_check(self, df=None):
        """
        手机实名状态
        :param user_name:
        :param id_card_no:
        :return: 如果匹配返回 0， 不匹配返回 1
        """
        if df is not None and 'result' in df.columns:
            # result is True
            if len(df) == 1 and df['result'][0] == 1:
                self.variables['phone_check'] = 0

    # 手机号状态非正常
    def _phone_on_line_state(self, df=None):
        if df is not None and 'mobile_state' in df.columns:
            if len(df) == 1:
                if pd.notna(df['mobile_state'][0]):
                    self.variables['phone_on_line_state'] = df['mobile_state'][0]

    # 手机号在网时长
    def _phone_on_line_days(self, df=None):
        if df is not None and 'on_line_days' in df.columns:
            if len(df) == 1:
                on_line_days_type = df['on_line_days'][0]
                self.variables['phone_on_line_days'] = transform_on_line_days(on_line_days_type)

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._phone_check(self._info_certification_df())
        self._phone_on_line_state(self._info_on_line_state_df())
        self._phone_on_line_days(self._info_on_line_duration_df())
