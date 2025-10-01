import json
from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df
from fileparser.trans_flow.trans_config import UP_DOWNSTREAM_THRESHOLD, UNUSUAL_OPPO_NAME
import re


class JsonUnionCounterpartyPortrait(TransFlow):
    """
        主要交易对手模块信息
        author:汪腾飞
        created_time:20200708
        updated_time_v1:
    """

    def process(self):
        self.read_u_counterparty_pt()

    def connect_json(self, json_data):
        string = ''
        for text in json_data:
            string += text
        return string[:-1]

    # 获取排名第n名交易对手的交易占比
    def get_topn_trans_amt_proportion(self, json_data, key):
        topn_amt_order_list = json_data[key]
        for data in topn_amt_order_list:
            month = data['month']
            trans_amt_proportion = data['trans_amt_proportion']
            if month == "汇总":
                return trans_amt_proportion

    def read_u_counterparty_pt(self):

        sql = """
            select *
            from trans_u_counterparty_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})

        df.drop(columns=['id', 'apply_no', 'report_req_no', 'create_time', 'update_time'], inplace=True)
        df = df[~df.opponent_name.astype(str).str.contains(UNUSUAL_OPPO_NAME[0])]

        # 删除交易对手中非汉字字符并将ascii码转为汉字
        def op_name_trans(op_name):
            all_ascii_str = re.findall(r'&#\d{2,5};', str(op_name))
            for s in all_ascii_str:
                ss = re.sub(r'\D', '', s)
                op_name.replace(s, chr(int(ss)))
            op_name = re.sub(r'[^\u4e00-\u9fa5]', '', str(op_name))
            return op_name
        df['opponent_name'] = df['opponent_name'].apply(op_name_trans)
        income_df = df[(pd.notnull(df.income_amt_order)) & (df['opponent_name'] != '')]
        expense_df = df[(pd.notnull(df.expense_amt_order)) & (df['opponent_name'] != '')]
        total_income, total_expense = 0, 0

        # 上下游汇总交易金额小于UP_DOWNSTREAM_THRESHOLD，进行剔除
        for i, tmp_df in enumerate([income_df, expense_df]):
            tot_df = tmp_df[(tmp_df.month == '汇总')]
            if tot_df.shape[0] > 0:
                total_amt = abs(tot_df.trans_amt.sum()) / tot_df.trans_amt_proportion.sum()
            else:
                total_amt = 0
            tmp_name_list = tmp_df[pd.notnull(tmp_df.opponent_name) & (tmp_df.month == '汇总')
                                   & (abs(tmp_df.trans_amt) < UP_DOWNSTREAM_THRESHOLD)].opponent_name.unique().tolist()
            if len(tmp_name_list) > 0:
                total_amt -= abs(tmp_df[(tmp_df.opponent_name.isin(tmp_name_list)) &
                                        (tmp_df.month == '汇总')]['trans_amt'].sum())
                tmp_df.drop(tmp_df[tmp_df.opponent_name.isin(tmp_name_list)].index.tolist(), axis=0, inplace=True)
            if i == 0:
                total_income = total_amt
            else:
                total_expense = total_amt

        #  剔除交易对手既是上游客户，也是下游客户
        for name in df[pd.notnull(df.opponent_name)].opponent_name.unique().tolist():
            if (name in income_df.opponent_name.unique()) and (name in expense_df.opponent_name.unique()):
                income_amt = income_df[(income_df['month'] == '汇总') & (income_df.opponent_name == name)].trans_amt
                expense_amt = expense_df[(expense_df['month'] == '汇总') & (expense_df.opponent_name == name)].trans_amt
                if abs(income_amt.values[0]) >= abs(expense_amt.values[0]):
                    total_expense += expense_df[(expense_df['opponent_name'] == name) &
                                                (expense_df['month'] == '汇总')].trans_amt.sum()
                    expense_df.drop(expense_df[expense_df['opponent_name'] == name].index, 0, inplace=True)
                else:
                    total_income -= income_df[(income_df['opponent_name'] == name) &
                                              (income_df['month'] == '汇总')].trans_amt.sum()
                    income_df.drop(income_df[income_df['opponent_name'] == name].index, 0, inplace=True)
        if expense_df.shape[0] > 0:
            expense_df = expense_df.groupby(['month', 'opponent_name']).agg({
                'income_amt_order': max,
                'expense_amt_order': min,
                'trans_amt': 'sum',
                'trans_month_cnt': max,
                'trans_cnt': max,
                'trans_mean': 'sum',
                'trans_amt_proportion': 'sum',
                'trans_gap_avg': 'sum',
                'income_amt_proportion': 'sum'
            }).reset_index()
            expense_df = expense_df.sort_values(['month', 'trans_amt'], ascending=[False, True])
        if income_df.shape[0] > 0:
            income_df = income_df.groupby(['month', 'opponent_name']).agg({
                'income_amt_order': max,
                'expense_amt_order': min,
                'trans_amt': 'sum',
                'trans_month_cnt': max,
                'trans_cnt': max,
                'trans_mean': 'sum',
                'trans_amt_proportion': 'sum',
                'trans_gap_avg': 'sum',
                'income_amt_proportion': 'sum'
            }).reset_index()
            income_df = income_df.sort_values(['month', 'trans_amt'], ascending=[False, False])
        #  重置进出账排名
        col_name = 'income_amt_order'
        for i, temp_df in enumerate([income_df, expense_df]):
            total_amt = total_income if i == 0 else total_expense
            count = 1
            for name in temp_df[pd.notnull(temp_df.opponent_name)].opponent_name.unique().tolist():
                temp_df.loc[temp_df.opponent_name == name, col_name] = str(count)
                temp_df.loc[(temp_df.opponent_name == name) & (temp_df.month == '汇总'), 'trans_amt_proportion'] = \
                    abs(temp_df[(temp_df.opponent_name == name) & (temp_df.month == '汇总')].trans_amt.sum()) \
                    / total_amt if total_amt > 0 else 0
                count += 1
            col_name = 'expense_amt_order'

        max_month = max([int(_) for _ in df.month.unique().tolist() if _.isnumeric()]) if not df.empty else 0
        month_str = [str(_ + 1) for _ in range(max_month)]
        month_str.append('汇总')
        full_df = pd.DataFrame({'month': month_str})
        income_json = []
        for income_order in income_df.income_amt_order.unique().tolist():
            if not income_order.isnumeric() or int(income_order) > 10:
                continue
            order_df = pd.merge(full_df, income_df[income_df.income_amt_order == income_order], how='left', on='month')
            order_df['trans_amt'].fillna(0., inplace=True)
            order_df['trans_cnt'].fillna(0, inplace=True)
            order_df['income_amt_order'].fillna(income_order, inplace=True)
            income_json.append("\"" + income_order + "\":" +
                               order_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        expense_json = []
        for expense_order in expense_df.expense_amt_order.unique().tolist():
            if not expense_order.isnumeric() or int(expense_order) > 10:
                continue
            order_df = pd.merge(full_df, expense_df[expense_df.expense_amt_order == expense_order],
                                how='left', on='month')
            order_df['trans_amt'].fillna(0., inplace=True)
            order_df['trans_cnt'].fillna(0, inplace=True)
            order_df['expense_amt_order'].fillna(expense_order, inplace=True)
            expense_json.append("\"" + expense_order + "\":" +
                                order_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        trans_u_counterparty_portrait_dict = json.loads("{\"income_amt_order\":{" +
                                                        self.connect_json(income_json) +
                                                        "},\"expense_amt_order\":{" +
                                                        self.connect_json(expense_json) + "}}")
        income_amt_order = trans_u_counterparty_portrait_dict['income_amt_order']
        expense_amt_order = trans_u_counterparty_portrait_dict['expense_amt_order']

        op_mapping = {1: '最大', 2: '前两大', 3: '前三大', 4: '前四大', 5: '前五大'}
        if len(income_json) > 0:
            max_income_order = max([int(_) for _ in income_amt_order.keys() if _.isnumeric()])
            # 下游客户专家经验
            # 获取下游客户前5大交易对手交易占比
            income_amt_top_trans_amt_proportion_list = [
                self.get_topn_trans_amt_proportion(income_amt_order, str(_))
                for _ in range(1, 1 + min(max_income_order, 5))]
            income_amt_top_trans_amt_proportion_list = [_ for _ in income_amt_top_trans_amt_proportion_list if
                                                        _ is not None]
            total_income_amt_proportion = sum(income_amt_top_trans_amt_proportion_list)
            if total_income_amt_proportion > 1:
                total_income_amt_proportion = 1
            if total_income_amt_proportion >= 0.5:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户比较集中，建议收集相关业务合同"
            elif total_income_amt_proportion <= 0.2:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户比较分散"
            else:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户构成无异常"
            income_amt_order['risk_tips'] = income_amt_risk_tips
            self.variables['trans_report_overview']['business_info']['downstream_customers'][
                'risk_tips'] = income_amt_risk_tips

        if len(expense_json) > 0:
            max_expense_order = max(map(int, expense_amt_order.keys()))
            # 上游客户专家经验
            # 获取上游客户前5大交易对手交易占比
            expense_amt_top_trans_amt_proportion_list = [
                self.get_topn_trans_amt_proportion(expense_amt_order, str(_))
                for _ in range(1, 1 + min(5, max_expense_order))]
            expense_amt_top_trans_amt_proportion_list = [_ for _ in expense_amt_top_trans_amt_proportion_list if
                                                         _ is not None]
            total_expense_amt_proportion = sum(expense_amt_top_trans_amt_proportion_list)
            if total_expense_amt_proportion > 1:
                total_expense_amt_proportion = 1
            if total_expense_amt_proportion >= 0.5:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户比较集中，建议收集相关业务合同"
            elif total_expense_amt_proportion <= 0.2:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户比较分散"
            else:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户构成无异常"

            expense_amt_order['risk_tips'] = expense_amt_risk_tips
            self.variables['trans_report_overview']['business_info']['upstream_customers'][
                'risk_tips'] = expense_amt_risk_tips

        self.variables["trans_u_counterparty_portrait"] = {"income_amt_order": income_amt_order,
                                                           "expense_amt_order": expense_amt_order}
