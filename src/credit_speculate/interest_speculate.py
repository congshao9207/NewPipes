import numpy as np


def irr(values):
    res = np.roots(values[::-1])
    mask = (res.imag == 0) & (res.real > 0)
    if not mask.any():
        return np.nan
    res = res[mask].real
    rate = 1 / res - 1
    rate = rate.item(np.argmin(np.abs(rate)))
    return rate


class InterestSpeculate:

    def __init__(self, params):
        self.params = params
        self.params.update({
            'nomi_rate': None,
            'real_rate': None,
            'repay_type': '无法推算',
            'is_end': 0
        })

    def main(self):
        freq = self.params['freq']
        self.non_month_quar_interest() if freq == 2 else self.quar_interest() \
            if freq == 3 else self.month_interest()

    def non_month_quar_interest(self):
        """
        非按月按季归还
        """
        org_type = self.params['org_type']
        real_amt = self.params['real_amt']
        balance = self.params['balance']
        amt = self.params['amt']
        month_period = self.params['month_period']
        quar_period = self.params['quar_period']
        # 当月实还款大于已还本金,且按月计息
        rate1 = (real_amt + balance - amt) * 360 / month_period / amt if month_period > 0 and amt > 0 else -1
        # 当月实还款大于已还本金,且按季计息
        rate2 = (real_amt + balance - amt) * 360 / quar_period / amt if quar_period > 0 and amt > 0 else -1
        # 当月实还款大于借款本金,且按月计息
        rate3 = (real_amt - amt) * 360 / month_period / amt if month_period > 0 and amt > 0 else -1
        # 当月实还款大于借款本金,且按季计息
        rate4 = (real_amt - amt) * 360 / quar_period / amt if quar_period > 0 and amt > 0 else -1
        for i, rate in enumerate([rate1, rate2, rate3, rate4]):
            if -0.01 < rate <= 0.28 + 0.12 * org_type:
                self.params['nomi_rate'] = max(rate, 0)
                self.params['real_rate'] = max(rate, 0)
                self.params['repay_type'] = 'XB_MONTH' if i in [0, 2] else 'XB_QUARTER'
                self.params['is_end'] = 1 if balance == 0 else 0
                return

    def quar_interest(self):
        """
        按季归还
        """
        org_type = self.params['org_type']
        real_amt = self.params['real_amt']
        should_amt = self.params['should_amt']
        balance = self.params['balance']
        amt = self.params['amt']
        quar_period = self.params['quar_period']
        # 当月实还款全部为当季利息
        rate1 = real_amt / quar_period * 360 / amt if quar_period > 0 and amt > 0 else -1
        # 当月应还款全部为当季利息
        rate2 = should_amt / quar_period * 360 / amt if quar_period > 0 and amt > 0 else -1
        # 当月实还款大于借款本金,且超出部分全部为当季利息
        rate3 = (real_amt - amt) / quar_period * 360 / amt if quar_period > 0 and amt > 0 else -1
        # 当月应还款大于借款本金,且超出部分全部为当季利息
        rate4 = (should_amt - amt) / quar_period * 360 / amt if quar_period > 0 and amt > 0 else -1
        for i, rate in enumerate([rate1, rate2, rate3, rate4]):
            if -0.01 < rate <= 0.28 + 0.12 * org_type:
                self.params['nomi_rate'] = max(rate, 0)
                self.params['real_rate'] = max(rate, 0)
                self.params['repay_type'] = 'XB_QUARTER'
                self.params['is_end'] = 1 if balance == 0 else 0
                return

    def month_interest(self):
        """
        按月归还
        """
        org_type = self.params['org_type']
        real_amt = self.params['real_amt']
        should_amt = self.params['should_amt']
        balance = self.params['balance']
        amt = self.params['amt']
        month_period = self.params['month_period']
        terms = self.params['terms']
        res_terms = self.params['res_terms']

        # 还款方式为等额本息(按月)且每月还款额为当月实还款的名义年利率
        nominal_rate3 = (real_amt * terms - amt) / amt * 12 / terms if terms > 0 and amt > 0 else -1
        # 还款方式为等额本息(按月)且每月还款额为当月应还款的名义年利率
        nominal_rate4 = (should_amt * terms - amt) / amt * 12 / terms if terms > 0 and amt > 0 else -1
        # 当月实还款全部为当月利息
        rate1 = real_amt / month_period * 360 / amt if month_period > 0 and amt > 0 else -1
        # 当月应还款全部为当月利息
        rate2 = should_amt / month_period * 360 / amt if month_period > 0 and amt > 0 else -1
        # 当月实还款为等额本息(按月)每期还款额(借款金额推算)
        rate3 = irr([-amt] + [real_amt] * terms) * 12 if terms > 0 else -1
        # 当月应还款为等额本息(按月)每期还款额(借款金额推算)
        rate4 = irr([-amt] + [should_amt] * terms) * 12 if terms > 0 else -1
        # 当月实还款为等额本金(按月)当期还款额
        rate5 = (real_amt - amt / terms) / month_period * 360 / (balance + amt / terms) \
            if terms > 0 and amt > 0 and month_period > 0 else -1
        # 当月应还款为等额本金(按月)当期还款额
        rate6 = (should_amt - amt / terms) / month_period * 360 / (balance + amt / terms) \
            if terms > 0 and amt > 0 and month_period > 0 else -1
        # 当月实还款为定期还本本息和
        rate7 = (real_amt + balance - amt) / month_period * 360 / amt if month_period > 0 and amt > 0 else -1
        # 当月应还款为定期还本本息和
        rate8 = (should_amt + balance - amt) / month_period * 360 / amt if month_period > 0 and amt > 0 else -1
        # 当月实还款为等额本息(按月)每期还款额(剩余本金推算)
        rate9 = irr([-balance] + [real_amt] * res_terms) * 12 if res_terms > 0 else -1
        # 当月应还款为等额本息(按月)每期还款额(剩余本金推算)
        rate10 = irr([-balance] + [should_amt] * res_terms) * 12 if res_terms > 0 else -1
        # 注: 判断0 < balance < amt成立后开始计算下面几个值
        # 以上述推算出的等额本息(按月)利率rate3计算当前剩余本金,并计算与实际剩余本金差异
        value3 = sum([real_amt / (1 + rate3 / 12) ** k for k in range(1, res_terms + 1)])
        diff3 = abs(value3 / balance - 1) if balance > 0 else 1
        # 以上述推算出的等额本息(按月)利率rate4计算当前剩余本金,并计算与实际剩余本金差异
        value4 = sum([should_amt / (1 + rate4 / 12) ** k for k in range(1, res_terms + 1)])
        diff4 = abs(value4 / balance - 1) if balance > 0 else 1
        # 计算还款方式等额本金(按月)时的当前剩余本金,并计算与实际剩余本金差异
        value5 = res_terms * amt / terms if terms > 0 else -1
        diff5 = abs(value5 / balance - 1) if balance > 0 else 1
        diff_min = min(diff3, diff4, diff5)

        # 余额与本金相等，要么为先息后本，要么为贷款第一期还款
        if balance == amt:
            if res_terms == terms:
                for i, rate in enumerate([rate2, rate4, rate6, rate1, rate3, rate5]):
                    if -0.01 < rate <= 0.28 + 0.12 * org_type:
                        self.params['nomi_rate'] = max(rate if i in [0, 3] else nominal_rate4 if i == 1 else
                                                       nominal_rate3 if i == 4 else rate / 2, 0)
                        self.params['real_rate'] = max(rate, 0)
                        self.params['repay_type'] = 'XB_MONTH' if i in [0, 3] else 'D_INTEREST' \
                            if i in [1, 4] else 'D_PRINCIPAL'
                        return
            else:
                for i, rate in enumerate([rate2, rate1]):
                    if -0.01 < rate <= 0.28 + 0.12 * org_type:
                        self.params['nomi_rate'] = max(rate, 0)
                        self.params['real_rate'] = max(rate, 0)
                        self.params['repay_type'] = 'XB_MONTH'
                        return
        # 余额为0，只能为最后一期还款
        elif balance == 0:
            for i, rate in enumerate([rate4, rate6, rate7, rate8]):
                if -0.01 < rate <= 0.28 + 0.12 * org_type:
                    self.params['nomi_rate'] = max(nominal_rate4 if i == 0 else rate / 2 if i == 1 else rate, 0)
                    self.params['real_rate'] = max(rate, 0)
                    self.params['repay_type'] = 'D_INTEREST' if i == 0 else 'D_PRINCIPAL' if i == 1 else 'XB_MONTH'
                    self.params['is_end'] = 1
                    return
        else:
            if diff_min < 0.01:
                if diff5 == diff_min:
                    for i, rate in enumerate([rate5, rate6, rate3, rate4]):
                        if -0.01 < rate <= 0.28 + 0.12 * org_type:
                            self.params['nomi_rate'] = max(rate / 2 if i in [0, 1] else nominal_rate3 if i == 2
                                                           else nominal_rate4, 0)
                            self.params['real_rate'] = max(rate, 0)
                            self.params['repay_type'] = 'D_PRINCIPAL' if i in [0, 1] else 'D_INTEREST_PRINCIPAL'
                            return
                else:
                    nomi_rate = nominal_rate3 if diff_min == diff3 else nominal_rate4
                    real_rate = rate3 if diff_min == diff3 else rate4
                    if -0.01 < real_rate <= 0.28 + 0.12 * org_type:
                        self.params['nomi_rate'] = max(nomi_rate, 0)
                        self.params['real_rate'] = max(real_rate, 0)
                        self.params['repay_type'] = 'D_INTEREST'
                        return
            else:
                if terms - res_terms <= 0:
                    return
                for i, rate in enumerate([rate7, rate8, rate4, rate10, rate3, rate9]):
                    if -0.01 < rate <= 0.28 + 0.12 * org_type:
                        self.params['nomi_rate'] = rate if i in [0, 1] else nominal_rate4 \
                            if i in [2, 3] else nominal_rate3
                        self.params['real_rate'] = max(rate, 0)
                        self.params['repay_type'] = 'CUSTOM_REPAY' if i in [0, 1] else 'D_INTEREST'
                        self.params['repay_period'] = terms - res_terms
                        self.params['period_amt'] = amt - balance
                        return
                # 已还款期数
                minus = terms - res_terms
                for m in range(1, minus):
                    if minus % m != 0:
                        continue
                    # 当月实还款为每m(m>=2)期定期还本的本息和
                    rate11 = (real_amt - (amt - balance) / (minus // m)) / \
                             (balance + (amt - balance) / (minus // m)) / month_period * 360 if month_period > 0 else -1
                    # 当月应还款为每m(m>=2)期定期还本的本息和
                    rate12 = (should_amt - (amt - balance) / (minus // m)) / \
                             (balance + (amt - balance) / (minus // m)) / month_period * 360 if month_period > 0 else -1
                    for rate in [rate11, rate12]:
                        if -0.01 < rate <= 0.28 + 0.12 * org_type:
                            self.params['nomi_rate'] = max(rate, 0)
                            self.params['real_rate'] = max(rate, 0)
                            self.params['repay_type'] = 'CUSTOM_REPAY'
                            self.params['repay_period'] = m
                            self.params['period_amt'] = (amt - balance) / (minus // m)
                            return
