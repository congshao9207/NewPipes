# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, Index, String, Text, Time, text, MetaData
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, LONGTEXT, MEDIUMTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base
from app import logger, sql_db
import pandas as pd
import re

metadata = MetaData(
    schema='gears'
)
Base = declarative_base(metadata=metadata)


def transform_date(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: format(pd.to_datetime(re.sub(r'[^\d:-]', ' ', str(x))), '%Y-%m-%d %H:%M:%S')
                if pd.notna(x) and x != '--' and x != '' else None)


def transform_amount(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(re.sub(r'[^\d.]', '', str(x)))
                                    if re.sub(r'[^\d.]', '', str(x)) != '' else 0)


def transform_count(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(''.join([i for i in str(x) if i.isdigit()]))
                                    if pd.notna(x) and x not in ['--', '', 'N'] else 0)


def transform_org(df, col):
    col1, col2 = col + '1', col + '2'
    df[col] = ''
    if col1 in df.columns:
        df[col] = df[col] + df[col1].astype(str)
    if col2 in df.columns:
        len_list = df[col2].apply(lambda x: len(str(x)) if pd.notna(x) else 0).tolist()
        if len(len_list) > 0 and min(len_list) > 2:
            df[col] = df[col2].astype(str)
        else:
            df[col] = df[col] + df[col2].astype(str)


def choose_one(df, cols):
    for col in cols:
        v1, v2 = col + '1', col + '2'
        df[col] = df.apply(lambda x: x[v1] if v1 in x and pd.notna(x[v1]) else x[v2] if v2 in x else None, axis=1)


def transform_enumerate(df, cols, mappings, non_strs):
    for i, col in enumerate(cols):
        if col in df.columns:
            df[col] = df[col].map(mappings[i], non_strs[i])


def transform_not_null(df, cols, non_strs):
    for i, col in enumerate(cols):
        if col in df:
            df[col].fillna(non_strs[i], inplace=True)
        else:
            df[col] = non_strs[i]


def transform_dict(params, class_name):
    f = eval(class_name + "()")
    col_list = [x for x in dir(f) if not x.startswith("_") and x not in ['id', 'metadata', 'registry']]
    func_str = class_name + '('
    for k, v in params.items():
        if pd.notna(v) and v != '' and k in col_list:
            func_str += k + "='" + re.sub(r'[\'"]', '', str(v)) + "',"
    func_str = func_str[:-1]
    func_str += ')'
    value = eval(func_str)
    return value


def transform_class_str(params, class_name):
    f = eval(class_name + "()")
    col_list = [x for x in dir(f) if not x.startswith("_") and x not in ['id', 'metadata', 'registry']]
    start = f"insert into gears.{f.__tablename__}({','.join(col_list)}) values "

    def sql_values(col_val):
        vals = []
        for col in col_list:
            temp_val = col_val.get(col)
            temp_val = f"'{','.join([str(x) for x in temp_val])}'" if type(temp_val) == list else \
                re.sub(r"(?<![\da-zA-Z]):", '-', f"'{temp_val}'") if pd.notna(temp_val) else 'null'
            vals.append(temp_val)
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


class CreditBaseInfo(Base):
    __tablename__ = 'credit_base_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    credit_type = Column(String(16))
    name = Column(String(20))
    certificate_type = Column(String(16))
    certificate_no = Column(String(32), index=True)
    queryer = Column(String(50))
    query_reason = Column(String(50))
    query_time = Column(DateTime)
    report_time = Column(DateTime)
    create_user = Column(String(10))
    create_time = Column(DateTime)
    create_dep = Column(String(10))
    update_user = Column(String(10))
    update_time = Column(DateTime)
    update_dep = Column(String(10))
    file_path = Column(String(200))
    json_id = Column(String(32))
    report_no = Column(String(32))
    report_type = Column(String(32))


class CreditParseRequest(Base):
    __tablename__ = 'credit_parse_request'

    id = Column(BIGINT(20), primary_key=True)
    app_id = Column(String(32))
    attachment_id = Column(BIGINT(20))
    out_req_no = Column(String(32), index=True)
    out_apply_no = Column(String(32))
    provider = Column(String(50))
    credit_type = Column(String(50))
    credit_version = Column(String(50))
    biz_req_no = Column(String(32))
    report_id = Column(String(32))
    resp_data = Column(LONGTEXT)
    process_status = Column(String(50))
    process_memo = Column(String(512))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class EcreditAssetsOutline(Base):
    __tablename__ = 'ecredit_assets_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    dispose_account_num = Column(INTEGER(11))
    dispose_balance = Column(DECIMAL(16, 4))
    last_dispose_date = Column(Date)
    advance_account_num = Column(INTEGER(11))
    advance_balance = Column(DECIMAL(16, 4))
    last_repay_date = Column(Date)
    overdue_principal = Column(DECIMAL(16, 4))
    overdue_interest = Column(DECIMAL(16, 4))
    overdue_amt = Column(DECIMAL(16, 4))


class EcreditBaseInfo(Base):
    __tablename__ = 'ecredit_base_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    report_no = Column(String(32), index=True)
    report_time = Column(DateTime)
    query_org = Column(String(20))
    query_reason = Column(String(20))
    ent_name = Column(String(100))
    credit_code = Column(String(30))
    unify_credit_code = Column(String(20))
    org_code = Column(String(20))
    tax_payer_id_c = Column(String(20))
    tax_payer_id_l = Column(String(20))
    org_credit_code = Column(String(20))
    registered_capital = Column(String(10))
    created_by = Column(String(50))
    created_date = Column(DateTime)
    last_modified_by = Column(String(50))
    last_modified_date = Column(DateTime)


class EcreditCivilJudgments(Base):
    __tablename__ = 'ecredit_civil_judgments'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    court = Column(String(200))
    case_no = Column(String(200))
    case_subject = Column(String(200))
    filing_date = Column(Date)
    target = Column(String(200))
    target_amt = Column(DECIMAL(16, 4))
    settle_type = Column(String(200))
    settle_result = Column(String(255))
    effective_date = Column(Date)
    lawsuit_standi = Column(String(200))
    judge_procedure = Column(String(200))


class EcreditControlsPerson(Base):
    __tablename__ = 'ecredit_controls_person'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    cust_name = Column(String(40))
    cert_type = Column(String(32))
    cert_no = Column(String(30))
    update_date = Column(Date)


class EcreditCreditBiz(Base):
    __tablename__ = 'ecredit_credit_biz'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    settle_status = Column(String(20), nullable=False)
    account_type = Column(String(20), nullable=False)
    account_org = Column(String(20))
    biz_type = Column(String(20))
    category = Column(String(20))
    account_num = Column(INTEGER(11))
    balance = Column(DECIMAL(16, 4))
    overdue_amt = Column(DECIMAL(16, 4))
    overdue_principal = Column(DECIMAL(16, 4))
    end_30_bal = Column(DECIMAL(16, 4))
    end_60_bal = Column(DECIMAL(16, 4))
    end_90_bal = Column(DECIMAL(16, 4))
    end_more_90_bal = Column(DECIMAL(16, 4))
    total_bal = Column(DECIMAL(16, 4))
    discount_amt = Column(DECIMAL(16, 4))


class EcreditCreditInfo(Base):
    __tablename__ = 'ecredit_credit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    credit_no = Column(String(20))
    account_org = Column(String(20))
    amt_type = Column(String(20))
    circle_status = Column(String(1))
    loan_date = Column(Date)
    end_date = Column(Date)
    currency = Column(String(10))
    amount = Column(DECIMAL(16, 4))
    used_amt = Column(DECIMAL(16, 4))
    jhi_quota = Column(DECIMAL(16, 4))
    quota_no = Column(String(20))
    stats_date = Column(Date)


class EcreditCreditOutline(Base):
    __tablename__ = 'ecredit_credit_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    acyclic_total_amt = Column(DECIMAL(16, 4))
    acyclic_used_amt = Column(DECIMAL(16, 4))
    acyclic_surplus_amt = Column(DECIMAL(16, 4))
    cycle_total_amt = Column(DECIMAL(16, 4))
    cycle_used_amt = Column(DECIMAL(16, 4))
    cycle_surplus_amt = Column(DECIMAL(16, 4))


class EcreditDebtHistor(Base):
    __tablename__ = 'ecredit_debt_histor'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    stats_date = Column(String(20))
    account_num = Column(INTEGER(11))
    overdue_account_num = Column(INTEGER(11))
    overdue_amt = Column(DECIMAL(16, 4))
    p_overdue_account_num = Column(INTEGER(11))
    overdue_principal = Column(DECIMAL(16, 4))
    status_type = Column(String(20))
    balance = Column(DECIMAL(16, 4))


class EcreditDraftLc(Base):
    __tablename__ = 'ecredit_draft_lc'

    id = Column(BIGINT(20), primary_key=True)
    biz_id = Column(BIGINT(20), nullable=False, index=True)
    settle_status = Column(String(20))
    account_org = Column(String(20))
    biz_type = Column(String(20))
    category = Column(String(20))
    account_no = Column(String(20))
    loan_date = Column(Date)
    end_date = Column(Date)
    currency = Column(String(10))
    amount = Column(DECIMAL(16, 4))
    counter_guarantee_type = Column(String(20))
    deposit_rate = Column(DECIMAL(16, 4))
    balance = Column(DECIMAL(16, 4))
    exposure_bal = Column(String(20))
    credit_no = Column(String(20))
    stats_date = Column(Date)
    finish_date = Column(Date)
    advanced_status = Column(String(1))
    report_id = Column(String(32), nullable=False)
    loan_guarantee_type = Column(String(20))


class EcreditFinancialSheet(Base):
    __tablename__ = 'ecredit_financial_sheet'

    id = Column(BIGINT(11), primary_key=True)
    report_id = Column(BIGINT(32), nullable=False)
    report_name = Column(String(32))
    report_type = Column(String(32))
    report_year = Column(String(32))
    report_nature = Column(String(32))
    org_code = Column(String(32))
    report_item = Column(String(128))
    report_value = Column(DECIMAL(16, 2))


class EcreditForceExecution(Base):
    __tablename__ = 'ecredit_force_execution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    court = Column(String(200))
    case_no = Column(String(200))
    case_subject = Column(String(200))
    filing_date = Column(Date)
    target = Column(String(200))
    target_amt = Column(DECIMAL(16, 4))
    settle_date = Column(Date)
    settle_type = Column(String(200))
    case_status = Column(String(200))
    execution_target = Column(String(200))
    execution_target_amt = Column(DECIMAL(16, 4))


class EcreditGeneralizeInfo(Base):
    __tablename__ = 'ecredit_generalize_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    economy_type = Column(String(20))
    org_type = Column(String(20))
    ent_scale = Column(String(20))
    industry = Column(String(20))
    launch_year = Column(String(20))
    reg_cert_duedate = Column(String(50))
    reg_site = Column(String(100))
    office_site = Column(String(100))
    life_status = Column(String(20))


class EcreditHistorPerfo(Base):
    __tablename__ = 'ecredit_histor_perfo'

    id = Column(BIGINT(20), primary_key=True)
    loan_id = Column(BIGINT(20), nullable=False, index=True)
    account_no = Column(String(20))
    account_org = Column(String(20))
    biz_type = Column(String(20))
    stats_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    bal_update_date = Column(Date)
    category = Column(String(20))
    category_date = Column(Date)
    overdue_amt = Column(DECIMAL(16, 4))
    overdue_principal = Column(DECIMAL(16, 4))
    overdue_period = Column(INTEGER(11))
    last_agreed_paydate = Column(Date)
    last_agreed_amt = Column(DECIMAL(16, 4))
    last_actual_paydate = Column(Date)
    last_actual_payamount = Column(DECIMAL(16, 4))
    last_payment_type = Column(String(20))
    report_id = Column(String(32), nullable=False)


class EcreditHouseFund(Base):
    __tablename__ = 'ecredit_house_fund'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    stats_date = Column(String(20))
    first_date = Column(String(20))
    staff_num = Column(INTEGER(11))
    base_amt = Column(DECIMAL(16, 4))
    last_date = Column(Date)
    end_date = Column(String(20))
    pay_status = Column(String(20))
    arrearage_amt = Column(DECIMAL(16, 4))


class EcreditInfoOutline(Base):
    __tablename__ = 'ecredit_info_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    first_loan_year = Column(String(20))
    loan_org_num = Column(INTEGER(11))
    remain_loan_org_num = Column(INTEGER(11))
    first_repay_duty_year = Column(String(20))
    not_loan_account_num = Column(INTEGER(11))
    owing_taxes_num = Column(INTEGER(11))
    judge_num = Column(INTEGER(11))
    enforce_num = Column(INTEGER(11))
    admin_penalty_num = Column(INTEGER(11))
    loan_bal = Column(DECIMAL(16, 4))
    secured_bal = Column(DECIMAL(16, 4))
    loan_recover_bal = Column(DECIMAL(16, 4))
    secured_special_mentioned_bal = Column(DECIMAL(16, 4))
    loan_special_mentioned_bal = Column(DECIMAL(16, 4))
    secured_non_performing_bal = Column(DECIMAL(16, 4))
    loan_non_performing_bal = Column(DECIMAL(16, 4))


class EcreditInvestorInfo(Base):
    __tablename__ = 'ecredit_investor_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    investor_type = Column(String(20))
    investor = Column(String(100))
    cert_type = Column(String(32))
    cert_no = Column(String(30))
    investor_rate = Column(DECIMAL(16, 4))
    update_date = Column(Date)


class EcreditLoan(Base):
    __tablename__ = 'ecredit_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    settle_status = Column(String(20), nullable=False)
    account_type = Column(String(20), nullable=False)
    account_no = Column(String(20))
    account_org = Column(String(20))
    biz_type = Column(String(20))
    loan_date = Column(Date)
    end_date = Column(Date)
    currency = Column(String(10))
    amount = Column(DECIMAL(16, 4))
    occur_type = Column(String(20))
    loan_guarantee_type = Column(String(20))
    balance = Column(DECIMAL(16, 4))
    category = Column(String(20))
    overdue_amt = Column(DECIMAL(16, 4))
    overdue_principal = Column(DECIMAL(16, 4))
    overdue_period = Column(INTEGER(11))
    last_repay_date = Column(Date)
    last_repay_amt = Column(DECIMAL(16, 4))
    last_payment_type = Column(String(20))
    special_briefgv = Column(String(20))
    credit_no = Column(String(20))
    stats_date = Column(Date)
    surplus_repay_period = Column(INTEGER(11))


class EcreditPersonConstituteInfo(Base):
    __tablename__ = 'ecredit_person_constitute_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    cust_position = Column(String(20))
    cust_name = Column(String(40))
    cert_type = Column(String(32))
    cert_no = Column(String(30))
    update_date = Column(Date)


class EcreditPunishment(Base):
    __tablename__ = 'ecredit_punishment'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    org_name = Column(String(100))
    wird_no = Column(String(100))
    illegal_act = Column(String(200))
    penalty_date = Column(Date)
    penalty_decision = Column(String(200))
    penalty_amt = Column(DECIMAL(16, 4))
    execute_status = Column(String(100))
    review_result = Column(String(200))


class EcreditRepayDutyBiz(Base):
    __tablename__ = 'ecredit_repay_duty_biz'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    account_no = Column(String(20))
    duty_type = Column(String(20))
    contract_no = Column(String(20))
    currency = Column(String(10))
    duty_amt = Column(DECIMAL(16, 4))
    org_no = Column(String(20))
    biz_type = Column(String(20))
    biz_date = Column(Date)
    end_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    balance = Column(DECIMAL(16, 4))
    category = Column(String(20))
    overdue_amt = Column(DECIMAL(16, 4))
    overdue_principal = Column(DECIMAL(16, 4))
    debt_status = Column(String(20))
    surplus_repay_period = Column(INTEGER(11))
    stats_date = Column(Date)
    account_org = Column(String(50))
    account_num = Column(INTEGER(11))


class EcreditRepayDutyDiscount(Base):
    __tablename__ = 'ecredit_repay_duty_discount'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    account_type = Column(String(20), nullable=False)
    duty_type = Column(String(20))
    contract_no = Column(String(20))
    duty_amt = Column(DECIMAL(16, 4))
    account_org = Column(String(20))
    biz_type = Column(String(20))
    category = Column(String(20))
    account_num = Column(INTEGER(11))
    amount = Column(DECIMAL(16, 4))
    balance = Column(DECIMAL(16, 4))
    overdue_amt = Column(DECIMAL(16, 4))
    overdue_principal = Column(DECIMAL(16, 4))


class EcreditRepayDutyOutline(Base):
    __tablename__ = 'ecredit_repay_duty_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    duty_type = Column(String(20))
    business_type = Column(String(20))
    duty_amt = Column(DECIMAL(16, 4))
    account_num = Column(INTEGER(11))
    balance = Column(DECIMAL(16, 4))
    non_performing_bal = Column(DECIMAL(16, 4))
    special_mentioned_bal = Column(DECIMAL(16, 4))


class EcreditSettleOutline(Base):
    __tablename__ = 'ecredit_settle_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    loan_type = Column(String(20))
    normal_num = Column(INTEGER(11))
    special_mentioned_num = Column(INTEGER(11))
    non_performing_num = Column(INTEGER(11))
    total = Column(INTEGER(11))


class EcreditSuperiorOrg(Base):
    __tablename__ = 'ecredit_superior_org'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    org_type = Column(String(10))
    org_name = Column(String(100))
    cert_type = Column(String(32))
    cert_no = Column(String(30))
    update_date = Column(Date)


class EcreditUnclearedOutline(Base):
    __tablename__ = 'ecredit_uncleared_outline'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    account_num = Column(INTEGER(11))
    balance = Column(DECIMAL(16, 4))
    loan_type = Column(String(20))
    status_type = Column(String(20))


class PcreditAccSpeculate(Base):
    __tablename__ = 'pcredit_acc_speculate'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    record_id = Column(BIGINT(20))
    speculate_record_id = Column(BIGINT(20))
    report_time = Column(DateTime)
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    repay_month = Column(String(32))
    repay_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    account_status = Column(String(20))
    settled = Column(TINYINT(5))
    create_time = Column(DateTime)


class PcreditAccountCollection(Base):
    __tablename__ = 'pcredit_account_collection'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    account_type = Column(INTEGER(11))
    total_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    overdue_count = Column(INTEGER(11))
    overdue_count_day_90 = Column(INTEGER(11))
    assure_count = Column(INTEGER(11))


class PcreditAssetsManage(Base):
    __tablename__ = 'pcredit_assets_manage'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    asset_manage_company = Column(String(20))
    debt_date = Column(Date)
    debt_amount = Column(DECIMAL(16, 4))
    lately_repay_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))


class PcreditBizInfo(Base):
    __tablename__ = 'pcredit_biz_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    biz_type = Column(String(16))
    biz_sub_type = Column(String(32))
    biz_counts = Column(INTEGER(11))
    biz_first_month = Column(String(16))


class PcreditCarTradeRecord(Base):
    __tablename__ = 'pcredit_car_trade_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    car_no = Column(String(32))
    engine_no = Column(String(128))
    car_brand = Column(String(64))
    car_type = Column(String(32))
    use_type = Column(String(32))
    car_status = Column(String(32))
    mortgage_tag = Column(String(32))
    info_update_date = Column(DateTime)


class PcreditCardInstitution(Base):
    __tablename__ = 'pcredit_card_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)


class PcreditCertInfo(Base):
    __tablename__ = 'pcredit_cert_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    cert_type = Column(String(32))
    cert_no = Column(String(32))


class PcreditCivilJudgmentsRecord(Base):
    __tablename__ = 'pcredit_civil_judgments_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    result = Column(String(256))
    effective_date = Column(DateTime)
    litigious = Column(String(64))
    litigious_amt = Column(DECIMAL(16, 4))


class PcreditCreditCard(Base):
    __tablename__ = 'pcredit_credit_card'

    id = Column(BIGINT(30), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    card_type = Column(INTEGER(11))
    account_type = Column(INTEGER(11))
    credit_line = Column(DECIMAL(16, 4))
    used_line = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    overdraft_month_year_5 = Column(INTEGER(11))
    overdraft_month_day_90 = Column(INTEGER(11))
    overdraft_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditCreditGuarantee(Base):
    __tablename__ = 'pcredit_credit_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    credit_limit = Column(DECIMAL(16, 4))
    card_grant_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    used_limit = Column(DECIMAL(16, 4))
    bill_date = Column(Date)


class PcreditCreditTaxRecord(Base):
    __tablename__ = 'pcredit_credit_tax_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    org_name = Column(String(64))
    amount = Column(DECIMAL(16, 4))
    stats_date = Column(DateTime)


class PcreditDebit(Base):
    __tablename__ = 'pcredit_debit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    used_limit = Column(DECIMAL(16, 4))
    avg_limit_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    paln_repay_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))
    share_amt = Column(DECIMAL(16, 4))


class PcreditDebitInfo(Base):
    __tablename__ = 'pcredit_debit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_times = Column(INTEGER(11))
    overdue_1_less_2 = Column(INTEGER(11))
    overdue_2_less_2 = Column(INTEGER(11))
    overdue_1_greater_2 = Column(INTEGER(11))
    overdue_2_greater_2 = Column(INTEGER(11))
    overdue_3_greater_2 = Column(INTEGER(11))
    overdue_4_greater_2 = Column(INTEGER(11))
    lowest_repayment_count = Column(INTEGER(11))


class PcreditDefaultInfo(Base):
    __tablename__ = 'pcredit_default_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), index=True)
    default_type = Column(String(32))
    default_subtype = Column(String(32))
    default_count = Column(INTEGER(11))
    default_month = Column(INTEGER(11))
    default_balance = Column(INTEGER(11))
    max_overdue_sum = Column(DECIMAL(16, 4))
    max_overdue_month = Column(INTEGER(11))


class PcreditForceExecutionRecord(Base):
    __tablename__ = 'pcredit_force_execution_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    case_status = Column(String(32))
    case_end_date = Column(DateTime)
    apply_execution_object = Column(String(128))
    apply_execution_object_amt = Column(DECIMAL(16, 4))
    executed_object = Column(String(128))
    executed_object_amt = Column(DECIMAL(16, 4))


class PcreditFundParticipationRecord(Base):
    __tablename__ = 'pcredit_fund_participation_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    address = Column(String(128))
    pay_date = Column(DateTime)
    total_months = Column(String(32))
    begin_work_date = Column(String(32))
    pay_status = Column(String(32))
    personal_pay_rate = Column(DECIMAL(16, 4))
    monthly_amt = Column(DECIMAL(16, 4))
    pay_company = Column(String(128))
    info_update_date = Column(DateTime)
    reason = Column(String(128))


class PcreditGuaranteeOthers(Base):
    __tablename__ = 'pcredit_guarantee_others'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    info = Column(String(200))
    start_date = Column(DateTime)
    bank_name = Column(String(100))
    money_type = Column(INTEGER(11))
    name = Column(String(32))
    contract_amount = Column(DECIMAL(16, 4))
    guarantee_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditGuaranteePay(Base):
    __tablename__ = 'pcredit_guarantee_pay'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    repay_org = Column(String(20))
    lately_repay_date = Column(Date)
    repay_amount = Column(DECIMAL(16, 4))
    lately_repayment_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    debt_date = Column(DateTime)
    debt_amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))
    account_closedate = Column(DateTime)


class PcreditGuaranteePayDetails(Base):
    __tablename__ = 'pcredit_guarantee_pay_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    guapay_id = Column(BIGINT(20))
    special_type = Column(String(32))
    occur_date = Column(DateTime)
    change_month = Column(INTEGER(11))
    occur_amont = Column(DECIMAL(16, 4))
    record = Column(String(128))


class PcreditHouseFundRecord(Base):
    __tablename__ = 'pcredit_house_fund_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    address = Column(String(128))
    pay_date = Column(DateTime)
    pay_month = Column(String(32))
    pay_duration_date = Column(String(32))
    pay_status = Column(String(32))
    month_fee_amt = Column(DECIMAL(16, 4))
    personal_pay_rate = Column(DECIMAL(16, 4))
    company_pay_rate = Column(DECIMAL(16, 4))
    pay_company_name = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditHouseLoan(Base):
    __tablename__ = 'pcredit_house_loan'

    id = Column(BIGINT(30), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    overdue_amout = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditInfo(Base):
    __tablename__ = 'pcredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    housing_loan_count = Column(INTEGER(11))
    biz_housing_loan_count = Column(INTEGER(11))
    other_loan_count = Column(INTEGER(11))
    loan_1st_date = Column(String(7))
    debit_card_count = Column(INTEGER(11))
    debit_card_1st_date = Column(String(7))
    semi_credit_card_count = Column(INTEGER(11))
    semi_credit_card_1st_date = Column(INTEGER(11))
    declare_count = Column(INTEGER(11))
    dissent_count = Column(INTEGER(11))
    bad_debts_count = Column(INTEGER(11))
    bad_debts_balance = Column(DECIMAL(16, 4))
    asset_deal_count = Column(INTEGER(11))
    asset_deal_balance = Column(DECIMAL(16, 4))
    replace_repay_count = Column(INTEGER(11))
    replace_repay_balance = Column(DECIMAL(16, 4))
    loan_overdue_count = Column(INTEGER(11))
    loan_overdue_month = Column(INTEGER(11))
    loan_overdue_month_max_total = Column(DECIMAL(16, 4))
    loan_overdue_max_month = Column(INTEGER(11))
    debit_card_overdue_count = Column(INTEGER(11))
    debit_card_month_count = Column(INTEGER(11))
    debit_card_month_max_total = Column(DECIMAL(16, 4))
    debit_card_max_month = Column(INTEGER(11))
    semi_credit_card_overdraft_acount_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_month_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_balance_60 = Column(DECIMAL(16, 4))
    semi_credit_card_overdraft_max_month_60 = Column(INTEGER(11))
    uncleared_legal_count = Column(INTEGER(11))
    uncleared_org_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    uncleared_contract_count = Column(DECIMAL(16, 4))
    uncleared_balance = Column(DECIMAL(16, 4))
    uncleared_avg_repaly_6 = Column(DECIMAL(16, 4))
    undestroy_legal_count = Column(INTEGER(11))
    undestroy_org_count = Column(INTEGER(11))
    undestroy_count = Column(INTEGER(11))
    undestroy_limit = Column(DECIMAL(16, 4))
    undestory_max_limit = Column(DECIMAL(16, 4))
    undestory_min_limt = Column(DECIMAL(16, 4))
    undestory_used_limit = Column(DECIMAL(16, 4))
    undestory_avg_use = Column(DECIMAL(16, 4))
    undestory_semi_legal_count = Column(INTEGER(11))
    undestory_semi_org_count = Column(INTEGER(11))
    undestory_semi_count = Column(INTEGER(11))
    undestory_semi_limit = Column(DECIMAL(16, 4))
    undestory_semi_max_limit = Column(DECIMAL(16, 4))
    undestory_semi_min_limt = Column(DECIMAL(16, 4))
    undestory_semi_overdraft = Column(DECIMAL(16, 4))
    undestory_semi_avg_overdraft = Column(DECIMAL(16, 4))
    guarantee_count = Column(INTEGER(11))
    guarantee_amont = Column(DECIMAL(16, 4))
    guarantee_principal = Column(DECIMAL(16, 4))
    guarantee_catagory = Column(String(2))
    score = Column(INTEGER(11))
    score_date = Column(String(7))
    non_revolloan_org_count = Column(INTEGER(11))
    non_revolloan_accountno = Column(INTEGER(11))
    non_revolloan_totalcredit = Column(DECIMAL(16, 4))
    non_revolloan_balance = Column(DECIMAL(16, 4))
    non_revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    revolcredit_org_count = Column(INTEGER(11))
    revolcredit_account = Column(INTEGER(11))
    revolcredit_totalcredit = Column(DECIMAL(16, 4))
    revolcredit_balance = Column(DECIMAL(16, 4))
    revolcredit_repayin_6_m = Column(DECIMAL(16, 4))
    revolloan_org_count = Column(INTEGER(11))
    revolloan_account_no = Column(INTEGER(11))
    revolloan_totalcredit = Column(DECIMAL(16, 4))
    revolloan_balance = Column(DECIMAL(16, 4))
    revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    ind_guarantee_count = Column(INTEGER(11))
    ind_guarantee_sum = Column(DECIMAL(16, 4))
    ind_guarantee_balance = Column(DECIMAL(16, 4))
    ind_repay_count = Column(INTEGER(11))
    ind_repay_sum = Column(DECIMAL(16, 4))
    ind_repay_balance = Column(DECIMAL(16, 4))
    ent_guarantee_count = Column(INTEGER(11))
    ent_guarantee_sum = Column(DECIMAL(16, 4))
    ent_guarantee_balance = Column(DECIMAL(16, 4))
    ent_repay_count = Column(INTEGER(11))
    ent_repay_sum = Column(DECIMAL(16, 4))
    ent_repay_balance = Column(DECIMAL(16, 4))


class PcreditInsuranceExtractRecord(Base):
    __tablename__ = 'pcredit_insurance_extract_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    address = Column(String(128))
    retirement_category = Column(String(64))
    retirement_date = Column(DateTime)
    begin_work_date = Column(DateTime)
    actual_amt = Column(DECIMAL(16, 4))
    stop_reason = Column(String(128))
    origin_company = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditLargeScale(Base):
    __tablename__ = 'pcredit_large_scale'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    large_scale_quota = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    usedsum = Column(DECIMAL(16, 4))


class PcreditLive(Base):
    __tablename__ = 'pcredit_live'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    no = Column(INTEGER(11))
    live_address = Column(String(50))
    live_address_type = Column(String(2))
    update_time = Column(DateTime)
    phone = Column(String(50))


class PcreditLoan(Base):
    __tablename__ = 'pcredit_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    category = Column(String(8))
    principal_amount = Column(DECIMAL(16, 4))
    surplus_repay_period = Column(INTEGER(11))
    repay_amount = Column(DECIMAL(16, 4))
    plan_repay_date = Column(Date)
    amout_replay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_31_principal = Column(DECIMAL(16, 4))
    overdue_61_principal = Column(DECIMAL(16, 4))
    overdue_91_principal = Column(DECIMAL(16, 4))
    overdue_180_principal = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))
    loan_date = Column(Date)
    loan_creditor = Column(String(50))
    loan_amount = Column(DECIMAL(20, 4))
    loan_type = Column(String(30))
    loan_guarantee_type = Column(String(8))
    loan_repay_type = Column(String(20))
    loan_expire_date = Column(Date)
    loan_end_date = Column(Date)
    loan_status = Column(String(8))
    loan_balance = Column(DECIMAL(16, 4))
    account_type = Column(String(8))
    account_org = Column(String(32))
    account_mark = Column(String(32))
    end_date = Column(DateTime)
    credit_purpose = Column(String(8))
    respon_object = Column(String(16))
    respon_type = Column(String(16))
    guarantee_no = Column(String(64))
    credit_limit = Column(DECIMAL(16, 4))
    credit_limit_no = Column(String(16))
    credit_share_amt = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    repay_period = Column(INTEGER(11))
    repay_frequency = Column(String(8))
    loan_repay_status = Column(String(16))
    joint_loan_mark = Column(String(32))
    expiry_date = Column(DateTime)
    loan_status_time = Column(DateTime)
    quota_used = Column(DECIMAL(16, 4))
    large_scale_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    bill_date = Column(DateTime)
    latest_category = Column(String(8))
    latest_loan_balance = Column(DECIMAL(16, 4))
    latest_replay_date = Column(DateTime)
    latest_replay_amount = Column(DECIMAL(16, 4))
    latest_repay_status = Column(String(16))


class PcreditLoanGuarantee(Base):
    __tablename__ = 'pcredit_loan_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    contract_amount = Column(DECIMAL(16, 4))
    loan_grant_date = Column(Date)
    loan_expire_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    principal_amount = Column(DECIMAL(16, 4))
    category = Column(String(8))
    plan_repay_date = Column(Date)


class PcreditLoanInstitution(Base):
    __tablename__ = 'pcredit_loan_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)
    special_remark = Column(String(128))
    special_remark_date = Column(DateTime)


class PcreditNoncreditDetails(Base):
    __tablename__ = 'pcredit_noncredit_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    record_id = Column(BIGINT(20))
    pay_years = Column(INTEGER(11))
    pay_month = Column(INTEGER(11))
    pay_status = Column(String(16))


class PcreditNoncreditInfo(Base):
    __tablename__ = 'pcredit_noncredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    noncredit_type = Column(String(10))
    noncredit_count = Column(INTEGER(11))
    noncredit_sum = Column(DECIMAL(16, 4))


class PcreditNoncreditList(Base):
    __tablename__ = 'pcredit_noncredit_list'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    biz_org = Column(String(32))
    biz_type = Column(String(16))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(16))
    current_arrears_amt = Column(DECIMAL(16, 4))
    record_date = Column(DateTime)


class PcreditObjectionMark(Base):
    __tablename__ = 'pcredit_objection_mark'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(String(32))
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditOtherLoan(Base):
    __tablename__ = 'pcredit_other_loan'

    id = Column(BIGINT(30), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditOverdraft(Base):
    __tablename__ = 'pcredit_overdraft'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditOverdue(Base):
    __tablename__ = 'pcredit_overdue'
    __table_args__ = (
        Index('IDX_PCREDIT_OVERDUE_R_ID', 'record_id', 'report_id', 'record_type'),
        {'comment': '���ڼ�¼'}
    )

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditPersonInfo(Base):
    __tablename__ = 'pcredit_person_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    sex = Column(String(8))
    birthday = Column(Date)
    marriage_status = Column(String(8))
    mobile_no = Column(String(20))
    work_tel = Column(String(20))
    home_tel = Column(String(20))
    education = Column(String(8))
    jhi_degree = Column(String(8))
    communication_address = Column(String(50))
    residence_address = Column(String(50))
    spouse_name = Column(String(20))
    spouse_certificate_type = Column(String(16))
    spouse_certificate_no = Column(String(32))
    spouse_work_unit = Column(String(50))
    spouse_mobile_no = Column(String(20))
    verifi_result = Column(String(64))
    authority = Column(String(64))
    employment = Column(String(16))
    nationality = Column(String(64))
    email = Column(String(32))


class PcreditPersonalStatement(Base):
    __tablename__ = 'pcredit_personal_statement'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditPhoneHis(Base):
    __tablename__ = 'pcredit_phone_his'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    no = Column(INTEGER(11))
    phone = Column(String(16))
    update_time = Column(DateTime)


class PcreditPortraitsMain(Base):
    __tablename__ = 'pcredit_portraits_main'

    id = Column(BIGINT(20), primary_key=True)
    reprot_id = Column(String(32), nullable=False)
    report_time = Column(DateTime)
    marital_status = Column(INTEGER(11))
    is_owened = Column(INTEGER(11))
    is_mortgage = Column(INTEGER(11))
    is_have_credit = Column(INTEGER(11))


class PcreditPortraitsQuery(Base):
    __tablename__ = 'pcredit_portraits_query'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_approval_inquiry_month_1 = Column(INTEGER(11))
    credit_approval_inquiry_month_1 = Column(INTEGER(11))
    self_inquiry_month_1 = Column(INTEGER(11))
    qualification_examination_year_2 = Column(INTEGER(11))
    loan_approval_month_2 = Column(INTEGER(11))
    credit_approval_month_2 = Column(INTEGER(11))
    loan_approval_month_3 = Column(INTEGER(11))
    credit_approval_month_3 = Column(INTEGER(11))
    loan_approval_month_6 = Column(INTEGER(11))
    credit_approval_month_6 = Column(INTEGER(11))
    loan_approval_year_1 = Column(INTEGER(11))
    credit_approval_year_1 = Column(INTEGER(11))
    qualification_examination_year_1 = Column(INTEGER(11))
    approvals_month_3 = Column(INTEGER(11))


class PcreditPortraitsSummary(Base):
    __tablename__ = 'pcredit_portraits_summary'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    house_loan_count = Column(INTEGER(11))
    first_loan_month = Column(String(10))
    first_credit_month = Column(String(10))
    sum_count = Column(INTEGER(11))
    loan_overdue_count = Column(INTEGER(11))
    loan_max_overdue_money = Column(DECIMAL(16, 4))
    loan_max_overdue_month = Column(INTEGER(11))
    credit_overdue_account_count = Column(INTEGER(11))
    credit_overdue_month_count = Column(INTEGER(11))
    credit_max_overdue_money = Column(DECIMAL(16, 4))
    credit_max_overdue_month = Column(INTEGER(11))
    unsettled_loan_organization_number = Column(INTEGER(11))
    unsettled_loan_number = Column(INTEGER(11))
    unsettled_loan_contract_total = Column(DECIMAL(16, 4))
    unsettled_loan_total_balance = Column(DECIMAL(16, 4))
    unsettled_loan_ave_month_6 = Column(DECIMAL(16, 4))
    uncancelled_credit_organization_number = Column(INTEGER(11))
    uncancelled_credit_total_money = Column(DECIMAL(16, 4))
    uncancelled_credit_max_money = Column(DECIMAL(16, 4))
    uncancelled_credit_used_money = Column(DECIMAL(16, 4))
    uncancellation_credit_average_month_6 = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_account_number = Column(INTEGER(11))
    uncancelled_quasicredit_total_money = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_average_month_6 = Column(DECIMAL(16, 4))
    foreign_guaranty_number = Column(INTEGER(11))
    foreign_guaranty_principal_balance = Column(DECIMAL(16, 4))
    credit_used_rate = Column(DECIMAL(16, 4))


class PcreditPortraitsTransaction(Base):
    __tablename__ = 'pcredit_portraits_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_account_abnormality = Column(INTEGER(11))
    loan_fivelevel_abnormality = Column(INTEGER(11))
    loan_overdue_month_6 = Column(INTEGER(11))
    loan_overdue_year_2 = Column(INTEGER(11))
    loan_overdue_year_5 = Column(INTEGER(11))
    loan_max_overdue_month = Column(INTEGER(11))
    loan_max_overdue_number = Column(INTEGER(11))
    unsettled_loan_max_overdue_year_2 = Column(INTEGER(11))
    large_loan_overdue_twice = Column(INTEGER(11))
    small_loan_overdue_twice = Column(INTEGER(11))
    business_loan_overdue_twice = Column(INTEGER(11))
    unsettled_loan_unbank_number = Column(INTEGER(11))
    unsettled_busloan_agency_number = Column(INTEGER(11))
    loan_new_total_month_6 = Column(DECIMAL(16, 4))
    loan_new_total_year_1 = Column(DECIMAL(16, 4))
    loan_new_agency_month_3 = Column(INTEGER(11))
    loan_new_agency_year_1 = Column(INTEGER(11))
    guaranteed_loan_total_month_6 = Column(DECIMAL(16, 4))
    guaranteed_loan_total_year_1 = Column(DECIMAL(16, 4))
    guaranteed_loan_agency_year_1 = Column(INTEGER(11))
    loan_expiration_total_month_3 = Column(DECIMAL(16, 4))
    loan_expiration_total_month_6 = Column(DECIMAL(16, 4))
    loan_expiration_total_year_1 = Column(DECIMAL(16, 4))
    business_loan_agency_year_2 = Column(INTEGER(11))
    unsettled_house_loan_number = Column(INTEGER(11))
    unsettled_car_loan_number = Column(INTEGER(11))
    unsettled_house_loan_payed = Column(DECIMAL(16, 4))
    unsettled_car_loan_payed = Column(DECIMAL(16, 4))
    unsettled_loan_overdue_money = Column(DECIMAL(16, 4))
    unsettled_loan_bank_number = Column(INTEGER(11))
    loan_gdz_year_2 = Column(INTEGER(11))
    extension_number = Column(INTEGER(11))
    loan_new_number_month_6 = Column(INTEGER(11))
    loan_new_number_year_1 = Column(INTEGER(11))
    loan_max_overdue_month_6 = Column(INTEGER(11))
    loan_max_overdue_year_1 = Column(INTEGER(11))
    loan_max_overdue_year_2 = Column(INTEGER(11))
    business_loan_corpus_overdue = Column(INTEGER(11))
    loan_doubtful = Column(INTEGER(11))
    credit_account_abnormality = Column(INTEGER(11))
    credit_overdue_month_6 = Column(INTEGER(11))
    credit_overdue_year_2 = Column(INTEGER(11))
    credit_overdue_year_5 = Column(INTEGER(11))
    credit_new_total_year_1 = Column(DECIMAL(16, 4))
    credit_new_total_month_6 = Column(DECIMAL(16, 4))
    credit_gdz_year_2 = Column(INTEGER(11))
    credit_max_overdue_number = Column(INTEGER(11))
    credit_max_overdue_year_2 = Column(INTEGER(11))
    credit_activated_number = Column(INTEGER(11))
    credit_new_number_month_6 = Column(INTEGER(11))
    credit_new_number_year_1 = Column(INTEGER(11))
    credit_now_overdue_money = Column(DECIMAL(16, 4))
    credit_min_payed_number = Column(INTEGER(11))
    credit_quasi_abnormality = Column(INTEGER(11))
    loan_scured_five_abnormality = Column(INTEGER(11))
    credit_financial_tension = Column(DECIMAL(16, 4))


class PcreditProfession(Base):
    __tablename__ = 'pcredit_profession'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    no = Column(INTEGER(11))
    work_unit = Column(String(100))
    work_address = Column(String(100))
    profession = Column(String(100))
    industry = Column(String(50))
    duty = Column(String(50))
    duty_title = Column(String(50))
    enter_date = Column(Date)
    update_time = Column(Date)
    work_type = Column(String(100))
    work_phone = Column(String(16))


class PcreditPubInfo(Base):
    __tablename__ = 'pcredit_pub_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    pub_type = Column(String(32))
    pub_count = Column(INTEGER(11))
    pub_sum = Column(DECIMAL(16, 4))


class PcreditPublicContent(Base):
    __tablename__ = 'pcredit_public_content'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), server_default=text("'null'"))
    jhi_comment = Column(String(200))
    detail_id = Column(String(32), server_default=text("'null'"))


class PcreditPunishmentRecord(Base):
    __tablename__ = 'pcredit_punishment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    org_name = Column(String(128))
    content = Column(LONGTEXT)
    amount = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    reconsideration_result = Column(String(128))


class PcreditQualificationRecord(Base):
    __tablename__ = 'pcredit_qualification_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    qualification_name = Column(String(128))
    grade = Column(String(32))
    award_date = Column(DateTime)
    expired_date = Column(DateTime)
    revoked_date = Column(DateTime)
    award_org = Column(String(128))
    org_address = Column(String(128))


class PcreditQueryRecord(Base):
    __tablename__ = 'pcredit_query_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    no = Column(INTEGER(11))
    jhi_time = Column(Date)
    operator = Column(String(50))
    reason = Column(String(20))


class PcreditQueryTimes(Base):
    __tablename__ = 'pcredit_query_times'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_org_1 = Column(INTEGER(11))
    credit_org_1 = Column(INTEGER(11))
    loan_times_1 = Column(INTEGER(11))
    credit_times_1 = Column(INTEGER(11))
    self_times_1 = Column(INTEGER(11))
    loan_times_2 = Column(INTEGER(11))
    guarantee_times_2 = Column(INTEGER(11))
    agreement_times_2 = Column(INTEGER(11))
    last_query_time = Column(DateTime)
    last_query_org = Column(String(32))
    last_query_type = Column(String(32))


class PcreditRepayment(Base):
    __tablename__ = 'pcredit_repayment'
    __table_args__ = (
        Index('IDX_PCREDIT_REPAYMENT_R_ID', 'record_id', 'report_id', 'record_type'),
        {'comment': '�����¼'}
    )

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    status = Column(String(8))
    repayment_amt = Column(DECIMAL(16, 4))


class PcreditRewardRecord(Base):
    __tablename__ = 'pcredit_reward_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    reward_org = Column(String(128))
    reward_content = Column(String(128))
    effective_date = Column(DateTime)
    expired_date = Column(DateTime)


class PcreditScoreInfo(Base):
    __tablename__ = 'pcredit_score_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    score = Column(INTEGER(11))
    position = Column(String(32))
    desc_content = Column(String(64))


class PcreditSemiCredit(Base):
    __tablename__ = 'pcredit_semi_credit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    overdraft_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_overdraft_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_repay_date = Column(Date)
    overdraft_amount_180 = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdraft_start_year = Column(INTEGER(11))
    overdraft_start_month = Column(INTEGER(11))
    overdraft_end_year = Column(INTEGER(11))
    overdraft_end_month = Column(INTEGER(11))
    share_amt = Column(DECIMAL(16, 4))


class PcreditSimplePortraits(Base):
    __tablename__ = 'pcredit_simple_portraits'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    house_count = Column(INTEGER(11))
    un_house_count = Column(INTEGER(11))
    house_amout = Column(DECIMAL(16, 4))
    un_house_amount = Column(DECIMAL(16, 4))
    un_house_balance = Column(DECIMAL(16, 4))
    un_other_count = Column(INTEGER(11))
    un_other_amount = Column(DECIMAL(16, 4))
    un_other_balance = Column(DECIMAL(16, 4))
    other_max_amout = Column(DECIMAL(16, 4))
    loan_car_count = Column(INTEGER(11))
    un_credit_amount = Column(DECIMAL(16, 4))
    un_credit_bank_amount = Column(DECIMAL(16, 4))
    un_credit_used = Column(DECIMAL(16, 4))
    foreign_guaranty_count = Column(INTEGER(11))
    foreign_guaranty_amount = Column(DECIMAL(16, 4))
    overdue_house_count = Column(INTEGER(11))
    overdue_other_count = Column(INTEGER(11))
    overdue_credit_count = Column(INTEGER(11))
    loan_current_amount = Column(DECIMAL(16, 4))
    credit_current_amount = Column(DECIMAL(16, 4))
    loan_overdue_day_90 = Column(INTEGER(11))
    credit_overdue_day_90 = Column(INTEGER(11))
    un_loan_year_5 = Column(INTEGER(11))
    un_credit_overdue_year_5 = Column(INTEGER(11))
    overdue_house_year_5 = Column(INTEGER(11))
    overdue_other_year_5 = Column(INTEGER(11))
    overdue_credit_year_5 = Column(INTEGER(11))
    credit_amount_month_6 = Column(DECIMAL(16, 4))
    credit_count_month_6 = Column(INTEGER(11))
    credit_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_month_6 = Column(DECIMAL(16, 4))
    loan_count_month_6 = Column(INTEGER(11))
    loan_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_future_6 = Column(DECIMAL(16, 4))
    loan_count_query_6 = Column(INTEGER(11))
    loan_count_query_3 = Column(INTEGER(11))
    credit_count_query_6 = Column(INTEGER(11))
    credit_count_query_3 = Column(INTEGER(11))
    self_count_query_6 = Column(INTEGER(11))
    self_count_query_3 = Column(INTEGER(11))
    report_time = Column(DateTime)
    self_house_count = Column(INTEGER(11))
    self_fund_count = Column(INTEGER(11))
    un_house_month_pay = Column(DECIMAL(16, 4))
    clear_house_loan_amout = Column(DECIMAL(16, 4))
    un_house_loan_rate = Column(DECIMAL(16, 4))
    un_car_month_pay = Column(DECIMAL(16, 4))
    clear_car_amount = Column(DECIMAL(16, 4))
    clear_other_count = Column(INTEGER(11))
    un_other_loan_count = Column(INTEGER(11))
    un_other_limit_min = Column(DECIMAL(16, 4))
    un_other_limit_max = Column(DECIMAL(16, 4))
    clear_other_limit_min = Column(DECIMAL(16, 4))
    clear_other_limit_max = Column(DECIMAL(16, 4))
    other_month_amount = Column(DECIMAL(16, 4))
    credit_use_rate = Column(DECIMAL(16, 4))
    pb_loan_max_amount = Column(DECIMAL(16, 4))
    credit_report_month = Column(INTEGER(11))
    credit_yuan_count = Column(INTEGER(11))
    overdue_count_year_5 = Column(INTEGER(11))
    un_pb_overdue_count = Column(INTEGER(11))
    loan_amount_future_2 = Column(DECIMAL(16, 4))
    approve_count_month_3 = Column(INTEGER(11))
    approve_count_month_6 = Column(INTEGER(11))
    approve_count_year_2 = Column(INTEGER(11))
    credit_is_normal = Column(INTEGER(11))
    loan_is_normal = Column(INTEGER(11))
    inter_buzy_bank_limit_6_m = Column(DECIMAL(16, 4))
    inter_buzy_bank_limit_max = Column(DECIMAL(16, 4))
    inter_buzy_bank_count_6_m = Column(INTEGER(11))
    other_mont_fixed_payment = Column(DECIMAL(16, 4))
    other_mont_insert_first = Column(DECIMAL(16, 4))
    clear_other_amount_year_1 = Column(DECIMAL(16, 4))
    clear_other_amount_year_2 = Column(DECIMAL(16, 4))
    clear_other_amount_year_3 = Column(DECIMAL(16, 4))
    clear_other_amount_year_4 = Column(DECIMAL(16, 4))
    clear_other_amount_year_1_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_2_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_3_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_4_max = Column(DECIMAL(16, 4))
    credit_report_loan_month = Column(INTEGER(11))
    pb_loan_amount_future_12 = Column(DECIMAL(16, 4))
    loan_amount_future_12 = Column(DECIMAL(16, 4))
    overdue_un_mont_fixed = Column(INTEGER(11))
    un_loan_continued = Column(INTEGER(11))


class PcreditSpecial(Base):
    __tablename__ = 'pcredit_special'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False, index=True)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    special_type = Column(String(32))
    special_date = Column(DateTime)
    special_month = Column(INTEGER(11))
    special_money = Column(DECIMAL(16, 4))
    special_comment = Column(String(80))


class PcreditSpeculateRecord(Base):
    __tablename__ = 'pcredit_speculate_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    pre_start_date = Column(DateTime)
    pre_end_date = Column(DateTime)
    pre_acc_org = Column(INTEGER(11))
    pre_amt = Column(DECIMAL(16, 4))
    pre_balance = Column(DECIMAL(16, 4))
    pre_freq = Column(INTEGER(11))
    pre_terms = Column(INTEGER(11))
    pre_should_amt = Column(DECIMAL(16, 4))
    pre_real_amt = Column(DECIMAL(16, 4))
    pre_res_terms = Column(INTEGER(11))
    pre_should_date = Column(DateTime)
    pre_month_days = Column(INTEGER(11))
    pre_quar_days = Column(INTEGER(11))
    pre_real_date = Column(DateTime)
    pre_month_period = Column(INTEGER(11))
    pre_quar_period = Column(INTEGER(11))
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    settled = Column(TINYINT(5))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class PcreditTelecomPaymentRecord(Base):
    __tablename__ = 'pcredit_telecom_payment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    carrier = Column(String(32))
    biz_type = Column(String(128))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(32))
    current_arrears_amt = Column(DECIMAL(16, 4))
    current_arrears_months = Column(String(32))
    record_date = Column(DateTime)
    last_24_month_payment_history = Column(String(512))


class PcreditThresholdRecord(Base):
    __tablename__ = 'pcredit_threshold_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    seq = Column(String(32))
    personal_category = Column(String(32))
    address = Column(String(128))
    work_place = Column(String(128))
    home_monthly_income = Column(DECIMAL(16, 4))
    apply_date = Column(DateTime)
    approval_date = Column(DateTime)
    info_update_date = Column(DateTime)


class PcreditTransaction(Base):
    __tablename__ = 'pcredit_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    remarks = Column(String(200))


class PcreditWarnInfo(Base):
    __tablename__ = 'pcredit_warn_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    warn_content = Column(String(256))
    effective_date = Column(DateTime)
    expiry_date = Column(DateTime)


class RawDatum(Base):
    __tablename__ = 'raw_data'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20))
    api_no = Column(String(20), index=True)
    raw_data_desc = Column(String(64))
    risk_subject_id = Column(BIGINT(20), index=True)
    expired_at = Column(DateTime, index=True)
    req_message = Column(Text)
    data_type = Column(String(50))
    raw_data = Column(MEDIUMTEXT)
    cause = Column(String(64))
    create_time = Column(DateTime)
    req_msg_check_sum = Column(String(512), index=True)
    biz_type = Column(String(10))
    data_ids = Column(String(200))


class RequestRecord(Base):
    __tablename__ = 'request_record'

    id = Column(BIGINT(20), primary_key=True)
    biz_type = Column(String(50))
    api_no = Column(String(20))
    request_id = Column(String(50))
    user_name = Column(String(50))
    risk_subject_id = Column(BIGINT(20))
    user_seq = Column(String(64))
    merchant_no = Column(String(64))
    request_param = Column(Text)
    request_status = Column(String(50))
    data_fetch_mode = Column(String(50))
    create_time = Column(DateTime)
    raw_data_ids = Column(Text)


class RiskSubject(Base):
    __tablename__ = 'risk_subject'

    id = Column(BIGINT(20), primary_key=True)
    user_type = Column(String(50))
    name = Column(String(64))
    phone = Column(String(15))
    unique_no = Column(String(64), index=True)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    reg_code = Column(String(64))
