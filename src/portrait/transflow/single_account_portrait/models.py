# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, JSON, String, TIMESTAMP, Table, Text, Time
from sqlalchemy.dialects.mysql import BIGINT, BIT, INTEGER, LONGTEXT, MEDIUMTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class TransAccount(Base):
    __tablename__ = 'trans_account'

    id = Column(BIGINT(20), primary_key=True)
    risk_subject_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    account_name = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    bank = Column(String(32))
    account_no = Column(String(64))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    trans_flow_type = Column(INTEGER(11))
    update_time = Column(DateTime)
    account_state = Column(INTEGER(11))
    create_time = Column(DateTime)


class TransApply(Base):
    __tablename__ = 'trans_apply'

    id = Column(BIGINT(20), primary_key=True)
    out_req_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_no = Column(String(32))
    cus_name = Column(String(32))
    related_name = Column(String(32))
    relationship = Column(String(32))
    account_id = Column(BIGINT(20))
    industry = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlow(Base):
    __tablename__ = 'trans_flow'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    trans_time = Column(DateTime)
    opponent_name = Column(String(255))
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(16))
    trans_channel = Column(String(16))
    trans_type = Column(String(16))
    trans_use = Column(String(16))
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlowException(Base):
    __tablename__ = 'trans_flow_exception'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    trans_time = Column(DateTime)
    opponent_name = Column(String(255))
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(16))
    trans_channel = Column(String(16))
    trans_type = Column(String(16))
    trans_use = Column(String(16))
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlowPortrait(Base):
    __tablename__ = 'trans_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    account_id = Column(BIGINT(20))
    trans_date = Column(Date)
    trans_time = Column(Time)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    opponent_name = Column(String(64))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(32))
    trans_channel = Column(String(64))
    trans_type = Column(String(64))
    trans_use = Column(String(64))
    remark = Column(String(255))
    currency = Column(String(16))
    phone = Column(String(32))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    loan_type = Column(String(32))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(255))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    trans_flow_src_type = Column(INTEGER(11))
    usual_trans_type = Column(String(256))


class TransSingleAbnormalRecovery(Base):
    __tablename__ = 'trans_single_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleCounterpartyPortrait(Base):
    __tablename__ = 'trans_single_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(32))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleLoanPortrait(Base):
    __tablename__ = 'trans_single_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    loan_type = Column(String(16))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSinglePortrait(Base):
    __tablename__ = 'trans_single_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(INTEGER(11))
    normal_income_d_mean = Column(INTEGER(11))
    normal_income_m_mean = Column(INTEGER(11))
    normal_income_m_std = Column(INTEGER(11))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    income_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleRelatedPortrait(Base):
    __tablename__ = 'trans_single_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    relationship = Column(String(32))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    opponent_account_no = Column(String(64))


class TransSingleRemarkPortrait(Base):
    __tablename__ = 'trans_single_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    remark_type = Column(String(32))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleSummaryPortrait(Base):
    __tablename__ = 'trans_single_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    variable_cost_amt = Column(DECIMAL(16, 4))


class TransUAbnormalRecovery(Base):
    __tablename__ = 'trans_u_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(64))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUCounterpartyPortrait(Base):
    __tablename__ = 'trans_u_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(64))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUFlowPortrait(Base):
    __tablename__ = 'trans_u_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    apply_no = Column(String(32))
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    trans_date = Column(DateTime)
    trans_time = Column(DateTime)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    bank = Column(String(64))
    account_no = Column(String(64))
    opponent_name = Column(String(64))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(64))
    opponent_account_bank = Column(String(64))
    trans_channel = Column(String(64))
    trans_type = Column(String(32))
    trans_use = Column(String(64))
    remark = Column(String(64))
    currency = Column(String(16))
    phone = Column(String(16))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    loan_type = Column(String(16))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(64))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    trans_flow_src_type = Column(INTEGER(11))
    usual_trans_type = Column(String(256))


class TransULoanPortrait(Base):
    __tablename__ = 'trans_u_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    loan_type = Column(String(32))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUModelling(Base):
    __tablename__ = 'trans_u_modelling'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_amt = Column(DECIMAL(16, 4))
    pawn_cnt = Column(INTEGER(11))
    medical_cnt = Column(INTEGER(11))
    court_cnt = Column(INTEGER(11))
    insure_cnt = Column(INTEGER(11))
    night_trans_cnt = Column(INTEGER(11))
    fam_unstab_cnt = Column(INTEGER(11))
    balance_mean = Column(DECIMAL(16, 4))
    balance_max = Column(DECIMAL(16, 4))
    balance_max_0_to_5 = Column(DECIMAL(16, 4))
    balance_0_to_5_prop = Column(DECIMAL(16, 4))
    income_0_to_5_prop = Column(DECIMAL(16, 4))
    balance_min_weight = Column(DECIMAL(16, 4))
    balance_max_weight = Column(DECIMAL(16, 4))
    income_max_weight = Column(DECIMAL(16, 4))
    half_year_interest_amt = Column(DECIMAL(16, 4))
    half_year_balance_amt = Column(DECIMAL(16, 4))
    year_interest_amt = Column(DECIMAL(16, 4))
    q_2_balance_amt = Column(DECIMAL(16, 4))
    q_3_balance_amt = Column(DECIMAL(16, 4))
    year_interest_balance_prop = Column(DECIMAL(16, 4))
    q_4_interest_balance_prop = Column(DECIMAL(16, 4))
    income_mean = Column(DECIMAL(16, 4))
    mean_sigma_left = Column(DECIMAL(16, 4))
    mean_sigma_right = Column(DECIMAL(16, 4))
    mean_2_sigma_left = Column(DECIMAL(16, 4))
    mean_2_sigma_right = Column(DECIMAL(16, 4))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_amt_d_mean = Column(DECIMAL(16, 4))
    normal_income_amt_m_mean = Column(DECIMAL(16, 4))
    normal_expense_amt_m_std = Column(DECIMAL(16, 4))
    opponent_cnt = Column(INTEGER(11))
    income_rank_1_amt = Column(DECIMAL(16, 4))
    income_rank_2_amt = Column(DECIMAL(16, 4))
    income_rank_3_amt = Column(DECIMAL(16, 4))
    income_rank_4_amt = Column(DECIMAL(16, 4))
    income_rank_2_cnt_prop = Column(DECIMAL(16, 4))
    expense_rank_6_avg_gap = Column(DECIMAL(16, 4))
    income_rank_9_avg_gap = Column(DECIMAL(16, 4))
    expense_rank_10_avg_gap = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    enterprise_3_income_amt = Column(DECIMAL(16, 4))
    enterprise_3_expense_cnt_prop = Column(DECIMAL(16, 4))
    all_relations_expense_cnt_prop = Column(DECIMAL(16, 4))
    hit_loan_type_cnt_6_cm = Column(INTEGER(11))
    private_income_amt_12_cm = Column(DECIMAL(16, 4))
    private_income_mean_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_amt_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_mean_12_cm = Column(DECIMAL(16, 4))
    finlease_expense_cnt_6_cm = Column(INTEGER(11))
    otherfin_income_mean_3_cm = Column(DECIMAL(16, 4))
    all_loan_expense_cnt_3_cm = Column(DECIMAL(16, 4))
    income_net_rate_compare_2 = Column(DECIMAL(16, 4))
    cus_apply_amt_pred = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUPortrait(Base):
    __tablename__ = 'trans_u_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_d_mean = Column(DECIMAL(16, 4))
    normal_income_m_mean = Column(DECIMAL(16, 4))
    normal_income_m_std = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    income_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransURelatedPortrait(Base):
    __tablename__ = 'trans_u_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    relationship = Column(String(16))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    opponent_account_no = Column(String(64))


class TransURemarkPortrait(Base):
    __tablename__ = 'trans_u_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    remark_type = Column(String(64))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUSummaryPortrait(Base):
    __tablename__ = 'trans_u_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    variable_cost_amt = Column(DECIMAL(16, 4))


class ReportFeatureDetail(Base):
    __tablename__ = 'report_feature_detail'

    id = Column(BIGINT(20), primary_key=True)
    report_req_no = Column(String(100))
    product_code = Column(String(32))
    product_name = Column(String(50))
    unique_name = Column(String(50))
    unique_code = Column(String(50))
    level_1 = Column(String(50))
    level_2 = Column(String(50))
    level_3 = Column(String(50))
    variable_name = Column(String(128))
    variable_name_cn = Column(String(256))
    variable_values = Column(Text)
    create_time = Column(DateTime)
    update_time = Column(DateTime)
