# 流水所有参数

# 一.正则匹配格式
# 交易日期或者时间关键字
TRANS_TIME_PATTERN = r'(?<!记账)(日期|时间|交易日|账务日|[Tt]ime)'
# 记账日期或者时间关键字
ACCOUNTING_DATE_PATTERN = r'(?<=记账)(日期|时间|交易日|账务日|[Tt]ime)'
# 交易金额关键字
TRANS_AMT_PATTERN = \
    r"(收支|转入|转出|金额|发生额|支出|存入|收入|Debit(?!A)|Credit|记账方向|支取|出账|进账|取出|借|贷|汇出|汇入|交易类型)"
# 交易余额关键字
TRANS_BAL_PATTERN = r'(余额|[Bb]alance)'
# 交易对手关键字
TRANS_OPNAME_PATTERN = r"(?<!本.)(户名|方名称|姓名|单位名称|对方单位|公司名|对手名称|人名称|账号名称)"
# 币种关键字
TRANS_CUR_PATTERN = r'(币种|货币|[Cc]urrency)'
# 交易对手姓名关键字
TRANS_OPACCT_PATTERN = r'(?<!本.)(账号(?!名称)|账户(?!名称|明细|省市)|ID|户口号)'
# 交易对手开户行关键字
TRANS_OPBANK_PATTERN = r'(?<!本.)((?<!交易)行名|开户行|开户机构|开户网点|银行名称|银行)'
# 交易渠道关键字
TRANS_CHANNEL_PATTERN = r'(渠道|交易行|受理机构|交易网点|交易机构)'
# 交易方式关键字
TRANS_TYP_PATTERN = r'(方式|业务类型|[Tt]ype|业务类别|现转)'
# 交易用途关键字
TRANS_USE_PATTERN = r'(用途)'
# 交易备注关键字
TRANS_REMARK_PATTERN = r'(摘要|说明|附言|备注|[Dd]escription|个性化信息|其他|其它|对方信息)'

# 标准日期时间格式
#   时间+日期格式,年份20开头,00-29结尾,即2000年到2029年,月份01-12,日期01-31(暂时不区分大小月),小时00-23,分钟00-59
#   秒钟00-59,或者excel表示的日期,即40000-49999之间的数字(可以包含小数部分)
DTTIME_PATTERN = r"^20[012]\d(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])([01]\d|2[0-3])([0-5]\d){2}|^4\d{4}\d*[1-9]+\d*"
# 标准日期格式
#   日期格式,仅有年份,月份,日期,含义同上
DATE_PATTERN = r'^20[012]\d(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])|^4\d{4}'
# 标准时间格式
#   小时00-23,分钟00-59,秒钟00-59
TIME_PATTERN = r'^([01]\d|2[0-3])([0-5]\d){2}$|0\d*[1-9]+\d*'
# 短日期格式
#   小时00-23,分钟00-59
TIME_S_PATTERN = r'^([01]\d|2[0-3])[0-5]\d$'

# 金额格式
#   金额格式,以非0数字开头的整数,或者以0开头的小数
AMT_PATTERN = r'^[1-9]\d*|0\d*[1-9]+\d*'
# 忽略的格式
#   一旦某行包含下列关键字，大概率是统计性信息，需要将整行删除
IGNORE_PATTERN = r'.*(合计|累计|总计|总笔数|总额|记录数|参考|承前).*'

# 进账关键字
INCOME_PATTERN = r'[收入存贷进来]|Credit'
# 出账关键字
OUTCOME_PATTERN = r'[支出取借付往]|Debit'
# 出账全匹配
OUTCOME_FULL_PATTERN = r'.*[借出支往取付].*'

# 二.返回信息枚举
# resp_mapping = {
#     '01': {
#         "resCode": "1",
#         "resMsg": "失败",
#         "data": {
#             "warningMsg": ['上传文件类型错误,仅支持csv,xls,xlsx格式文件']}
#     },
#     '02': {
#         "resCode": "1",
#         "resMsg": "失败",
#         "data": {
#             "warningMsg": ['上传文件类型错误,仅支持csv,xls,xlsx格式文件']}
#     }
# }


# 三.所有数字配置

# 标题行最大匹配行数
MAX_TITLE_NUMBER = 30
# 最小交易间隔
MIN_TRANS_INTERVAL = 160
# 最小查询间隔
MIN_QUERY_INTERVAL = 180
# 最小导入间隔
MIN_IMPORT_INTERVAL = 45

# 结息日均利率计算参数,不能为0
INTEREST_MULTIPLIER = 0.0035

# 异常交易金额
UNUSUAL_TRANS_AMT = [
    5.20, 5.21, 13.14, 14.13, 20.20, 20.13, 20.14, 131.4, 201.3, 201.4, 520, 520.20, 521, 1314, 1314.2, 1413,
    1413.2, 2013.14, 2014.13, 201314, 2020.2, 202020.2, 13145.2, 1314520, 52013.14, 5201314]
# 家庭不稳定交易密度阈值
UNSTABLE_DENSITY = 0.3

# 最小民间借贷金额
MIN_PRIVATE_LENDING = 500
# 最小民间借贷持续月份数
MIN_CONTI_MONTHS = 6
# 最大民间借贷日期差别
MAX_INTERVAL_DAYS = 5

# 多久内导入的流水视为同一笔业务的流水
MONTH_LIMIT = 2

# 四.二进制类型枚举
# csv文件分隔符类型枚举值
CSV_DELIMITER = [',', ';', '|', '\t', ':']
# csv文件编码格式
CSV_ENCODING = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'cp936', 'big5']

# 强关联关系
# 20220916 维护新的强关联关系
STRONGER_RELATIONSHIP = ["U_PERSONAL",
                         "U_PER_LG_COMPANY",
                         "U_PER_SH_H_COMPANY",
                         "U_PER_SH_M_COMPANY",
                         "U_PER_SP_PERSONAL",
                         "U_PER_SP_LG_COMPANY",
                         "U_PER_SP_SH_H_COMPANY",
                         "U_PER_SP_SH_M_COMPANY",
                         "U_PER_CT_COMPANY",
                         "U_PER_SP_CT_COMPANY",
                         "U_PER_PARENTS_PERSONAL",
                         "U_PER_CHILDREN_PERSONAL",
                         "U_PER_COMPANY_SH_PERSONAL",
                         "U_PER_COMPANY_FIN_PERSONAL",
                         "U_COMPANY",
                         "U_COM_LEGAL_PERSONAL",
                         "U_COMPANY_SH_H_COMPANY",
                         "U_COMPANY_SH_M_COMPANY",
                         "U_COM_CT_PERSONAL",
                         "U_COM_CT_CT_COMPANY",
                         "U_COM_CT_LG_COMPANY",
                         "U_COM_CT_SH_H_COMPANY",
                         "U_COM_CT_SH_M_COMPANY",
                         "U_COM_CT_SP_PERSONAL",
                         "U_COM_CT_SP_CT_COMPANY",
                         "U_COM_CT_SP_LG_COMPANY",
                         "U_COM_CT_SP_SH_H_COMPANY",
                         "U_COM_CT_SP_SH_M_COMPANY",
                         "U_COM_FIN_PERSONAL"]
# 交易对手剔除以下单位：
UNUSUAL_OPPO_NAME = ['对私|跨行|淘宝|转账|转存|[收存取付]款|批量|支付|网银在线|银联|中国平安|中国太平洋|信托|基金|冲正|中国人寿|'
                     '小额贷款|中金同盛|磁石供应链商业保理|晋福融资担保|孚厘|中金同盛商业保理|转支|保险|社保|二维码|备用金|'
                     '备付金|零钱通|充值|微信红包|网转|退款|理财|缴费|他行汇入|提现|财付通|电汇|国家金库|公积金管理中心|'
                     '待报解预算|信用卡|放款|还款|贷款|汇兑|退汇|待清算|清算过渡|资金结算|中间账户|内部账户|工资|消费|待报解共享收入']

# 上下游金额上下限金额（元）
UP_DOWNSTREAM_THRESHOLD = 1000

# 同一交易账号对应多个交易对手
IGNORE_ACC_NO = ['', '0', '0.0', '1.0000000000000002e+17', '-', '99900001262112001358', '1504292651']
IGNORE_ACC_NO_PATTERN = '999999.*99|105331.*0875|105584.*0061|105100.*2625|105551.*8273|1500947831|215.*690' \
                        '|210401344|105584.*|100000000000000000|243.*133|48429202|1000049901|1000107101|1000050001'
IGNORE_OPPO_NAME_PATTERN = '微信|支付宝|财付通|余额宝|滴滴出行'

# 结息交易对手标签
INTEREST_OPPO_KEY_WORD = '银行|利息|结息|个人活期结息|批量结息|存息|付息|存款利息|批量业务'
# 结息标签
INTEREST_KEY_WORD = '利息|结息|个人活期结息|批量结息|存息|付息|存款利息|批量业务|入息|季息|收息|1126|interest|INTEREST'
# 非结息标签
NON_INTEREST_KEY_WORD = '理财|钱生钱|余额宝|零钱通|招财盈|宜人财富|保证金|透支|智能|其他账户|税后|转存'
