PARAM = {
    "benchmark": "MXWO",
    "universe": "clarifi_mxcn1a_afl",
    "server_name": "10.206.1.19,9433",
    "db_name": "GLOBAL",
    "user_name": "sa",
    "user_pwd": "REDACTED",
    "odbc_name": "ODBC Driver 17 for SQL Server"
}

FILED_KEY = {
    'LSEG': [

    ],
    'SNP': [
        'MXCN1A_WGT', 'GROSS_PROFIT', 'CUR_MKT_CAP',
        'CF_CASH_FROM_OPER', 'IS_OPER_INC', 'PRC_ADJ', 'NET_INCOME', 'SALES_REV_TURN',
        'RETURN_ON_ASSET', 'BS_TOT_ASSET', 'RETURN_ON_ASSET',
        'BS_LT_BORROW', 'BS_CUR_ASSET_REPORT', 'BS_CUR_LIAB', 'M_RETURN', 'SHRS_OUTSTAND'
    ]
}

FACTOR_KEY = {
    'LSEG': [
        'GICS_sector', 'country', 'ISIN', 'wgt', 'rtn',
        'normalized_niq', 'normalized_eqy', 'normalized_gpr',
        'normalized_coa', 'normalized_opi', 'normalized_srt',
        'normalized_frm', 'normalized_frm6', 'normalized_mom1', 'normalized_mom12_1',
        'normalized_mom12', 'normalized_mret', 'normalized_div',
        'normalized_nis', 'normalized_ois', 'normalized_roa',
        'normalized_roe', 'normalized_roic', 'normalized_oia',
        'normalized_cal', 'normalized_lla', 'normalized_lsm',
        'normalized_dni', 'normalized_sra', 'normalized_icc'
    ],
    'SNP': [
        'normalized_gpr', 'normalized_coa', 'normalized_opi',
        'normalized_mom', 'normalized_nis', 'normalized_ois',
        'normalized_roa', 'normalized_oia', 'normalized_ptf',
        'normalized_grw', 'rtn'
    ]

} # -> will be deprecated soon.


# FACTOR_KEY = {
#     'LSEG': {
#         'GICS_sector', 'country', 'ISIN', 'wgt', 'rtn',
#         'normalized_niq', 'normalized_eqy', 'normalized_gpr',
#         'normalized_coa', 'normalized_opi', 'normalized_srt',
#         'normalized_frm', 'normalized_frm6', 'normalized_mom1', 'normalized_mom12_1',
#         'normalized_mom12', 'normalized_mret', 'normalized_div',
#         'normalized_nis', 'normalized_ois', 'normalized_roa',
#         'normalized_roe', 'normalized_roic', 'normalized_oia',
#         'normalized_cal', 'normalized_lla', 'normalized_lsm',
#         'normalized_dni', 'normalized_sra', 'normalized_icc',
#         'normalized_ptf', 'normalized_growth'
#     },
#     'SNP': {
#         'normalized_gpr', 'normalized_coa', 'normalized_opi',
#         'normalized_mom', 'normalized_nis', 'normalized_ois',
#         'normalized_roa', 'normalized_oia', 'normalized_ptf',
#         'normalized_grw', 'rtn'
#     }
#
# }

FIELD_MXWO = {
    "fld_name": ["sec",  # 0
                 "country",  # 1
                 "isin",  # 2
                 "MXWO_WGT",  # 3
                 "BS_CUR_ASSET_REPORT",  # 4
                 "SALES_REV_TURN",  # 5
                 "BS_CUR_LIAB",  # 6
                 "BS_INVENTORIES",  # 7
                 "BS_LT_BORROW",  # 8
                 "BS_ST_BORROW",  # 9
                 "BS_TOT_ASSET",  # 10
                 "BS_TOT_CAP",  # 11
                 "BS_TOT_EQY",  # 12
                 "CF_CASH_FROM_OPER",  # 13
                 "CF_FREE_CASH_FLOW",  # 14
                 "GROSS_PROFIT",  # 15
                 "IS_INT_EXPENSE",  # 16
                 "IS_INT_INC",  # 17
                 "IS_OPER_INC",  # 18
                 "NET_INCOME",  # 19
                 "NET_OPER_PROFIT_AFTER_TAX",  # 20
                 "NET_REV_AFT_PROV",  # 21
                 "OPER_MARGIN",  # 22
                 "PROF_MARGIN",  # 23
                 "RETURN_COM_EQY",  # 24
                 "RETURN_ON_ASSET",  # 25
                 "RETURN_ON_INV_CAPITAL",  # 26
                 "TOT_COMMON_EQY",  # 27
                 "TOTAL_EQUITY",  # 28
                 "TOTAL_INVESTED_CAPITAL",  # 29
                 "CUR_MKT_CAP",  # 30
                 "M_RETURN",  # 31
                 "DIVIDEND_YIELD",  # 32
                 "SHRS_OUTSTAND",  # 33
                 "PRC_ADJ"  # 34
                 ]
}


FIELD_MXCN1A = {
    "fld_name": ["sec",  # 0
                 "country",  # 1
                 "isin",  # 2
                 "MXCN1A_WGT",  # 3
                 "BS_CUR_ASSET_REPORT",  # 4
                 "SALES_REV_TURN",  # 5
                 "BS_CUR_LIAB",  # 6
                 "BS_INVENTORIES",  # 7
                 "BS_LT_BORROW",  # 8
                 "BS_ST_BORROW",  # 9
                 "BS_TOT_ASSET",  # 10
                 "BS_TOT_CAP",  # 11
                 "BS_TOT_EQY",  # 12
                 "CF_CASH_FROM_OPER",  # 13
                 "CF_FREE_CASH_FLOW",  # 14
                 "GROSS_PROFIT",  # 15
                 "IS_INT_EXPENSE",  # 16
                 "IS_INT_INC",  # 17
                 "IS_OPER_INC",  # 18
                 "NET_INCOME",  # 19
                 "NET_OPER_PROFIT_AFTER_TAX",  # 20
                 "NET_REV_AFT_PROV",  # 21
                 "OPER_MARGIN",  # 22
                 "PROF_MARGIN",  # 23
                 "RETURN_COM_EQY",  # 24
                 "RETURN_ON_ASSET",  # 25
                 "RETURN_ON_INV_CAPITAL",  # 26
                 "TOT_COMMON_EQY",  # 27
                 "TOTAL_EQUITY",  # 28
                 "TOTAL_INVESTED_CAPITAL",  # 29
                 "CUR_MKT_CAP",  # 30
                 "M_RETURN",  # 31
                 "DIVIDEND_YIELD",  # 32
                 "SHRS_OUTSTAND",  # 33
                 "PRC_ADJ"  # 34
                 ]
}

# sec =                          # 00 -> get_universe() -> GICS Sector
# country =                      # 01 -> get_universe() -> country / headquartercountry / incorporationcountry
# isin =                         # 02 -> get_universe() -> ISIN
# MXWO_WGT =                     # 03
# BS_CUR_ASSET_REPORT =          # 04 -> Total Current Assets
# SALES_REV_TURN =               # 05 -> Total Revenue
# BS_CUR_LIAB =                  # 06 -> Total Current Liabilities
# BS_INVENTORIES =               # 07 -> Total Inventory
# BS_LT_BORROW =                 # 08 -> Total Long Term Debt
# BS_ST_BORROW =                 # 09 -> Notes Payable/Short Term Debt
# BS_TOT_ASSET =                 # 10 -> Total Assets
# BS_TOT_CAP =                   # 11 -> Total Equity + Total Debt
# BS_TOT_EQY =                   # 12 -> Total Equity
# CF_CASH_FROM_OPER =            # 13 -> Cash from Operating Activities
# CF_FREE_CASH_FLOW =            # 14 -> Cash from Operating Activities + Capital Expenditures + Total Cash Dividends Paid
# GROSS_PROFIT =                 # 15 -> Gross Profit
# IS_INT_EXPENSE =               # 16 -> Interest Expense, Non-Operating
# IS_INT_INC =                   # 17 -> Interest Income, Non-Operating
# IS_OPER_INC =                  # 18 -> Operating Income
# NET_INCOME =                   # 19 -> Net Income
# NET_OPER_PROFIT_AFTER_TAX =    # 20 -> Operating Income - Provision for Income Taxes
# NET_REV_AFT_PROV =             # 21 -> Gross Profit - Provision for Doubtful Accounts
# OPER_MARGIN =                  # 22 -> Operating Income / Total Revenue
# PROF_MARGIN =                  # 23 -> Net Income / Total Revenue
# RETURN_ON_EQY =                # 24 -> Net Income / Total Equity
# RETURN_ON_ASSET =              # 25 -> Net Income / Total Assets
# RETURN_ON_INV_CAPITAL =        # 26 -> NOPAT / Total Invested Capital
# TOT_COMMON_EQY =               # 27 -> Total Common Stock
# TOTAL_INVESTED_CAPITAL =       # 29 -> Net Working Capital + Net, PP&E
# CUR_MKT_CAP =                  # 30 -> Market Capitalization
# M_RETURN =                     # 31 -> Adjusted Close Price.pct_change()
# DIVIDEND_YIELD =               # 32 -> DPS - Common Stock Primary Issue / Adjusted Close Price
# SHRS_OUTSTAND =                # 33 -> Shares Outstanding
# PRC_ADJ =                      # 34 -> Adjusted Close Price
