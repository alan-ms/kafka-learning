package com.alanms.demos.bankbalance.producer

import java.math.BigDecimal

data class BankBalanceTransactionDTO(val name: String, val amount: BigDecimal, val time: String)
