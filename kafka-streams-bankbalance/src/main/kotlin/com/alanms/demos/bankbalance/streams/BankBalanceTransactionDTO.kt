package com.alanms.demos.bankbalance.streams

import java.math.BigDecimal
import java.time.LocalDateTime

data class BankBalanceTransactionDTO(val name: String, val amount: BigDecimal, val time: LocalDateTime)
