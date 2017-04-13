package atconsulting.system

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by Александр on 05.04.2017.
  */
object TimeHelper {
  val firstDayOfTheFirstWeekOf2017 = "20170102000000"
  //проверка даты ( чтоб былв между 2.01.2017 и 9.01.2017
  val patternCDR = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  def checkJarusTimeInFirstWeekOf2017(jarusTransactionTime: Long): Boolean = {
    val transactionDateTime = new DateTime(jarusTransactionTime * 1000L)
    val firstDay = patternCDR.parseDateTime(firstDayOfTheFirstWeekOf2017)
    transactionDateTime.isAfter(firstDay) && transactionDateTime.isBefore(firstDay.plusDays(7))
  }

  def checkDateTimeInFirstWeekOf2017(TransactionTime: String): Boolean = {
    val transactionDateTime = patternCDR.parseDateTime(TransactionTime)
    val firstDay = patternCDR.parseDateTime(firstDayOfTheFirstWeekOf2017)
    transactionDateTime.isAfter(firstDay) && transactionDateTime.isBefore(firstDay.plusDays(7))
  }

}
