package atconsulting.system

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.Map

/**
  * Created by Александр on 01.04.2017.
  */
object Params {
  //необходимые индексы
  val dataTimeTransaction = 2
  val callActionCode = 6
  val atCallDurSec = 8
  val callTo = 9
  val callFrom = 10
  val basicServiceType = 32
  val lac = 26
  val cell = 23
  val unixTimeStamp = 0
  val msisdn = 2
  val lacJarus = 6
  val cellJarus = 7
  val hostOrIp = 9
  val url = 10
}
