package atconsulting.staging

import atconsulting.system.Params._
import atconsulting.system.CaseClasses._
import atconsulting.system.TimeHelper._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.Map

/**
  * Created by Александр on 01.04.2017.
  */
object Stage1 {
  def apply(inputRDDHttp: RDD[String], inputRDDIp: RDD[String], inputRDDCdr: RDD[String],
            dimHttp: Broadcast[Map[String, String]],
            dimIp: Broadcast[Map[String, String]],
            dimPhones: Broadcast[Map[String, RegionPlusServiceName]],
            dimBS: Broadcast[Map[String, String]]
           ): RDD[StgTaxiUserActiviry] = {

    val outOnlineRDD = aggOnlineUserActivity(inputRDDHttp, inputRDDIp, dimHttp, dimIp, dimBS)
    val outOfflineRDD = aggOfflineUserActivity(inputRDDCdr, dimPhones, dimBS)
    outOfflineRDD.union(outOnlineRDD)
  }

  def aggOnlineUserActivity(inputRDDHttp: RDD[String], inputRDDIp: RDD[String], dimHttp: Broadcast[Map[String, String]], dimIp: Broadcast[Map[String, String]], dimBS: Broadcast[Map[String, String]]): RDD[StgTaxiUserActiviry] = {
    val outIp = getOnlineUserActivity(inputRDDIp, dimIp, dimBS)
    val outHttp = getOnlineUserActivity(inputRDDHttp, dimHttp, dimBS)

    val out = outIp.union(outHttp)
      .reduceByKey((x, y) => x + y)
      .filter { case ((userActivity, isDriver), amount) => userActivity.taxiServicesName != "-99" }
      .map { case ((userActivity, isDriver), amount) =>
        StgTaxiUserActiviry(patternCDR.parseDateTime(firstDayOfTheFirstWeekOf2017), userActivity.user, userActivity.taxiServicesName, 1, userActivity.region, userActivity.regionBs, isDriver, amount)
      }
    out
  }

  def aggOfflineUserActivity(inputRDD: RDD[String], dimPhones: Broadcast[Map[String, RegionPlusServiceName]], dimBS: Broadcast[Map[String, String]]): RDD[StgTaxiUserActiviry] = {
    val filterRDD = inputRDD.filter(cdrStr => cdrStr.split("\\|")(atCallDurSec).toInt > 5 //Фильтр по продолж. звонка
      && cdrStr.split("\\|")(basicServiceType) == "V" //только голосовые
      && checkDateTimeInFirstWeekOf2017(cdrStr.split("\\|")(dataTimeTransaction))) //фильт по дате ( между 02.01.2017 и 09.01.2017

    val out = filterRDD.map(str => {
      val split = str.split("\\|")
      (UserActivity(split(callTo), split(callFrom), "-99", dimBS.value.getOrElse(split(lac) + "-" + split(cell), "-99")).whoIsWho(dimPhones), patternCDR.parseDateTime(split(dataTimeTransaction)).getMillis)
    })
      .filter { case (cdrEnt, timeStamp) => !cdrEnt.taxiServicesName.equals("-99") }
      .groupByKey()
      .map { case (cdrEntity, allTransTime) => (cdrEntity, countSession(allTransTime)) }
      .reduceByKey((x, y) => x + y)
      .map { case (userActivity, amount) =>
        StgTaxiUserActiviry(patternCDR.parseDateTime(firstDayOfTheFirstWeekOf2017), userActivity.user, userActivity.taxiServicesName, 0, userActivity.region, userActivity.regionBs, 0, amount)
      }
    out
  }

  def getOnlineUserActivity(inputRDD: RDD[String], dim: Broadcast[Map[String, String]], dimBS: Broadcast[Map[String, String]]) = {
    val filterRDD = inputRDD
      .filter(str => checkJarusTimeInFirstWeekOf2017(str.split("\t")(unixTimeStamp).toLong))

    val out = filterRDD.map(str => {
      val split = str.split("\t")
      var serviceName = "-99"
      dim.value.foreach { case (hostOrIpKey, serviceNameValue) => if (split(hostOrIp).contains(hostOrIpKey)) serviceName = serviceNameValue }
      (UserActivity(serviceName, split(msisdn), "-99", dimBS.value.getOrElse(split(lacJarus) + "-" + split(cellJarus), "-99")), split(unixTimeStamp).toLong)
    })
      .groupByKey()
      .map { case (userActivity, allTransTime) => ((userActivity, isDriver(allTransTime)), 1) }


    out
  }

  def countSession(sessions: Iterable[Long]): Int = {
    val sortSessions = sessions.toArray.sorted
    var countSessions = 1
    var begin = sortSessions(0)
    sortSessions.foreach(x => {
      if (x - begin > 3600000) {
        begin = x
        countSessions += 1
      }
    })
    countSessions
  }

  def isDriver(transactions: Iterable[Long]): Int = {
    val sortTrans = transactions.toArray.sorted
    var begin = sortTrans(0)
    var prevTrans = sortTrans(0)
    var isDriver = 0
    if (sortTrans.length >= 3) {
      sortTrans.foreach(currTrans => {
        if (Math.abs(currTrans - prevTrans) > 3600)
          begin = currTrans
        else {
          if (Math.abs(currTrans - begin) >= 7200)
            isDriver = 1
        }
        prevTrans = currTrans
      })
    }
    isDriver
  }
}
