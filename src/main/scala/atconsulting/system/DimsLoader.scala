package atconsulting.system

import atconsulting.system.CaseClasses.RegionPlusServiceName
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Александр on 02.04.2017.
  */
object DimsLoader {
  var bCastDimPhones: Broadcast[scala.collection.Map[String, RegionPlusServiceName]] = _
  var bCastDimBS: Broadcast[scala.collection.Map[String, String]] = _
  var bCastDimHttp: Broadcast[scala.collection.Map[String, String]] = _
  var bCastDimIp: Broadcast[scala.collection.Map[String, String]] = _

  def loadAllDims(sc: SparkContext) = {
    var dimPhones = sc.textFile("src\\dictionary\\phones.tsv")
      .map(str => {
        val split = str.split("\t")
        (split(2), RegionPlusServiceName(split(0), split(1)))
      }).collectAsMap()

    val dimBs = sc.textFile("src\\dictionary\\region_bs.csv")
      .map(str => {
        val split = str.split(",")
        (split(0) + "-" + split(1), split(2))
      }).collectAsMap()

    val dimJarusHttp = sc.textFile("src\\dictionary\\dim_host_jarus.csv")
      .map(str => {
        val split = str.split(";")
        (split(1), split(0))
      }).collectAsMap()

    val dimJarusIp = sc.textFile("src\\dictionary\\ips.tsv")
      .map(str => {
        val split = str.split("\t")
        (split(1), split(0))
      }).collectAsMap()
    bCastDimBS = sc.broadcast(dimBs)
    bCastDimHttp = sc.broadcast(dimJarusHttp)
    bCastDimIp = sc.broadcast(dimJarusIp)
    bCastDimPhones = sc.broadcast(dimPhones)
  }
}
