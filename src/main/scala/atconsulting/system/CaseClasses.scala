package atconsulting.system

import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime

import scala.collection.Map

/**
  * Created by Александр on 05.04.2017.
  */
object CaseClasses {

  //на выход
  case class AggTaxiServices(
                              dateTaxi: DateTime, serviceName: String,
                              serviceType: Int, regionDim: String,
                              regionBs: String, countTaxiTrips: Int
                            ) {
    override def toString: String = {
      dateTaxi.toString("E yyyy-MM-dd HH:mm") + "\t" + serviceName + "\t" + serviceType + "\t" + regionDim + "\t" + regionBs + "\t" + countTaxiTrips
    }
  }

  case class StgTaxiUserActiviry(
                                  dateTaxi: DateTime, subsKey: String,
                                  serviceName: String, serviceType: Int,
                                  regionDim: String, regionBs: String,
                                  isDriver: Int, countTaxiTrips: Int
                                ) {
    override def toString: String = {
      dateTaxi.toString("E yyyy-MM-dd HH:mm") + "\t" + subsKey + "\t" + serviceName + "\t" + regionDim + "\t" + regionBs + "\t" + serviceType + "\t" + +isDriver + "\t" + countTaxiTrips
    }
  }

  //кейс класс словаря
  case class RegionPlusServiceName(region: String, serviceName: String)

  //вспомогательные
  case class UserActivity(taxiServicesName: String, user: String, region: String, regionBs: String) {
    def whoIsWho(dimPhones: Broadcast[Map[String, RegionPlusServiceName]]): UserActivity = {
      if (dimPhones.value.get(taxiServicesName).isDefined)
        UserActivity(dimPhones.value.get(taxiServicesName).get.serviceName, user, dimPhones.value.get(taxiServicesName).get.region, regionBs)
      else {
        if (dimPhones.value.get(user).isDefined)
          UserActivity(dimPhones.value.get(user).get.serviceName, taxiServicesName, dimPhones.value.get(user).get.region, regionBs)
        else
          UserActivity("-99", taxiServicesName, "-99", regionBs)
      }


    } //кто -такси, кто - клиент
  }


}
