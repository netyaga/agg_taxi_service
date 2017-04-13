package atconsulting.staging

import atconsulting.system.CaseClasses._
import org.apache.spark.rdd.RDD

/**
  * Created by Александр on 02.04.2017.
  */
object Stage2 {
  def apply(usersRdd: RDD[StgTaxiUserActiviry]): RDD[AggTaxiServices] = {
    val out = usersRdd //.filter(userActiv => userActiv.isDriver!=1) // избавляемся от водителей
      .map { userActiv => ((userActiv.dateTaxi, userActiv.serviceName, userActiv.serviceType, userActiv.regionDim, userActiv.regionBs), userActiv.countTaxiTrips) }
      .reduceByKey((x, y) => x + y)
      .map { case ((dateTime, serviceName, serviceType, region, regionBs), amount) =>
        AggTaxiServices(dateTime, serviceName, serviceType, region, regionBs, amount)
      }
    out
  }

}
