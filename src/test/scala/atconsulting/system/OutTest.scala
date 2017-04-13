package atconsulting.system

import atconsulting.system.LoadPaths._
import java.io.PrintWriter

import atconsulting.staging._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import atconsulting.system.DimsLoader._

/**
  * Created by Александр on 01.04.2017.
  */
class OutTest extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    sparkConf = new SparkConf()
      .setAppName("spark-test-app")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
  }

  test("spark-test") {
    val outFileTaxi = new PrintWriter("src\\output\\outputUserStat.txt")
    val outFileUser = new PrintWriter("src\\output\\outputTaxiStat.txt")
    loadAllDims(sc)

    val outUserActivity = Stage1(sc.textFile(httpPath), sc.textFile(sslPath), sc.textFile(cdrPath), bCastDimHttp, bCastDimIp, bCastDimPhones, bCastDimBS)
      .persist()

    outUserActivity
      .collect()
      .foreach(str =>
        outFileTaxi.println(str.toString))

    val outAggTaxiServices = Stage2(outUserActivity)
      .collect()
      .foreach(str =>
        outFileUser.println(str.toString))
    outFileTaxi.close()
    outFileUser.close()

  }

  override def afterAll(): Unit = {
    sc.stop()
  }

}
