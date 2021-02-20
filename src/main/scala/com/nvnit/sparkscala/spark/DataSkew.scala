package com.nvnit.sparkscala.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, concat, explode, lit, rand}
import org.apache.spark.sql.types.IntegerType

object DataSkew extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  //Data Skewness on large dataset while aggregate

  import spark.implicits._

  case class Person(name: String, country: String, age: Int)

  val data = Seq(Person("Michael", "India", 19), Person("Andy", "India", 18), Person("Justin", "SriLanka", 16),
    Person("Jim", "India", 12), Person("Mans", "India", 15), Person("arun", "India", 16),
    Person("Jann", "Nepal", 24), Person("Shet", "India", 24), Person("Jooly", "Nepal", 25),
    Person("Pai", "India", 25), Person("Shetty", "India", 23), Person("Mau", "SriLanka", 29))

  val personDS = spark.createDataset(data)
  personDS.show(false)

  // Normal Aggregation to get avg value
  val avgAgeDS = personDS
    .groupBy(col("country"))
    .agg(avg(col("age")).cast(IntegerType) as "avgAge")
  avgAgeDS.show(false)

  // Aggregating to remove data skewness
  val aggWithSaltKey = personDS
    .withColumn("saltKey", (rand * 8).cast(IntegerType))
    .groupBy("country", "saltKey")
    .agg(avg(col("age")) as "avgAgeWithSalt")

  val avgAgeWithoutSkew = aggWithSaltKey
    .groupBy("country")
    .agg(avg(col("avgAgeWithSalt")).cast(IntegerType) as "avgAge")
  avgAgeWithoutSkew.show(false)


  //3. Data Skewness on dataset joins

  case class CustomerInfo(custId: String, custName: String)

  case class OrderInfo(orderId: Int, custId: String, billAmount: Int)

  val customerDF = spark.createDataset(Seq(CustomerInfo("CUS-121", "Venky"), CustomerInfo("CUS-172", "Ajay"),
    CustomerInfo("CUS-134", "Sham"), CustomerInfo("CUS-119", "Ram")))
  val orderDF = spark.createDataset(Seq(OrderInfo(101, "CUS-121", 80), OrderInfo(102, "CUS-172", 140), OrderInfo(102, "CUS-134", 180),
    OrderInfo(201, "CUS-121", 90), OrderInfo(202, "CUS-121", 40), OrderInfo(203, "CUS-172", 110), OrderInfo(301, "CUS-121", 100)))


  // normal JOin
  val joinedDF = orderDF.join(customerDF, Seq("custId"), "left_outer")
  joinedDF.show(false)

  // join removing Skew
  val tempOrderDf = orderDF.withColumn("saltKey" , concat(col("custId"), lit("_"), lit(rand * 5).cast(IntegerType)))
  tempOrderDf.show((false))

  val tempCustDf = customerDF
    .withColumn("arrayKey" , explode(array((0 to 10).map(lit(_)): _*)))
    .withColumn("saltKey", concat(col("custId") , lit("_") , col("arrayKey")))
  tempCustDf.show(false)

  val joinedSaltDF = tempOrderDf.join(tempCustDf, Seq("saltKey"), "left_outer")
  joinedSaltDF.show(false)

  //--
   case class RoomBooking(hotelId: String, roomId: String, roomCount: Int)

   case class RoomItem(hotelId: String, roomId: String, itemId: String)

   val roomData = Seq(RoomBooking("BHAG", "RM-1", 10),
     RoomBooking("BHAG", "RM-1", 9),
     RoomBooking("BHAG", "RM-1", 6), RoomBooking("BHAG", "RM-1", 2),
     RoomBooking("BHAG", "RM-2", 10), RoomBooking("BHAG", "RM-1", 10), RoomBooking("BHAG", "RM-1", 1),
     RoomBooking("BHAG", "RM-3", 4), RoomBooking("BHAG", "RM-3", 5), RoomBooking("BHAG", "RM-1", 18))

   val roomBookingData = Seq(RoomItem("BHAG", "RM-1", "RMIT-12"), RoomItem("BHAG", "RM-1", "RMIT-03"),
     RoomItem("BHAG", "RM-1", "RMIT-04"), RoomItem("BHAG", "RM-2", "RMIT-08"))

   val roomDS = spark.createDataFrame(roomData)
   val roomBookingDS = spark.createDataFrame(roomBookingData)

   val joinedDS = roomDS.join(roomBookingDS, Seq("hotelId", "roomId"), "left_outer")
   joinedDS.show(100, false)


   def removeSkew(DF1: DataFrame, DF2: DataFrame) = {

     val tempDF1 = DF1.withColumn("saltKey", concat(col("hotelId"), lit("_"), col("roomId"), lit("_"), (rand * 5).cast(IntegerType)))

     val tempDF2 = DF2
       .withColumn("arrayOfSalt", explode(array((0 to 5).map(lit(_)): _*)))
       .withColumn("saltKey", concat(col("hotelId"), lit("_"), col("roomId"), lit("_"), col("arrayOfSalt")))

     (tempDF1, tempDF2)
   }

   // Remove dataSkew introducing the saltKey
   val (tempDF1, tempDF2) = removeSkew(roomDS, roomBookingDS)

   val joinWithoutSKewDF = tempDF1.join(tempDF2, Seq("saltKey"), "left_outer")
   joinWithoutSKewDF.show(100, false)

}
