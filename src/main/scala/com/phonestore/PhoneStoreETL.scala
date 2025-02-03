package com.phonestore

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.types._
import java.sql.{DriverManager, SQLException}

object PhoneStoreETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PhoneStoreETL")
      .config("spark.master", "local")
      .getOrCreate()

    // Загрузка данных из Excel с явной схемой
    val ordersDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("OrderID", IntegerType, nullable = false),
        StructField("CustomerName", StringType, nullable = false),
        StructField("TotalAmount", DecimalType(10, 2), nullable = false)
      )))
      .load("data/orders.xlsx")

    val orderDetailsDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("OrderID", IntegerType, nullable = false),
        StructField("ProductName", StringType, nullable = false),
        StructField("Category", StringType, nullable = false),
        StructField("Quantity", IntegerType, nullable = false),
        StructField("Price", DecimalType(10, 2), nullable = false)
      )))
      .load("data/order_details.xlsx")

    // Нормализация данных
    val productsDF = orderDetailsDF.select("ProductName", "Category", "Price").distinct()
    val normalizedOrderDetailsDF = orderDetailsDF.select("OrderID", "ProductName", "Quantity")

    // Настройки подключения к MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/PhoneStoreDB"
    val user = "root"
    val password = "*****"

    val properties = new java.util.Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)

    // Загрузка данных в MySQL
    try {
      // Загрузка Orders
      ordersDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "Orders", properties)

      // Загрузка Products
      productsDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "Products", properties)

      // Загрузка OrderDetails
      normalizedOrderDetailsDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "OrderDetails", properties)

      println("Данные успешно загружены в MySQL.")
    } catch {
      case e: Exception =>
        println(s"Ошибка при загрузке данных: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

