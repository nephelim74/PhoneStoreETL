package com.phonestore

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.sql.{DriverManager, SQLException}

object PhoneCreateBDETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PhoneStoreETL")
      .config("spark.master", "local")
      .getOrCreate()

    // Загрузка данных из Excel
    val ordersDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/orders.xlsx")

    val orderDetailsDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/order_details.xlsx")

    // Нормализация данных
    val productsDF = orderDetailsDF.select("ProductName", "Category", "Price").distinct()
    val normalizedOrderDetailsDF = orderDetailsDF.select("OrderID", "ProductName", "Quantity")

    // Настройки подключения к MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/"
    val dbName = "PhoneStoreDB"
    val user = "root"
    val password = "******"

    var connection = DriverManager.getConnection(jdbcUrl, user, password)
    var statement = connection.createStatement()

    try {
      // Создание базы данных, если она не существует
      statement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $dbName")
      println(s"База данных $dbName создана.")

      // Закрываем текущее соединение
      statement.close()
      connection.close()

      // Подключаемся к созданной базе данных
      connection = DriverManager.getConnection(jdbcUrl + dbName, user, password)
      statement = connection.createStatement()

      // Создание таблиц
      statement.executeUpdate(
        """
          |CREATE TABLE IF NOT EXISTS Orders (
          |    OrderID INT PRIMARY KEY,
          |    CustomerName VARCHAR(255),
          |    TotalAmount DECIMAL(10, 2)
          |)
                """.stripMargin)

      statement.executeUpdate(
        """
          |CREATE TABLE IF NOT EXISTS Products (
          |    ProductName VARCHAR(255) PRIMARY KEY,
          |    Category VARCHAR(255),
          |    Price DECIMAL(10, 2)
          |)
                """.stripMargin)

      statement.executeUpdate(
        """
          |CREATE TABLE IF NOT EXISTS OrderDetails (
          |    OrderID INT,
          |    ProductName VARCHAR(255),
          |    Quantity INT,
          |    PRIMARY KEY (OrderID, ProductName),
          |    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
          |    FOREIGN KEY (ProductName) REFERENCES Products(ProductName)
          |)
                """.stripMargin)

      println("Таблицы Orders, Products и OrderDetails созданы.")

    } catch {
      case e: SQLException =>
        println(s"Ошибка SQL: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      statement.close()
      connection.close()
    }

    // Сохранение данных в MySQL
    val properties = new java.util.Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)

    ordersDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl + dbName, "Orders", properties)

    productsDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl + dbName, "Products", properties)

    normalizedOrderDetailsDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl + dbName, "OrderDetails", properties)

    println("Данные успешно загружены в MySQL.")

    // Закрытие SparkSession
    spark.stop()
  }
}