// ใน SimpleOracleConnectionTest.scala
package com.siebel.oracle

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.log4j.{Logger, Level} 

object SimpleOracleConnectionTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.siebel.oracle").setLevel(Level.DEBUG) 
    Logger.getLogger("oracle.jdbc").setLevel(Level.DEBUG) 

    val spark = SparkSession.builder()
      .appName("Scala Oracle Connection Test")
      .getOrCreate()

    // Oracle RDS connection configuration
    val oracleUrl = sys.env("ORACLE_URL") // **บังคับอ่านจาก Environment Variable**
    val username = sys.env("ORACLE_USER") // **บังคับอ่านจาก Environment Variable**
    val password = sys.env("ORACLE_PASSWORD") // **บังคับอ่านจาก Environment Variable**

    // **DEBUG Print (จะแสดงใน Log Output)**
    println(s"DEBUG: Actual ORACLE_URL from env: $oracleUrl")
    println(s"DEBUG: Actual ORACLE_USER from env: $username")
    println(s"DEBUG: Actual ORACLE_PASSWORD from env: ${"*" * password.length}") 

    println("==========================================")
    println(s"🔍 Connecting to: $oracleUrl")
    println(s"👤 User: $username")
    println("==========================================")

    try {
      println("\n📋 Test: Basic Connection to DUAL table")
      val df_dual = spark.read.format("jdbc")
        .option("url", oracleUrl)
        .option("dbtable", "(SELECT 'Scala Connection Successful!' as status, SYSDATE as server_time, USER as current_user FROM dual)")
        .option("user", username)
        .option("password", password)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()

      df_dual.show(false)
      println("✅ Basic connection successful!")

      println("\n🎉 All Scala Oracle tests completed successfully!")
      println("==========================================")

    } catch {
      case e: Exception =>
        println(s"❌ An error occurred during Oracle connection: ${e.getMessage}")
        e.printStackTrace() 
        println("==========================================")
    } finally {
      spark.stop()
    }
  }
}