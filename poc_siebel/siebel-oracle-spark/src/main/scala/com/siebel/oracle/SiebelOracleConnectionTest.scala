package com.siebel.oracle

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.log4j.{Logger, Level} // สำหรับการ Debug Logging

import com.google.cloud.secretmanager.v1.{AccessSecretVersionRequest, SecretManagerServiceClient, SecretVersionName}
import org.json4s._ // สำหรับ DefaultFormats
import org.json4s.native.JsonMethods._ // สำหรับ parse method

object SimpleOracleConnectionTest {
  // ฟังก์ชันช่วยดึง Secret จาก Secret Manager
  def getSecret(projectId: String, secretId: String, versionId: String = "latest"): String = {
    var client: SecretManagerServiceClient = null
    try {
      client = SecretManagerServiceClient.create()
      val secretVersionName = SecretVersionName.of(projectId, secretId, versionId)
      val request = AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()
      val response = client.accessSecretVersion(request)
      response.getPayload.getData.toStringUtf8()
    } catch {
      case e: Exception =>
        println(s"❌ Error accessing secret '$secretId': ${e.getMessage}")
        e.printStackTrace()
        throw e // Re-throw exception if secret cannot be accessed
    } finally {
      if (client != null) client.close()
    }
  }


  def main(args: Array[String]): Unit = {

    // กำหนด Log Level ให้ละเอียดขึ้น (สำหรับการ Debug)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.siebel.oracle").setLevel(Level.DEBUG)
    Logger.getLogger("oracle.jdbc").setLevel(Level.DEBUG) // สำคัญสำหรับการ Debug JDBC
    Logger.getLogger("org.apache.spark.deploy").setLevel(Level.TRACE) // สำหรับ Deploy
    Logger.getLogger("org.apache.spark.scheduler").setLevel(Level.TRACE) // สำหรับ Scheduler
    // sqlplus 'siebeladmin/Siebel2024@//siebel-oracle-20250614-114148.cbxbuf3oy0lw.ap-southeast-1.rds.amazonaws.com:1521/SIEBEL'

    val gcpProjectId = sys.env.getOrElse("GCP_PROJECT_ID", "ntt-test-data-bq-looker") // ควรส่งจาก gcloud command

    val spark = SparkSession.builder()
      .appName("Scala Oracle Connection Test")
      // ไม่ต้องตั้งค่า spark.jars หรือ extraClassPath ที่นี่โดยตรง
      // เพราะจะส่งผ่าน --jars และ --properties ใน gcloud command
      .getOrCreate()

    // Oracle RDS connection configuration
    // อ่านค่าจาก Environment Variables
    val oracleUrl = sys.env.getOrElse("ORACLE_URL", 
      "jdbc:oracle:thin:@//siebel-oracle-20250614-114148.cbxbuf3oy0lw.ap-southeast-1.rds.amazonaws.com:1521/SIEBEL") // Default สำหรับทดสอบ
    val username = sys.env.getOrElse("ORACLE_USER", "siebeladmin") // Default สำหรับทดสอบ
    val password = sys.env.getOrElse("ORACLE_PASSWORD", "Siebel2024") // Default สำหรับทดสอบ
    
    // val connectionJsonString = getSecret(gcpProjectId, "sieble-connection") // ชื่อ Secret ที่คุณตั้งไว้
    // implicit val formats: DefaultFormats.type = DefaultFormats // จำเป็นสำหรับ json4s
    // val connectionDetails = parse(connectionJsonString).extract[Map[String, String]] // Parse เป็น Map

    // ดึง user และ pass จาก Map
    // val username = connectionDetails("user")
    // val password = connectionDetails("pass")

    println(s"DEBUG: Actual ORACLE_URL from env: $oracleUrl")
    println(s"DEBUG: Actual ORACLE_USER from env: $username")
    println(s"DEBUG: Actual ORACLE_PASSWORD from env: ${"*" * password.length}")

    // พิมพ์ค่าที่ใช้เพื่อยืนยัน (จะปรากฏใน Driver logs)
    println("==========================================")
    println(s"🔍 Connecting to: $oracleUrl")
    println(s"👤 User: $username")
    println("==========================================")

    try {
      // Test: Basic connection to DUAL table
      println("\n📋 Test: Basic Connection to DUAL table")
      val df_dual = spark.read.format("jdbc")
        .option("url", oracleUrl)
        .option("dbtable", "(SELECT * FROM departments)")
        .option("user", username)
        .option("password", password)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()
      print("======================================================================  after spark read ======================================================================")
      df_dual.show(false)
      println("✅ Basic connection successful")

      println("\n🎉 All Scala Oracle tests completed successfully")
      println("==========================================")

    } catch {
      case e: Exception =>
        println(s"❌ An error occurred during Oracle connection: ${e.getMessage}")
        e.printStackTrace() // พิมพ์ Stack Trace
        // ไม่ต้อง System.exit(1) เพื่อให้ Log ออกมาครบ
        // และ Thread.sleep ก็ไม่จำเป็นแล้วถ้าไม่มี System.exit(1)
        // เพิ่ม Thread.sleep เพื่อให้มีเวลาเขียน Log ออกมา
        Thread.sleep(60000) // รอ 60 วินาที
        println("==========================================")
    } finally {
      spark.stop()
    }
  }
}
