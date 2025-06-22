// SimpleOracleTNFMTest.scala
package com.siebel.oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, Level}

object SimpleOracleTNFMTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.siebel.oracle").setLevel(Level.INFO) // เปลี่ยนเป็น INFO สำหรับ Production Logs
    Logger.getLogger("oracle.jdbc").setLevel(Level.INFO) // เปลี่ยนเป็น INFO สำหรับ Production Logs
    // ถ้าต้องการ debug เพิ่มเติม สามารถเปลี่ยนเป็น Level.DEBUG/TRACE ได้
    Logger.getLogger("org.apache.spark.sql.execution.datasources.jdbc").setLevel(Level.DEBUG) // สำหรับ JDBC Query Details

    val spark = SparkSession.builder()
      .appName("Scala Oracle Transform and Iceberg Test")
      // เพิ่ม Iceberg Catalog configuration ที่นี่
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop") // หรือ "hive"
      .config("spark.sql.catalog.spark_catalog.warehouse", "gs://your-iceberg-warehouse-bucket/warehouse") // GCS bucket สำหรับ Iceberg warehouse
      .getOrCreate()

    // Oracle RDS connection configuration (รับจาก Environment Variables)
    val oracleUrl = sys.env("ORACLE_URL")
    val username = sys.env("ORACLE_USER")
    val password = sys.env("ORACLE_PASSWORD")

    println(s"DEBUG: Actual ORACLE_URL from env: $oracleUrl")
    println(s"DEBUG: Actual ORACLE_USER from env: $username")
    println(s"DEBUG: Actual ORACLE_PASSWORD from env: ${"*" * password.length}")

    try {
      // 1. อ่าน Data จาก Oracle (Bronze Layer)
      println("\n--- Step 1: Ingesting Bronze Layer from Oracle ---")
      val bronzeDF = spark.read.format("jdbc")
        .option("url", oracleUrl)
        // ดึงตารางจริงที่คุณต้องการ (เช่น EMPLOYEES)
        .option("dbtable", "EMPLOYEES") // สมมติว่ามีตาราง EMPLOYEES
        .option("user", username)
        .option("password", password)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()

      bronzeDF.printSchema()
      bronzeDF.show(5)
      println(s"Bronze Layer: Read ${bronzeDF.count()} records from EMPLOYEES.")

      // สร้าง Temp View สำหรับ Bronze Layer
      bronzeDF.createOrReplaceTempView("bronze_employees")
      println("--- Bronze Layer (bronze_employees) created as Temp View ---")

      // 2. Transform Data (Silver Layer)
      println("\n--- Step 2: Transforming to Silver Layer ---")
      val silverDF = spark.sql(
        """
          SELECT
            EMP_ID,
            NAME,
            DEPARTMENT,
            SALARY,
            SALARY * 1.05 as ANNUAL_SALARY_INCREASED, -- ตัวอย่างการ Transform
            current_timestamp() as load_timestamp
          FROM bronze_employees
          WHERE SALARY > 50000
        """
      )

      silverDF.printSchema()
      silverDF.show(5)
      println(s"Silver Layer: Transformed ${silverDF.count()} records.")

      // 3. Insert into Iceberg Table (Gold Layer หรือ Staging Iceberg)
      println("\n--- Step 3: Inserting into Iceberg Table ---")
      // ตรวจสอบว่า spark_catalog.your_iceberg_table มีอยู่แล้ว หรือจะสร้างใหม่
      // ใน Dataproc Cluster ต้องเปิด Iceberg Component
      
      // สร้างตาราง Iceberg หากยังไม่มี
      spark.sql(
        s"CREATE TABLE IF NOT EXISTS spark_catalog.employee_transformed (" +
        s"  EMP_ID BIGINT," +
        s"  NAME STRING," +
        s"  DEPARTMENT STRING," +
        s"  SALARY DOUBLE," +
        s"  ANNUAL_SALARY_INCREASED DOUBLE," +
        s"  load_timestamp TIMESTAMP" +
        s") USING iceberg"
      )
      println("Iceberg table 'employee_transformed' ensured exists.")

      // เขียนข้อมูลลง Iceberg
      silverDF.writeTo("spark_catalog.employee_transformed").append() // หรือ .overwrite() หากต้องการเขียนทับ
      println("✅ Data successfully written to Iceberg table 'employee_transformed'!")

      // (Optional) Query จาก Iceberg เพื่อยืนยัน
      println("\n--- Verifying Data from Iceberg ---")
      val finalDF = spark.table("spark_catalog.employee_transformed")
      finalDF.show()
      println(s"Total records in Iceberg table: ${finalDF.count()}")

      println("\n🎉 All Scala Oracle, Transform, and Iceberg tests completed successfully!")
      println("==========================================")

    } catch {
      case e: Exception =>
        println(s"❌ An error occurred: ${e.getMessage}")
        e.printStackTrace()
        println("==========================================")
    } finally {
      spark.stop()
    }
  }
}