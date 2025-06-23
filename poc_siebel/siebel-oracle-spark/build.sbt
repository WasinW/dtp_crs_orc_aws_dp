// build.sbt
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "siebel-oracle-spark",
    resolvers += "Oracle Maven Repository" at "https://maven.oracle.com",
    
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "com.google.cloud" % "google-cloud-secretmanager" % "2.33.0",
      "org.json4s" %% "json4s-native" % "4.0.7"
    ),
  )

// **แก้ไข assembly settings ตรงนี้ให้เป็นรูปแบบที่เรียบง่ายและปลอดภัยที่สุด**
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.filterDistinctLines
  // จัดการไฟล์ License/Notice/Manifest/Signature ที่มักจะ Conflict
  case PathList("META-INF", "LICENSE") | PathList("META-INF", "LICENSE.txt") | PathList("META-INF", "NOTICE") | PathList("META-INF", "NOTICE.txt") => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  
  // รูปแบบใหม่สำหรับไฟล์ .DSA, .RSA, .SF โดยไม่ใช้ xs.last
  case PathList("META-INF", file) if file.endsWith(".DSA") || file.endsWith(".RSA") || file.endsWith(".SF") => MergeStrategy.discard

  case PathList("META-INF", "maven", _*) => MergeStrategy.discard // สำหรับ Maven POM files
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("version.properties") => MergeStrategy.concat

  // Default: ใช้ไฟล์แรกที่เจอสำหรับกรณีอื่นๆ ทั้งหมด
  // ซึ่งจะรวม Class files และทรัพยากรอื่นๆ ที่ไม่ถูกจัดการโดย MergeStrategy ด้านบน
  case x => MergeStrategy.first 
}

// Main class (ยืนยันชื่อ Class และชื่อ JAR อีกครั้ง)
assembly / mainClass := Some("com.siebel.oracle.SimpleOracleConnectionTest")
assembly / assemblyJarName := "siebel-oracle-spark-1.0.0.jar"

