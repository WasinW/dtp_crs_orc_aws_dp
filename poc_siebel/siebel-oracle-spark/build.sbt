ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "siebel-oracle-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "com.oracle.database.jdbc" % "ojdbc11" % "23.7.0.0.1" % "provided" // สำคัญ: "provided"
    ),
  )

// Assembly settings for creating fat JAR (ถ้าใช้ sbt-assembly)
// ถ้าไม่ใช้ sbt-assembly, ไม่ต้องใส่ส่วนนี้
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

// Main class
assembly / mainClass := Some("com.siebel.oracle.SimpleOracleConnectionTest") // ชื่อ Class ที่คุณจะรัน
assembly / assemblyJarName := "siebel-oracle-spark-1.0.0.jar"