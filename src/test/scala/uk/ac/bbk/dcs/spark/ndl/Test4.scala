package uk.ac.bbk.dcs.spark.ndl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.scalatest.FunSpec

/**
  * Created by rapissal on 03/08/2017.
  */
class Test4 extends FunSpec {

  describe("ndl rewriting") {
    it("should run the  following tests") {

      val sparkSession = SparkSession.builder.appName("Test-4core").master("local[4]").config("spark.eventLog.enabled", true).config("spark.eventLog.dir", "/").getOrCreate
      sparkSession.conf.set("spark.ui.enabled", true)

      val customSchema_1 = new StructType(Array[StructField](
        StructField("att_0", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val customSchema_2 = new StructType(Array[StructField](
        StructField("att_0", DataTypes.LongType, nullable = false, Metadata.empty),
        StructField("att_1", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val df_a = sparkSession.read.option("header", "true").schema(customSchema_1).csv("src/resources/data/20mb-a.csv")
      df_a.createOrReplaceTempView("A")

      val df_b = sparkSession.read.option("header", "true").schema(customSchema_1).csv("src/resources/data/20mb-b.csv")
      df_b.createOrReplaceTempView("B")

      val df_r = sparkSession.read.option("header", "true").schema(customSchema_2).csv("src/resources/data/20mb-r.csv")
      df_r.createOrReplaceTempView("R")

      val df_s = sparkSession.read.option("header", "true").schema(customSchema_2).csv("src/resources/data/20mb-s.csv")
      df_s.createOrReplaceTempView("S")

      val P_7_9 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0, atom_1.att_1  AS att_1   FROM R AS atom_0, R AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_7_9.createOrReplaceTempView("P_7_9")


      val P_5_7 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,S AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM B AS atom_0 )")

      P_5_7.createOrReplaceTempView("P_5_7")


      val P_1_3 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,S AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM B AS atom_0 )")

      P_1_3.createOrReplaceTempView("P_1_3")


      val P_4_7 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM S AS atom_0,P_5_7 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM A AS atom_0,S AS atom_1  WHERE atom_0.att_0 = atom_1.att_0)")

      P_4_7.createOrReplaceTempView("P_4_7")


      val P_9_11 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM S AS atom_0,R AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM A AS atom_0 )")

      P_9_11.createOrReplaceTempView("P_9_11")


      val P_13_15 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM S AS atom_0,R AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM A AS atom_0 )")

      P_13_15.createOrReplaceTempView("P_13_15")


      val P_11_13 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,S AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM B AS atom_0 )")

      P_11_13.createOrReplaceTempView("P_11_13")


      val P_11_15 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_11_13 AS atom_0,P_13_15 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_11_15.createOrReplaceTempView("P_11_15")


      val P_8_11 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,P_9_11 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM B AS atom_0,R AS atom_1  WHERE atom_0.att_0 = atom_1.att_0)")

      P_8_11.createOrReplaceTempView("P_8_11")


      val P_8_15 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_8_11 AS atom_0,P_11_15 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_8_15.createOrReplaceTempView("P_8_15")


      val P_3_5 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,S AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM B AS atom_0 )")

      P_3_5.createOrReplaceTempView("P_3_5")


      val P_3_7 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_3_5 AS atom_0,P_5_7 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_2.att_1  AS att_1   FROM R AS atom_0,A AS atom_1,S AS atom_2  WHERE atom_0.att_1 = atom_1.att_0 AND atom_0.att_1 = atom_2.att_0)")

      P_3_7.createOrReplaceTempView("P_3_7")


      val P_4_6 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM S AS atom_0,R AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_0.att_0  AS att_1   FROM A AS atom_0 )")

      P_4_6.createOrReplaceTempView("P_4_6")


      val P_3_6 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,P_4_6 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM B AS atom_0,R AS atom_1  WHERE atom_0.att_0 = atom_1.att_0)")

      P_3_6.createOrReplaceTempView("P_3_6")


      val P_7_11 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_7_9 AS atom_0,P_9_11 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_2.att_1  AS att_1   FROM R AS atom_0,B AS atom_1,R AS atom_2  WHERE atom_0.att_1 = atom_1.att_0 AND atom_0.att_1 = atom_2.att_0)")

      P_7_11.createOrReplaceTempView("P_7_11")


      val P_7_15 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_7_11 AS atom_0,P_11_15 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_7_15.createOrReplaceTempView("P_7_15")


      val P_0_2 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,R AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_0_2.createOrReplaceTempView("P_0_2")


      val P_0_3 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM R AS atom_0,P_1_3 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0)")

      P_0_3.createOrReplaceTempView("P_0_3")


      val P_0_6 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_0_3 AS atom_0,P_3_6 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_2.att_1  AS att_1   FROM P_0_2 AS atom_0,A AS atom_1,P_4_6 AS atom_2  WHERE atom_0.att_1 = atom_1.att_0 AND atom_0.att_1 = atom_2.att_0)")

      P_0_6.createOrReplaceTempView("P_0_6")


      val P_0_7 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_0_3 AS atom_0,P_3_7 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_2.att_1  AS att_1   FROM P_0_2 AS atom_0,A AS atom_1,P_4_7 AS atom_2  WHERE atom_0.att_1 = atom_1.att_0 AND atom_0.att_1 = atom_2.att_0)")

      P_0_7.createOrReplaceTempView("P_0_7")


      val P_0_15 = sparkSession.sql("( SELECT DISTINCT atom_0.att_0  AS att_0,atom_1.att_1  AS att_1   FROM P_0_7 AS atom_0,P_7_15 AS atom_1  WHERE atom_0.att_1 = atom_1.att_0) UNION ( SELECT DISTINCT atom_0.att_0  AS att_0,atom_2.att_1  AS att_1   FROM P_0_6 AS atom_0,A AS atom_1,P_8_15 AS atom_2  WHERE atom_0.att_1 = atom_1.att_0 AND atom_0.att_1 = atom_2.att_0)")

      P_0_15.createOrReplaceTempView("P_0_15")

      P_0_15.count


    }
  }
}
