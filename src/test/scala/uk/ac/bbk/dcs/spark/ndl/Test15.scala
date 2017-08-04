package uk.ac.bbk.dcs.spark.ndl



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec
import pl.appsilon.marek.sparkdatalog.{Database, Relation}

/**
  * Created by rapissal on 03/08/2017.
  */
class Test15 extends FunSpec {

  describe("ndl rewriting") {
    it("should run the  following tests") {
      val config = new SparkConf().setAppName("Test15").setMaster("local[2]").set("spark.executor.memory", "1g")
      val sc = SparkContext.getOrCreate(config)
      sc.setCheckpointDir("/Users/rapissal/development/uni/sparkdatalog/sparkdatalog/src/test")

      //val sparkSession = SparkSession.builder.appName("Test-4core").master("local[4]").config("spark.eventLog.enabled", true).config("spark.eventLog.dir", "/").getOrCreate
      //sparkSession.conf.set("spark.ui.enabled", true)

      val customSchema_1 = new StructType(Array[StructField](
        StructField("att_0", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val customSchema_2 = new StructType(Array[StructField](
        StructField("att_0", DataTypes.LongType, nullable = false, Metadata.empty),
        StructField("att_1", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val a: RDD[Int] = sc.textFile("src/resources/data/20mb-a.txt").map( p => p.toInt )

      val b = sc.textFile("src/resources/data/20mb-b.txt").map( p => p.toInt )

      val r: RDD[(Int, Int)] = sc.textFile("src/resources/data/20mb-r.txt").map(line => (line.split(',')(0).toInt, line.split(',')(1).toInt) )

      val s = sc.textFile("src/resources/data/20mb-s.txt").map(line => (line.split(',')(0).toInt, line.split(',')(1).toInt) )

      //   1. Create a Database from Relations built from RDDs.
      val database = Database(
        Relation.unary("A", a),
        Relation.unary("B", b),
        Relation.binary("R", r),
        Relation.binary("S", s)
      )

      //   2. Execute a Datalog query on the database, producing a new Database.
      val query = """
                    |P_0_15 (x,y) :- P_0_7 (x, z),P_7_15 (z, y).
                    |P_0_7 (x,y) :- P_0_3 (x, z),P_3_7 (z, y).
                    |P_0_3 (x,y) :- R(x, z),P_1_3 (z, y).
                    |P_1_3 (x,y) :- R (x, z),S (z, y).
                    |P_1_3 (x,x) :- B(x).
                    |P_3_7 (x,y) :- P_3_5 (x, z),P_5_7 (z, y).
                    |P_3_5 (x,y) :- R (x, z),S (z, y).
                    |P_3_5 (x,x) :- B(x).
                    |P_5_7 (x,y) :- R (x, z),S (z, y).
                    |P_5_7 (x,x) :- B(x).
                    |P_3_7 (x,y) :- R(x, z), A(z),S (z, y).
                    |P_0_7 (x,y) :- P_0_2(x, z), A(z),P_4_7 (z, y).
                    |P_4_7 (x,y) :- S (x, z),P_5_7 (z, y).
                    |P_4_7 (x,y) :-  A(x), S(x, y) .
                    |P_0_2 (x,y) :- R (x, z),R (z, y).
                    |P_7_15 (x,y) :- P_7_11 (x, z),P_11_15 (z, y).
                    |P_7_11 (x,y) :- P_7_9 (x, z),P_9_11 (z, y).
                    |P_7_9 (x,y) :- R (x, z),R (z, y).
                    |P_9_11 (x,y) :- S (x, z),R (z, y).
                    |P_9_11 (x,x) :- A(x).
                    |P_7_11 (x,y) :- R(x, z), B(z),R (z, y).
                    |P_11_15 (x,y) :- P_11_13 (x, z),P_13_15 (z, y).
                    |P_11_13 (x,y) :- R (x, z),S (z, y).
                    |P_11_13 (x,x) :- B(x).
                    |P_13_15 (x,y) :- S (x, z),R (z, y).
                    |P_13_15 (x,x) :- A(x).
                    |P_0_15 (x,y) :- P_0_6(x, z), A(z),P_8_15 (z, y).
                    |P_8_15 (x,y) :- P_8_11 (x, z),P_11_15 (z, y).
                    |P_8_11 (x,y) :- R (x, z),P_9_11 (z, y).
                    |P_8_11 (x,y) :- B(x),R(x, y).
                    |P_0_6 (x,y) :- P_0_3 (x, z),P_3_6 (z, y).
                    |P_3_6 (x,y) :- R (x, z),P_4_6 (z, y).
                    |P_4_6 (x,y) :- S (x, z),R (z, y).
                    |P_4_6 (x,x) :- A(x).
                    |P_3_6 (x,y) :- B(x),R(x, y).
                    |P_0_6 (x,y) :- P_0_2(x, z), A(z),P_4_6 (z, y).
                  """.stripMargin
      val resultDatabase: Database = database.datalog(query)

      //   3. Retrieve the result from the new Database.
      val resultPathsRdd: RDD[Seq[Int]] = resultDatabase("P_0_15")


      // We can now save the paths RDD to distributed storage
      // or perform further computations on it.

      // We can of course also print it to stdout:
      print(resultPathsRdd.collect().map("Path(" + _.mkString(", ") + ")").mkString("\n"))


    }
  }
}
