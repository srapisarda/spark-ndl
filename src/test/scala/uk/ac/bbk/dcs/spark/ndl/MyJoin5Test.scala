package uk.ac.bbk.dcs.spark.ndl


import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec


/**
  * Created by rapissal on 03/08/2017.
  */
class MyJoin5Test extends FunSpec {

  describe("ndl rewriting") {
    it("should run the  following tests") {
      val config = new SparkConf().setAppName("Test15").setMaster("local[2]").set("spark.executor.memory", "1g")

      val sc = SparkContext.getOrCreate(config)
      sc.setCheckpointDir("/Users/rapissal/development/uni/sparkdatalog/sparkdatalog/src/test")


      val a = sc.textFile("src/resources/data/20mb-a.txt").map(x => (x.toInt, x.toInt))

      val b = sc.textFile("src/resources/data/20mb-b.txt").map(x => (x.toInt, x.toInt))

      val r = new IndexedRdd(sc.textFile("src/resources/data/20mb-r.txt").map(line => (line.split(',')(0).toInt, line.split(',')(1).toInt)))

      val s = new IndexedRdd(sc.textFile("src/resources/data/20mb-s.txt").map(line => (line.split(',')(0).toInt, line.split(',')(1).toInt)))

      def myJoin(firstRelation: Any, secondRelation: Any): org.apache.spark.rdd.RDD[(Int, Int)] = {
        ???
//        val actualSecondRelation:RDD[(Int, Int)] = secondRelation match {
//          case i: IndexedRdd => i.relation
//          case rdd: org.apache.spark.rdd.RDD[(Int, Int)] => secondRelation
//        }
//        val actualFirstRelation = firstRelation match {
//          case i: IndexedRdd => i.index
//          case rdd: org.apache.spark.rdd.RDD[(Int, Int)] => firstRelation.relation.map(t => (t._2, t._1))
//        }
//        actualFirstRelation.join(secondRelation).values
      }


      //P_7_9(X,Y) :- R(X,Z), R(Z,Y).

      val P_7_9 = myJoin(r, r)

      //P_5_7(X,Y) :- R(X,Z), S(Z,Y).
      //P_5_7(X,X) :- B(X).

      val P_5_7 = b.union(myJoin(r, s))

      //P_1_3(X,Y) :- R(X,Z), S(Z,Y).
      //P_1_3(X,X) :- B(X).

      val P_1_3 = b.union(myJoin(r, s))

      //P_4_7(X,Y) :- S(X,Z), P_5_7(Z,Y).
      //P_4_7(X,Y) :- A(X), S(X,Y).

      val P_4_7 = a.union(myJoin(s, P_5_7))

      //P_9_11(X,Y) :- S(X,Z), R(Z,Y).
      //P_9_11(X,X) :- A(X).

      val P_9_11 = a.union(myJoin(s, r))

      //P_13_15(X,Y) :- S(X,Z), R(Z,Y).
      //P_13_15(X,X) :- A(X).

      val P_13_15 = a.union(myJoin(s, r))

      //P_11_13(X,Y) :- R(X,Z), S(Z,Y).
      //P_11_13(X,X) :- B(X).

      val P_11_13 = b.union(myJoin(r, s))

      //P_11_15(X,Y) :- P_11_13(X,Z), P_13_15(Z,Y).

      val P_11_15 = myJoin(P_11_13, P_13_15)

      //P_8_11(X,Y) :- R(X,Z), P_9_11(Z,Y).
      //P_8_11(X,Y) :- B(X), R(X,Y).

      val P_8_11 = myJoin(r, P_9_11).union(myJoin(b, r))

      //P_8_15(X,Y) :- P_8_11(X,Z), P_11_15(Z,Y).

      val P_8_15 = myJoin(P_8_11, P_11_15)

      //P_3_5(X,Y) :- R(X,Z), S(Z,Y).
      //P_3_5(X,X) :- B(X).

      val P_3_5 = b.union(myJoin(r, s))

      //P_3_7(X,Y) :- P_3_5(X,Z), P_5_7(Z,Y).
      //P_3_7(X,Y) :- R(X,Z), A(Z), S(Z,Y).

      val P_3_7 = myJoin(P_3_5, P_5_7).union(myJoin(myJoin(r, a), s))

      //P_4_6(X,Y) :- S(X,Z), R(Z,Y).
      //P_4_6(X,X) :- A(X).

      val P_4_6 = a.union(myJoin(s, r))


      //P_3_6(X,Y) :- R(X,Z), P_4_6(Z,Y).
      //P_3_6(X,Y) :- B(X), R(X,Y).

      val P_3_6 = myJoin(r, P_4_6).union(myJoin(b, r))


      //P_7_11(X,Y) :- P_7_9(X,Z), P_9_11(Z,Y).
      //P_7_11(X,Y) :- R(X,Z), B(Z), R(Z,Y).

      val P_7_11 = myJoin(P_7_9, P_9_11).union(myJoin(myJoin(r, b), r))

      //P_7_15(X,Y) :- P_7_11(X,Z), P_11_15(Z,Y).

      val P_7_15 = myJoin(P_7_11, P_11_15)

      //P_0_2(X,Y) :- R(X,Z), R(Z,Y).

      val P_0_2 = myJoin(r, r)

      //P_0_3(X,Y) :- R(X,Z), P_1_3(Z,Y).

      val P_0_3 = myJoin(r, P_1_3)


      //P_0_6(X,Y) :- P_0_3(X,Z), P_3_6(Z,Y).
      //P_0_6(X,Y) :- P_0_2(X,Z), A(Z), P_4_6(Z,Y).

      val P_0_6 = myJoin(P_0_3, P_3_6).union(myJoin(myJoin(P_0_2, a), P_4_6))

      //P_0_7(X,Y) :- P_0_3(X,Z), P_3_7(Z,Y).
      //P_0_7(X,Y) :- P_0_2(X,Z), A(Z), P_4_7(Z,Y).

      val P_0_7 = myJoin(P_0_3, P_3_7).union(myJoin(myJoin(P_0_2, a), P_4_7))


      //P_0_15(X,Y) :- P_0_7(X,Z), P_7_15(Z,Y).
      //P_0_15(X,Y) :- P_0_6(X,Z), A(Z), P_8_15(Z,Y).

      val P_0_15 = myJoin(P_0_7, P_7_15).union(myJoin(myJoin(P_0_6, a), P_8_15))

      assert( P_0_15.distinct.count >0 )

    }
  }
}
