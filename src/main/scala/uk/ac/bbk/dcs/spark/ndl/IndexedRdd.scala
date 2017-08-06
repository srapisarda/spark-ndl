package uk.ac.bbk.dcs.spark.ndl

import org.apache.spark.rdd.RDD

/**
  * Created by salvo on 06/08/2017.
  */
case class IndexedRdd(relation: org.apache.spark.rdd.RDD[(Int, Int)]) {
  val index: RDD[(Int, Int)] = relation.map(t => (t._2, t._1))
}
