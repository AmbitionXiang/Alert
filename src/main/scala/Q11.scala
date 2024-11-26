package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q11 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }
    val mulString = udf { (x: String, y: String) => s"($x)*($y)" }
    val mul01String = udf { (x: String) => s"($x)*0.0001" }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"), mulString($"ps_supplycost_var", $"ps_availqty_var").as("value_var"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"), concat_ws("+", collect_list($"value_var")).as("total_value_var"))

    Seq(tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"), concat_ws("+", collect_list($"value_var")).as("part_value_var"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc))
  }

}
