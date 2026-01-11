package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q15 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (100 - y) }
    val decreaseString = udf { (x: String, y: String) => s"(($x)*(100-($y)))" }

    val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
      $"l_shipdate" < "1996-04-01")
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"), decreaseString($"l_extendedprice_var", $"l_discount_var").as("value_var"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"), concat(lit("("), concat_ws("+", collect_list($"value_var")), lit(")")).as("total_var"))
    // .cache

    Seq(revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total", $"total_var")
      .sort($"s_suppkey"))
  }

}
