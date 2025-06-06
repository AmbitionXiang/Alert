package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q05 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val decreaseString = udf { (x: String, y: String) => s"(($x)*(1-($y)))" }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    Seq(region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_extendedprice_var", $"l_discount", $"l_discount_var", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"), decreaseString($"l_extendedprice_var", $"l_discount_var").as("value_var"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"), concat(lit("["), concat_ws("+", collect_list($"value_var")), lit("]")))
      .sort($"revenue".desc))
  }

}
