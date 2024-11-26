package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q03 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val decreaseString = udf { (x: String, y: String) => s"($x)*(1-($y))" }

    val fcust = customer.filter($"c_mktsegment" === "BUILDING")
    val forders = order.filter($"o_orderdate" < "1995-03-15")
    val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

    Seq(fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        decreaseString($"l_extendedprice_var", $"l_discount_var").as("volume_var"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"), concat_ws("+", collect_list($"volume_var")))
      .sort($"revenue".desc, $"o_orderdate")
      .limit(10))
  }

}
