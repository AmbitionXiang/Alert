package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q09 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }
    val exprString =  udf { (x: String, y: String, v: String, w: String) => s"(($x)*(1-($y))-($v)*($w))"}

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    Seq(linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"),
        exprString($"l_extendedprice_var", $"l_discount_var", $"ps_supplycost_var", $"l_quantity_var").as("amount_var"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"), concat_ws("+", collect_list($"amount_var")))
      .sort($"n_name", $"o_year".desc))

  }

}
