package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q18 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    Seq(lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"), concat(lit("("), concat_ws("+", collect_list($"l_quantity_var")), lit(")")).as("sum_quantity_var"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity", $"sum_quantity_var")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"l_quantity_var", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice") // remove o_totalprice
      .agg(sum("l_quantity").as("sum_quantity"), concat(lit("("), concat_ws("+", collect_list($"l_quantity_var")), lit(")")).as("sum_quantity_var"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .select($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"sum_quantity", $"sum_quantity_var")
      .limit(100))
  }

}
