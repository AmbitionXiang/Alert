package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q08 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val decreaseString = udf { (x: String, y: String) => s"($x)*(1-($y))" }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }
    val isBrazilString = udf { (x: String, y: String) => if (x == "BRAZIL") y else "0" }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume"),
      decreaseString($"l_extendedprice_var", $"l_discount_var").as("volume_var")).
      join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    Seq(nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume", $"volume_var",
        isBrazil($"n_name", $"volume").as("case_volume"),
        isBrazilString($"n_name", $"volume_var").as("case_volume_var"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"),  concat(lit("("), concat_ws("+", collect_list($"case_volume_var")), lit(")/("), concat_ws("+", collect_list($"volume_var")), lit(")")))
      .sort($"o_year"))
  }

}
