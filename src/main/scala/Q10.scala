package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q10 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val decreaseString = udf { (x: String, y: String) => s"(($x)*(1-($y)))" }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    Seq(order.filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"), decreaseString($"l_extendedprice_var", $"l_discount_var").as("volume_var"), 
        $"n_name", $"c_address", $"c_phone", $"c_comment")  // remove c_acctbal
      .groupBy($"c_custkey", $"c_name", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"), concat(lit("["), concat_ws("+", collect_list($"volume_var")).as("revenue_var"), lit("]")))
      .sort($"revenue".desc)
      .limit(20))
  }

}
