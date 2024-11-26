package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q01 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }
    val decreaseString = udf { (x: String, y: String) => s"($x)*(1-($y))" }
    val increaseString = udf { (x: String, y: String) => s"($x)*(1+($y))" }
    val avgString = udf { (x: String, y: String) => s"($x)/($y)" }

    Seq(schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), concat_ws("+", collect_list($"l_quantity_var")),
        sum($"l_extendedprice"), concat_ws("+", collect_list($"l_extendedprice_var")),
        sum(decrease($"l_extendedprice", $"l_discount")), concat_ws("+", collect_list(decreaseString($"l_extendedprice_var", $"l_discount_var"))),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")), concat_ws("+", collect_list(increaseString(decreaseString($"l_extendedprice_var", $"l_discount_var"), $"l_tax_var"))),
        avg($"l_quantity"),  
        avg($"l_extendedprice"), 
        avg($"l_discount"), avgString(concat_ws("+", collect_list($"l_discount_var")), count($"l_discount")),
        count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus"))
  }
}
