package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q17 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val mul02 = udf { (x: Double) => x * 0.2 }
    val mul02String = udf { (x: String) => s"($x)*0.2" }
    val avgString = udf { (x: String, y: String) => s"($x)/($y)" }
    val div7 = udf { (x: String) => s"($x)/1.0" }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_quantity_var", $"l_extendedprice", $"l_extendedprice_var")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    Seq(fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"), mul02String(avgString(concat_ws("+", collect_list($"l_quantity_var")), count($"l_quantity"))).as("avg_quantity_var"))
      .select($"p_partkey".as("key"), $"avg_quantity", $"avg_quantity_var")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 1.0, div7(concat_ws("+", collect_list($"l_extendedprice_var")))))
  }

}
