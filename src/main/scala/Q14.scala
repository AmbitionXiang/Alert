package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q14 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val reduceString = udf { (x: String, y: String) => s"($x)*(1-($y))"}
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }
    val promoString = udf { (x: String, y: String) => if (x.startsWith("PROMO")) y else "0" }
    val mul100divString = udf { (x: String, y: String) => s"(($x)*100)/($y)"}

    Seq(part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"), reduceString($"l_extendedprice_var", $"l_discount_var").as("value_var"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"), mul100divString(concat_ws("+", collect_list(promoString($"p_type", $"value_var"))), concat_ws("+", collect_list($"value_var")))))
  }

}
