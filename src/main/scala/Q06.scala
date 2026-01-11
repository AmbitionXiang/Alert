package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Q06 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): Seq[DataFrame] = {
    import spark.implicits._
    import schemaProvider._

    val multString = udf { (x: String, y: String) => s"(($x)*($y))" }

    Seq(lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 5 && $"l_discount" <= 7 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"), concat(lit("["), concat_ws("+", collect_list(multString($"l_extendedprice_var", $"l_discount_var"))), lit("]"))))
  }

}
