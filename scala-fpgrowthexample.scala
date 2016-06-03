import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

object FPgrowthExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FPGrowth Example")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth().setMinSupport(0.5).setNumPartitions(10)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset => println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq) }

    val minConfidence = 0.8

    model.generateAssociationRules(minConfidence).collect().foreach { rule => println(rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence) }

    model.freqItemsets.map { fis => s"""[${fis.items.mkString(",")}], ${fis.freq}"""}.coalesce(1, true).saveAsTextFile(args(1))
  }
}

