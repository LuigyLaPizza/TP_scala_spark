import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object Main {
  def main(args : Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()



    //Exercice 1 : RDD

    //1
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")
    rdd.foreach(println)

    //2
    val rddDiCaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    val nbFilmsDiCaprio = rddDiCaprio.count()
    println("Nombre de films de Di Caprio")
    println(nbFilmsDiCaprio)

    //3
    val MoyenneDiCaprio = rddDiCaprio.map(elem => elem.split(";")(2).toDouble).sum() / nbFilmsDiCaprio
    println("Moyenne des films de Di Caprio")
    println(MoyenneDiCaprio)

    //4
    val TotalVuesDiCaprio = rddDiCaprio.map(elem => elem.split(";")(1).toDouble).sum()
    val TotalVues = rdd.map(elem => elem.split(";")(1).toDouble).sum()
    println("Pourcentage Vues Di Caprio")
    println(TotalVuesDiCaprio / TotalVues)

    //5
    val rdd_notes_acteurs = rdd.map(elem => (elem.split(";")(3).trim, elem.split(";")(2).toDouble))
    val rdd_nbFilms_acteurs = rdd.map(elem => (elem.split(";")(3).trim, 1))
    val reduce_nbFilms_notes = rdd_nbFilms_acteurs.reduceByKey(_ + _).join(rdd_notes_acteurs.reduceByKey(_ + _))
    val rdd_moyenne_acteur = reduce_nbFilms_notes.map(elem => (elem._1,elem._2._2 / elem._2._1))

    println("note moyenne pour chaque acteur")
    rdd_moyenne_acteur.foreach(println)



    // Exercice 2 DataFrame

    //1
    val df = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")
    df.show
    df.printSchema()

    //2
    val df1 = df.withColumnRenamed("_c0","nom_film")
      .withColumnRenamed("_c1","nombre_vues")
      .withColumnRenamed("_c2","note_film")
      .withColumnRenamed("_c3", "acteur_principal")
      .withColumn("acteur_principal", trim(col("acteur_principal")))
    df1.show
    df1.printSchema()

    //3
    import sparkSession.implicits._
    val DiCaprio_df = df1.filter($"acteur_principal".contains("Di Caprio"))
    println("nombre de films de Di Caprio")
    println(DiCaprio_df.count())

    println("moyenne des films de Di Caprio")
    DiCaprio_df.groupBy().mean("note_film").show

    println("pourcentage de vues de Di Caprio")
    val total_vues = df1.groupBy().sum("nombre_vues").as[Long].collect()
    val vues_dicaprio = DiCaprio_df.groupBy().sum("nombre_vues").as[Long].collect()
    println(vues_dicaprio(0).toFloat / total_vues(0))

    println("moyenne des notes des acteurs")
    val df_moyenne_acteurs = df1.groupBy("acteur_principal").mean("note_film")
    df_moyenne_acteurs.select("acteur_principal", "avg(note_film)").show()

    //4
    val df_prcntVues = df1.groupBy("acteur_principal").sum("nombre_vues").withColumn("pourcentage_vues", col("sum(nombre_vues)") / total_vues(0))
    println("pourcentage de vues des films")
    df_prcntVues.show
    // il semble qu'il y ait une erreur dans les entrées du tableau vu de la réppartition des vues

  }

}