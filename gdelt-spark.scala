import com.aamend.spark.gdelt._

val gdeltEventDS: Dataset[Event] = spark.read.gdeltEvent("/path/to/event.csv")
val gdeltGkgDS: Dataset[GKGEvent] = spark.read.gdeltGkg("/path/to/gkg.csv")
val gdeltMention: Dataset[Mention] = spark.read.gdeltMention("/path/to/mention.csv")
