package services

import org.apache.spark.rdd.RDD

trait WordCounter {
  /**
    *
    * @param lines: RDD[String] representing a distribute collection os strings
    * @return RDD[(String, Int)] distribute collection of (key, value) par where each
    *         key represent the word and value representing the times this word
    *         is repeated on the collections of string
    */
  def call(input: RDD[String]): RDD[(String, Int)]
}

class WordCounterImpl extends WordCounter {

  def call(lines: RDD[String]): RDD[(String, Int)] = {
    val output = lines.flatMap(line => line.split(' '))
      // each line is split on a collection of word (map part) and
      // then all collections are concatenated on a big collection of
      // strings (flat part)
      .map(word => (word, 1))
      // Each element on previous collection is mapped to a (key, value) par
      // key == word, value == 1
      .reduceByKey((v1, v2) => v1 + v2)
      // grouping by key and applying reduce operation (+) over the values on
      // each key

    //last sentence equivalent to return
    output
  }
}
