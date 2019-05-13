# Lab

## Downloading laboratory boilerplate

```terminal
git clone https://gitlab.com/liesner/bts-rtda-lab-5.git
cd bts-rtda-lab-5
```

## Data sets

- The data is a variation of  [population-city](https://github.com/datasets/population-city) data-sets.
    - ```data/city_male_population``` witch contain tha male population distributed by city
    - ```data/city_female_population``` witch contain tha female population distributed by city
- Each comma separate value on the rows of both data sets correspond to the headers: 

```terminal
Country or Area
Year
Area
Sex
City
City type
Record Type
Reliability
Source Year
Value
Value Footnotes
```    

## Running spark cluster container
- Start spark container and run spark-shell with 2 processor

```
docker-compose run spark bash
spark-shell --master local[2]
```

- Convert ```city_male_population``` and ```city_female_population``` onto RDD collection of row arrays:

```terminal
scala> val patternCsvCommaSeparate = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
patternCsvCommaSeparate: String = ,(?=(?:[^"]*"[^"]*")*[^"]*$)

scala> val city_male_population = sc.textFile("/appdata/data/city_male_population").map(l=>l.split(patternCsvCommaSeparate).map(term => term.trim()))
city_male_population: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[132] at map at <console>:26

scala> val city_female_population = sc.textFile("/appdata/data/city_female_population").map(l=>l.split(patternCsvCommaSeparate).map(term => term.trim()))
city_female_population: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[135] at map at <console>:26
```

- How many entries on ```city_male_population``` and .

```terminal
scala> city_female_population.count()
res30: Long = 14181

scala> city_male_population.count()
res31: Long = 14185
```

- Union of both collections
```terminal
scala> val city_population = city_female_population.union(city_male_population)
city_population: org.apache.spark.rdd.RDD[Array[String]] = UnionRDD[140] at union at <console>:27

scala> city_population.count()
res29: Long = 28366
```

- All component years on both collections

```terminal
scala> val allTrackedYears = city_population.map(row => row(1)).distinct().collect
allTrackedYears: Array[String] = Array(1999, 2006, 1988, 1991, 2013, 1995, 1984, 2002, 1985, 2010, 2003, 1989, 1996, 2014, 1992, 2007, 1970, 2008, 2011, 2004, 1986, 1993, 2000, 1990, 2001, 2012, 1987, 2009, 2005, 1998)
```

- Years tracked in one collection intersected with the years component of the other

```terminal
scala> val intercectedTrackedYears = city_male_population.map(row=>row(1)).intersection(city_female_population.map(row=>row(1))).collect()
intercectedTrackedYears: Array[String] = Array(2008, 1986, 2011, 1999, 2006, 1988, 2013, 2004, 1991, 2002, 1984, 1995, 2000, 1993, 1985, 1990, 2010, 2003, 2001, 1989, 2012, 1987, 1996, 2014, 1992, 2009, 2007, 2005, 1998, 1970)
```

- Total count of females and male grouped by year component
 
 ```terminal
scala> val male_by_year = city_male_population.map(row=>(row(1), row(9).toDouble)).reduceByKey((c1, c2)=>c1+c2)
male_by_year: org.apache.spark.rdd.RDD[(String, Double)] = ShuffledRDD[228] at reduceByKey at <console>:25

scala> male_by_year.first()
res42: (String, Double) = (2008,1.37132355E8)

scala> val female_by_year = city_female_population.map(row=>(row(1), row(9).toDouble)).reduceByKey((c1, c2)=>c1+c2)
female_by_year: org.apache.spark.rdd.RDD[(String, Double)] = ShuffledRDD[230] at reduceByKey at <console>:25

scala> female_by_year.first()
res43: (String, Double) = (2008,1.516464445E8)
```

- Inner join of ```female_by_year``` and ```male_by_year```

```terminal
scala> val innerJoinByYear = male_by_year.join(female_by_year)
innerJoinByYear: org.apache.spark.rdd.RDD[(String, (Double, Double))] = MapPartitionsRDD[245] at join at <console>:27

scala> innerJoinByYear.first()
res44: (String, (Double, Double)) = (2008,(1.37132355E8,1.516464445E8))
```











