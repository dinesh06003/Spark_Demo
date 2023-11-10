val loadfile = sc.textFile("hdfs://localhost:9000/user/mrpk9/InputFolder/input1.txt")
val words = loadfile.flatMap(line => line.split(" "))
val wordMap = words.map(word => (word,1))
val wordCount = wordMap.reduceByKey(_+_)
val sorted = wordCount.sortBy(-_._2)
val filter = sorted.filter(_._2 > 1)
filter.saveAsTextFile("hdfs://localhost:9000/user/mrpk9/OutputFold/")


//Run the scala file using the following command 
//spark-shell -i '/home/mrpk9/test_scala.scala'

//Please replace the paths according to your file locations
