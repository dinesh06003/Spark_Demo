def pairs(str: Array[String]) = {
 val users = str(1).split(",")
val user=str(0)

val n = users.length

 for(i <- 0 until n) yield {
 
  val pair = if(user < users(i)) {
    (user,users(i))
  } else {
   (users(i),user)
  }
(pair, users)
 }
 
   
}

val startTime = System.nanoTime()

val data = sc.textFile("hdfs://localhost:9000/user/sg8rq/InputFolder/soc-LiveJournal1Adj.txt")
//val data1 = sc.parallelize(data.take(44997))

val pairCounts  = data.map(x=>x.split("\t")).filter(list => (list.size == 2)).flatMap(pairs)
val p = pairCounts.reduceByKey({ case (parameter1,parameter2) => (parameter1.intersect(parameter2)) })

val p1=p.map({case ((parameter1, parameter2),parameter3) => (parameter1+"\t"+parameter2+"\t"+parameter3.mkString(","))})

//p1.saveAsTextFile("hdfs://localhost:9000/user/sg8rq/OutputFolder1/")


val friendscount = p.map{case(friend, friendslist) =>(friend, friendslist.size)}

val maxValue = friendscount.values.max

friendscount.filter { case (_, value) => value == maxValue }.saveAsTextFile("hdfs://localhost:9000/user/sg8rq/OutputFolderB/")



val endTime = System.nanoTime()
val elapsedTimeInMillis = (endTime - startTime) / 1000000.0

System.out.println(s"Time taken: $elapsedTimeInMillis milliseconds")


