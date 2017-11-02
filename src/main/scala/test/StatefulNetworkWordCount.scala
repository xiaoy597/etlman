package test

/**
  * Created by xiaoy on 2017/11/2.
  */
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming._

/**
  * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
  * second starting with initial value of word count.
  * Usage: StatefulNetworkWordCount <hostname> <port>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
  * data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example
  * org.apache.spark.examples.streaming.StatefulNetworkWordCount localhost 9999`
  */
object StatefulNetworkWordCount {
  def myUpdateFunc(newValues: Seq[Int], runningCount: Option[Int]):Option[Int] = {
    Some(0)
  }

  def main(args: Array[String]) {

    ///函数常量定义，返回类型是Some(Int)，表示的含义是最新状态
    ///函数的功能是将当前时间间隔内产生的Key的value集合，加到上一个状态中，得到最新状态
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    ///入参是三元组遍历器，三个元组分别表示Key、当前时间间隔内产生的对应于Key的Value集合、上一个时间点的状态
    ///newUpdateFunc的返回值要求是iterator[(String,Int)]类型的
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      ///对每个Key调用updateFunc函数(入参是当前时间间隔内产生的对应于Key的Value集合、上一个时间点的状态）得到最新状态
      ///然后将最新状态映射为Key和最新状态
      // Some(x).map(x => (x, 0)): Some((x, 0))
      // Iterator[x].flatmap(x => Some((x,y))): Iterator[(x,y)]
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[3]")
    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint(".")
    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc')
    val lines = ssc.socketTextStream("192.168.26.140", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using updateStateByKey
    // This will give a Dstream made of state (which is the cumulative count of the words)
    //注意updateStateByKey的四个参数，第一个参数是状态更新函数
    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)

    // 另外一种创建state stream的写法，默认partitioner，初始状态定义和状态更新都在myUpdateFunc中实现。
    val stateStream1 = wordDstream.updateStateByKey(myUpdateFunc)

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}