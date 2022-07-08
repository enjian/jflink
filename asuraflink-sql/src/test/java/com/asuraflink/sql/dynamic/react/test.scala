package com.asuraflink.sql.dynamic.react

import com.asuraflink.sql.dynamic.utils.{FailingCollectionSource, TestingRetractSink}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.types.Row
import org.junit.Test

import java.util
import scala.collection.mutable
import scala.sys.env

/**
 * ${DESCRIPTION}.
 */
class test {
  val env = StreamExecutionEnvironment.getExecutionEnvironment;

  @Test
  def testNonWindoswRand(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)


    var data1 = new mutable.MutableList[(Int, String, String)]
    data1.+=((1, "5", "Hi3"))
    data1.+=((2, "7", "Hi5"))
    data1.+=((1, "5", "Hi6"))
    data1.+=((2, "7", "Hi8"))
    data1.+=((3, "8", "Hi9"))
    data1.+=((3, "9", "Hi9"))
    data1.+=((3, "8", "Hi9"))
    data1.+=((3, "8", "Hi9"))
    data1.+=((3, "8", "Hi9"))
    data1.+=((3, "8", "Hi9"))
    data1.+=((3, "8", "Hi9"))


    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))
    data2.+=((3, 2L, "HeHe"))

    failingDataSource(data1).print()

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b, 'c)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)

    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
//        val tableConfig = tEnv.getConfig
//        tableConfig.getConfiguration.setBoolean(TABLE_EXEC_MINIBATCH_ENABLED, true)
//        tableConfig.getConfiguration.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
//        tableConfig.getConfiguration.setLong(TABLE_EXEC_MINIBATCH_SIZE, 3L)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
								  |SELECT a,c,substring(b,1) as b from
								  |(SELECT a,b,c from
								  |  (SELECT *,ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime() ASC) as row_num FROM T1)
								  |WHERE row_num = 1) where c in ('1','2','3','4')
								""".stripMargin


    //    val sqlQuery =
    //      """
    //        |SELECT t2.a, t2.c, t1.c
    //        |FROM (
    //        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1
    //        |) as t1
    //        |JOIN (
    //        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2
    //        |) as t2
    //        |ON t1.a = t2.a AND t1.b > t2.b
    //        |""".stripMargin
    //
    //    val sink = new TestingAppendSink
//    tEnv.sqlQuery(sqlQuery)


    //    val sink = new TestingRetractSink()

    //    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)


    tEnv.sqlQuery(sqlQuery).printSchema()

    env.execute("test")

  }

  def failingDataSource[T: TypeInformation](data: Seq[T]): DataStream[T] = {

//    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    // reset failedBefore flag to false
    FailingCollectionSource.reset()

    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]

    val collection: util.Collection[T] = scala.collection.JavaConversions.asJavaCollection(data)
    // must not have null elements and mixed elements
    FromElementsFunction.checkCollection(collection, typeInfo.getTypeClass)

    val function = new FailingCollectionSource[T](
      typeInfo.createSerializer(env.getConfig),
      collection,
      data.length / 2
    ) // fail after half elements

    env.addSource(function)(typeInfo).setMaxParallelism(1)
  }
}
