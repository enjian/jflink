//package org.apache.flink.table.planner.plan.nodes.physical.stream
//
//import com.google.common.collect.Lists
//import org.apache.calcite.jdbc.CalciteSchemaBuilder
//import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
//import org.apache.calcite.rex.RexBuilder
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
//import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
//import org.apache.flink.table.api.{EnvironmentSettings, TableConfig, TableException}
//import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
//import org.apache.flink.table.operations.{ModifyOperation, Operation}
//import org.apache.flink.table.planner.calcite.{FlinkContext, SqlExprToRexConverterFactory}
//import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
//import org.apache.flink.table.planner.delegation.{PlannerBase, PlannerContext}
//import org.apache.flink.table.planner.operations.PlannerQueryOperation
//import org.apache.flink.table.planner.plan.`trait`._
//import org.apache.flink.table.planner.plan.optimize.program.{FlinkStreamProgram, StreamOptimizeContext}
//import org.apache.flink.table.planner.utils.TableConfigUtils
//import org.apache.flink.util.Preconditions
//
//import java.util
//import scala.collection.JavaConversions._
//
//
//class OpertimizerPlanTest {
//
//  def main(args: Array[String]) {
//
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    val settings = EnvironmentSettings.newInstance.inStreamingMode.useBlinkPlanner //                .useBlinkPlanner()
//      .build
//    val env = StreamTableEnvironment.create(senv, settings)
//    val sEnv = env.asInstanceOf[StreamTableEnvironmentImpl]
//
//    val parser = sEnv.getParser
//    val function = "create function json_value1 as 'com.cyh.kafka.function.JsonStringUdf'"
//    env.executeSql(function)
//
//    val kafkaSource = "CREATE TABLE kafka_source_table (  \n" + "   `log`  String,  \n" + "  `topic`  varchar METADATA VIRTUAL,  \n" + "  `partition` INT METADATA VIRTUAL,  \n" + "  `offset` BIGINT METADATA VIRTUAL \n" + " ) WITH (  \n" + "  'connector' = 'kafka',  \n" + "  'topic' = 'binlog',  \n" + "  'properties.bootstrap.servers' = '10.88.30.24:9092,10.88.30.15:9092,10.88.30.23:9092',  \n" + "  'properties.group.id' = 'test0105',  \n" + "  'scan.startup.mode' = 'earliest-offset',  \n" + "  'format' = 'raw'\n" + ")"
//    val kafkaSink = "CREATE TABLE kafka_sink_table (  \n" + " `offset` bigint, `topic` varchar , `log` varchar \n" + " ) WITH (  \n" + "  'connector' = 'print'" + ")"
//    env.executeSql(kafkaSource);
//    env.executeSql(kafkaSink);
//    val insertSql =
//      """insert into kafka_sink_table
//        |  SELECT `offset`,`topic`,
//        |//        |b
//        |substring(`log`,1) as `log`
//        |from
//        |(SELECT `offset`,log,topic from
//        |  (SELECT *,ROW_NUMBER() OVER (PARTITION BY `offset` ORDER BY proctime() ASC) as row_num FROM kafka_source_table)
//        |WHERE row_num = 1)
//        | where `offset`
//        |//        | = 1
//        | in (1,2)
//        |""".stripMargin
//
//    val operation: Operation = parser.parse(insertSql).get(0)
//    val modifyOperations: util.ArrayList[ModifyOperation] = Lists.newArrayList()
//
//    if (operation.isInstanceOf[ModifyOperation]) {
//      modifyOperations.add(operation.asInstanceOf[ModifyOperation])
//      val child = modifyOperations.get(0).getChild.asInstanceOf[PlannerQueryOperation]
//      val relNode = child.getCalciteTree
//      val planner = sEnv.getPlanner.asInstanceOf[PlannerBase]
//
//      val config = planner.getTableConfig
//      val calciteConfig = TableConfigUtils.getCalciteConfig(config)
//      val programs = calciteConfig.getStreamProgram
//        .getOrElse(FlinkStreamProgram.buildProgram(config.getConfiguration))
//      Preconditions.checkNotNull(programs)
//
//      val context = relNode.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
//
//      val sContext = new StreamOptimizeContext() {
//
//        override def getTableConfig: TableConfig = config
//
//        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog
//
//        override def getCatalogManager: CatalogManager = planner.catalogManager
//
//        override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory =
//          context.getSqlExprToRexConverterFactory
//
//        override def getRexBuilder: RexBuilder = {
//          val currentCatalogName = getCatalogManager.getCurrentCatalog
//          val currentDatabase = getCatalogManager.getCurrentDatabase
//          val list: util.List[RelTraitDef[_ <: RelTrait]] = Lists.newArrayList(
//            ConventionTraitDef.INSTANCE,
//            FlinkRelDistributionTraitDef.INSTANCE,
//            MiniBatchIntervalTraitDef.INSTANCE,
//            ModifyKindSetTraitDef.INSTANCE,
//            UpdateKindTraitDef.INSTANCE)
//
//          val plannerContext = new PlannerContext(
//            config,
//            getFunctionCatalog,
//            getCatalogManager,
//            CalciteSchemaBuilder.asRootSchema(new CatalogManagerCalciteSchema(getCatalogManager, true)),
//            list
//          )
//
//          plannerContext.createRelBuilder(currentCatalogName, currentDatabase).getRexBuilder
//        }
//
//        override def isUpdateBeforeRequired: Boolean = false
//
//        def getMiniBatchInterval: MiniBatchInterval = MiniBatchInterval.NONE
//
//        override def needFinalTimeIndicatorConversion: Boolean = true
//      }
//
//      programs.getProgramNames.foldLeft(relNode) {
//        (input, name) =>
//          val program = programs.get(name).getOrElse(throw new TableException(s"This should not happen."))
//
//          val start = System.currentTimeMillis()
//
//
//          val result = program.optimize(input, sContext)
//          val end = System.currentTimeMillis()
//
//          //          if (LOG.isDebugEnabled) {
//          //            LOG.debug(s"optimize $name cost ${end - start} ms.\n" +
//          //              s"optimize result: \n${FlinkRelOptUtil.toString(result)}")
//          //          }
//          if ("predicate_pushdown".equals(name)) {
//            println(result.explain())
//          }
//          result
//      }
//    }
//  }
//
//}
