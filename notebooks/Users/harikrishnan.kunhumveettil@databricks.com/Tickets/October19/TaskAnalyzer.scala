// Databricks notebook source
dbutils.widgets.text("delayInMinutes", "", "1. Task Time in Minutes")
val delayInMinutes = dbutils.widgets.get("delayInMinutes").trim.toInt

// COMMAND ----------

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.TaskEndReason
import scala.collection.mutable
 import spark.implicits._
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

val taskInfoMetrics = mutable.Buffer[(Int, Int, String, TaskInfo, TaskEndReason)]()
class TaskInfoRecorderListener extends SparkListener {
  

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    print("Task Ended")
    if (taskEnd.taskInfo != null && taskEnd.taskMetrics != null) {
      taskInfoMetrics += ((taskEnd.stageId,taskEnd.stageAttemptId,"End",taskEnd.taskInfo,taskEnd.reason))
    }
  }
  
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    print("Task Started")
    println((taskStart.stageId,taskStart.stageAttemptId,"Start",taskStart.taskInfo.launchTime, taskStart.taskInfo.finishTime,null))
    taskInfoMetrics += ((taskStart.stageId,taskStart.stageAttemptId,"Start",taskStart.taskInfo,null))
    
  } 
}

// COMMAND ----------

case class TaskInfoMetrics(stageId: Int,
                           stageAttemptId: Int,
                           taskType: String,
                           index: Long,
                           taskId: Long,
                           attemptNumber: Int,
                           launchTime: Long,
                           finishTime: Long,
                          // duration: Long,
                           schedulerDelay: Long,
                           executorId: String,
                           host: String,
                           taskLocality: String,
                           speculative: Boolean,
                           gettingResultTime: Long,
                           successful: Boolean
                           )


// COMMAND ----------

def createDF(taskEnd: mutable.Buffer[(Int, Int, String, TaskInfo, TaskEndReason)]): DataFrame = {
    lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
    val rows = taskEnd.map { case (stageId, stageAttemptId, taskType, taskInfo, taskEndReason) =>
      val gettingResultTime = {
        if (taskInfo.gettingResultTime == 0L) 0L
        else taskInfo.finishTime - taskInfo.gettingResultTime
      }
      TaskInfoMetrics(
        stageId,
        stageAttemptId,
        taskType,
        taskInfo.index,
        taskInfo.taskId,
        taskInfo.attemptNumber,
        taskInfo.launchTime,
        taskInfo.finishTime,
        //taskInfo.duration,
        0L,
        taskInfo.executorId,
        taskInfo.host,
        taskInfo.taskLocality.toString,
        taskInfo.speculative,
        gettingResultTime,
        taskInfo.successful)
    }
    rows.toDF()
  }

// COMMAND ----------

val listenerTask = new TaskInfoRecorderListener()
spark.sparkContext.addSparkListener(listenerTask)

// COMMAND ----------

def writeMetricsOnDBFS()
{
  
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.text.SimpleDateFormat
import java.util.Date

var res=sc.runOnEachExecutor[String]({ () =>
import sys.process._
  
       var cmd_Result=Seq("bash", "-c", "hostname").!! +  Seq("bash", "-c", "sudo ss -a -t -m -o --info ").!!
      cmd_Result
    }, 100.seconds)


val DATE_FORMAT = "EEE_MMM_dd_yyyy_h_mm_ss_a"

    def getDateAsString(d: Date): String = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.format(d)
    }

dbutils.fs.put("/tmp/databricks_debug/apple/result_"+ getDateAsString(new Date()) ,res.toString,true)
}


// COMMAND ----------

import scala.collection.JavaConverters._
import scala.concurrent.duration._



while (true){
   createDF(taskInfoMetrics).createOrReplaceTempView("tasks")
    val list = spark.sql(s"""select current_timestamp() as time,
    t1.taskId, 
    t1.taskType, 
    t1.stageId, 
    t1.executorId, 
    t1.host, 
    (UNIX_TIMESTAMP() - (t1.launchTime/1000))/60 as durationMins  
    from 
    (select * from tasks t1 where taskType="Start")t1 
    left outer join 
    (select taskId from tasks where taskType="End")t2
    on t1.taskId = t2.taskId 
    where t2.taskId is null and   ((UNIX_TIMESTAMP() - (t1.launchTime/1000))/60) > $delayInMinutes """)
  
  
  if(!list.head(1).isEmpty) {
    list.show(truncate=false)
    writeMetricsOnDBFS()
  }
  Thread.sleep(15000)
}