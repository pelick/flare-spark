package org.apache.spark.flare.rdd

import java.text.SimpleDateFormat
import java.util.Date
import java.io.EOFException

import scala.collection.immutable.Map

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.Reader
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag

private[spark] class OrcfilePartition(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplit.value.isInstanceOf[FileSplit]) {
      val is: FileSplit = inputSplit.value.asInstanceOf[FileSplit]
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since its not removed yet
      Map("map_input_file" -> is.getPath().toString(),
        "mapreduce_map_input_file" -> is.getPath().toString())
    } else {
      Map()
    }
    envVars
  }
}

class OrcfileRDD[T: ClassTag](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    path: String,
    minSplits: Int)
  extends RDD[T](sc, Nil) with Logging {

  def this(
      sc: SparkContext,
      conf: JobConf,
      path: String,
      minSplits: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None /* initLocalJobConfFuncOpt */,
      path: String,
      minSplits)
  }

  protected val jobConfCacheKey = "rdd_%d_job_conf".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value

    conf.set("mapred.input.dir", path);
    
    if (conf.isInstanceOf[JobConf]) {
      // A user-broadcasted JobConf was provided to the HadoopRDD, so always use it.
      conf.asInstanceOf[JobConf]
    } else if (OrcfileRDD.containsCachedMetadata(jobConfCacheKey)) {
      // getJobConf() has been called previously, so there is already a local cache of the JobConf
      // needed by this RDD.
      OrcfileRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
    } else {
      // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in the
      // local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
      // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
      val newJobConf = new JobConf(broadcastedConf.value.value)
      initLocalJobConfFuncOpt.map(f => f(newJobConf))
      OrcfileRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
      newJobConf
    }
  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    
    val inputFormat = new OrcInputFormat()
    val inputSplits = inputFormat.getSplits(jobConf, minSplits)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new OrcfilePartition(id, i, inputSplits(i))
    }
    logInfo("Partition size: " + array.length)
    array
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[T] = {
    val iter = new NextIterator[T] {
      val split = theSplit.asInstanceOf[OrcfilePartition]
      logInfo("Input split: " + split.inputSplit)
      
      val jobConf = getJobConf()
      logInfo("JobConf: " + jobConf)
      var reader: RecordReader[NullWritable, Writable] = null
      
      val filePath = new Path(path)
      var orcReader: Reader = OrcFile.createReader(filePath.getFileSystem(jobConf), filePath)
      val inspector: StructObjectInspector = orcReader.getObjectInspector().asInstanceOf[StructObjectInspector]
      val fields = inspector.getAllStructFieldRefs()
      
      val inputFormat = new OrcInputFormat()
      OrcfileRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(new Date()),
        context.stageId, theSplit.index, context.attemptId.toInt, jobConf)
      reader = inputFormat.asInstanceOf[InputFormat[NullWritable, Writable]].getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback{ () => closeIfNeeded() }
      
      val key: NullWritable = reader.createKey()
      val value: Writable = reader.createValue()
      
      override def getNext() = {
        try {
          finished = !reader.next(key, value)
          val allWritable = inspector.getStructFieldsDataAsList(value)
          var objs = new Array[Object](allWritable.size)
          for (i <- 0 to allWritable.size-1) {
            val objInspector = fields.get(i).getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
            objs(i) = objInspector.getPrimitiveJavaObject(allWritable.get(i)).asInstanceOf[Object]
          }
          objs.asInstanceOf[T]
        } catch {
          case eof: EOFException => {
            finished = true
            logWarning("EOFException in Iterator`s getNext()")
            Nil.asInstanceOf[T]
          }
        }
      }

      override def close() {
        try {
          reader.close();
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    new InterruptibleIterator[T](context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    // TODO: Filtering out "localhost" in case of file:// URLs
    val hadoopSplit = split.asInstanceOf[OrcfilePartition]
    hadoopSplit.inputSplit.value.getLocations.filter(_ != "localhost")
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object OrcfileRDD {
  /**
   * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
   * the local process.
   */
  def getCachedMetadata(key: String) = SparkEnv.get.hadoopJobMetadata.get(key)

  def containsCachedMetadata(key: String) = SparkEnv.get.hadoopJobMetadata.containsKey(key)

  def putCachedMetadata(key: String, value: Any) =
    SparkEnv.get.hadoopJobMetadata.put(key, value)
    
  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf) {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, true, splitId), attemptId)

    conf.set("mapred.tip.id", taId.getTaskID.toString)
    conf.set("mapred.task.id", taId.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", splitId)
    conf.set("mapred.job.id", jobID.toString)
  }
}
