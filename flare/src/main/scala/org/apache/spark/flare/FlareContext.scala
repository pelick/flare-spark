package org.apache.spark.flare

import java.util.{ArrayList => JArrayList}
import java.lang.NullPointerException

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd._
import org.apache.spark.flare.rdd._
import org.apache.spark._

import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan
import org.apache.hadoop.hive.ql.exec.Utilities

class FlareContext (@transient val sparkContext: SparkContext) extends Logging {
  
  def orcfileRDD(
      conf: JobConf,
      path: String,
      minSplits: Int = sparkContext.defaultMinSplits
      ): RDD[Array[_]] = {
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new OrcfileRDD(sparkContext, conf, path, minSplits)
  }
  
  def orcfileRDDWithExpr(
      conf: JobConf,
      expr: String,
      path: String,
      minSplits: Int = sparkContext.defaultMinSplits
      ): RDD[Array[_]] = {
    
    val expression = OrcfileExpressionParser.parseToExprNodeDesc(expr)
 
    try {
      logInfo("Add Expression " + expression.getExprString())
      conf.set("hive.io.filter.expr.serialized", Utilities.serializeExpression(expression))
    } catch {
      case npe: NullPointerException => logWarning("Expression is null")
      case _: Throwable => logWarning("Expression is null, exception unknown")
    }
    
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new OrcfileRDD(sparkContext, conf, path, minSplits)
  }
  
}

private[spark] object OrcfileExpressionParser extends Logging {
  
  val longType = TypeInfoFactory.longTypeInfo
  val boolType=TypeInfoFactory.booleanTypeInfo
  
  def parseToExprNodeDesc(expr: String): ExprNodeDesc = {
    
    if (expr.contains(">")) {
      val splited: Array[String] = expr.replaceAll(" ", "").split(">")
      if (splited.length == 2) {
        val columnName = splited(0)
        val number = splited(1).toLong
        
        val columnExprNode = new ExprNodeColumnDesc(longType, columnName, "table_name", false)
        val constantExprNode = new ExprNodeConstantDesc(longType, number)
        
        logInfo("Column Name is " + columnExprNode.getExprString())
        logInfo("Constant is " + constantExprNode.getExprString())
        
        var exprNodes = new JArrayList[ExprNodeDesc]()
        exprNodes.add(columnExprNode)
        exprNodes.add(constantExprNode)
        
        logInfo("ExpressionNode is " + exprNodes.toString())
        new ExprNodeGenericFuncDesc(boolType, new GenericUDFOPGreaterThan(), exprNodes)
      } else {
        logWarning("Expression Error")
        // treat it as nothing
        new ExprNodeGenericFuncDesc()
      }
    } else {
      logWarning("Expression Unsupport")
      // treat it as nothing
      new ExprNodeGenericFuncDesc()
    }
    
  }
  
}