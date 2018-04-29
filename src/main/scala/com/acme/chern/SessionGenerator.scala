package com.acme.chern

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Spark UDF. Session Id generator
 */
class SessionGenerator(val inactivityInterval : Int) extends UserDefinedAggregateFunction {
  
  override def inputSchema : StructType = StructType(StructField("category", StringType) :: 
                                                    StructField("eventTime", TimestampType) :: Nil)
                                                    
  override def bufferSchema : StructType = inputSchema.add(StructField("ssId", IntegerType))
                                                     .add(StructField("blChangeSession", BooleanType))
                                                     
  override def dataType : DataType = IntegerType
  override def deterministic: Boolean = true
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = { }
  override def evaluate(buffer: Row) : Any = session_hash(buffer)
  
  val COL_CATEGORY       = 0
  val COL_EVENT_TIME     = 1
  val COL_SESSION_ID     = 2
  val COL_CHANGE_SESSION = 3
  
  override def initialize(buffer : MutableAggregationBuffer): Unit = {
    buffer(COL_CATEGORY)   = null          // category, None doesn't work here
    buffer(COL_SESSION_ID) = 0            //  sessionId
    buffer(COL_CHANGE_SESSION) = false    // change session on the next row flag
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
   if (null == buffer(COL_CATEGORY)) {
      onStartCategory(buffer, input)
    } else {
        if (diffTimeSec(buffer, input) > inactivityInterval) {
            onStartNewSession(buffer, input)
        } else {
            onContinueSession(buffer)
        }
        updateEventTime(buffer, input)      // keep last seen event time
    }
  }

  protected def onContinueSession(buffer: MutableAggregationBuffer) {
    changeSession(buffer, false) 
  }
  
  protected def onStartNewSession(buffer: MutableAggregationBuffer, input: Row) {
    buffer(COL_SESSION_ID) = buffer.getInt(COL_SESSION_ID) + (if (buffer.getBoolean(COL_CHANGE_SESSION)) 0 else 1)
    changeSession(buffer, true)    
  }
  
  protected def onStartCategory(buffer: MutableAggregationBuffer, input: Row) {
      buffer(COL_CATEGORY) = input(COL_CATEGORY);  
      updateEventTime(buffer, input)
  }
  
  protected def updateEventTime(buffer: MutableAggregationBuffer, input: Row) {
    buffer(COL_EVENT_TIME) = input(COL_EVENT_TIME) 
  }
  
  @inline final private[this] def changeSession(buffer: MutableAggregationBuffer, flag : Boolean) {
    buffer(COL_CHANGE_SESSION) = flag
  }
  
  @inline final private[this] def diffTimeSec(buffer : MutableAggregationBuffer, input: Row) = {
    (input.getTimestamp(COL_EVENT_TIME).getTime - buffer.getTimestamp(COL_EVENT_TIME).getTime) / 1000
  }

  @inline final private[this] def session_hash(bf : Row) = bf.getString(COL_CATEGORY).hashCode() + bf.getInt(COL_SESSION_ID)

  
}