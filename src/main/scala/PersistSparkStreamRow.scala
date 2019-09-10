
package com.objectstream.app.sparkstreaming

import org.apache.spark.sql.ForeachWriter

class PersistSparkStreamRow extends ForeachWriter[org.apache.spark.sql.Row] {

  var cassandraInstance: CassandraPersistor = null;
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    if (cassandraInstance == null) {
      cassandraInstance = new CassandraPersistor();
    }
    
    cassandraInstance.connector.withSessionDo(session =>
      session.execute(s"""
       insert into ${cassandraInstance.namespace}.${cassandraInstance.foreachTableSink} (item_id, item_sold, item_name, timestamp_ms, store_id, timestamp_dt)
       values('${record(0)}', '${record(1)}', '${record(2)}', '${record(3)}', '${record(4)}', '${record(5)}')""")
      //Process new [Hermitage - 1,1530305100936,1255.89,2018-06-29]

    )
  }
  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}
