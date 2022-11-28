import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.ForeachWriter

import java.io.IOException

class HbaseDataWriter  extends ForeachWriter[Row]{

  private var table : Table = _

  private var connection : Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    val conf = HBaseConfiguration.create
    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf("phone-calls"))
      true
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
  }

  override def process(value: Row): Unit = {
    val p = new Put(Bytes.toBytes(value.getLong(0)))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("callerName"), Bytes.toBytes(value.getString(1)))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("callerPhone"), Bytes.toBytes(value.getString(2)))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("calleeName"), Bytes.toBytes(value.getString(3)))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("calleePhone"), Bytes.toBytes(value.getString(4)))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("timeStamp"), Bytes.toBytes(String.valueOf(value.getTimestamp(5))))
    p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("Duration"), Bytes.toBytes(value.getInt(6)))
    try table.put(p)
    catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
    System.out.println("data inserted")
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      table.close()
      connection.close()
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
  }
}
