import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.util.StringTokenizer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


object HBaseFromFile {
  def main(args: Array[String]): Unit = {

    val conn = ConnectionFactory.createConnection(HBaseConfiguration.create())

      if (!conn.getAdmin.tableExists(TableName.valueOf("emp"))) {

        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("sample"))
        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("region"))
        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("time"))
        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("product"))
        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("sale"))
        new HTableDescriptor(TableName.valueOf("emp")).addFamily(new HColumnDescriptor("profit"))
        conn.getAdmin.createTable(new HTableDescriptor(TableName.valueOf("emp")))

        println("New Table Created")

        val f = new File("B:\\IdeaProjects\\bigDataEchoSystem\\src\\main\\resources\\emp.txt")
        val br = new BufferedReader(new FileReader(f))
        var line = new BufferedReader(new FileReader(f)).readLine
        var i = 1
        while (line != null && line.length != 0)
        {
          val tokens = new StringTokenizer(line, ",")
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("sample"), Bytes.toBytes("sampleNo."), Bytes.toBytes(tokens.nextToken.toInt))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("region"), Bytes.toBytes("country"), Bytes.toBytes(tokens.nextToken))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("region"), Bytes.toBytes("state"), Bytes.toBytes(tokens.nextToken))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("region"), Bytes.toBytes("city"), Bytes.toBytes(tokens.nextToken))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("time"), Bytes.toBytes("year"), Bytes.toBytes(tokens.nextToken.toInt))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("time"), Bytes.toBytes("month"), Bytes.toBytes(tokens.nextToken))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("product"), Bytes.toBytes("productNo."), Bytes.toBytes(tokens.nextToken))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("sale"), Bytes.toBytes("quantity"), Bytes.toBytes(tokens.nextToken.toInt))
          new Put(Bytes.toBytes("row" + i)).addColumn(Bytes.toBytes("profit"), Bytes.toBytes("earnings"), Bytes.toBytes(tokens.nextToken))
          i += 1
          conn.getTable(TableName.valueOf("emp")).put(new Put(Bytes.toBytes("row" + i)))
          line = br.readLine
        }
        br.close()
        conn.getTable(TableName.valueOf("emp")).close()
      }
      else System.out.println("Table Already exists.Please enter another table name")

  }
}