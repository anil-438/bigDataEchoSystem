
import org.apache.hadoop.hbase.{HBaseConfiguration,HColumnDescriptor,HTableDescriptor}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

object createHBaseTable {

  def main(args: Array[String]): Unit = {
// create connection
  //  val conf =
    val conn = ConnectionFactory.createConnection(HBaseConfiguration.create() )

    val admin = conn.getAdmin
// create table
    val tableName = TableName.valueOf("emp")
    val HBaseTableName = new HTableDescriptor(tableName)
// add column family to table
    HBaseTableName.addFamily(new HColumnDescriptor("personal"))
    HBaseTableName.addFamily(new HColumnDescriptor("professional"))
    admin.createTable(HBaseTableName)

// list tables
    admin.listTables.foreach(println)

// insert data into hbase table
    // val hTable = conn.getTable(tableName)
    val p = new Put(Bytes.toBytes("row1"))
    p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("raju"))
    p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"))
    conn.getTable(tableName).put(new Put(Bytes.toBytes("row1")))
    println("data inserted")
    conn.getTable(tableName).close()

// Updating a column value
    p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Delih"))
    conn.getTable(tableName).put(p)
  println("data Updated")
    conn.getTable(tableName).close()

//HBtable exits
    if(admin.tableExists(tableName)){
      println("table exit")
    }
//enable table
    if (!admin.isTableEnabled(tableName)) {
      admin.enableTable(tableName)
       println("Table Enabled")
    }
// Adding column
    admin.addColumn(tableName, new HColumnDescriptor("contactDetails"))

 // read data from hbase table -> read data from a table in HBase. Using get command

    val result = conn.getTable(tableName).get(new Get(Bytes.toBytes("row1")))
    val value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"))
    val value1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"))
     println("name: " + Bytes.toString(value) + " city: " + Bytes.toString(value1))
// Scan data
    val scan = new Scan()
    // set filter condition
    scan.setFilter(new PrefixFilter(Bytes.toBytes("anil")))
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"))
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"))

    val scanner = conn.getTable(tableName).getScanner(scan)
    var scanResult = scanner.next
    while (scanResult != null) {
      System.out.println("Found row : " + scanResult)
      scanResult = scanner.next
    }
    scanner.close

//disable table
    if (!admin.isTableDisabled(tableName)) {
      admin.disableTable(tableName)
      println("Table Enabled")
    }
// deleting the data
    admin.deleteColumn(tableName,Bytes.toBytes("contactDetails"))
// delete table
    admin.deleteTable(tableName)


    admin.shutdown

  }
}
