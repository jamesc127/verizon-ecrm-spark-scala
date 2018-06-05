
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

object marketing_report_data {
  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("marketing report data").getOrCreate()
    val conf = new SparkConf().setAppName("marketing report data")
    val sc = new SparkContext(conf)

    val sql = "(" +
    "SELECT TI.TEMPLATE_NAME,  " +
      "MARKETING_ID ,   " +
      "TI.INPUT_ID FILE_NAME, " +
      "(CASE WHEN (ti.input_id LIKE '%.shp') THEN (SUBSTR( TI.INPUT_ID, 3, INSTR(TI.INPUT_ID,'_')-3)) ELSE null end) JCN_NO," +
      "(CASE WHEN (ti.input_id LIKE '%.shp') THEN (SUBSTR( input_id, INSTR(input_id,'_')+1, 3)) ELSE null end) JCN_CYCLE," +
      "ti.DISP_TEMPLATE_NAME DISPLAY_TEMP_NAME,  " +
      "et.SUBJECT ,  " +
      "ET.CATEGORY,  " +
      "MIN (TI.SENT_DATE) SENT_DATE,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = 3    " +
        "OR STATUS_ID    = -9)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) ATTEMPTS,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = 3)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) DELIVERED,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = -9)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) BOUNCED,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID < 0    " +
        "AND STATUS_ID  != -9)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) SCRUBBED,  " +
      "ROUND ( SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = 3)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) * 100 / NULLIF ( SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = 3    " +
        "OR STATUS_ID    = -9)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END), 0), 2) DELIVERY_RATE,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (CLICKED > 0)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) UNIQUE_CLICKS,  " +
      "SUM (CLICKED) TOTAL_CLICKS,  " +
      "SUM (  " +
      "CASE    " +
        "WHEN (OPENED > 0)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) UNIQUE_OPENS,  " +
      "SUM (OPENED) TOTAL_OPENS,  " +
      "ROUND ( SUM (  " +
      "CASE    " +
        "WHEN (OPENED > 0)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) * 100 / NULLIF (SUM (  " +
      "CASE    " +
        "WHEN (STATUS_ID = 3)    " +
        "THEN 1    " +
        "ELSE 0  END), 0), 2) UNIQUE_OPEN_RATE100,  " +
      "ROUND ( SUM (  " +
      "CASE    " +
        "WHEN (CLICKED > 0)    " +
        "THEN 1    " +
        "ELSE 0  " +
      "END) * 100 / NULLIF (SUM (  " +
      "CASE    " +
      "WHEN (OPENED > 0)    " +
        "THEN 1    " +
        "ELSE 0  END), 0), 2) UNIQUE_CLICK_RATE100, " +
      "count(OOUT.EMAIL_ADDRESS) OPT_OUT,   " +
      "ROUND (count(OOUT.EMAIL_ADDRESS)/NULLIF (SUM (  " +
      "CASE    WHEN (STATUS_ID = 3)    " +
        "THEN 1    " +
        "ELSE 0  END), 0), 2) OPT_OUT1000,  " +
      "SUM(decode(PRI_SPOTLIGHT,'N',1,0)) SPOTLIGHT," +
      "SUM(decode(PRI_SPORTS,'N',1,0)) SPORTS," +
      "SUM(decode(PRI_PAY_PER_VIEW,'N',1,0))  PAYPERVIEW," +
      "SUM(decode(PRI_MOVIE_VOD,'N',1,0))  VOD," +
      "SUM(decode(PRI_PRODUCT_ENHANCE,'N',1,0))  PRODENHANCE," +
      "SUM(decode(PRI_PREMIUM_VIDEO,'N',1,0))  PREMIUMVD," +
      "SUM(decode(PRI_EQUIP_ACCESSORIES,'N',1,0))  EQUIP," +
      "TI.DEL_MEDIUM TACTIC," +
      "(DECODE(ET.LOB,'C','CONSUMER','B','BUSINESS','BC','BOTH')) LOB" +
      "FROM TRANSACTION_IDENTIFIER ti,  " +
        "ecrm_templates et,  " +
        "email_optin_tracking oin,  " +
        "email_optout_tracking oout" +
      "WHERE et.seq_id = ti.template_seq_id and" +
      "ti.tx_id=oin.tx_id(+) and" +
      "ti.tx_id=oout.tx_id(+)" +
      "AND ti.app_name = 'AEDW'" +
      "AND TI.SENT_DATE BETWEEN TRUNC (SYSDATE - 30) AND SYSDATE" +
      "GROUP BY TI.TEMPLATE_NAME,  " +
        "MARKETING_ID,  " +
        "TI.INPUT_ID,  " +
        "ti.DISP_TEMPLATE_NAME,  " +
        "et.SUBJECT,  " +
        "TRUNC(TI.SENT_DATE),  " +
        "et.template_id,  " +
        "ET.CATEGORY,  " +
        "TI.DEL_MEDIUM,  " +
        "ET.LOB;" +
    ")"

    val df = spark
      .read
      .format("jdbc")
      .options(Map[String, String](
        "url" -> "jdbc:oracle:thin:username:/password@//hostname:port/oracle_svc",
                "dbtable" -> sql
        )
      )
      .load()
      //.cache()

//    val extract = Seq(df.map(dbRow => dbRow.getAs[String]("column1")),
//                      df.map(dbRow => dbRow.getAs[String]("column2")),
//                      df.map(dbRow => dbRow.getAs[String]("column3")),
//                      df.map(dbRow => dbRow.getAs[String]("column4")),
//                      df.map(dbRow => dbRow.getAs[String]("column5")),
//                      df.map(dbRow => dbRow.getAs[String]("column6")),
//                      df.map(dbRow => dbRow.getAs[String]("column7")),
//                      df.map(dbRow => dbRow.getAs[String]("column8")),
//                      df.map(dbRow => dbRow.getAs[String]("column9"))
//    )

    val cRow = Seq(df.map
    { dbRow =>
      val template_name = dbRow.getAs[String]("TI.TEMPLATE_NAME")
      val marketing_id = dbRow.getAs[String]("MARKETING_ID")
      val file_name = dbRow.getAs[String]("FILE_NAME")
      val jcn_no = dbRow.getAs[String]("JCN_NO")
      val jcn_cycle = dbRow.getAs[String]("JCN_CYCLE")
      val tmp_name = dbRow.getAs[String]("DISPLAY_TEMP_NAME")

      val allValues = IndexedSeq[AnyRef](template_name,marketing_id,file_name,jcn_no,jcn_cycle,tmp_name)
      val allColumnNames = Array[String](
        "TEMPLATE_NAME",
        "MARKETING_ID",
        "FILE_NAME",
        "JCN_NO",
        "JCN_CYCLE",
        "DISPLAY_TEMP_NAME"
      )
      val rowMetadata = new CassandraRowMetadata(allColumnNames)
      new CassandraSQLRow(rowMetadata, allValues)
    }
    )





    val cassRDD = sc.parallelize(cRow)
    cassRDD.saveToCassandra("PNP_ADMIN","MKT_TRANS_DATA")
  }
}