import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by lementec on 30/10/2016.
 */


public class ConsistencyCheck {

    private static Configuration config = null;
    private static final String tableName = "olementec";

    /**
     * Initialization
     */
    static {
        config = HBaseConfiguration.create();
    }

        public static boolean RecordEmpty(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(config, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        return rs.isEmpty();
    }


    /**
     * Scan (or list) a table
     */
    public static void getAllRecord(String tableName) throws IOException {

            HTable table = new HTable(config, tableName);
            String [] friends;
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("others"));
            ResultScanner scanner = table.getScanner(s);

            // Scanners return Result instances.
            // Now, for the actual iteration. One way is to use a while loop
            // like so:
            for (Result rr:scanner) {
                for(KeyValue kv : rr.raw())
                {
                    // print out the row we found and the columns we were looking
                    // for
                    String f = new String(kv.getValue());
                    friends = f.split(";");
                    //friends = rr.value()

                    for(String friend: friends)
                    {
                        if(ConsistencyCheck.RecordEmpty(tableName, friend))
                        {
                            System.out.println("Not consistency : \n  friend : "+friend+" is not in the database");
                        }
                    }
                }

            }

            //check bff
            s.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("bff"));
            scanner = table.getScanner(s);
            table.getScanner(s);
            for (Result rr:scanner) {
                for (KeyValue kv : rr.raw()) {
                    String bf = new String(kv.getValue());
                    if(ConsistencyCheck.RecordEmpty(tableName, bf))
                    {
                        System.out.println("Not consistency : \n  bff : "+bf+" is not in the database");
                    };
                }
            }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("get all record : ");
        ConsistencyCheck.getAllRecord("olementec");

    }

}
