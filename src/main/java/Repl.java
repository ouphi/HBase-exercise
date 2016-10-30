/**
 * Created by lementec on 27/10/2016.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Scanner;

public final class Repl {

    private static final String tableName = "olementec";
    private static Configuration config = null;


    /**
     * Initialization
     */
    static {
        config = HBaseConfiguration.create();
    }


    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }


    /**
     * Add record
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(config, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Get a row
     */
    public static void getOneRecord(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(config, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }


    /** addPeople : add people */
    private static void addPeople() throws Exception {
        Scanner scanner = new Scanner(System.in);
        String firstname, address, birthday, nationality;
        String friends;
        String BFF = "";

        //infos
        System.out.println("First Name: ");
        firstname = scanner.nextLine();

        System.out.print("Adress: ");
        address = scanner.nextLine();
        Repl.addRecord(tableName, firstname, "infos", "adress", address);

        System.out.print("Birthday: ");
        birthday = scanner.nextLine();
        Repl.addRecord(tableName, firstname, "infos", "birthday", birthday);

        System.out.print("Nationality: ");
        nationality = scanner.nextLine();
        Repl.addRecord(tableName, firstname, "infos", "nationality", nationality);

        //friends
        System.out.println("List of friends (separated by ';')");
        friends = scanner.nextLine();
        Repl.addRecord(tableName, firstname, "friends", "others", friends);

        // get best friend forever
        while (BFF.trim().equals("")) {
            System.out.println("Please enter a best friend forever: ");
            BFF = scanner.nextLine();
            if(BFF.trim().equals("")) {
                System.out.println("BFF:" + BFF);
            }
        }
        Repl.addRecord(tableName, firstname, "friends", "bff", BFF);
        System.out.println("You added "+ firstname + " to the database");
    }


    public static void main(String[] args) throws Exception {
        //create table if not exist
        String[] familys = { "infos", "friends" };
        Repl.createTable(Repl.tableName, familys);
        Scanner sc = new Scanner(System.in);
        String s;
        //read user input
        while(true) {
            System.out.println("What do you wan to do ?\n Add a new person: a \n quit: q");
            s = sc.nextLine();
            if(s.equals("a")) addPeople();
            else if(s.equals("q")) break;
        }
    }
}