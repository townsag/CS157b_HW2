import java.io.BufferedReader;
import java.sql.*;
import java.io.FileReader;
import java.io.IOException;

public class Database {
    private String dbName;
    private Connection connection = null;
    
    private String sqlite_url = "jdbc:sqlite:";

    public Database(String db_name) {
        this.dbName = db_name;
        this.connectToDbms(this.dbName);
    }

    public boolean connectToDbms(String db_name) {
        String url = sqlite_url + db_name;
        try {
            Class.forName("org.sqlite.JDBC");
            this.connection = DriverManager.getConnection(url);
        } catch (Exception e) {
            System.out.println("encountered an error connecting to sqlite file at");
            System.out.println("url: " + url);
            System.out.println(e);
        }

        return connection == null;
    }

    public void initial_tables() {
        if (this.connection == null) {
            System.out.print("db connection is null");
            return;
        }
        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS PH_TABLE (" +
                    "   TABLE_ID INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "   NAME TEXT UNIQUE," +
                    "   NUM_COLUMNS INTEGER," +
                    "   LAST_ROW INTEGER" +
                    ");");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS PH_TABLE_NAME_INDEX ON PH_TABLE(NAME)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS PH_COL_RANGES (" +
                    "   TABLE_ID INTEGER," +
                    "   COL_NUM INTEGER," +
                    "   COL_RANGE INTEGER," +
                    "   PRIMARY KEY (TABLE_ID, COL_NUM)" +
                    ");");
            statement.executeUpdate("CREATE TABL IF NOT EXISTS PH_TABLE_ROWS (" +
                    "   TABLE_ID INTEGER," +
                    "   ROW_NUM INTEGER," +
                    "   COL_NUM INTEGER," +
                    "   VALUE TEXT," +
                    "   PRIMARY KEY (TABLE_ID, ROW_NUM, COL_NUM)" +
                    ");");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS PH_HASH_BUCKETS (" +
                    "   TABLE_ID INTEGER," +
                    "   HASH_BUCKET INTEGER," +
                    "   ROW_NUM INTEGER," +
                    "   PRIMARY KEY (TABLE_ID, HASH_BUCKET)" +
                    ");");

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void parse_instructions(String instuctions) {
        try (BufferedReader br = new BufferedReader(new FileReader(instuctions))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+", 2);
                if (words[0].equals("c")) {
                    create_table(words[1]);
                } else if (words[0].equals("i")) {
                    insert_into_table();
                } else if (words[0].equals("l")) {
                    lookup_in_table();
                } else {
                    System.out.println(
                            "Please enter one of the following format: \nc table_name col_1_hash_range col_2_hash_range  ... col_n_hash_range"
                                    +
                                    "\ni table_name col_1_value col_2_value ... col_n_value" +
                                    "\nl table_name use_index_or_not col_choice_1 ... col_choice_n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*  
        A create, "c", command should insert a row to PH_TABLE with the name of the table, 
        and the number of columns that table has, and set LAST_ROW. Do not give a TABLE_ID 
        value for this insert (Sqlite will default to using a new unused value). Take the 
        value of the TABLE_ID from this insert then also insert rows in PH_COL_RANGES for 
        the ranges given in the c instruction. 
    */
    public void create_table(String argumentsString) {
        String[] args = argumentsString.split(" ");
        String tableName = args[0];
        int numColumns = args.length - 1;
        String sqlInsertTable = "INSERT INTO PH_TABLE (NAME, NUM_COLUMNS, LAST_ROW) VALUES (?, ?, ?)";
        String sqlInsertColRange = "INSERT INTO PH_COL_RANGES (TABLE_ID, COL_NUM, COL_RANGE) VALUES (?, ?, ?)";
        int newID = -1;
        int runningColumnRangeSum = 0;
        try{
            PreparedStatement insertStatementTable = 
                connection.prepareStatement(sqlInsertTable, PreparedStatement.RETURN_GENERATED_KEYS);
            PreparedStatement insertStatementColRanges = 
                connection.prepareStatement(sqlInsertColRange);
            insertStatementTable.setString(1, tableName);
            insertStatementTable.setInt(2, numColumns);
            insertStatementTable.setInt(3, 1);
            insertStatementTable.executeUpdate();
            ResultSet keys = insertStatementTable.getGeneratedKeys();
            if (keys.next()) {
                newID = keys.getInt(1);
            } else {
                System.out.println("No auto-generated keys returned.");
                System.exit(1);
            }

            for(int i = 1; i < args.length; i++){
                if (Integer.parseInt(args[i]) < 1){
                    System.out.println("error creating table: " + tableName + ", non-positive hash range");
                    System.out.println("column num: " + Integer.toString(i) + ", range value: " + args[i]);
                }
                runningColumnRangeSum += Integer.parseInt(args[i]);
                if (runningColumnRangeSum > 64) {
                    System.out.println("Sum of the bits for the columns exceeds 64 for table: " + tableName);
                    System.exit(1);
                }
                insertStatementColRanges.setInt(1, newID);
                insertStatementColRanges.setInt(2, i);
                insertStatementColRanges.setInt(3, Integer.parseInt(args[i]));
                insertStatementColRanges.executeUpdate();
            }
            insertStatementTable.close();
            insertStatementColRanges.close();

        } catch (SQLException e) {
            System.out.println("error creating table: " + tableName);
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("CREATED " + tableName);
    }

    public void insert_into_table() {

    }

    public void lookup_in_table() {

    }

    
}
