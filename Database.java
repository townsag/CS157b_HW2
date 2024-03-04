import java.io.BufferedReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
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
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS PH_TABLE_ROWS (" +
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
                    insert_into_table(words[1]);
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
     * A create, "c", command should insert a row to PH_TABLE with the name of the table, 
     * and the number of columns that table has, and set LAST_ROW. Do not give a TABLE_ID 
     * value for this insert (Sqlite will default to using a new unused value). Take the 
     * value of the TABLE_ID from this insert then also insert rows in PH_COL_RANGES for 
     * the ranges given in the c instruction. 
     */
    public void create_table(String argumentsString) {
        String[] args = argumentsString.split(" ");
        String tableName = args[0];
        int numColumns = args.length - 1;

        int newID;
        List<Integer> columnRanges = List.of(Arrays.copyOfRange(args, 1, args.length))
                                         .stream().map(Integer::parseInt)
                                         .collect(Collectors.toList());
        try{
            newID = insertTableData(tableName, numColumns);
            insertColumnRanges(tableName, columnRanges, newID);
        } catch (SQLException e) {
            System.out.println("error creating table: " + tableName);
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("CREATED " + tableName);
    }

    /*
     * An insert, "i", should look up the row for the table_name of the insert in PH_TABLE. 
     * It should use the current value of LAST_ROW in this row as ROW_NUM. Then it should 
     * add rows, one for each column value in the "i" command, into PH_TABLE_ROWS. It should 
     * also add a row into PH_HASH_BUCKETS using the appropriate hash value. 
     */
    public void insert_into_table(String argumentString) {
        // resetrict inserts to not be of the symbol *
        
        // output INSERT table_name on success
        String[] args = argumentString.split(" ");
        String tableName = args[0];
        int numColumnsArg = args.length - 1;
        List<String> argsList = new ArrayList<>(Arrays.asList(args));
        List<String> valuesList = argsList.subList(1, argsList.size());
        // System.out.println("table name: " + tableName);
        // System.out.println("num cols arg: " + Integer.toString(numColumnsArg));


        try {
            // check that the number of columns in the insert matches the number of columns in table
            List<Integer> lookupResult = lookupPHTable(tableName);
            int tableID = lookupResult.get(0);
            int numColumns = lookupResult.get(1);
            int lastRow = lookupResult.get(2);
            if (numColumnsArg != numColumns){
                System.out.println("error encountered when trying to insert: " + argumentString);
                System.out.println("num columns in insert does not match num columns in table");
                System.exit(1);
            }
            // System.out.println("table id: " + Integer.toString(tableID));
            // System.out.println("num cols: " + Integer.toString(numColumns));
            // System.out.println("last row: " + Integer.toString(lastRow));

            List<Pair<Integer, Integer>> columnNumRangePairs = lookupHashInfo(tableID);
            List<Integer> columHashRanges = columnNumRangePairs.stream()
                                            .map(Pair::getSecond)
                                            .collect(Collectors.toList());
            // System.out.println("printing column num, range pairs: ");
            // for (Pair<Integer, Integer> pair : columnNumRangePairs) {
            //     System.out.print("(" + Integer.toString(pair.getFirst()) + ", " + Integer.toString(pair.getSecond()) + ") ");
            // } System.out.println();

            // System.out.println("\nPrinting column hash ranges");
            // for (Integer elem : columHashRanges) {
            //     System.out.print(Integer.toString(elem) + ", ");
            // } System.out.println();

            // System.out.println("\nPrinting values list");
            // for (String value : valuesList) {
            //     System.out.print(value + ", ");
            // } System.out.println();

            insertRow(tableID, lastRow, valuesList);
            updateLastRow(tableID, lastRow);
            insertHashBucket(tableID, lastRow, valuesList, columHashRanges);

            System.out.println("Inserted into: " + tableName);
        } catch (SQLException e){
            System.out.println("encountered an sql exception when trying to insertt: " + argumentString);
            e.printStackTrace();
        }


    }

    public void lookup_in_table() {

    }

    // ========== helper functions for create table ========== //

    private int insertTableData(String tableName, int numColumns) throws SQLException{
        int newID = -1;
        String sqlInsertTable = "INSERT INTO PH_TABLE (NAME, NUM_COLUMNS, LAST_ROW) VALUES (?, ?, ?)";
        PreparedStatement insertStatementTable = 
                connection.prepareStatement(sqlInsertTable, PreparedStatement.RETURN_GENERATED_KEYS);
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
        insertStatementTable.close();
        return newID;
    }

    private void insertColumnRanges(String tableName, List<Integer> columnRanges, int tableID) throws SQLException {
        String sqlInsertColRange = "INSERT INTO PH_COL_RANGES (TABLE_ID, COL_NUM, COL_RANGE) VALUES (?, ?, ?)";
        PreparedStatement insertStatementColRanges = connection.prepareStatement(sqlInsertColRange);
        int runningColumnRangeSum = 0;
        
        // design choice java array list is zero indexed but sql table is one indexed
        for(int i = 0; i < columnRanges.size(); i++){
            if (columnRanges.get(i).intValue() < 1){
                System.out.println("error creating table: " + tableName + ", non-positive hash range");
                System.out.println("column num: " + Integer.toString(i + 1) + ", range value: " + columnRanges.get(i).intValue());
            }
            runningColumnRangeSum += columnRanges.get(i).intValue();
            if (runningColumnRangeSum > 64) {
                System.out.println("Sum of the bits for the columns exceeds 64 for table: " + tableName);
                System.exit(1);
            }
            insertStatementColRanges.setInt(1, tableID);
            insertStatementColRanges.setInt(2, i + 1);
            insertStatementColRanges.setInt(3, columnRanges.get(i).intValue());
            insertStatementColRanges.executeUpdate();
        }
        // insertStatementTable.close();
        insertStatementColRanges.close();

    }


    // ========== helper functions for insert in table ========== //

    private List<Integer> lookupPHTable(String name) throws SQLException{
        String sql = "SELECT TABLE_ID, NUM_COLUMNS, LAST_ROW FROM PH_TABLE WHERE NAME = ?";
        PreparedStatement pStatement = this.connection.prepareStatement(sql);
        pStatement.setString(1, name);
        ResultSet result = pStatement.executeQuery();
        if (!result.next()){
            System.out.println("Table: " + name + "cannot be inserted because it does not exist in the database");
            System.exit(1);
        }
        int tableID = result.getInt(1);
        int numColumns = result.getInt(2);
        int lastRow = result.getInt(3);
        pStatement.close();
        return List.of(tableID, numColumns, lastRow);
    }

    private List<Pair<Integer, Integer>> lookupHashInfo(int tableID) throws SQLException {
        String sqlGetHashInfo = "SELECT COL_NUM, COL_RANGE FROM PH_COL_RANGES WHERE TABLE_ID = ?";
        PreparedStatement pStatement = this.connection.prepareStatement(sqlGetHashInfo);
        pStatement.setInt(1, tableID);
        ResultSet result = pStatement.executeQuery();
        if (!result.next()){
            System.out.println("TableID: " + Integer.toString(tableID) + "not found in col ranges information table");
            System.exit(1);
        }
        List<Pair<Integer, Integer>> columnNumRangePairs = new ArrayList<>();
        do {
            columnNumRangePairs.add(new Pair<Integer,Integer>(result.getInt(1), result.getInt(2)));
        } while (result.next());
        pStatement.close();
        Collections.sort(columnNumRangePairs, Comparator.comparing(Pair::getFirst));
        return columnNumRangePairs;
    }

    // design choice: column numbers are 1 indexed
    private void insertRow(int tableID, int rowNum, List<String> valuesList) throws SQLException{
        String sqlAddRows = "INSERT INTO PH_TABLE_ROWS (TABLE_ID, ROW_NUM, COL_NUM, VALUE) VALUES (?, ?, ?, ?)";
        PreparedStatement pStatement = this.connection.prepareStatement(sqlAddRows);
        pStatement.setInt(1, tableID);
        pStatement.setInt(2, rowNum);
        for(int i = 0; i < valuesList.size(); i++){
            pStatement.setInt(3, i + 1);
            pStatement.setString(4, valuesList.get(i));
            pStatement.executeUpdate();
        }
        pStatement.close();
    }

    private void updateLastRow(int tableID, int previousValue) throws SQLException{
        String sqlUpdateLastRow = "UPDATE PH_TABLE SET LAST_ROW = ? WHERE TABLE_ID = ?";
        PreparedStatement pStatement = this.connection.prepareStatement(sqlUpdateLastRow);
        pStatement.setInt(1, previousValue + 1);
        pStatement.setInt(2, tableID);
        pStatement.executeUpdate();
        pStatement.close();
    }

    private void insertHashBucket(int tableID, int rowNum, List<String> valuesList, 
                                  List<Integer> columHashRanges) throws SQLException {
        String sqlAddBucket = "INSERT INTO PH_HASH_BUCKETS (TABLE_ID, HASH_BUCKET, ROW_NUM) VALUES (?, ?, ?)";
        // System.out.println("calling hash here");
        long hashValue = Util.partitionedHash(valuesList, columHashRanges);
        PreparedStatement pStatement = this.connection.prepareStatement(sqlAddBucket);
        pStatement.setInt(1, tableID);
        pStatement.setLong(2, hashValue);
        pStatement.setInt(3, rowNum);
        pStatement.executeUpdate();
        pStatement.close();
    }

}


