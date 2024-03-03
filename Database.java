import java.io.BufferedReader;
import java.sql.*;
import java.io.FileReader;
import java.io.IOException;
public class Database {
    private String dbName;
    private String instructions;
    private Connection connection = null;
    private String sqlite_url = "jdbc:sqlite:";
    public Database(String db_name, String instructions){
        this.dbName = db_name;
        this.instructions = instructions;
        this.connectToDbms(db_name);
    }

    public boolean connectToDbms(String db_name){
        String url = sqlite_url + db_name;
        try{
            Class.forName("org.sqlite.JDBC");
            this.connection = DriverManager.getConnection(url);
        } catch (Exception e){
            System.out.println("encountered an error connecting to sqlite file at");
            System.out.println("url: " + url);
            System.out.println(e);
        }

        return connection == null;
    }

    public void initial_tables(){
        if(this.connection == null){
            System.out.print("db connection is null");
            return;
        }
        try{
            Statement statement = this.connection.createStatement();
            statement.executeUpdate("CREATE TABLE PH_TABLE (" +
                    "   TABLE_ID INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "   NAME TEXT UNIQUE," +
                    "   NUM_COLUMNS INTEGER," +
                    "   LAST_ROW INTEGER" +
                    ");");
            statement.executeUpdate("CREATE INDEX PH_TABLE_NAME_INDEX ON PH_TABLE(NAME)");
            statement.executeUpdate("CREATE TABLE PH_COL_RANGES (" +
                    "   TABLE_ID INTEGER," +
                    "   COL_NUM INTEGER," +
                    "   COL_RANGE INTEGER," +
                    "   PRIMARY KEY (TABLE_ID, COL_NUM)" +
                    ");");
            statement.executeUpdate("CREATE TABLE PH_TABLE_ROWS (" +
                    "   TABLE_ID INTEGER," +
                    "   ROW_NUM INTEGER," +
                    "   COL_NUM INTEGER," +
                    "   VALUE TEXT," +
                    "   PRIMARY KEY (TABLE_ID, ROW_NUM, COL_NUM)" +
                    ");");
            statement.executeUpdate("CREATE TABLE PH_HASH_BUCKETS (" +
                    "   TABLE_ID INTEGER," +
                    "   HASH_BUCKET INTEGER," +
                    "   ROW_NUM INTEGER," +
                    "   PRIMARY KEY (TABLE_ID, HASH_BUCKET)" +
                    ");");


        } catch(Exception e){
            System.out.println(e);
        }
    }

    public void parse_instructions(String instructions){
        String fileName = instructions;

        try (BufferedReader br = new BufferedReader(new FileReader(instructions))){
            String line;
            while ((line = br.readLine()) != null){
                String[] words = line.split("\\s+");
                if(words[0].equals("c")){
                    create_table();
                }
                else if(words[0].equals("i")){
                    insert_into_table();
                }
                else if(words[0].equals("l")){
                    lookup_in_table();
                }
                else{
                    System.out.println("Please enter one of the following format: \nc table_name col_1_hash_range col_2_hash_range  ... col_n_hash_range" +
                            "\ni table_name col_1_value col_2_value ... col_n_value" +
                            "\nl table_name use_index_or_not col_choice_1 ... col_choice_n");
                }
            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public void create_table(){

    }

    public void insert_into_table(){

    }

    public void lookup_in_table(){

    }
}
