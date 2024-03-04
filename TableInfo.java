public class TableInfo {
    private int tableID;
    private int numColumns;
    private int lastRow;
    private String tableName;
    
    public TableInfo(String name, int id, int numC, int lastR){
        this.tableName = name;
        this.tableID = id;
        this.numColumns = numC;
        this.lastRow = lastR;
    }

    public String getName(){ return this.tableName; }
    public int getID(){ return this.tableID; }
    public int getLastRow() { return this.lastRow; }
    public int getNumColumns() { return this.numColumns; }
}
