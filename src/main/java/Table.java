import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;
import com.opencsv.CSVWriter;
public class Table {
    String tableName;
    int numOfPages;
    int numOfRows;
    Vector<Pair<Integer,Integer>> minMaxOfPages;

    public Table(String strTableName, String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                 Hashtable<String,String> htblColNameMax ) throws IOException {
        this.tableName = strTableName;
        this.numOfPages = 0;
        this.numOfRows = 0;
        this.minMaxOfPages = new Vector<Pair<Integer,Integer>>();

        CSVWriter writer = new CSVWriter(new FileWriter("metadata.csv"));
    }
}
