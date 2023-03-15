import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;
import com.opencsv.CSVWriter;
public class Table {
    String sTableName;
    int iNumOfPages;
    int iNumOfRows;
    Vector<Pair<Integer,Integer>> vecMinMaxOfPagesForClusteringKey;

    public Table(String strTableName, String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                 Hashtable<String,String> htblColNameMax ) throws IOException, DBAppException {
        this.sTableName = strTableName;
        this.iNumOfPages = 0;
        this.iNumOfRows = 0;
        this.vecMinMaxOfPagesForClusteringKey = new Vector<Pair<Integer,Integer>>();

        //check if the table sizes match
        if (htblColNameType.size() != htblColNameMin.size() ||
                htblColNameType.size() != htblColNameMax.size()) {
            throw new DBAppException("Table sizes do not match");
        }

        //making sure column data types are valid
        //the valid datatypes are: java.lang.Integer , java.lang.String ,
        //java.lang.Double , java.util.Date (Note: date acceptable format is "YYYY-MM-DD")
        for (String key : htblColNameType.keySet()) {
            if (!htblColNameType.get(key).equals("java.lang.Integer") &&
                    !htblColNameType.get(key).equals("java.lang.String") &&
                    !htblColNameType.get(key).equals("java.lang.Double") &&
                    !htblColNameType.get(key).equals("java.util.Date")) {
                    throw new DBAppException("Invalid data type for column " + key);
            }
        }

        //declaring the metadata file and setting it to append mode
        CSVWriter writer = new CSVWriter(new FileWriter("src/main/java/metadata.csv", true));

        //add new line to metadata file
        String[] startAtNewLine = new String[1];
        writer.writeNext(startAtNewLine);

        //filling the metadata file
        //Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max
        String[] metadata = new String[8];
        for (String col: htblColNameType.keySet()) {
            metadata[0] = strTableName;
            metadata[1] = col;
            metadata[2] = htblColNameType.get(col);
            metadata[3] = (col.equals(strClusteringKeyColumn)) ? "True" : "False";
            metadata[4] = "null"; //IndexName will be null for now
            metadata[5] = "null"; //IndexType will be null for now
            metadata[6] = htblColNameMin.get(col);
            metadata[7] = htblColNameMax.get(col);
            writer.writeNext(metadata);
        }
        writer.close();

    }
}
