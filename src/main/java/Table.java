import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class Table implements java.io.Serializable {
    String sTableName;
    String sClusteringKey;
    int iNumOfPages;
    int iNumOfRows;
    Vector<Pair<Object,Object>> vecMinMaxOfPagesForClusteringKey;

    public Table(String strTableName, String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                 Hashtable<String,String> htblColNameMax ) throws IOException, DBAppException, CsvValidationException {
        this.sTableName = strTableName;
        this.iNumOfPages = 0;
        this.iNumOfRows = 0;
        this.vecMinMaxOfPagesForClusteringKey = new Vector<Pair<Object,Object>>();
        this.sClusteringKey = strClusteringKeyColumn;

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

        //declaring csv reader
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));

        //check if table name already exists
        String[] line;
        while ((line = reader.readNext()) != null) {
            if (line[0].equals(strTableName)) {
                throw new DBAppException("A table with this name already exists");
            }
        }

        //declaring the metadata file and setting it to append mode
        CSVWriter writer = new CSVWriter(new FileWriter("src/main/java/metadata.csv", true));

        //add new line to metadata file
        String[] startAtNewLine = new String[1];
        writer.writeNext(startAtNewLine);

        //filling the metadata file. The order of the columns is:
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

    public void insertIntoTable() {

    }

    public void deleteFromTable() {

    }

    public void updateTable() {

    }

    public void searchInTable() {

    }

    public void deleteTable() {

    }

    public Vector<Hashtable<String,Object>> searchRecords(Hashtable<String,Object> hCondition) {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        Vector<Hashtable<String, Object>> vRecords = new Vector<>(); // actual page records

        // Check approach: Cluster Key present ? Binary Search : Linear search
        if (hCondition.keySet().contains(sClusteringKey)) { // eles go, binary search
            Object oClusterValue = hCondition.get(sClusteringKey);
            // consult hanti-kanti-ultra-omega-gadaym-speedy minProMax vector to fetch da page
            for (int i = 0; i < iNumOfPages; i++) {
                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) <= 0
                && ((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).max).compareTo((Comparable) oClusterValue) >= 0) {
                    try {
                        FileInputStream fis = new FileInputStream(sTableName+"_page"+ i + ".class");
                        ObjectInputStream ois = new ObjectInputStream(fis);
                        vRecords = (Vector<Hashtable<String, Object>>) ois.readObject();
                        ois.close();
                        fis.close();
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    // binary search
                    Page pCurrentPage = new Page(vRecords);
                    int lo = 0;
                    int hi = pCurrentPage.size() - 1;
                    while (lo <= hi) {
                        // hmm... problem: binary search returns 1 val
                        // does he want to binary search on pages?
                        // like try middle page then go left or right accordingly?
                        // DEAD LOCK :/
                        // hours wasted counter: 2h
                    }

                }
            }


        } else { // no clustering key :(, search linearly
            // for each page
            for (int i = 0; i < iNumOfPages; i++) {
                // load (de-serialize the page)
                try {
                    FileInputStream fis = new FileInputStream(sTableName+"_page"+ i + ".class");
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    vRecords = (Vector<Hashtable<String, Object>>) ois.readObject();
                    ois.close();
                    fis.close();
                    System.out.println("Object deserialized from outputFile.class");
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }

                // for each record in page
                for (Hashtable<String, Object> ht : vRecords) {
                    // for each key in condition, compare with current record
                    boolean bAllSatisfied = true;
                    for (String col : hCondition.keySet()) {
                        if (!hCondition.get(col).equals(ht.get(col))) {
                            bAllSatisfied = false;
                            break;
                        }
                    }

                    if (bAllSatisfied) result.add(ht);
                }
            }
        }

        return result;
    }
}
