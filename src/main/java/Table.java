import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;


public class Table implements java.io.Serializable {
    String sTableName;
    String sClusteringKey;
    int iNumOfPages;
    int iNumOfRows;
    Hashtable<Integer, Boolean> hPageFullStatus;
    Vector<rangePair<Object,Object>> vecMinMaxOfPagesForClusteringKey;
    Vector<Integer> vNumberOfRowsPerPage;

    public Table(String strTableName, String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                 Hashtable<String,String> htblColNameMax ) throws IOException, DBAppException, CsvValidationException {
        this.sTableName = strTableName;
        this.iNumOfPages = 0;
        this.iNumOfRows = 0;
        this.hPageFullStatus = new Hashtable<>();
        this.vNumberOfRowsPerPage = new Vector<>();
        this.vecMinMaxOfPagesForClusteringKey = new Vector<>();
        this.sClusteringKey = strClusteringKeyColumn;

        //check if the table sizes match
        if (htblColNameType.size() != htblColNameMin.size() ||
                htblColNameType.size() != htblColNameMax.size()) {
            throw new DBAppException("Number of Columns does not match their given properties");
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

    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws IOException, CsvValidationException {
        /* NOTES:
            - check for input (size and datatypes), (Note: date acceptable format is "YYYY-MM-DD")
            - don't insert more than N (consult DBApp.config)
            - always update minMax
            - save page after modification/creation
            - save table at the end
            - fill fullStatus HashTable
            - fill vNumberOfPagesPerRow
        */

        // check for input data validity
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        while ((line = reader.readNext()) != null) {
            // Process each line of the CSV file
            for (String field : line) {
                System.out.print(field + " ");
            }
            System.out.println();
        }


        // fetch max page size
        String sFilename = "DBApp.config";
        Properties configProperties = new Properties();

        try {
            FileInputStream fis = new FileInputStream(sFilename);
            configProperties.load(fis);
        }
        catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        catch (IOException e)  {
            e.printStackTrace();
        }

        int N = Integer.parseInt(configProperties.getProperty("DBApp.MaximumRowsCountinTablePage"));

        // check if this is the first insert
        if (iNumOfPages == 0) {
            iNumOfPages++;
            Page pPage1 = new Page(sTableName, iNumOfPages - 1, false);
            pPage1.vRecords.add(htblColNameValue);
            Object oMinClusterVal = pPage1.vRecords.get(0).get(sClusteringKey);
            Object oMaxClusterVal = pPage1.vRecords.get(pPage1.size()-1).get(sClusteringKey);
            // update minMax vector
            vecMinMaxOfPagesForClusteringKey.add(new rangePair<>(oMinClusterVal, oMaxClusterVal));
            hPageFullStatus.put(iNumOfPages-1, false);
            pPage1.serializePage();
        } else { // insert in some page
            Object oClusterValue = htblColNameValue.get(sClusteringKey);
            int iInsertPageNum = 0;
            Page pInsertPage = null;
            // consult minMax to fetch da page
            for (int i = 0; i < iNumOfPages; i++) {
                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) <= 0
                        && ((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).max).compareTo((Comparable) oClusterValue) >= 0) {
                    iInsertPageNum = i;
                }

                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) > 0) {
                    break;
                }
            }
            //pInsertPage = new Page()

        }
    }

    public void deleteFromTable() {

    }

    public void updateTable() {
        /* 2 cases:
            1- updating a value of the cluster key
            2- updating a values not related to the cluster key

            for case 1:
                call delete method then call insert with the updated record
            for case 2:
                call search then update
            goodluck future implementer :)
        */
    }

    public void searchInTable() {
        // use searchRecords instead?
    }

    public void deleteTable() {

    }

    public void serializeTable() {
        try {
            FileOutputStream fos = new FileOutputStream(sTableName+".class");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // chad search; serves selectFromTable, deleteFromTable (supplies the records and their locations: page, index)
    public Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>> searchRecords(Hashtable<String,Object> hCondition) {
        Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>> result = new Vector<>();
        Vector<Hashtable<String, Object>> vRecords = new Vector<>(); // actual page records

        // Check approach: Cluster Key present ? Binary Search : Linear search
        if (hCondition.keySet().contains(sClusteringKey)) { // eles go, binary search
            Object oClusterValue = hCondition.get(sClusteringKey);
            // consult hanti-kanti-ultra-omega-gadaym-speedy minProMax vector to fetch da page
            for (int i = 0; i < iNumOfPages; i++) {
                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) <= 0
                && ((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).max).compareTo((Comparable) oClusterValue) >= 0) {
                    // binary search (only one row has this primary cluster key)
                    Page pCurrentPage = new Page(sTableName, i, true);
                    int lo = 0;
                    int hi = pCurrentPage.size() - 1;
                    while (lo <= hi) {
                        int mid = (lo + hi) / 2;
                        if (((Comparable)pCurrentPage.vRecords.get(mid).get(sClusteringKey)).compareTo(oClusterValue) < 0) {
                            lo = mid + 1;
                        } else if (((Comparable)pCurrentPage.vRecords.get(mid).get(sClusteringKey)).compareTo(oClusterValue) > 0) {
                            hi = mid - 1;
                        } else {
                            // check other conditions
                            Hashtable<String, Object> hCurrRecord = vRecords.get(mid);
                            boolean bAllSatisfied = true;
                            for (String col : hCondition.keySet()) {
                                if (!hCondition.get(col).equals(hCurrRecord.get(col))) {
                                    bAllSatisfied = false;
                                    break;
                                }
                            }

                            if (bAllSatisfied)
                                result.add(new Pair<>((new Pair<>(i, mid)), hCurrRecord));
                            break;
                        }
                    }
                    break; // cuz only one page has this primary cluster key
                }
            }


        } else { // no clustering key :(, search linearly
            // for each page
            for (int i = 0; i < iNumOfPages; i++) {
                // load (de-serialize the page)
                Page pCurrentPage = new Page(sTableName, i, true);

                int index = 0;
                // for each record in page
                for (Hashtable<String, Object> ht : pCurrentPage.vRecords) {
                    // for each key in condition, compare with current record
                    boolean bAllSatisfied = true;
                    for (String col : hCondition.keySet()) {
                        if (!hCondition.get(col).equals(ht.get(col))) {
                            bAllSatisfied = false;
                            break;
                        }
                    }

                    if (bAllSatisfied)
                        result.add(new Pair<>((new Pair<>(i, index)), ht));

                    index++;
                }
            }
        }

        return result;
    }

    public static Table loadTable (String sTableName) {
        Table tTable = null;
        try {
            FileInputStream fis = new FileInputStream(sTableName+".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            tTable = (Table) ois.readObject();
            ois.close();
            fis.close();
            return (Table) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return tTable;
    }
}
