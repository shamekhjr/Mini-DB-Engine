import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListSet;


public class Table implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 123862598456l;
    String sTableName;
    String sClusteringKey;
    int iNumOfPages;
    int iNumOfRows;
    Hashtable<Integer, Boolean> hPageFullStatus;
    ConcurrentSkipListSet<Object> cslsClusterValues;
    ConcurrentSkipListSet<String> cslsColNames;
    Vector<rangePair<Serializable, Serializable>> vecMinMaxOfPagesForClusteringKey;
    Vector<Integer> vNumberOfRowsPerPage;

    public Table(String strTableName, String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                 Hashtable<String,String> htblColNameMax ) throws IOException, DBAppException, CsvValidationException {

        //initializing instance vars
        this.sTableName = strTableName;
        this.iNumOfPages = 0;
        this.iNumOfRows = 0;
        this.hPageFullStatus = new Hashtable<>();
        this.cslsClusterValues = new ConcurrentSkipListSet<>();
        this.cslsColNames = new ConcurrentSkipListSet<>();
        this.vNumberOfRowsPerPage = new Vector<>();
        this.vecMinMaxOfPagesForClusteringKey = new Vector<rangePair<Serializable, Serializable>>();
        this.sClusteringKey = strClusteringKeyColumn;

        //check if table name already exists
        if (DBApp.isExistingTable(strTableName)) {
            throw new DBAppException("Table " + strTableName + " already exists");
        }


        //check if the table sizes match
        if (htblColNameType.size() != htblColNameMin.size() ||
                htblColNameType.size() != htblColNameMax.size()) {
            throw new DBAppException("Number of Columns does not match their given properties");
        }

        //making sure column data types are valid
        //the valid datatypes are: java.lang.Integer , java.lang.String ,
        //java.lang.Double , java.util.Date (Note: date acceptable format is "YYYY-MM-DD")
        if (!isColDataTypeValid(htblColNameType)) {
            throw new DBAppException("Invalid column data type");
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
            cslsColNames.add(col);
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

    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws IOException, CsvValidationException, DBAppException {
        /* NOTES:
            - check for input (size and datatypes), (Note: date acceptable format is "YYYY-MM-DD")
            - don't insert more than N (consult DBApp.config)
            - always update minMax, hPageFullStatus, vNumberOfRowsPerPage, iNumOfPages, iNumOfRows
            - save page after modification/creation
            - save table at the end (done in DBApp.java)
            - check if primary key is unique
            - check if primary key present
            - insert nulls in missing columns
        */

        // check for input data validity
        checkValidityOfData(htblColNameValue);

        // insert nulls in the missing columns
        for (String col: cslsColNames) {
            if (!htblColNameValue.containsKey(col)) {
                htblColNameValue.put(col, null);
            }
        }

        // check if primary key already exists
        if (cslsClusterValues.contains(htblColNameValue.get(sClusteringKey))) { // instead of searching for the record
            // trade-off between speed and memory usage; this improves speed but consumes alot of memory
            // O(log n) but n may be huge, so is it better than searching for the record?
            throw new DBAppException("Primary key already exists");
        }

        cslsClusterValues.add(htblColNameValue.get(sClusteringKey)); // add to the ClusterValues list

        // check if this is the first insert
        if (iNumOfPages == 0) {
            iNumOfPages++;
            Page pPage1 = new Page(sTableName, sClusteringKey, iNumOfPages - 1, false);
            pPage1.vRecords.add(htblColNameValue);
            updatePageMeta(pPage1);
            pPage1.serializePage();

        } else { // insert in some page
            Object oClusterValue = htblColNameValue.get(sClusteringKey);
            int iInsertPageNum = 0; // case that the input is the smallest value
            Page pInsertPage = null;
            // consult minMax to fetch da page
            for (int i = 0; i < iNumOfPages; i++) {
                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) <= 0) {
                    iInsertPageNum = i;
                }

                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) > 0) {
                    break;
                }
            }

            pInsertPage = new Page(sTableName, sClusteringKey, iInsertPageNum, true);


            boolean bIsFull = pInsertPage.isFull();

            // insert in page
            pInsertPage.sortedInsert(htblColNameValue, sClusteringKey);


            // check if page is full
            if (bIsFull) {
                // get the last row in the page and delete it
                Hashtable<String, Object> htblLastRow = pInsertPage.vRecords.get(pInsertPage.size()-1);
                pInsertPage.vRecords.remove(pInsertPage.size()-1);

                // check if last page
                if (iInsertPageNum == iNumOfPages - 1) {
                    // insert in new page
                    iNumOfPages++;
                    Page pNewPage = new Page(sTableName, sClusteringKey,iNumOfPages - 1, false);
                    pNewPage.vRecords.add(htblLastRow);
                    updatePageMeta(pNewPage);
                    pNewPage.serializePage();

                } else {
                    // loop to shift to the next pages
                    for (int i = iInsertPageNum + 1; i < iNumOfPages; i++) {
                        Page pPage = new Page(sTableName, sClusteringKey, i, true);
                        if (pPage.isFull()) {
                            // add to page and shift the extra row in this page to the next page if exists
                            pPage.sortedInsert(htblLastRow, sClusteringKey);

                            // get the last row in the page and delete it
                            htblLastRow = pPage.vRecords.get(pPage.size()-1);
                            pPage.vRecords.remove(pPage.size()-1);
                            updatePageMeta(pPage);
                            pPage.serializePage();

                            // check if last page
                            if (pPage.index == iNumOfPages - 1) {
                                // insert in new page
                                iNumOfPages++;
                                Page pNewPage = new Page(sTableName, sClusteringKey, iNumOfPages - 1, false);
                                pNewPage.vRecords.add(htblLastRow);
                                updatePageMeta(pNewPage);
                                pNewPage.serializePage();
                                break;
                            }
                        } else { // page is not full
                            // add to page and break
                            pPage.sortedInsert(htblLastRow, sClusteringKey);
                            updatePageMeta(pPage);
                            pPage.serializePage();
                            break;
                        }
                    }
                }
            }
            // update minMax vector of insertPage
            updatePageMeta(pInsertPage);
            pInsertPage.serializePage();
        }
        iNumOfRows++;
    }

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) {

        //TODO
        //1- search for all relevant records based on conditions (DONE)
        //2- remove the records in descending order (akher index fe akher page le awel index fe awel page) (DONE)
        //3- update minMax (DONE)
        //4- inter-vector shiftation (not needed anymore)
        //5- re-serialize and save pages (do in dbapp.java) (DONE)

        //search for all relevant data given the conditions
        //first integer is pageNumber
        //second integer is recordIndex
        Vector<Pair<Pair<Integer, Integer>, Hashtable<String, Object>>> vRelevantRecords = searchRecords(htblColNameValue);

        //init hashtable of pages to keep stuff in memory
        Hashtable<Integer,Page> htblPagesTemp = new Hashtable<>();

        //remove the records in descending order
        for (int i = vRelevantRecords.size() - 1; i >= 0; i--) {

            //find pageNumber and recordIndexInPage
            int iPageToLoad = vRelevantRecords.get(i).val1.val1;
            int iRecordIndexInPage = vRelevantRecords.get(i).val1.val2;

            //load the page
            Page pPageToLoad = new Page(strTableName, sClusteringKey, iPageToLoad, true);
            htblPagesTemp.put(iPageToLoad, pPageToLoad);

            //remove the record
            pPageToLoad.vRecords.remove(iRecordIndexInPage);

            //decrement number of rows in the page
            this.vNumberOfRowsPerPage.set(iPageToLoad, vNumberOfRowsPerPage.get(iPageToLoad) - 1);

            //update page meta
            updatePageMeta(pPageToLoad);

            }
        //if page is empty, delete page
        for (int i = 0; i < vNumberOfRowsPerPage.size(); i++) {
            if (vNumberOfRowsPerPage.get(i) == 0) {
                File f = new File("src/main/resources/data/" + strTableName + "/" + sClusteringKey + "/" + i + ".class");
                f.delete();
                vNumberOfRowsPerPage.remove(i);
                hPageFullStatus.remove(i);
                iNumOfPages--;
                i--;
            }

        }

        //TODO:need to handle deleting page if empty
        //TODO:need to handle deleting table if empty

        //inter-vector shiftation (not needed anymore)
//        for (int k = vRelevantRecords.get(0).val1.val1; k < this.iNumOfPages; k++) {
//
//            //check if page is not full
//            //if it is not full, i know i should get values from next non-empty pages and insert
//            if(!hPageFullStatus.get(k)) {
//                for (int j = k + 1; j < this.iNumOfPages; j++) {
//
//                    if (vNumberOfRowsPerPage.get(j) > 0) {
//                        //we need to remove records from this page and put them in page k
//                    }
//                }
//            }
//
//        }

        //if table is empty, delete entire table
        if (iNumOfRows == 0) {
            File f = new File("src/main/java/data/" + strTableName);
            f.delete();
        }

    }

    public void updateTable(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, CsvValidationException, IOException, ParseException {
        /* 2 cases:
            1- updating a value of the cluster key --> No update of key (Check Piazza).
            2- updating a values not related to the cluster key

            for case 1:
                call delete method then call insert with the updated record
                    --> We do not have to delete the record as it would be sorted based on its Primary key which is *fixed*
            for case 2:
                call search then update
                    --> Hence our update method will relay on just searching for the Clustering key of the record we want to update and change the values in the columns defined in the htblColNameValue
        */

        // Recall: we update one row *only*
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);

        // Check that data that need to be updated is valid
        // check if all columns in input are valid
        for (String col : htblColNameValue.keySet()) {
            if (!cslsColNames.contains(col)) {
                throw new DBAppException("Column " + col + " does not exist");
            }
        }

        while ((line = reader.readNext()) != null) {
            // Process each line of the CSV file
            if (line[0].equals(sTableName) && htblColNameValue.get(line[1]) != null) {

                // check for data type
                if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                    throw new DBAppException("Invalid data type for column " + line[1]);
                }

                // check for min and max
                if (castAndCompare(htblColNameValue.get(line[1]),line[6]) < 0
                        || castAndCompare(htblColNameValue.get(line[1]),line[7]) > 0) {
                    throw new DBAppException("Value for column " + line[1] + " is out of range");
                }

                // check if date in input is in the correct format "YYYY-MM-DD"
                if (line[2].equals("java.util.Date")) {
                    String[] date = ((String) htblColNameValue.get(line[1])).split("-");
                    if (date.length != 3) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                    if (date[0].length() != 4 || date[1].length() != 2 || date[2].length() != 2) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                }

            }

        }

        reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        // Check that data of the clustering key is valid
        while ((line = reader.readNext()) != null)
        {
            if (line[0].equals(sTableName) && line[1].equals(sClusteringKey) && Boolean.parseBoolean(line[3]))
            {
                if (line[2].equals("java.util.Date")) {
                    Date date = formatter.parse(strClusteringKeyValue);
                    htblColNameValue.put(sClusteringKey, date);
                }

                if (line[2].equals("java.lang.Integer")) {
                    Integer i = Integer.parseInt(strClusteringKeyValue);
                    htblColNameValue.put(sClusteringKey, i);
                }

                if (line[2].equals("java.lang.String"))  {
                    String s = strClusteringKeyValue;
                    htblColNameValue.put(sClusteringKey, s);
                }

                if (line[2].equals("java.lang.Double")) {
                    Double d = Double.parseDouble(strClusteringKeyValue);
                    htblColNameValue.put(sClusteringKey, d);
                }

                // check for data type
                if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                    throw new DBAppException("Invalid data type for column " + line[1]);
                }

                // check for min and max
                if (castAndCompare(htblColNameValue.get(line[1]),line[6]) < 0
                        || castAndCompare(htblColNameValue.get(line[1]),line[7]) > 0) {
                    throw new DBAppException("Value for column " + line[1] + " is out of range");
                }

                // check if date in input is in the correct format "YYYY-MM-DD"
                if (line[2].equals("java.util.Date")) {
                    String[] date = ((String) htblColNameValue.get(line[1])).split("-");
                    if (date.length != 3) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                    if (date[0].length() != 4 || date[1].length() != 2 || date[2].length() != 2) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                }
            }
        }

        // Step 1: Search for the page that contain the Clustering key value.
        Page targetPage;
        int indexPage = -1;

        for (int i = 0; i < iNumOfPages; i++) {

            if (((Comparable)htblColNameValue.get(sClusteringKey)).compareTo((Comparable)vecMinMaxOfPagesForClusteringKey.get(i).min) >= 0) {
                indexPage = i;
            }
            else {
                break;
            }
        }


        // If the user tried to update a record with invalid primary key
        if (indexPage == -1) {
            throw new DBAppException("Primary Key does not exists");
        }

        // Step 2: Deserialize the page at index [indexPage]
        targetPage = new Page(sTableName, sClusteringKey, indexPage, true);

        // Step 3: Update the record
        targetPage.updatePage(strClusteringKeyValue, htblColNameValue);

        // Step 4: Serialize the page
        targetPage.serializePage();
    }

    public void searchInTable() {
        // use searchRecords instead?
    }

    // Note: this method is not used in the project, not required
    public void deleteTable() {
        File myObj = new File(this.sTableName + ".class");
        myObj.delete();
        for (int i = 0; i < iNumOfPages; i++) {
            Page tmpPage = new Page (sTableName, sClusteringKey, i, false);
            tmpPage.deletePage();
        }
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
        Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>> result = new Vector<>(); // page, index, record

        // Check approach: Cluster Key present ? Binary Search : Linear search
        if (hCondition.keySet().contains(sClusteringKey)) { // binary search
            Object oClusterValue = hCondition.get(sClusteringKey);
            // consult hanti-kanti-ultra-omega-gadaym-speedy minProMax vector to fetch da page
            int iPageNum = 0;
            for (int i = 0; i < iNumOfPages; i++) {
                if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) <= 0
                && ((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).max).compareTo((Comparable) oClusterValue) >= 0) {
                    // must be within min and max as we are searching not inserting
                    iPageNum = i;
                    break;
                } else if (((Comparable) vecMinMaxOfPagesForClusteringKey.get(i).min).compareTo((Comparable) oClusterValue) > 0) {
                    // if the value is less than the min of the page, then it is not in the table
                    return result;
                }
            }
            Page pCurrentPage = new Page(sTableName, sClusteringKey , iPageNum, true);
            // binary search (only one row has this primary cluster key)
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
                    Hashtable<String, Object> hCurrRecord = pCurrentPage.vRecords.get(mid);
                    boolean bAllSatisfied = true;
                    for (String col : hCondition.keySet()) {
                        if (!hCondition.get(col).equals(hCurrRecord.get(col))) {
                            bAllSatisfied = false;
                            break;
                        }
                    }

                    if (bAllSatisfied)
                        result.add(new Pair<>((new Pair<>(iPageNum, mid)), hCurrRecord));
                    break;
                }
            }


        } else { // no clustering key :(, search linearly
            // for each page
            for (int i = 0; i < iNumOfPages; i++) {
                // load (de-serialize the page)
                Page pCurrentPage = new Page(sTableName, sClusteringKey, i, true);

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

    public static Table loadTable (String sTableName) throws  Exception {
        Table tTable = null;
        try {
            FileInputStream fis = new FileInputStream(sTableName+".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            tTable = (Table) ois.readObject();
            ois.close();
            fis.close();
            return tTable;
        } catch (IOException | ClassNotFoundException e) {
            throw new Exception(e.getMessage() + " Table: " + sTableName);
        }
    }

    public static int castAndCompare(Object input, String csv) {
        Class<?> clazz = input.getClass();
        int result = 0;
        if (clazz == Double.class) {
            Double d = Double.parseDouble(csv);
            result = ((Double) input).compareTo(d);
        } else if (clazz == Integer.class) {
            Integer d = Integer.parseInt(csv);
            result = ((Integer) input).compareTo(d);
        } else if (clazz == Date.class) {
            long d = Date.parse(csv);
            long input_date = ((Date) input).getTime();
            result = Long.compare(input_date, d);
        } else if (clazz == String.class) {
            String input_string = (String) input;
            result = input_string.length()-csv.length();
        }
        return result;

    }

    public void updatePageMeta(Page p) {
        Serializable oMinClusterVal = (Serializable) p.vRecords.get(0).get(sClusteringKey);
        Serializable oMaxClusterVal = (Serializable) p.vRecords.get(p.size()-1).get(sClusteringKey);

        if (vecMinMaxOfPagesForClusteringKey.size() > p.index) { // if the page index already exists in the vecMinMaxOfPagesForClusteringKey then we only need to update its minimum and maximum values
            vecMinMaxOfPagesForClusteringKey.set(p.index, new rangePair<>(oMinClusterVal, oMaxClusterVal));
        }
        else {
            vecMinMaxOfPagesForClusteringKey.add(new rangePair<>(oMinClusterVal, oMaxClusterVal));
        }
        hPageFullStatus.put(p.index, p.isFull());
        vNumberOfRowsPerPage.add(p.index, p.size());
    }

    public void checkValidityOfData(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, CsvValidationException {
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;

        // check if all columns in input are valid
        for (String col : htblColNameValue.keySet()) {
            if (!cslsColNames.contains(col)) {
                throw new DBAppException("Column " + col + " does not exist");
            }
        }

        // check if primary key is in input
        if (!htblColNameValue.containsKey(sClusteringKey)) {
            throw new DBAppException("Primary key " + sClusteringKey + " is not in input");
        }

        boolean found = false;
        while ((line = reader.readNext()) != null) {
            // Process each line of the CSV file

            if (line[0].equals(sTableName)) {
                found = true;

                // check for data type
                if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                    throw new DBAppException("Invalid data type for column " + line[1]);
                }

                // check for min and max
                if (castAndCompare(htblColNameValue.get(line[1]),line[6]) < 0
                        || castAndCompare(htblColNameValue.get(line[1]),line[7]) > 0) {
                    throw new DBAppException("Value for column " + line[1] + " is out of range");
                }

                // check if date in input is in the correct format "YYYY-MM-DD"
                if (line[2].equals("java.util.Date")) {
                    String[] date = ((String) htblColNameValue.get(line[1])).split("-");
                    if (date.length != 3) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                    if (date[0].length() != 4 || date[1].length() != 2 || date[2].length() != 2) {
                        throw new DBAppException("Invalid date format for column " + line[1]);
                    }
                }

            } else if (found) {
                break; // no need to continue searching
            }

        }
    }

    public static boolean isColDataTypeValid(Hashtable<String,String> htblColNameType) {
        for (String col : htblColNameType.keySet()) {
            if (!htblColNameType.get(col).equals("java.lang.Integer") &&
                    !htblColNameType.get(col).equals("java.lang.String") &&
                    !htblColNameType.get(col).equals("java.lang.Double") &&
                    !htblColNameType.get(col).equals("java.util.Date")) {
                return false;
            }
        }
        return true;
    }

    public void showPage(int iPageNum) throws IOException, CsvValidationException {
        Page p = new Page(sTableName, sClusteringKey, iPageNum, true);
        System.out.println("Page " + iPageNum + " has " + p.size() + " records ======");
        int i = 1;
        for (Hashtable<String, Object> ht : p.vRecords) {
            System.out.println("" + i + "- " + ht);
            i++;
        }
        System.out.println("====================================");
    }
}
