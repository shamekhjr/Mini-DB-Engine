import java.io.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListSet;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;


public class Table implements java.io.Serializable {
    String sTableName;
    String sClusteringKey;
    int iNumOfPages;
    int iNumOfRows;
    Hashtable<Integer, Boolean> hPageFullStatus;
    ConcurrentSkipListSet<Object> cslsClusterValues;
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
        this.vNumberOfRowsPerPage = new Vector<>();
        this.vecMinMaxOfPagesForClusteringKey = new Vector<rangePair<Serializable, Serializable>>();
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

    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws IOException, CsvValidationException, DBAppException {
        /* NOTES:
            - check for input (size and datatypes), (Note: date acceptable format is "YYYY-MM-DD")
            - don't insert more than N (consult DBApp.config)
            - always update minMax, hPageFullStatus, vNumberOfRowsPerPage, iNumOfPages, iNumOfRows
            - save page after modification/creation
            - save table at the end (done in DBApp.java)
            - check if primary key is unique
        */

        // check for input data validity
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        while ((line = reader.readNext()) != null) {
            // Process each line of the CSV file
            for (String field : line) {
               if (field.equals(sTableName)) {
                   // check for data type
                   if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                       throw new DBAppException("Invalid data type for column " + line[1]);
                   }
                   // check for min and max
                   castAndCompare(htblColNameValue.get(line[1]),line[6]);
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
        }

        // check if primary key already exists
        Hashtable<String, Object> hPrimaryKey = new Hashtable<>();
        hPrimaryKey.put(sClusteringKey, htblColNameValue.get(sClusteringKey));

//        if (searchRecords(hPrimaryKey).size() != 0) {
//            throw new DBAppException("Primary key already exists");
//        }

        if (cslsClusterValues.contains(htblColNameValue.get(sClusteringKey))) { // instead of searching for the record
            throw new DBAppException("Primary key already exists");
        }

        cslsClusterValues.add(htblColNameValue.get(sClusteringKey)); // add to the ClusterValues list

        // check if this is the first insert
        if (iNumOfPages == 0) {
            iNumOfPages++;
            Page pPage1 = new Page(sTableName, iNumOfPages - 1, false);
            pPage1.vRecords.add(htblColNameValue);
            Serializable oMinClusterVal = (Serializable) pPage1.vRecords.get(0).get(sClusteringKey);
            Serializable oMaxClusterVal = (Serializable) pPage1.vRecords.get(pPage1.size()-1).get(sClusteringKey);
            // update minMax vector
            vecMinMaxOfPagesForClusteringKey.add(new rangePair<>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));
            hPageFullStatus.put(iNumOfPages - 1, false);
            vNumberOfRowsPerPage.add(iNumOfPages - 1, 1);
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

            pInsertPage = new Page(sTableName, iInsertPageNum, true);


            boolean bIsFull = false;
            if (pInsertPage.isFull()) {
                bIsFull = true;
            }

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
                    Page pNewPage = new Page(sTableName, iNumOfPages - 1, false);
                    pNewPage.vRecords.add(htblLastRow);

                    // update minMax vector
                    Serializable oMinClusterVal = (Serializable) pNewPage.vRecords.get(0).get(sClusteringKey);
                    Serializable oMaxClusterVal = (Serializable) pNewPage.vRecords.get(pNewPage.size()-1).get(sClusteringKey);
                    vecMinMaxOfPagesForClusteringKey.add(new rangePair<Serializable, Serializable>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));
                    hPageFullStatus.put(iNumOfPages - 1, pNewPage.isFull());
                    vNumberOfRowsPerPage.add(iNumOfPages - 1, pNewPage.size());
                    pNewPage.serializePage();
                } else {
                    // loop to shift to the next pages
                    for (int i = iInsertPageNum + 1; i < iNumOfPages; i++) {
                        Page pPage = new Page(sTableName, i, true);
                        if (pPage.isFull()) {
                            // add to page and shift the extra row in this page to the next page if exists
                            pPage.sortedInsert(htblLastRow, sClusteringKey);

                            // get the last row in the page and delete it
                            htblLastRow = pPage.vRecords.get(pPage.size()-1);
                            pPage.vRecords.remove(pPage.size()-1);

                            // update minMax vector
                            Serializable oMinClusterVal = (Serializable) pPage.vRecords.get(0).get(sClusteringKey);
                            Serializable oMaxClusterVal = (Serializable) pPage.vRecords.get(pPage.size()-1).get(sClusteringKey);
                            vecMinMaxOfPagesForClusteringKey.set(i, new rangePair<Serializable, Serializable>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));
                            hPageFullStatus.put(i, pPage.isFull());
                            vNumberOfRowsPerPage.set(i, pPage.size());
                            pPage.serializePage();

                            // check if last page
                            if (pPage.index == iNumOfPages - 1) {
                                // insert in new page
                                iNumOfPages++;
                                Page pNewPage = new Page(sTableName, iNumOfPages - 1, false);
                                pNewPage.vRecords.add(htblLastRow);

                                // update minMax vector
                                oMinClusterVal = (Serializable) pNewPage.vRecords.get(0).get(sClusteringKey);
                                oMaxClusterVal = (Serializable) pNewPage.vRecords.get(pNewPage.size() - 1).get(sClusteringKey);
                                vecMinMaxOfPagesForClusteringKey.add(new rangePair<Serializable, Serializable>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));
                                hPageFullStatus.put(iNumOfPages - 1, pNewPage.isFull());
                                vNumberOfRowsPerPage.add(iNumOfPages - 1, pNewPage.size());
                                pNewPage.serializePage();
                                break;
                            }
                        } else { // page is not full
                            // add to page and break
                            pPage.sortedInsert(htblLastRow, sClusteringKey);

                            // update minMax vector of insertPage
                            Serializable oMinClusterVal = (Serializable) pPage.vRecords.get(0).get(sClusteringKey);
                            Serializable oMaxClusterVal = (Serializable) pPage.vRecords.get(pInsertPage.size()-1).get(sClusteringKey);
                            vecMinMaxOfPagesForClusteringKey.set(pPage.index, new rangePair<Serializable, Serializable>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));

                            // update page full status
                            hPageFullStatus.put(pPage.index, pPage.isFull());

                            // update number of rows in page
                            vNumberOfRowsPerPage.set(pPage.index, pPage.size());
                            pPage.serializePage();
                            break;
                        }
                    }
                }
            }
            // update minMax vector of insertPage
            Serializable oMinClusterVal = (Serializable) pInsertPage.vRecords.get(0).get(sClusteringKey);
            Serializable oMaxClusterVal = (Serializable) pInsertPage.vRecords.get(pInsertPage.size()-1).get(sClusteringKey);
            vecMinMaxOfPagesForClusteringKey.set(iInsertPageNum, new rangePair<Serializable, Serializable>((Serializable)oMinClusterVal, (Serializable)oMaxClusterVal));

            // update page full status
            hPageFullStatus.put(iInsertPageNum, pInsertPage.isFull());

            // update number of rows in page
            vNumberOfRowsPerPage.set(iInsertPageNum, pInsertPage.size());
            pInsertPage.serializePage();
        }
        iNumOfRows++;
    }

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) {

        //TODO
        //1- search for all relevant records based on conditions (DONE)
        //2- remove the records in descending order (akher index fe akher page le awel index fe awel page) (DONE)
        //3- update minMax
        //4- inter-vector shiftation
        //5- re-serialize and save pages

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
            Page pPageToLoad = new Page(strTableName, iPageToLoad, true);
            htblPagesTemp.put(iPageToLoad, pPageToLoad);

            //remove the record
            pPageToLoad.vRecords.remove(iRecordIndexInPage);

            //decrement number of rows in the page
            this.vNumberOfRowsPerPage.set(iPageToLoad, vNumberOfRowsPerPage.get(iPageToLoad) - 1);

            //update hPageFullStatus
            hPageFullStatus.put(iPageToLoad, false);
        }

        //inter-vector shiftation
        for (int k = vRelevantRecords.get(0).val1.val1; k < this.iNumOfPages; k++) {

            //check if page is not full
            //if it is not full, i know i should get values from next non-empty pages and insert
            if(!hPageFullStatus.get(k)) {
                for (int j = k + 1; j < this.iNumOfPages; j++) {

                    if (vNumberOfRowsPerPage.get(j) > 0) {
                        //we need to remove records from this page and put them in page k
                    }
                }
            }

        }
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
                            Hashtable<String, Object> hCurrRecord = pCurrentPage.vRecords.get(mid);
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
            return tTable;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return tTable;
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



}
