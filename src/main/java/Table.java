import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
                 Hashtable<String,String> htblColNameMax ) throws DBAppException {

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

        //check if same col names in all htbls
        ConcurrentSkipListSet<String> cols = new ConcurrentSkipListSet<>();
        cols.addAll(htblColNameType.keySet());
        cols.addAll(htblColNameMax.keySet());
        cols.addAll(htblColNameMin.keySet());

        if (cols.size() != htblColNameType.size()) {
            throw new DBAppException("Column names are not consistent");
        }

        //making sure column data types are valid
        //the valid datatypes are: java.lang.Integer , java.lang.String ,
        //java.lang.Double , java.util.Date (Note: date acceptable format is "YYYY-MM-DD")
        if (!isColDataTypeValid(htblColNameType)) {
            throw new DBAppException("Invalid column data type");
        }



        //declaring the metadata file and setting it to append mode
        try {
            CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/metadata.csv", true));
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
                if (castAndCompare(metadata[6], metadata[7]) > 0) {
                    throw new DBAppException("Minimum value of column " + col + " is greater than its maximum value");
                }
                writer.writeNext(metadata);
            }
            writer.close();
        } catch (Exception e) {
            throw new DBAppException(e);
        }

    }

    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws DBAppException {
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

        // insert nulls in the missing columns (Using a custom Null class as nulls are not allowed in the hashtable)
        for (String col: cslsColNames) {
            if (!htblColNameValue.containsKey(col)) {
                htblColNameValue.put(col, Null.getInstance());
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

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {


        int deletedRecords = 0;
        boolean hasIndex = false;
        //HashSet<String> indexCols = new HashSet<String>(); //store names of cols with an index
        Hashtable<String, Object> colNames = getColNames(strTableName); //get col names of table
        String bestIndex = ""; //name of the best index to use

        Hashtable<String, Object> relIndices = getRelevantIndices(htblColNameValue);
        Hashtable<String, Object> allIndices = getRelevantIndices(colNames);
        if (!relIndices.isEmpty()) { //there are useful indices
            hasIndex = true;
            bestIndex = (String) relIndices.get("max");

            //find col names of that index
            try {
                CSVReader reader = new CSVReader(new FileReader("data/metadata.csv"));

                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    if (nextLine[0].equals(strTableName) && nextLine[4].equals(bestIndex)) {
                        //indexCols.add(nextLine[1]);
                        break;
                    }
                }

            } catch(Exception e) {
                throw new DBAppException(e);
            }
        }
        //String[] indexColsArr = indexCols.toArray(new String[indexCols.size()]);



        //load the index if it exists
        OctTree index = (hasIndex)?OctTree.deserializeIndex(bestIndex):null;

        //delete table if the input is empty
        if (htblColNameValue.isEmpty()) {
            //clear the table (delete all records and pages but do not delete table
            for (int i = 0; i < iNumOfPages; i++) {
                Page pPage = new Page(strTableName, sClusteringKey, i, true);
                pPage.deletePage();
            }
            if (hasIndex) {
                //call deleteAll() method that deletes all points from the index
                index.deleteAll();

                //call deleteAll() from other indices in relIndices
                for (String indexName : allIndices.keySet()) {
                    if (!indexName.equals(bestIndex)) {
                        OctTree indexToClear = OctTree.deserializeIndex(indexName);
                        indexToClear.deleteAll();
                    }
                }
            }

            //deleteTable();
            //output to user
            System.out.println("Table " + strTableName + " cleared successfully");
            return;
        }

        //search for all relevant data given the conditions
        //first integer is pageNumber
        //second integer is recordIndex
        Vector<Pair<Pair<Integer, Integer>, Hashtable<String, Object>>> vRelevantRecords = new Vector<Pair<Pair<Integer, Integer>, Hashtable<String, Object>>>();

        if (hasIndex) {
            Vector<OctTreeNode> nodesToCheck = index.root.searchExactQueries(htblColNameValue);
            //TODO: populate the vRelevantRecords vector

        } else {
            vRelevantRecords = searchRecords(htblColNameValue);
        }


        if (vRelevantRecords.isEmpty()) {
            //output to user
            System.out.println("Deleted 0 records from table " + strTableName);
            return;
        }


        int oldPageNum = -1;
        Page pPageToLoad = null;
        //remove the records in descending order
        for (int i = vRelevantRecords.size() - 1; i >= 0; i--) {

            //find pageNumber and recordIndexInPage
            int iPageToLoad = vRelevantRecords.get(i).val1.val1;
            int iRecordIndexInPage = vRelevantRecords.get(i).val1.val2;

            if (oldPageNum == -1) { //this is the first iteration

                oldPageNum = iPageToLoad;

                //load the page
                pPageToLoad = new Page(sTableName, sClusteringKey, iPageToLoad, true);

            } else if (oldPageNum != iPageToLoad) {
                pPageToLoad.serializePage();

                //load the new page
                pPageToLoad = new Page(sTableName, sClusteringKey, iPageToLoad, true);
                oldPageNum = iPageToLoad;

            }


            //remove the record
            pPageToLoad.vRecords.remove(iRecordIndexInPage);
            deletedRecords++;

            //TODO: load octtrees and call delete on all of them to delete that point


            //remove primary key
            cslsClusterValues.remove(vRelevantRecords.get(i).val2.get(sClusteringKey));

            //decrement number of rows in the page
            this.vNumberOfRowsPerPage.set(iPageToLoad, vNumberOfRowsPerPage.get(iPageToLoad) - 1);
            iNumOfRows--;

            //update page meta (in case page is still not empty)
            if (!pPageToLoad.vRecords.isEmpty())
                updatePageMeta(pPageToLoad);
        }
        pPageToLoad.serializePage();

        //output to user that the records have been deleted
        System.out.println(deletedRecords + " record(s) from "+this.sTableName+ " table deleted successfully");

        int minIndex = Integer.MAX_VALUE;
        Vector<Integer> pagesToRename = new Vector<Integer>();

        //if page is empty, delete page

        //the j keeps track of the page I am currently on (the old values)
        //while the i makes sure I do not get out of bounds while traversing the vector
        for (int i = 0, j = 0; i < vNumberOfRowsPerPage.size(); i++,j++) {
            if (vNumberOfRowsPerPage.get(i) == 0) {
                //loading the page object and then getting rid of it
                Page p = new Page(strTableName, sClusteringKey, i, true);
                p.deletePage();
                vNumberOfRowsPerPage.remove(i);
                vecMinMaxOfPagesForClusteringKey.remove(i);
                hPageFullStatus.remove(i);
                iNumOfPages--;

                if (j < minIndex)
                    minIndex = j;

                i--;
            } else {
                pagesToRename.add(j);
            }
        }

        //rename pages after deletion
        int newIndex = minIndex;
        for (int i = 0; i < pagesToRename.size(); i++) {
            int oldIndex = pagesToRename.get(i);
            if (oldIndex > minIndex){
                File f2 = new File("src/main/resources/data/"+strTableName+"/"+sTableName+"_page"+oldIndex+".class");
                f2.renameTo(new File("src/main/resources/data/"+strTableName+"/"+sTableName+"_page"+newIndex+".class"));
                //TODO: update reference number in index (find any point with reference as the old page and change it to the new page)
//                Page pToBeRenamed = new Page(strTableName, sClusteringKey, oldIndex, true);
//                pToBeRenamed.index = newIndex;
                newIndex++;
            }

        }

    }

    public void updateTable(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (Exception e) {
            throw new DBAppException(e);
        }


        String[] line;
        SimpleDateFormat formatter;
        try {
            formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        Hashtable<String, Object> hCondition = new Hashtable<String, Object>();


        // Check that there is no update of the Clustering key.
        if (htblColNameValue.containsKey(sClusteringKey)) {
            throw new DBAppException("Can not update the clustering key: " + sClusteringKey);
        }

        // Check if all columns in input are valid
        for (String col : htblColNameValue.keySet()) {
            if (!cslsColNames.contains(col)) {
                throw new DBAppException("Column " + col + " does not exist");
            }
        }

        try {
            while ((line = reader.readNext()) != null) {
                // Process each line of the CSV file
                // Note: Did not use the method (checkValidityOfData) as the update query does not need to update every column in the table. Hence, I added the condition (htblColNameValue.get(line[1]) != null)
                if (line[0].equals(sTableName) && htblColNameValue.get(line[1]) != null) {

                    // Check for data type
                    if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                        throw new DBAppException("Invalid data type for column " + line[1]);
                    }

                    // Check for min and max
                    if (castAndCompare(htblColNameValue.get(line[1]), line[6]) < 0
                            || castAndCompare(htblColNameValue.get(line[1]), line[7]) > 0) {
                        throw new DBAppException("Value for column " + line[1] + " is out of range");
                    }

                    // Check if date in input is in the correct format "YYYY-MM-DD"
                    if (line[2].equals("java.util.Date")) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String dateString = sdf.format(htblColNameValue.get(line[1]));
                        String[] date = dateString.split("-");
                        //System.out.println(date);
                        if (date.length != 3) {
                            throw new DBAppException("Invalid date format for column " + line[1]);
                        }
                        if (date[0].length() != 4 || date[1].length() != 2 || date[2].length() != 2) {
                            throw new DBAppException("Invalid date format for column " + line[1]);
                        }
                    }
                }

                // Insert the Clustering key inside the htblColNameValue to help me do the search later.
                // Note: I already checked that the Hash table (htblColNameValue) does not contain a key for the Clustering key.
                if (line[0].equals(sTableName) && line[1].equals(sClusteringKey) && Boolean.parseBoolean(line[3])) {
                    if (line[2].equals("java.util.Date")) {
                        Date date = formatter.parse(strClusteringKeyValue);
                        hCondition.put(sClusteringKey, date);
                    }

                    if (line[2].equals("java.lang.Integer")) {
                        Integer i = Integer.parseInt(strClusteringKeyValue);
                        hCondition.put(sClusteringKey, i);
                    }

                    if (line[2].equals("java.lang.String")) {
                        String s = strClusteringKeyValue;
//                        if (!cslsClusterValues.contains(s)) {
//                            throw new DBAppException("Can not update at the clustering key: " + sClusteringKey);
//                        }
                        hCondition.put(sClusteringKey, s);
                    }

                    if (line[2].equals("java.lang.Double")) {
                        Double d = Double.parseDouble(strClusteringKeyValue);
                        hCondition.put(sClusteringKey, d);
                    }
                }
            }
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        // check for relevant indices
        boolean applicableSearchIndex = false;
        String applicableIndexName = "";
        // load all relevant indices from metadata
        Hashtable<String, Object> indices = getRelevantIndices(htblColNameValue);
        for (String indexName : indices.keySet()) {
            // check if the index is applicable for search (contains the clustering key)
            if (indexName.substring(0,3).equals("pk_")) {
                applicableSearchIndex = true;
                applicableIndexName = indexName;
                break;
            }
        }

        // check for an index with the pk
        if (applicableSearchIndex) {
            // load the index
            OctTree index = OctTree.deserializeIndex(applicableIndexName);
            int pageNum = index.getPageByPkAndUpdate(sClusteringKey, ((Comparable) htblColNameValue.get(sClusteringKey)), htblColNameValue);
            if (pageNum == -1) {
                return; // pk not found
            }
            // update using index
            Page targetPage = new Page(sTableName, sClusteringKey, pageNum, true);

            // binary search on the page
            int lo = 0;
            int hi = targetPage.size() - 1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                if (((Comparable) targetPage.vRecords.get(mid).get(sClusteringKey)).compareTo(htblColNameValue.get(sClusteringKey)) < 0) {
                    lo = mid + 1;
                } else if (((Comparable) targetPage.vRecords.get(mid).get(sClusteringKey)).compareTo(htblColNameValue.get(sClusteringKey)) > 0) {
                    hi = mid - 1;
                } else { // found the record
                    Hashtable<String, Object> hTemp = targetPage.vRecords.get(mid);

                    // Step 3: Update the record
                    for (String key : htblColNameValue.keySet()) {
                        hTemp.put(key, htblColNameValue.get(key));
                    }

                    // Step 4: Serialize the page
                    targetPage.serializePage();
                }
            }
            index.serializeIndex();

        } else {

            // Step 1: Search for the page that contain the Clustering key value.
            Vector<Pair<Pair<Integer, Integer>, Hashtable<String, Object>>> v = this.searchRecords(hCondition);

            // If the user tried to update a record with invalid primary key
            //System.out.println(v.size());
            if (v.isEmpty()) {
//            throw new DBAppException("Primary Key does not exists");
                return;
            }

            for (Pair<Pair<Integer, Integer>, Hashtable<String, Object>> pair : v) {
                int pageNum = pair.val1.val1;
                int recordIndex = pair.val1.val2;

                // Step 2: Deserialize the page at index [pageNum]
                Page targetPage = new Page(sTableName, sClusteringKey, pageNum, true);
                Hashtable<String, Object> hTemp = targetPage.vRecords.get(recordIndex);

                // Step 3: Update the record
                for (String key : htblColNameValue.keySet()) {
                    hTemp.put(key, htblColNameValue.get(key));
                }

                // Step 4: Serialize the page
                targetPage.serializePage();
            }

        }
        // update all indices
        for (String indexName : indices.keySet()) {
            // check if the index is applicable for search (contains the clustering key)
            if (!indexName.substring(0,3).equals("pk_") && !indexName.equals("max")) {
                OctTree index = OctTree.deserializeIndex(indexName);
                index.updateIndex(sClusteringKey, ((Comparable) htblColNameValue.get(sClusteringKey)), htblColNameValue);
                index.serializeIndex();
            }
        }

    }

    public void searchInTable() {
        // use searchRecords instead?
    }

    // gets all the indices that index any of the input cols, also returns the index with highest colCount
    public Hashtable<String, Object> getRelevantIndices(Hashtable<String, Object> colsOfInterest) throws DBAppException {
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            boolean found = false;
            Hashtable<String, Object> result = new Hashtable<>();
            int max = 0;
            String maxIndex = "";
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line[0].equals(sTableName)) {
                    found = true;
                    for (String col: colsOfInterest.keySet()) {
                        if (line[1].equals(col)) {
                            if (!line[4].equals("null")) {
                                if (result.containsKey(line[4])) {
                                    int newCount = (int) result.get(line[4]) + 1;
                                    if (newCount > max) {
                                        max = newCount;
                                        maxIndex = line[4];
                                    }
                                    result.put(line[4], newCount);
                                } else {
                                    result.put(line[4], 1);
                                }
                            }
                        }
                    }
                } else if (found) {
                    break;
                }
            }
            if (result.size() > 0) {
                result.put("max", maxIndex);
            }
            return result;
        } catch (Exception e) {
            throw new DBAppException(e);
        }
    }

    // Note: this method is not used in the project, not required
    public void deleteTable() throws DBAppException {
        File myObj = new File("src/main/resources/data/"+this.sTableName+"/"+this.sTableName+".class");
        myObj.delete();
        for (int i = 0; i < iNumOfPages; i++) {
            Page tmpPage = new Page (sTableName, sClusteringKey, i, false);
            tmpPage.deletePage();
        }
        myObj = new File("src/main/resources/data/"+this.sTableName);
        myObj.delete();

        // delete from metadata the rows of this table
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            List<String[]> allElements = reader.readAll();
            List<String[]> removeElements = new LinkedList<>();
            int size = allElements.size();

            for (int i = 1; i < size; i++) {
                if (allElements.get(i)[0].equals(sTableName))
                    removeElements.add(allElements.get(i));
            }
            allElements.removeAll(removeElements);

            FileWriter sw = new FileWriter("src/main/resources/metadata.csv");
            CSVWriter writer = new CSVWriter(sw);
            writer.writeAll(allElements);
            writer.close();
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        System.gc();


    }



    public void serializeTable() throws DBAppException{
        try {
            File folder = new File("src/main/resources/data/"+this.sTableName);

            if (!folder.exists()) {
                boolean created = folder.mkdir();
                if (!created) {
                    throw new DBAppException("Could not create directory");
                }
            }
            FileOutputStream fos = new FileOutputStream("src/main/resources/data/"+this.sTableName+"/"+sTableName+".class");
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
        if (iNumOfPages == 0) {
            return result;
        }

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
                int mid = (lo + hi) / 2; //Mazen: better to do it as : mid = (lo + hi - lo)/2
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

    public Vector<Hashtable<String, Object>> selectFromTable(SQLTerm[] queries, String[] operations) throws DBAppException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        // TODO search for compatible index
        if (true) { // if a compatible one found
            // load from disk

            // search for
        } else { // load all pages and perform operation
            for (int i = 0; i < iNumOfPages; i++) {
                // load (de-serialize the page)
                Page pCurrentPage = new Page(sTableName, sClusteringKey, i, true);

                int index = 0;
                // for each record in page perform boolean operation vector
                Vector<Boolean> operationVector = new Vector<>();
                for (Hashtable<String, Object> ht : pCurrentPage.vRecords) {
                    // get the boolean value of each query
                    for (SQLTerm query : queries) {
                        operationVector.add(operate(query._strOperator, ht.get(query._strColumnName), query._objValue));
                    }

                    // evaluate the boolean vector from left to right
                    for (String operator: operations) {
                        boolean b1 = operationVector.remove(0);
                        boolean b2 = operationVector.remove(0);
                        switch (operator.toUpperCase()) {
                            case "AND":
                                operationVector.add(0, b1 && b2);
                                break;
                            case "OR":
                                operationVector.add(0, b1 || b2);
                                break;
                            case "XOR":
                                operationVector.add(0, b1 ^ b2);
                                break;
                        }
                    }

                    if (operationVector.get(0)) { // sql expression evaluated to true with current row
                        result.add(ht);
                    }
                }

                pCurrentPage = null;
                System.gc();
            }
        }
        return result;
    }

    public boolean operate(String operation, Object o1, Object o2) throws DBAppException {
        switch(operation) {
            case "=":
                return ((Comparable)o1).compareTo(o2) == 0;
            case ">":
                return ((Comparable)o1).compareTo(o2) > 0;
            case ">=":
                return ((Comparable)o1).compareTo(o2) >= 0;
            case "<":
                return ((Comparable)o1).compareTo(o2) < 0;
            case "<=":
                return ((Comparable)o1).compareTo(o2) <= 0;
            case "!=":
                return ((Comparable)o1).compareTo(o2) != 0;
            default: throw new DBAppException("Invalid operator: " + operation);
        }
    }

    public static Table loadTable (String sTableName) throws  DBAppException {
        Table tTable = null;
        try {
            FileInputStream fis = new FileInputStream("src/main/resources/data/"+sTableName+"/"+sTableName+".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            tTable = (Table) ois.readObject();
            ois.close();
            fis.close();
            return tTable;
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException(e);
        }
    }

    public static int castAndCompare(Object input, String csv) throws DBAppException {
        Class<?> clazz = input.getClass();
        int result = 0;
        if (clazz == Double.class) {
            Double d = Double.parseDouble(csv);
            result = ((Double) input).compareTo(d);
        } else if (clazz == Integer.class) {
            Integer d = Integer.parseInt(csv);
            result = ((Integer) input).compareTo(d);
        } else if (clazz == Date.class) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date d = null;
            try {
                d = sdf.parse(csv);
            } catch (ParseException e) {
                throw new DBAppException(e);
            }
            result = ((Date) input).compareTo(d);
        } else if (clazz == String.class) {
            String input_string = (String) input;
            result = input_string.toLowerCase().compareTo(csv.toLowerCase());
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
        if (vNumberOfRowsPerPage.size() > p.index) { // if the page index already exists in the vNumberOfRowsPerPage then we only need to update its value
            vNumberOfRowsPerPage.set(p.index, p.size());
        }
        else {
            vNumberOfRowsPerPage.add(p.index, p.size());
        }
    }

    public void checkValidityOfData(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (Exception e) {
            throw new DBAppException(e);
        }

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

        // check if primary key is null
        if (htblColNameValue.get(sClusteringKey) == null) {
            throw new DBAppException("Primary key " + sClusteringKey + " cannot be null");
        }

        boolean found = false;
        try {
            while ((line = reader.readNext()) != null) {
                // Process each line of the CSV file

                if (line[0].equals(sTableName)) {
                    found = true;

                    // check if column is in input
                    if (!htblColNameValue.containsKey(line[1])) {
                        continue;
                    }

                    if (htblColNameValue.get(line[1]) == null) { // ignore checks for null values
                        continue;
                    }

                    // check for data type
                    if (!htblColNameValue.get(line[1]).getClass().getName().equals(line[2])) {
                        throw new DBAppException("Invalid data type for column " + line[1]);
                    }

                    // check for min and max
                    if (castAndCompare(htblColNameValue.get(line[1]), line[6]) < 0
                            || castAndCompare(htblColNameValue.get(line[1]), line[7]) > 0) {
                        throw new DBAppException("Value for column " + line[1] + " is out of range");
                    }

                    // check if date in input is in the correct format "YYYY-MM-DD"
                    if (line[2].equals("java.util.Date")) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String dateString = sdf.format(htblColNameValue.get(line[1]));
                        String[] date = dateString.split("-");
                        //System.out.println(date);
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
        } catch (Exception e) {
            throw new DBAppException(e);
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

    public void showPage(int iPageNum) throws DBAppException {
        Page p = new Page(sTableName, sClusteringKey, iPageNum, true);
        System.out.println("Page " + iPageNum + " has " + p.size() + " records ======");
        int i = 1;
        for (Hashtable<String, Object> ht : p.vRecords) {
            System.out.println("" + i + "- " + ht);
            i++;
        }
        System.out.println("====================================");
    }

    // Before running the main method, insert the following line the CSV file.
    // "Student", "","","","","","",""
    public static void main (String args[]) {
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            boolean found = false;
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line[0].equals("Student")) {
                    if (line[4] == null) {
                        System.out.println(line); //  No printing will occurs but if change it to line[4].equals("") a print will occur.
                    }
                }
            }
        }
        catch (Exception e) {

        }
    }

    public static Hashtable<String,Object> getColNames(String sTableName) throws DBAppException {
        Hashtable<String,Object> htblColNames = new Hashtable<>();
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        String[] line;
        boolean found = false;
        try {
            while ((line = reader.readNext()) != null) {
                // Process each line of the CSV file

                if (line[0].equals(sTableName)) {
                    found = true;
                    htblColNames.put(line[1], line[2]);
                } else if (found) {
                    break; // no need to continue searching
                }

            }
        } catch (Exception e) {
            throw new DBAppException(e);
        }
        return htblColNames;
    }
}
