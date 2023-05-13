import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class DBApp {
    public void init() {

    }

    //edit al niggaz
    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {

        //create instance of Table class
        Table table = new Table(strTableName, strClusteringKeyColumn,
                htblColNameType, htblColNameMin,
                htblColNameMax);

        // save the table to hard disk
        table.serializeTable();
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        try {
            // Load Table.
            Table tTable = Table.loadTable(strTableName);

            // Get the old minMax before insertion
            Vector<Integer> oldPageRowCount = tTable.vNumberOfRowsPerPage;

            // Call getRelevantIndices built on the table of the inserted row. (Thank you Omar ^-^)
            Hashtable<String, Object> colsOfInterest = new Hashtable<>();
            // We do not care about the value of each column in the Hashtable, we only care that all columns of the table exists in the Hashtable.
            for (String key : tTable.cslsColNames) {
                colsOfInterest.put(key, new Null());
            }
            Hashtable<String, Object> builtInIndices = tTable.getRelevantIndices(colsOfInterest);
            Vector<OctTree> octTrees = new Vector<>();

            // Check that all columns that indices are built on contains a value in the inserted record (a.k.a Hashtable). Otherwise, throw a DBAppException.
            for (String key : builtInIndices.keySet()) {
                if (!key.equals("max")) {
                    // Load Octree index.
                    OctTree builtInOctree = OctTree.deserializeIndex(key);
                    octTrees.add(builtInOctree);
                    String[][] colNamesDatatypes = builtInOctree.colNamesDatatypes;
                    builtInOctree.serializeIndex();
                    if (!htblColNameValue.containsKey(colNamesDatatypes[0][0])) {
                        throw new DBAppException("To insert into the " + key + " index, The value of the column " + colNamesDatatypes[0][0] + " can not be null.");
                    }
                    if (!htblColNameValue.containsKey(colNamesDatatypes[1][0])) {
                        throw new DBAppException("To insert into the " + key + " index, The value of the column " + colNamesDatatypes[1][0] + " can not be null.");
                    }
                    if (!htblColNameValue.containsKey(colNamesDatatypes[2][0])) {
                        throw new DBAppException("To insert into the " + key + " index, The value of the column " + colNamesDatatypes[2][0] + " can not be null.");
                    }
                }
            }
            // 4. Insert the record into the Table.
            Vector<Hashtable<String,Object>> firstRecords = tTable.insertIntoTable(htblColNameValue);
            // 5. Search the table for the record to get the page number where the record is inserted in it.
            Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>> searchPageNumber = tTable.searchRecords(htblColNameValue);
            // 6. Serialize Table.
            tTable.serializeTable();

            Pair<Pair<Integer,Integer>,Hashtable<String,Object>> pair = searchPageNumber.firstElement();
            int pageNumber = pair.val1.val1;

            int pageMaxEntries = Table.loadMaxEntries();

            // 7. Call Octree insert and update shifted records.
            int j = 0;
            for (int i = pageNumber; i < tTable.iNumOfPages - 1; i++) {
                for (OctTree octTree : octTrees) {
                    if (pageNumber == i) {
                        octTree.insert(htblColNameValue, pageNumber, tTable.sClusteringKey);
                    }
                    if (oldPageRowCount.get(pageNumber) == pageMaxEntries) {
                        // update shifted records
                        Hashtable<String, Object> indexCols = new Hashtable<>();
                        for (int k = 0; k < octTree.colNamesDatatypes.length; k++) {
                            indexCols.put(octTree.colNamesDatatypes[k][0], firstRecords.get(j).get(octTree.colNamesDatatypes[k][0]));
                        }
                        octTree.updateShiftedRecords(indexCols, htblColNameValue.get(tTable.sClusteringKey), i + 1);
                        j++;
                    }
                }
                j = 0;
            }

            // 8. Serialize Octrees.
            for (OctTree octTree : octTrees) {
                octTree.serializeIndex();
            }

        } catch (Exception e) { // if table does not exist or some error happened
            throw new DBAppException(e);
        }
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // load table data from hard disk
        try {
            Table tTable = Table.loadTable(strTableName);
            tTable.updateTable(strClusteringKeyValue, htblColNameValue);
            tTable.serializeTable();
        } catch (Exception e) { // if table does not exist or some error happened
            throw new DBAppException(e);
        }
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        //call isValidForDeletion in try/catch block
        try {
            if (isValidForDeletion(strTableName, htblColNameValue)) {
                //load table
                Table tTable = Table.loadTable(strTableName);

                //delete from table
                tTable.deleteFromTable(strTableName, htblColNameValue);

                //serialize table
                tTable.serializeTable();
            }

        } catch (Exception e) {
            throw new DBAppException(e);
        }


    }

    public void validateDataTypeAndRange (SQLTerm term, String dataType, String min, String max) throws DBAppException {
        Class<?> classType = term._objValue.getClass();
        if (classType == Integer.class && dataType.equals("java.lang.Integer")) {
            Integer sqlValue = (Integer)term._objValue;
            Integer minimum = Integer.parseInt(min);
            Integer maximum = Integer.parseInt(max);
            if (sqlValue.compareTo(minimum) < 0 || sqlValue.compareTo(maximum) > 0) {
                throw new DBAppException("SQLTerm value " + term._objValue + " does not lie within the range of the column " + term._strColumnName);
            }
        }
        else if (classType == String.class && dataType.equals("java.lang.String")) {
            String sqlValue = (String)term._objValue;
            if (sqlValue.compareTo(min) < 0 || sqlValue.compareTo(max) > 0) {
                throw new DBAppException("SQLTerm value " + term._objValue + " does not lie within the range of the column " + term._strColumnName);
            }
        }
        else if (classType == Double.class && dataType.equals("java.lang.Double")) {
            Double sqlValue = (Double)term._objValue;
            Double minimum = Double.parseDouble(min);
            Double maximum = Double.parseDouble(max);
            if (sqlValue.compareTo(minimum) < 0 || sqlValue.compareTo(maximum) > 0) {
                throw new DBAppException("SQLTerm value " + term._objValue + " does not lie within the range of the column " + term._strColumnName);
            }
        }
        else if (classType == Date.class && dataType.equals("java.util.Date")) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
            boolean condition = false;
            try {
                Date minimum = dateFormat.parse(min);
                Date maximum = dateFormat.parse(max);
                condition = ((Date)term._objValue).compareTo(minimum) < 0 || ((Date)term._objValue).compareTo(maximum) > 0;
            }
            catch (Exception e) {
                throw new DBAppException("SQLTerm has invalid date format, Given: " + term._objValue + " ,Required Format: YYYY-MM-DD");
            }
            if (condition) {
                throw new DBAppException("SQLTerm value " + term._objValue + " does not lie within the range of the column " + term._strColumnName);
            }
        }
        else {
            throw new DBAppException("SQLTerm has invalid datatype");
        }
    }

    public void validateSQLTerms (SQLTerm[] arrSQLTerms) throws DBAppException {
        // 1. Check that 3 SQLTerms are on the same table.
        // Get the Table Name of each SQLTerm
        String tableName = arrSQLTerms[0]._strTableName;
        if (!tableName.equals(arrSQLTerms[1]._strTableName) || !tableName.equals(arrSQLTerms[2]._strTableName)) {
            throw new DBAppException("SQLTerms on multiple tables is not allowed.");
        }

        // 2. Check that the table name exists in the CSV file. (condition2)
        // 3. Check that the column exists in the table. (condition3)
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            String[] line;
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
            boolean condition2 = false;
            boolean condition3 = false;
            boolean condSQlTermColName1 = false;
            boolean condSQlTermColName2 = false;
            boolean condSQlTermColName3 = false;

            while ((line = reader.readNext()) != null) {
                if (line[0].equals(tableName)) { // Meet Second Check: Check that the table name exists in the CSV file.
                    condition2 = true;
                    // 4. Check that the data type of the SQLTerm is convenient to the column's data type.
                    // 5. Check that the value of SQLTerm satisfies the column range.
                    if (line[1].equals(arrSQLTerms[0]._strColumnName)) {
                        condSQlTermColName1 = true;
                        validateDataTypeAndRange(arrSQLTerms[0], line[2], line[6], line[7]);
                    }
                    else if (line[1].equals(arrSQLTerms[1]._strColumnName)) {
                        condSQlTermColName2 = true;
                        validateDataTypeAndRange(arrSQLTerms[1], line[2], line[6], line[7]);
                    }
                    else if (line[1].equals(arrSQLTerms[2]._strColumnName)) {
                        condSQlTermColName3 = true;
                        validateDataTypeAndRange(arrSQLTerms[1], line[2], line[6], line[7]);
                    }
                }
            }
          if (condSQlTermColName1 && condSQlTermColName2 && condSQlTermColName3) { // Meet Third Condition: Check that the column exists in the table.
                condition3 = true;
            }

          if (!condition2) {
              throw new DBAppException("Table " + tableName + " does not exist.");
          }

          if (!condition3) {
              if (!condSQlTermColName1) {
                  throw new DBAppException("Table " + tableName + " does not contain column " + arrSQLTerms[0]._strColumnName);
              }
              if (!condSQlTermColName2) {
                  throw new DBAppException("Table " + tableName + " does not contain column " + arrSQLTerms[1]._strColumnName);
              }
              if (!condSQlTermColName3) {
                  throw new DBAppException("Table " + tableName + " does not contain column " + arrSQLTerms[2]._strColumnName);
              }
          }
        }
        catch (Exception e) {
            throw new DBAppException(e);
        }
    }



    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        // check for correct number of operators
        if (arrSQLTerms.length - 1 != strarrOperators.length) {
            throw new DBAppException("Invalid number of operators");
        }

        validateSQLTerms(arrSQLTerms);

        try {
            // load the table from hard disk
            Table tTable = Table.loadTable(arrSQLTerms[0]._strTableName);

            // add all search results to the vector
            Vector<Hashtable<String, Object>> result = new Vector<>();
            result = tTable.selectFromTable(arrSQLTerms, strarrOperators);

            // return the vector as an iterator
            return result.iterator();

        } catch (Exception e) { // if table does not exist or some error happened
            throw new DBAppException(e);
        }
    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        // check if the number of columns is correct
        if (strarrColName.length != 3) {
            throw new DBAppException("Invalid number of columns. Must be 3, supplied: " + strarrColName.length);
        }

        // check on col names and table name
        boolean tableFound = false;
        boolean[] colsFound = new boolean[strarrColName.length];
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line[0].equals(strTableName)) {
                    tableFound = true;
                    int i = 0;
                    for (String colName: strarrColName) {
                        if (line[1].equals(colName)) {
                            colsFound[i] = true;
                        }
                        i++;
                    }
                } else if (tableFound) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        if (!tableFound) {
            throw new DBAppException("Table " + strTableName + " does not exist.");
        }

        if (!colsFound[0] || !colsFound[1] || !colsFound[2]) {
            throw new DBAppException("One or more columns do not exist in table " + strTableName);
        }

        // load the table from hard disk
        Table t = Table.loadTable(strTableName);

        // check if an index exists on one of the cols
        Hashtable<String, Object> htTest = new Hashtable<>();
        for (String colName: strarrColName) {
            htTest.put(colName, 0);
        }
        Hashtable<String, Object> res = t.getRelevantIndices(htTest);
        if (res.size() != 0) {
            throw new DBAppException("Index already exists on one or more columns, " + res.keySet());
        }

        // Create index name
        boolean onPrimaryKey = false;
        String indexName = "";
        for (String col: strarrColName) {
            if (col.equals(t.sClusteringKey)) {
                onPrimaryKey = true;
            }
            indexName += col;
        }

        if (onPrimaryKey) {
             indexName = "pk_" + indexName;
        }
        OctTree octTree = new OctTree(indexName, strarrColName[0], strarrColName[1], strarrColName[2], strTableName, onPrimaryKey);

        // insert all records into the octree
        for (int i = 0; i < t.iNumOfPages; i++) {
            Page p = new Page(strTableName, t.sClusteringKey, i, true);
            for (Hashtable<String, Object> record: p.vRecords) {
                for (String col: strarrColName) {
                    if (record.get(col).equals(Null.getInstance())) {
                        throw new DBAppException("Cannot create index on null values");
                    }
                }
                octTree.insert(record, i, t.sClusteringKey);
            }
            p = null;
            System.gc();
        }

        // save the index on disk
        octTree.serializeIndex();

        // update the metadata file
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            List<String[]> lines = reader.readAll();
            reader.close();
            CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/metadata.csv"));
            for (String[] line: lines) {
                if (line[0].equals(strTableName)) {
                    for (String colName : strarrColName) {
                        if (line[1].equals(colName)) {
                            line[4] = indexName;
                            line[5] = "OcTree";
                        }
                    }
                }
                writer.writeNext(line);
            }
            writer.close();
        } catch (Exception e) {
            throw new DBAppException(e);
        }

        t = null;
        octTree = null;
        System.gc();

    }

    //bonus
    public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {

        // https://theantlrguy.atlassian.net/wiki/spaces/ANTLR3/pages/2687210/Quick+Starter+on+Parser+Grammars+-+No+Past+Experience+Required
        return null;
    }


    public static void main(String[] args) throws DBAppException {

        //testing Table class creation
        String strTableName = "Student";
        DBApp dbApp = new DBApp();

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");

        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "1000000000");
        htblColNameMax.put("name", "ZZZZZZZZZZZ");
        htblColNameMax.put("gpa", "4.0");


        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

        // count time
        long startTime = System.currentTimeMillis();
        for (int i = 500; i >= 1; i--) {
            int finalI = i;
            dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
                put("id", finalI);
                put("name", (finalI >= 100 && finalI <= 200) ? "n1" : (finalI >= 300 && finalI <= 400) ? "n2" : "n3");
                put("gpa", 0.9);
            }});
            System.out.println("inserted " + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Took " + ((endTime - startTime)/1000) + " seconds");

        dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
                put("name", "n1");
            }});

        dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
            put("name", "n2");
        }});

//        for (int i = 200; i >= 100; i--) {
//            int finalI = i;
//            dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
//                put("id", finalI);
//                put("name", "n" + finalI);
//                put("gpa", 0.9);
//            }});
//            System.out.println("deleted " + i);
//        }

        //Table t = Table.loadTable("Student");
        //t.showPage(0);

//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 2);
//            put("name", "AAAAA");
////            put("gpa", 0.34);
//        }});
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 3);
//            put("name", "AAAAA");
////            put("gpa", 0.34);
//        }});

//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 5);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});
//
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 4);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});
//
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 6);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});
//
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 12);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});
//
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 13);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});
//
//        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 0);
//            put("name", "AAAAA");
//            put("gpa", 0.34);
//        }});


        Table t = Table.loadTable("Student");
        t.showPage(0);

//        dbApp.updateTable(strTableName, "2", new Hashtable<String, Object>() {{
//            put("gpa", 0.01);
//        }});
//
//        dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
//            put("id", 2);
//        }});
//        dbApp.updateTable(strTableName, "12", new Hashtable<String, Object>() {{
//            put("gpa", 3.0);
//            put("name","AAAA");
//        }});
//        dbApp.updateTable(strTableName, "5", new Hashtable<String, Object>() {{
//            put("name","BBBB");
//        }});
//        dbApp.updateTable(strTableName, "0", new Hashtable<String, Object>() {{
//            put("name","CCCC");
//            put("gpa", 2.0);
//        }});

        t.showPage(0);
        //t.deleteTable();
        //clearCSV();

    }

    public static boolean isValidForDeletion(String strTableName, Hashtable<String, Object> htblColNameValue) throws  DBAppException {

        //declaring csv reader
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));

            Hashtable<String,String> htblColNameTypeTemp = new Hashtable<String,String>();

            //check if table exists
            String[] line;
            boolean found = false;
            while ((line = reader.readNext()) != null) {
                if (line[0].equals(strTableName)) {
                    htblColNameTypeTemp.put(line[1], line[2]);
                }
            }

            if (htblColNameTypeTemp.isEmpty()) {
                throw new DBAppException("Table does not exist");
            }

            //check that column names and types are valid
            for (String key : htblColNameValue.keySet()) {

                if (htblColNameTypeTemp.containsKey(key)) {
                    found = true;

                    //check that column type is valid
                    //since the column exists in metadata file we are sure the column type should
                    //be one of the following: java.lang.Integer, java.lang.String, java.lang.Double
                    if (!htblColNameTypeTemp.get(key).equals(htblColNameValue.get(key).getClass().getName())) {
                        throw new DBAppException("Invalid column type for deletion");
                    }

                } else {
                    throw new DBAppException("Invalid column name for deletion");
                }
            }
            return true;

        } catch (Exception e) {
            throw new DBAppException(e);
        }
    }

    public static boolean isExistingTable(String strTableName) throws DBAppException {
        //declaring csv reader
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line[0].equals(strTableName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new DBAppException(e);
        }
    }

    /*
     * remove all the data in the CSV file
     */
    public static void clearCSV() throws DBAppException {
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            List<String[]> allElements = reader.readAll();
            List<String[]> removeElements = new LinkedList<>();
            int size = allElements.size();

            for (int i = 1; i < size; i++) {
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


    }
}
