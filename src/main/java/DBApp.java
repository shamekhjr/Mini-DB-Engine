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

            //System.out.println("builtInIndices: " + builtInIndices);
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
            for (int i = pageNumber; i <= tTable.iNumOfPages - 1; i++) {
                for (OctTree octTree : octTrees) {
                    if (pageNumber == i) {
                        //System.out.println("inserting into page " + i);
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
        for (int i = 1; i < arrSQLTerms.length; i++) {
            if (!tableName.equals(arrSQLTerms[i]._strTableName)) {
                throw new DBAppException("SQLTerms on multiple tables is not allowed.");
            }
        }

        // 2. Check that the table name exists in the CSV file. (condition2)
        // 3. Check that the column exists in the table. (condition3)
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
            String[] line;
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
            boolean condition = false;
            boolean[] found = new boolean[arrSQLTerms.length];
            for (int i = 0; i < arrSQLTerms.length; i++) {
                found[i] = false;
            }

            while ((line = reader.readNext()) != null) {
                if (line[0].equals(tableName)) { // Meet Second Check: Check that the table name exists in the CSV file.
                    condition = true;
                    // 4. Check that the data type of the SQLTerm is convenient to the column's data type.
                    // 5. Check that the value of SQLTerm satisfies the column range.
                    for (int i = 0; i <arrSQLTerms.length; i++) {
                        if (line[1].equals(arrSQLTerms[i]._strColumnName)) {
                            found[i] = true;
                            validateDataTypeAndRange(arrSQLTerms[i], line[2], line[6], line[7]);
                        }
                    }
                }
            }

          if (!condition) {
              throw new DBAppException("Table " + tableName + " does not exist.");
          }

          for (int i = 0; i < found.length; i++) {
              if (!found[i]) {
                  throw new DBAppException("Table " + tableName + " does not contain column " + arrSQLTerms[i]._strColumnName);
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
                //System.out.println("Inserted record " + record + " into octree");
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


    public static void main(String[] args) throws Exception {
        /*
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        Hashtable htblColNameType = new Hashtable( );
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
        dbApp.createIndex( strTableName, new String[] {"gpa", "id", "name"} );
    Hashtable htblColNameValue = new Hashtable( );
    htblColNameValue.put("id", new Integer( 2343432 ));
    htblColNameValue.put("name", new String("Ahmed Noor" ) );
    htblColNameValue.put("gpa", new Double( 0.95 ) );
    dbApp.insertIntoTable( strTableName , htblColNameValue );
    htblColNameValue.clear( );
    htblColNameValue.put("id", new Integer( 453455 ));
    htblColNameValue.put("name", new String("Ahmed Noor" ) );
    htblColNameValue.put("gpa", new Double( 0.95 ) );
    dbApp.insertIntoTable( strTableName , htblColNameValue );
    htblColNameValue.clear( );
    htblColNameValue.put("id", new Integer( 5674567 ));
    htblColNameValue.put("name", new String("Dalia Noor" ) );
    htblColNameValue.put("gpa", new Double( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
    htblColNameValue.clear( );
    htblColNameValue.put("id", new Integer( 23498 ));
    htblColNameValue.put("name", new String("John Noor" ) );
    htblColNameValue.put("gpa", new Double( 1.5 ) );
    dbApp.insertIntoTable( strTableName , htblColNameValue );
    htblColNameValue.clear( );
    htblColNameValue.put("id", new Integer( 78452 ));
    htblColNameValue.put("name", new String("Zaky Noor" ) );
    htblColNameValue.put("gpa", new Double( 0.88 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        htblColNameValue.clear( );
        htblColNameValue.put("id", new Integer( 23143 ));
        htblColNameValue.put("name", new String("Lmao Noor" ) );
        htblColNameValue.put("gpa", new Double( 0.83 ) );
    dbApp.insertIntoTable( strTableName , htblColNameValue );
        htblColNameValue.clear( );
        htblColNameValue.put("id", new Integer( 23498 ));
    dbApp.deleteFromTable( strTableName , htblColNameValue );
        htblColNameValue.clear( );
        htblColNameValue.put("id", new Integer( 78452 ));
        htblColNameType.clear();
        htblColNameType.put("name", new String("Omar Nour" ));

        dbApp.updateTable( strTableName , "453455", htblColNameType );

    SQLTerm[] arrSQLTerms;
    arrSQLTerms = new SQLTerm[2];
    for (int i = 0; i < arrSQLTerms.length; i++) {
        arrSQLTerms[i] = new SQLTerm();
    }
    arrSQLTerms[0]._strTableName = "Student";
    arrSQLTerms[0]._strColumnName= "name";
    arrSQLTerms[0]._strOperator = "=";
    arrSQLTerms[0]._objValue = "Ahmed Noor";
    arrSQLTerms[1]._strTableName = "Student";
    arrSQLTerms[1]._strColumnName= "gpa";
    arrSQLTerms[1]._strOperator = "=";
    arrSQLTerms[1]._objValue = new Double( 1.5 );
    String[]strarrOperators = new String[1];
    strarrOperators[0] = "OR";
    Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
        for (Iterator it = resultSet; it.hasNext(); ) {
            Hashtable<String, Object> ht = (Hashtable<String, Object>) it.next();
            System.out.println("out -> "+ ht);
        }
            //testing Table class creation
//        String strTableName = "Student";
//        DBApp dbApp = new DBApp();
//
//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//
//        Hashtable htblColNameMin = new Hashtable();
//        htblColNameMin.put("id", "0");
//        htblColNameMin.put("name", "A");
//        htblColNameMin.put("gpa", "0.0");
//
//        Hashtable htblColNameMax = new Hashtable();
//        htblColNameMax.put("id", "1000000000");
//        htblColNameMax.put("name", "ZZZZZZZZZZZ");
//        htblColNameMax.put("gpa", "4.0");
//
//
//        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
//
//        // count time
//        long startTime = System.currentTimeMillis();
//        for (int i = 500; i >= 1; i--) {
//            int finalI = i;
//            dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
//                put("id", finalI);
//                put("name", (finalI >= 100 && finalI <= 200) ? "n1" : (finalI >= 300 && finalI <= 400) ? "n2" : "n3");
//                put("gpa", 0.9);
//            }});
//            System.out.println("inserted " + i);
//        }
//        long endTime = System.currentTimeMillis();
//        System.out.println("Took " + ((endTime - startTime)/1000) + " seconds");
//
//        dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
//                put("name", "n1");
//            }});
//
//        dbApp.deleteFromTable(strTableName, new Hashtable<String, Object>() {{
//            put("name", "n2");
//        }});

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

            //t.showPage(0);
            //t.deleteTable();
            //clearCSV();

         */
        DBApp db = new DBApp();
//        Final_Project dbApp = new Final_Project();
//
//        dbApp.createCoursesTable(db);
//        dbApp.createPCsTable(db);
//        dbApp.createTranscriptsTable(db);
//        dbApp.createStudentTable(db);
//        dbApp.insertPCsRecords(db,200);
//        dbApp.insertTranscriptsRecords(db,200);
//        dbApp.insertStudentRecords(db,200);
//        dbApp.insertCoursesRecords(db,200);
//        String table = "students";
       //Hashtable<String, Object> row = new Hashtable();
//        row.put("id", 123);
//
//        row.put("first_name", "foo");
//        row.put("last_name", "bar");
//
//        Date dob = new Date(1995 - 1900, 4 - 1, 1);
//        row.put("dob", dob);
//        row.put("gpa", 1.1);
//        String table = "transcripts";
//        Hashtable<String, Object> row = new Hashtable();
//        row.put("gpa", 1.5);
//        row.put("student_id", "44-9874");
        //row.put("course_name", "bar");
        //row.put("elective", true);

        //db.insertIntoTable(table, row);
        Hashtable<String, Object> row = new Hashtable();
        String table = "students";

        //row.put("first_name", "noura");
        //row.put("last_name", "sadek");
        row.put("id", "54-9948");

        //Date dob = new Date(1992 - 1900, 9 - 1, 8);
        //row.put("dob", dob);
        //row.put("gps", 1.1);

        //db.updateTable(table, "47-7573", row);
        //String[] colNames = {"first_name", "last_name", "gpa"};
        //db.createIndex(table, colNames);
        //db.deleteFromTable(table, row);
//        Table students = Table.loadTable("students");
//        for (int i = 0; i < students.vNumberOfRowsPerPage.size(); i++) {
//            students.showPage(i);
//        }
        //OctTree index = OctTree.deserializeIndex("first_namelast_namegpa");
//        for (OctTreeNode node : index.root.children) {
//            System.out.println(node.points);
//        }
        //System.out.println(Arrays.toString(index.root.children));
//        for (Point p: index.root.points) {
//            System.out.println("-"+p);
//        }
//        index.insert(new Hashtable<String, Object>(){{
//            put("first_name", "uvRhTC");
//            put("last_name", "gOgOWT");
//            put("gpa",4.74);
//        }},0, "id");
//        index.updateIndex("id", "55-2235", new Hashtable<String, Object>(){{
//            put("first_name", "noura");
//        }});

//        Table t = Table.loadTable("students");
//        t.updateTable("47-7573", new Hashtable<String, Object>(){{
//            put("first_name", "nourb");
//        }});
//        //noura piQJLG 4.62 0 pk: 55-2235
//        t.deleteFromTable("students", new Hashtable<String, Object>(){{
//            put("id", "71-5689");
//        }});

        //index.serializeIndex();


//        index.insert(new Hashtable<String, Object>(){{
//            put("first_name", "uvRhTC");
//            put("last_name", "gOgOWT");
//            put("gpa",4.75);
//        }},0, "id");
//        index.insert(new Hashtable<String, Object>(){{
//            put("first_name", "uvRhTC");
//            put("last_name", "gOgOWT");
//            put("gpa",4.76);
//        }},0, "id");
        //index.printTree(index.root);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[] {new SQLTerm(), new SQLTerm(), new SQLTerm()};
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = "xxxxxx";
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "last_name";
        arrSQLTerms[1]._strOperator = ">";
        arrSQLTerms[1]._objValue = "mmmmmmm";
        arrSQLTerms[2]._strTableName = "students";
        arrSQLTerms[2]._strColumnName= "gpa";
        arrSQLTerms[2]._strOperator = "<";
        arrSQLTerms[2]._objValue = new Double( 1.2 );
        String[]strarrOperators = new String[] {new String(), new String()};
        strarrOperators[0] = "AND";
        strarrOperators[1] = "AND";
// select * from Student where name = “John Noor” or gpa = 1.5;
        Iterator resultSet = db.selectFromTable(arrSQLTerms , strarrOperators);
        while (resultSet.hasNext()) {
            System.out.println(resultSet.next());
        }
        //db.selectFromTable(new SQLTerm[]{new SQLTerm()}, new String[]{});

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
