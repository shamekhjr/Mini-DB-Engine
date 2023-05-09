import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.*;
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
        // load table data from hard disk
        try {
            Table tTable = Table.loadTable(strTableName);
            tTable.insertIntoTable(htblColNameValue);
            tTable.serializeTable();
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

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        //TODO add input validation (colNames, object types, range check, etc.)

        // check for correct number of operators
        if (arrSQLTerms.length - 1 != strarrOperators.length) {
            throw new DBAppException("Invalid number of operators");
        }

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
        //TODO add input validation (colNames, object types, range check, etc.)

        // check if the number of columns is correct
        if (strarrColName.length != 3) {
            throw new DBAppException("Invalid number of columns");
        }



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
