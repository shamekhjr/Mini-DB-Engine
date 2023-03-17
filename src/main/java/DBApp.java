import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
    public void init() {
        //very bri'ish amirit
        //ya m8
        //stop tolkin

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
                            Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException, CsvValidationException {

        //create instance of Table class
        Table table = new Table(strTableName, strClusteringKeyColumn,
                htblColNameType, htblColNameMin,
                htblColNameMax);

        // save the table to hard disk
        table.serializeTable();
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, CsvValidationException, IOException {
        // load table data from hard disk
        Table tTable = Table.loadTable(strTableName);
        tTable.insertIntoTable(htblColNameValue);
        tTable.serializeTable();
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException {

    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, CsvException {

        //declaring csv reader
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));

        //check if table exists
        String[] line;
        while ((line = reader.readNext()) != null) {
            if (line[0].equals(strTableName)) {

                //load table
                Table tTable = Table.loadTable(strTableName);

                //check that column names and types are valid
                for (String key : htblColNameValue.keySet()) {

                    //look in metadata file for column name
                    boolean found = false;
                    for (String[] line2 : reader.readAll()) {
                        //reminder:
                        //line2[0] = table name
                        //line2[1] = column name
                        //line2[2] = column type

                        if (line2[0].equals(strTableName) && line2[1].equals(key)) {
                            found = true;

                            //check that column type is valid
                            //since the column exists in metadata file we are sure the column type should
                            //be one of the following: java.lang.Integer, java.lang.String,
                            // java.lang.Double
                            if (!line2[2].equals(htblColNameValue.get(key).getClass().getName())) {
                                throw new DBAppException("Invalid column type for deletion");
                            }
                            break;
                        }
                        throw new DBAppException("Invalid column name for deletion");
                    }
                }

                //delete from table
                tTable.deleteFromTable(strTableName, htblColNameValue);
                return;
            }
        }
        throw new DBAppException("Table does not exist");

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        return null;

    }

    //bonus
    public Iterator parseSQL( StringBuffer strbufSQL ) throws DBAppException {
        return null;
    }

    public static void main(String[] args) throws DBAppException, IOException, CsvValidationException {

        //testing Table class creation
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );

        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable htblColNameMin = new Hashtable( );
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");

        Hashtable htblColNameMax = new Hashtable( );
        htblColNameMax.put("id", "1000000000");
        htblColNameMax.put("name","ZZZZZZZZZZZ");
        htblColNameMax.put("gpa", "4.0");


        //dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax );
        dbApp.insertIntoTable(strTableName, new Hashtable<String, Object>() {{
            put("id", 2);
            put("name", "Ahmed");
            put("gpa", 0.9);
        }});
    }
}
