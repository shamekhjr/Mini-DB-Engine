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
                            Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException {

        Table table = new Table(strTableName, strClusteringKeyColumn,
                htblColNameType, htblColNameMin,
                htblColNameMax);

    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

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
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        return null;

    }

    //bonus
    public Iterator parseSQL( StringBuffer strbufSQL ) throws DBAppException {
        return null;
    }

    public static void main(String[] args) throws DBAppException, IOException {

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


        dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax );

//        try {
//            FileOutputStream fileOut =
//                    new FileOutputStream("trial.class");
//            ObjectOutputStream out = new ObjectOutputStream(fileOut);
//            out.writeObject("Hello World");
//            out.close();
//            fileOut.close();
//            System.out.println("serialized in trial.class");
//        } catch (IOException i) {
//            i.printStackTrace();
//        }
//
//        String help = "";
//
//        try {
//            FileInputStream fileIn = new FileInputStream("trial.class");
//            ObjectInputStream in = new ObjectInputStream(fileIn);
//            help = (String) in.readObject();
//            in.close();
//            fileIn.close();
//        } catch (IOException i) {
//            i.printStackTrace();
//            return;
//        } catch (ClassNotFoundException c) {
//            System.out.println("Data not found");
//            c.printStackTrace();
//            return;
//        }
//
//        System.out.println("Deserialized: " + help);
    }
}
