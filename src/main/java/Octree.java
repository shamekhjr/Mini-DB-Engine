import com.opencsv.CSVReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Vector;

public class Octree {

    public String indexName;
    public Node root;
    public String strTableName;
    public String strClusteringKey;
    public String strColName1;
    public String strColName2;
    public String strColName3;
    public String dataTypeCol1;
    public String dataTypeCol2;
    public String dataTypeCol3;

    public Octree(String indexName,String strTableName, String strColName1, String strColName2, String strColName3, String strClusteringKey) throws Exception {
        this.indexName = indexName;
        this.strTableName = strTableName;
        this.strColName1 = strColName1;
        this.strColName2 = strColName2;
        this.strColName3 = strColName3;
        this.strClusteringKey = strClusteringKey;
        this.root = new Node(null, this.strTableName, this.strColName1, this.strColName2, this.strColName3, this.rootMinMax());
    }

    public void bulkLoad() {

    }

    public Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>> searchIndex (Hashtable<String,Object> hCondition) throws Exception {

        // Check that the search is done on at most 3 columns.
        if (hCondition.size() > 3) {
            throw new Exception("Must specify at most 3 columns for search");
        }
        else {
            Vector<Node> vecLeafNode = this.root.search(hCondition);
            Vector<Pair<Pair<Integer,Integer>, Hashtable<String, Object>>> vecResult = new Vector<Pair<Pair<Integer,Integer>,Hashtable<String,Object>>>();
            for (Node node : vecLeafNode) { // for each Leaf Node get its List of Keys
                for (Key key : (Vector<Key>)node.vKeys) { // for each Key get its list of References
                    for (Pair<Hashtable<String,Object>,Integer> pair : (Vector<Pair<Hashtable<String,Object>,Integer>>)key.vecRowNumberPerPage) {
                        Hashtable<String,Object> htblColNameValue = pair.val1; // get reference to the record the key is pointing to.
                        int pageNumber = pair.val2; // get page number that contain the record that the key is pointing to.
                        Page page = new Page(strTableName,strClusteringKey,pageNumber,true);
                        Vector<Pair<Integer,Hashtable<String, Object>>> vecRowNumbers = page.searchFromPage(hCondition); // search page to get the record row number.
                        for (Pair<Integer,Hashtable<String,Object>> p : vecRowNumbers) {
                            if (p.val2.equals(htblColNameValue)) {
                                vecResult.add(new Pair<Pair<Integer,Integer>,Hashtable<String,Object>>(new Pair<Integer,Integer>(pageNumber,p.val1),htblColNameValue));
                            }
                        }
                    }
                }
            }
            return vecResult;
        }
    }

    public void insertIntoIndex (Hashtable<String,Object> htblColNameValue, int pageNumber) throws Exception {

        if (htblColNameValue.get(strColName1) == null || htblColNameValue.get(strColName2) == null || htblColNameValue.get(strColName3) == null) {
            throw new Exception("Column + " + strColName1 + ", Column " + strColName2 + " and Column " + strColName3 + " can not be nulls");
        }
        else {
            Vector<Node> leafNode = this.root.search(htblColNameValue); // Should return a single leaf node where we would insert into it
            for (Node leaf : leafNode) {
                boolean found = false;
                // Check whether the inserted key is duplicate
                for (Key key : (Vector<Key>)leaf.vKeys) {
                    if (key.valueCol1.equals(htblColNameValue.get(strColName1)) && key.valueCol2.equals(htblColNameValue.get(strColName2)) && key.valueCol3.equals(htblColNameValue.get(strColName3))) {
                        found = true; // the inserted point into the index is a duplicate
                        key.insertRow(htblColNameValue, pageNumber);
                    }
                }

                // If the inserted point is not a duplicate
                if (!found) {
                    // Create a new key with the values of the
                    Key key = new Key(strColName1, (Serializable) htblColNameValue.get(strColName1), strColName2, (Serializable) htblColNameValue.get(strColName2), strColName3, (Serializable) htblColNameValue.get(strColName3));

                    if (leaf.numOfKeys == leaf.maxNumOfKeys) { // Split the leaf node


                    }
                    else { // Otherwise, insert the key
                        leaf.vKeys.add(key);
                        leaf.numOfKeys++;
                    }
                }
            }
        }
    }

    public void deleteFromIndex (Hashtable<String,Object> htblColNameValue) {

    }

    public void updateIndex (String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) {

    }

    public void serializeIndex () {
        try {
            FileOutputStream fos = new FileOutputStream(indexName + ".class");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Hashtable<String, Pair<Object, Object>> rootMinMax () throws Exception {
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
        Hashtable<String, Pair<Object, Object>> hMinMaxPerColumn = new Hashtable<>();

        while ((line = reader.readNext()) != null) {
            if (line[0].equals(strTableName)) {
                if (line[1].equals(strColName1)) {
                    dataTypeCol1 = line[2];
                    if (dataTypeCol1.equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(strColName1,new Pair(min, max));
                    }
                    else if (dataTypeCol1.equals(" java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(strColName1,new Pair(min, max));
                    }
                    else if (dataTypeCol1.equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(strColName1,new Pair(min, max));
                    }
                    else if (dataTypeCol1.equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(strColName1,new Pair(min, max));
                    }
                    else {
                        throw new Exception("Invalid data type: " + dataTypeCol1);
                    }
                }
                if (line[1].equals(strColName2)) {
                    dataTypeCol2 = line[2];
                    if (dataTypeCol2.equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(strColName2,new Pair(min, max));
                    }
                    else if (dataTypeCol2.equals(" java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(strColName2,new Pair(min, max));
                    }
                    else if (dataTypeCol2.equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(strColName2,new Pair(min, max));
                    }
                    else if (dataTypeCol2.equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(strColName2,new Pair(min, max));
                    }
                    else {
                        throw new Exception("Invalid data type: " + dataTypeCol2);
                    }
                }
                if (line[1].equals(strColName3)) {
                    dataTypeCol3 = line[2];
                    if (dataTypeCol3.equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(strColName3,new Pair(min, max));
                    }
                    else if (dataTypeCol3.equals("java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(strColName3,new Pair(min, max));
                    }
                    else if (dataTypeCol3.equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(strColName3,new Pair(min, max));
                    }
                    else if (dataTypeCol3.equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(strColName3,new Pair(min, max));
                    }
                    else {
                        throw new Exception("Invalid data type: " + dataTypeCol3);
                    }
                }
            }
        }
        return hMinMaxPerColumn;
    }
/*
    static String printMiddleString(String S, String T, int N)
    {
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int)a1[i] / 2;
        }

        for (int i = 1; i <= N; i++) {
            System.out.print((char)(a1[i] + 97));
        }
    }
*/
}

