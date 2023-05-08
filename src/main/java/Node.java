import com.opencsv.CSVReader;
import jdk.incubator.vector.VectorOperators;

import java.io.*;
import java.nio.DoubleBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

public class Node <T extends Serializable, M extends Serializable, N extends Serializable> implements Serializable{
    public Node parent;
    public Node children[]; // [Child 1, Child 2, Child 3, Child 4, ...]
    public boolean leaf; // true: if the node is a leaf.
    public String strTableName;
    public String colName1, dataTypeCol1;
    public String colName2, dataTypeCol2;
    public String colName3, dataTypeCol3;
    public Hashtable<String, Pair<Object, Object>> hMinMaxPerColumn; // <Column Name, Minimum Value, Maximum Value>
    public int numOfKeys;   // 0: if the node is a non-leaf or initially created.
    public int maxNumOfKeys;
    public Vector<Key> vKeys; // null: if the node is a non-leaf.

    public Node (Node parent, String strTableName, String colName1, String colName2, String colName3, Hashtable<String, Pair<Object, Object>> hMinMaxPerColumn) throws Exception {
        this.parent = parent;
        this.children = new Node[8];
        this.leaf = true; // When we create a child node at the beginning it is always a leaf node.
        this.strTableName = strTableName;
        this.colName1 = colName1;
        this.colName2 = colName2;
        this.colName3 = colName3;
        this.hMinMaxPerColumn = hMinMaxPerColumn;
        this.numOfKeys = 0;
        this.maxNumOfKeys = maxNumOfKeys;
        this.vKeys = new Vector<Key>();
        this.setDatatypeColumns();
        String _filename = "src/main/resources/DBApp.config"; // File that contain configuration
        Properties configProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(_filename))
        {
            configProperties.load(fis);
        }
        catch (FileNotFoundException ex)
        {
            ex.printStackTrace();
        }
        catch (IOException e)  {
            e.printStackTrace();
        }
        this.maxNumOfKeys = Integer.parseInt(configProperties.getProperty("MaximumRowsCountinTablePage"));
    }

    public Pair<Vector<Key>,Vector<Key>> splitColumn (Vector<Key> spltVectorOfKeys, String spltColumnName, String dataType, T maxValue, T minValue) throws Exception {
        Vector<Key> lessThanMiddleValue = new Vector<Key>();
        Vector<Key> greaterThanOrEqualMiddleValue = new Vector<Key>();

        if (dataType.equals("java.lang.Integer")) {
            Integer max = (Integer)maxValue;
            Integer min = (Integer)minValue;
            Integer middleValue = (max - min) / 2 + min;
            for (Key key : this.vKeys) {
                Integer targetColumnKeyValue = null;
                if (key.colName1.equals(spltColumnName)) {
                    targetColumnKeyValue = (Integer)key.valueCol1;
                }
                else if (key.colName2.equals(spltColumnName)) {
                    targetColumnKeyValue = (Integer)key.valueCol2;
                }
                else if (key.colName3.equals(spltColumnName)) {
                    targetColumnKeyValue = (Integer)key.valueCol3;
                }
                else {
                    throw new Exception("Invalid column");
                }

                if (targetColumnKeyValue.compareTo(middleValue) < 0) {
                    lessThanMiddleValue.add(key);
                }
                else {
                    greaterThanOrEqualMiddleValue.add(key);
                }
            }
        }
        else if (dataType.equals("java.lang.String")) {

        }
        else if (dataType.equals("java.lang.Double")) {
            Double max = (Double) maxValue;
            Double min = (Double) minValue;
            Double middleValue = (max - min) / 2.0 + min;
            for (Key key : this.vKeys) {
                Double targetColumnKeyValue = null;
                if (key.colName1.equals(spltColumnName)) {
                    targetColumnKeyValue = (Double) key.valueCol1;
                }
                else if (key.colName2.equals(spltColumnName)) {
                    targetColumnKeyValue = (Double)key.valueCol2;
                }
                else if (key.colName3.equals(spltColumnName)) {
                    targetColumnKeyValue = (Double)key.valueCol3;
                }
                else {
                    throw new Exception("Invalid column");
                }

                if (targetColumnKeyValue.compareTo(middleValue) < 0) {
                    lessThanMiddleValue.add(key);
                }
                else {
                    greaterThanOrEqualMiddleValue.add(key);
                }
            }
        }
        else if (dataType.equals("java.util.Date")) {

        }

        return new Pair<Vector<Key>,Vector<Key>> (lessThanMiddleValue, greaterThanOrEqualMiddleValue);
    }

    /*
    column 1 : split nodes 0,1,4,5 and 2,3,6,7
    column 3 : split nodes 0,1,2,3 and 4,5,6,7
    column 2 : split nodes 0,2,4,6 and 1,3,5,7

    Illustration for the division
     column 1
    0 1 | 2 3
    --------- column 2
    4 5 | 6 7

   col2   col3
    *     *  ________________
    *    *  /  0   /     2  /
    *   *  /  1   /     3  /
    *  *  /------|--------/ col1
    * *   |  4 . |   6    |
    **    |------|--------|
    **    |  5   | . 7    |
    *     |______|________|
    **************************** col1

     */


    public Vector<Node> search (Hashtable<String, Object> htblColNameValue) {
        T valColumn1 = (T) htblColNameValue.get(colName1);
        M valColumn2 = (M) htblColNameValue.get(colName2);
        N valColumn3 = (N) htblColNameValue.get(colName3);

        if (htblColNameValue.size() == 3) {
            return searchThreeColumns(valColumn1,valColumn2,valColumn3);
        }
        else if (htblColNameValue.size() == 2) {
            if (valColumn1 == null) {
                return searchTwoColumns(new Pair(colName2, valColumn2), new Pair(colName3, valColumn3));
            }
            else  if (valColumn2 == null) {
                return searchTwoColumns(new Pair(colName1, valColumn1), new Pair(colName3, valColumn3));
            }
            else {
                return searchTwoColumns(new Pair(colName1, valColumn1), new Pair(colName2, valColumn2));
            }
        }
        else if (htblColNameValue.size() ==1) {
            if (valColumn1 != null) {
                return searchOneColumn(new Pair(colName1, valColumn1));
            }
            else if (valColumn2 != null) {
                return searchOneColumn(new Pair(colName2, valColumn2));
            }
            else if (valColumn3 != null) {
                return searchOneColumn(new Pair(colName3, valColumn3));
            }
        }
        return new Vector<Node>(); // Empty list of nodes
    }

    /*
    If node is a leaf:
       1. Return the current node in a vector
    If node is a non-leaf:
       1. Decide which child node to go next.
    */
    public Vector<Node> searchThreeColumns (T valCol1, M valCol2, N valCol3) {
        if (leaf) {
            Vector<Node> vecResultNode = new Vector<Node>();
            vecResultNode.add(this);
            return vecResultNode;
        }
        else {
            Node next = null;
            for (int i = 0; i < children.length; i++) {
                Node current = children[i];
                Pair<T,T> minMaxCol1 = ((Pair<T,T>)(current.hMinMaxPerColumn.get(colName1)));
                Pair<M,M> minMaxCol2 = ((Pair<M,M>)(current.hMinMaxPerColumn.get(colName2)));
                Pair<N,N> minMaxCol3 = ((Pair<N,N>)(current.hMinMaxPerColumn.get(colName3)));
                boolean cond1 = ((Comparable<T>)minMaxCol1.val1).compareTo(valCol1) <= 0 && ((Comparable<M>)minMaxCol2.val1).compareTo(valCol2) <= 0 && ((Comparable<N>)minMaxCol3.val1).compareTo(valCol3) <= 0;
                boolean cond2 = ((Comparable<T>)minMaxCol1.val2).compareTo(valCol1) >= 0 && ((Comparable<M>)minMaxCol2.val2).compareTo(valCol2) >= 0 && ((Comparable<N>)minMaxCol3.val2).compareTo(valCol3) >= 0;
                if (cond1 && cond2) {
                    next = current;
                }
            }
            if (next == null) {
                return new Vector<Node>(); // Empty list of keys
            }
            return next.searchThreeColumns(valCol1,valCol2,valCol3);
        }
    }

    /*
    If node is a leaf:
       1. Return the current node in a vector
    If node is a non-leaf:
       1. Decide which child node(s) to go next.
    */
    public Vector<Node> searchTwoColumns (Pair<String, T> col1,Pair<String, M> col2) {
        if (leaf) {
            Vector<Node> vecResultNode = new Vector<Node>();
            vecResultNode.add(this);
            return vecResultNode;
        }
        else {
            Vector<Node> vecNextNodes = new Vector<>();
            for (int i = 0; i < children.length; i++) {
                Node current = children[i];
                Pair<T,T> minMaxCol1 = ((Pair<T,T>)(current.hMinMaxPerColumn.get(col1.val1)));
                Pair<M,M> minMaxCol2 = ((Pair<M,M>)(current.hMinMaxPerColumn.get(col2.val1)));
                boolean cond1 = ((Comparable<T>)minMaxCol1.val1).compareTo(col1.val2) <= 0 && ((Comparable<M>)minMaxCol2.val1).compareTo(col2.val2) <= 0;
                boolean cond2 = ((Comparable<T>)minMaxCol1.val2).compareTo(col1.val2) >= 0 && ((Comparable<M>)minMaxCol2.val2).compareTo(col2.val2) >= 0;
                if (cond1 && cond2) {
                    vecNextNodes.add(current);
                }
            }
            Vector<Node> vecResultNodes = new Vector<Node>();
            for (Node next : vecNextNodes) {
                Vector<Node> vecTmpNodes = next.searchTwoColumns(col1,col2);
                for(Node node : vecTmpNodes) {
                    vecResultNodes.add(node);
                }
            }
            return vecResultNodes;
        }
    }
    /*
    If node is a leaf:
       1. Return the current node in a vector
    If node is a non-leaf:
       1. Decide which child node(s) to go next.
     */
    public Vector<Node> searchOneColumn (Pair<String, T> col) {
        if (leaf) {
            Vector<Node> vecResultNode = new Vector<Node>();
            vecResultNode.add(this);
            return vecResultNode;
        }
        else {
            Vector<Node> vecNextNode = new Vector<>();
            for (int i = 0; i < children.length; i++) {
                Node current = children[i];
                Pair<T,T> minMaxCol = ((Pair<T,T>)(current.hMinMaxPerColumn.get(col.val1)));
                boolean cond1 = ((Comparable<T>)minMaxCol.val1).compareTo(col.val2) <= 0;
                boolean cond2 = ((Comparable<T>)minMaxCol.val2).compareTo(col.val2) >= 0;
                if (cond1 && cond2) {
                    vecNextNode.add(current);
                }
            }

            Vector<Node> vecResultNodes = new Vector<Node>();
            for (Node next : vecNextNode) {
                Vector<Node> vecTmpKeys = next.searchOneColumn(col);
                for(Node node : vecTmpKeys) {
                    vecResultNodes.add(node);
                }
            }
            return vecResultNodes;
        }
    }

    public void setDatatypeColumns () throws Exception {
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);

        while ((line = reader.readNext()) != null) {
            if (line[0].equals(strTableName)) {
                if (line[1].equals(colName1)) {
                    dataTypeCol1 = line[2];
                }
                if (line[1].equals(colName2)) {
                    dataTypeCol2 = line[2];
                }
                if (line[1].equals(colName3)) {
                    dataTypeCol3 = line[2];
                }
            }
        }
    }
}
