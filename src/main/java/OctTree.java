import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OctTree implements Serializable {
    OctTreeNode root;
    int maxEntries;
    String[][] colNamesDatatypes;
    boolean onPrimaryKey;
    String sTableName;
    String sIndexName;

    public OctTree(String sIndexName, String col1Name, String col2Name, String col3Name, String sTableName, boolean onPrimaryKey) throws DBAppException {
        this.maxEntries = loadMaxEntries();
        this.sIndexName = sIndexName;
        this.colNamesDatatypes = new String[3][2];
        colNamesDatatypes[0][0] = col1Name;
        colNamesDatatypes[1][0] = col2Name;
        colNamesDatatypes[2][0] = col3Name;
        this.sTableName = sTableName;
        try {
            Hashtable<String, Pair<Comparable, Comparable>> htblMinMax = rootMinMax(sTableName);
            root = new OctTreeNode(colNamesDatatypes, htblMinMax, maxEntries);
        } catch (Exception e) {
            throw new DBAppException(e);
        }
        this.onPrimaryKey = onPrimaryKey;
    }

    public int loadMaxEntries() throws DBAppException {
        String _filename = "src/main/resources/DBApp.config"; // File that contain configuration
        Properties configProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(_filename))
        {
            configProperties.load(fis);
        }
        catch (Exception e)  {
            throw new DBAppException(e);
        }
        return Integer.parseInt(configProperties.getProperty("MaximumEntriesinOctreeNode"));
    }

    public Hashtable<String, Pair<Comparable, Comparable>> rootMinMax (String sTableName) throws DBAppException, IOException, CsvValidationException, ParseException {
        CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"));
        String[] line;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
        Hashtable<String, Pair<Comparable, Comparable>> hMinMaxPerColumn = new Hashtable<>();

        while ((line = reader.readNext()) != null) {
            if (line[0].equals(sTableName)) {
                if (line[1].equals(colNamesDatatypes[0][0])) {
                    colNamesDatatypes[0][1] = line[2];
                    if (colNamesDatatypes[0][1].equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[0][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[0][1].equals(" java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(colNamesDatatypes[0][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[0][1].equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[0][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[0][1].equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[0][0],new Pair(min, max));
                    }
                    else {
                        throw new DBAppException("Invalid data type: " + colNamesDatatypes[0][1]);
                    }
                }
                if (line[1].equals(colNamesDatatypes[1][0])) {
                    colNamesDatatypes[1][1] = line[2];
                    if (colNamesDatatypes[1][1].equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[1][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[1][1].equals(" java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(colNamesDatatypes[1][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[1][1].equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[1][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[1][1].equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[1][0],new Pair(min, max));
                    }
                    else {
                        throw new DBAppException("Invalid data type: " + colNamesDatatypes[1][1]);
                    }
                }
                if (line[1].equals(colNamesDatatypes[2][0])) {
                    colNamesDatatypes[2][1] = line[2];
                    if (colNamesDatatypes[2][1].equals("java.lang.Integer")) {
                        Integer min = Integer.parseInt(line[7]);
                        Integer max = Integer.parseInt(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[2][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[2][1].equals("java.lang.String")) {
                        String min = line[7];
                        String max = line[8];
                        hMinMaxPerColumn.put(colNamesDatatypes[2][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[2][1].equals("java.lang.Double")) {
                        Double min = Double.parseDouble(line[7]);
                        Double max = Double.parseDouble(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[2][0],new Pair(min, max));
                    }
                    else if (colNamesDatatypes[2][1].equals("java.util.Date")) {
                        Date min = formatter.parse(line[7]);
                        Date max = formatter.parse(line[8]);
                        hMinMaxPerColumn.put(colNamesDatatypes[2][0],new Pair(min, max));
                    }
                    else {
                        throw new DBAppException("Invalid data type: " + colNamesDatatypes[2][1]);
                    }
                }
            }
        }
        return hMinMaxPerColumn;
    }

    public void insert(Hashtable<String, Object> record, int page, String sClusteringKey) throws DBAppException { // finds the not full wrapping node and inserts
        Comparable[] colVals = new Comparable[3];
        for (int i = 0; i < colNamesDatatypes.length; i++) {
            if (record.get(colNamesDatatypes[i][0]).getClass().equals(Null.class))
                throw new DBAppException("Cannot create index on null values");
            else {
                colVals[i] = (Comparable) record.get(colNamesDatatypes[i][0]);
            }
        }
        Point e = new Point(colVals[0], colVals[1], colVals[2], page, (Comparable) record.get(sClusteringKey));
        root.insert(e);
    }

    //deletes one record (point) from the index
    //we pass in (something)
    public void delete(Pair<Pair<Integer, Integer>, Hashtable<String, Object>> record) throws DBAppException {
        //compare col names and datatypes of records to that of OctTree
        Comparable col1 = null;
        Comparable col2 = null;
        Comparable col3 = null;
        for (String colName: record.val2.keySet()) {
            //if colName is in octree and datatypes match then we can create Comparable to add to Point
            if (colName.equals(colNamesDatatypes[0][0]) && record.val2.get(colName).getClass().equals(colNamesDatatypes[0][1])) {
                col1 = (Comparable) record.val2.get(colName);
            }
            else if (colName.equals(colNamesDatatypes[1][0]) && record.val2.get(colName).getClass().equals(colNamesDatatypes[1][1])) {
                col2 = (Comparable) record.val2.get(colName);
            }
            else if (colName.equals(colNamesDatatypes[2][0]) && record.val2.get(colName).getClass().equals(colNamesDatatypes[2][1])) {
                col3 = (Comparable) record.val2.get(colName);
            }
            else {
                throw new DBAppException("Invalid data type");
            }

        }

        //TODO: handle case if one of the col vals remains null
        if (col1 == null && col2 == null && col3 == null)
            return;


        Point p = new Point(col1, col2, col3, record.val1.val1, null); //idk what to put makan el null
        root.delete(p);
    }

    //used to clear a table (delete all records without deleting whole table)
    public void deleteAll() {
        //remove all points inside of root
        root.points.removeAllElements();

        //get rid of the children once and for all
        //in a very brute force, barbaric and inelegant way
        for (int i = 0; i < root.children.length; i++) {
            root.children[i] = null;
        }
        System.gc();
        //re-serialize OctTree
        serializeIndex();

    }

    //will be used in deleteFromTable to find records and update the page numbers
    public void updateRefNum(int oldIndex, int newIndex) {
        root.updateRefNum(oldIndex, newIndex);
    }

    public Vector<Point> search(Point e) {
        // TODO
        return null;
    }

    // helper for searching on the index if it indexes the pk
    public Point getPointByPkAndUpdate(String pkColName, Comparable pk, Hashtable<String, Object> htblColNameValue) {
        if (!onPrimaryKey)
            return null;

        // search for the leaf nodes that wrap the pk
        Vector<OctTreeNode> relevantNodes = root.searchOneColumn(new Pair<>(pkColName, pk));
        Point point = null;
        // for each relevant record
        for (OctTreeNode node : relevantNodes) {
            boolean brk = false;
            // check on each entry in a node
            Vector<Point> entries = node.points;
            for (int i = 0; i < entries.size(); i++) {
                Point entry = entries.get(i);
                // compare pk with current points' pk
                if (entry.pkValue.equals(pk)) {
                    point = entries.remove(i); // remove the point from the index
                    break;
                }

                // no duplicates if pk in index
                /*
                for (Point duplicate : entry.duplicates) {
                    if (duplicate.pkValue.equals(pk)) {
                        point = duplicate;
                        brk = true;
                        break;
                    }
                }

                if (brk) break;  */
            }
        }
        if (point != null) { // update the point
            for (int i = 0; i < colNamesDatatypes.length; i++) {
                if (htblColNameValue.containsKey(colNamesDatatypes[i][0])) {
                    point.cols[i] = (Comparable) htblColNameValue.get(colNamesDatatypes[i][0]);
                }
            }
            root.insert(point); // insert the new point
            return point;
        }
        return null; // not found
    }

    // used when updating only without having to search for the point [index not on pk]
    public void updateIndex(String pkColName, Comparable pkValue, Hashtable<String, Object> htblColNameValue) {
        // check if cols updated are indexed
        boolean anyColUpdatedIsIndexed = false;
        for (int i = 0; i < colNamesDatatypes.length; i++) {
            if (htblColNameValue.containsKey(colNamesDatatypes[i][0])) {
                anyColUpdatedIsIndexed = true;
                break;
            }
        }

        if (!anyColUpdatedIsIndexed)
            return; // no indexed cols updated

        // search for the point in the leaf node that contains the pk and delete it
        Point p = root.searchPkAttributeAndDelete(pkValue);
        if (p == null)
            return; // not found

        // re-insert the point with the updated values
        for (int i = 0; i < colNamesDatatypes.length; i++) {
            if (htblColNameValue.containsKey(colNamesDatatypes[i][0])) {
                p.cols[i] = (Comparable) htblColNameValue.get(colNamesDatatypes[i][0]);
            }
        }
        root.insert(p);// insert the updated point
    }


    // used to get the page and update the index [if index is on pk]
    public int getPageByPkAndUpdate(String pkColName, Comparable pkValue, Hashtable<String, Object> htblColNameValue) {
        Point p = getPointByPkAndUpdate(pkColName, pkValue, htblColNameValue);
        if (p == null)
            return -1; //not found
        return p.reference;
    }

    public  Vector<Pair<Integer, Comparable>> search (SQLTerm[] arrSQLTerms) {
        // TODO CODE
        Vector<Pair<Integer, Comparable>> resultPagePK = new Vector<>();
        Vector<OctTreeNode> vecResultOctTreeNodes = root.searchNode(arrSQLTerms);
        // For each leaf node extracted from the search query.
        for (OctTreeNode node : vecResultOctTreeNodes) {
            // For each point in the leaf nodes.
            for (Point point : node.points) {
                // Get all page numbers that the point references.
                resultPagePK.add(new Pair<>(point.reference, point.pkValue));
                for (Point p : point.duplicates) {
                    resultPagePK.add(new Pair<>(p.reference, p.pkValue));
                }
            }
        }
        return resultPagePK;
    }

    public void updateShiftedRecords(Hashtable<String, Object> htblColNameValue, Object pk, int newReference) {
       Comparable[] cols = new Comparable[3];
       for (int i = 0; i < colNamesDatatypes.length; i++) {
           cols[i] = (Comparable) htblColNameValue.get(colNamesDatatypes[i][0]);
       }

       Vector<OctTreeNode> node = root.searchThreeColumns(cols[0], cols[1], cols[2]);
       for (Point p : node.firstElement().points) {
           if (p.pkValue.equals(pk)) {
               p.reference = newReference;
               return;
           } else {
               for (Point duplicate : p.duplicates) {
                   if (duplicate.pkValue.equals(pk)) {
                       duplicate.reference = newReference;
                       return;
                   }
               }
           }
       }
    }

    public void serializeIndex () {
        try {
            FileOutputStream fos = new FileOutputStream("src/main/resources/indices/" + sIndexName + ".class");
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

    public static OctTree deserializeIndex(String IndexName) throws DBAppException {
        try {
            FileInputStream fis = new FileInputStream("src/main/resources/indices/" + IndexName + ".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            OctTree index = (OctTree) ois.readObject();
            ois.close();
            fis.close();
            return index;
        } catch (Exception e) {
            throw new DBAppException(e);
        }
    }

}
