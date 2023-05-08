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

    public OctTree(String sIndexName, String col1Name, String col2Name, String col3Name, String sTableName) throws DBAppException {
        this.maxEntries = loadMaxEntries();
        this.sIndexName = sIndexName;
        this.colNamesDatatypes = new String[3][2];
        colNamesDatatypes[0][0] = col1Name;
        colNamesDatatypes[1][0] = col2Name;
        colNamesDatatypes[2][0] = col3Name;
        this.sTableName = sTableName;
        try {
            Hashtable<String, Pair<Comparable, Comparable>> htblMinMax = rootMinMax();
            root = new OctTreeNode(colNamesDatatypes, htblMinMax, maxEntries);
        } catch (Exception e) {
            throw new DBAppException(e);
        }
        // TODO update metadata
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

    public Hashtable<String, Pair<Comparable, Comparable>> rootMinMax () throws DBAppException, IOException, CsvValidationException, ParseException {
        CSVReader reader = new CSVReader(new FileReader("src/main/java/metadata.csv"));
        String[] line;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);
        Hashtable<String, Pair<Object, Object>> hMinMaxPerColumn = new Hashtable<>();

        while ((line = reader.readNext()) != null) {
            if (line[0].equals()) {
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

    public void insert(Hashtable<String, Object> record, int page) throws DBAppException { // finds the not full wrapping node and inserts
        Comparable[] colVals = new Comparable[3];
        int i = 0;
        for (String col: colNames) {
                if (record.get(col).getClass().equals(Null.class))
                    throw new DBAppException("Cannot create index on null values");
                else {
                    colVals[i] = (Comparable) record.get(col);
                    i++;
                }
        }
        Point e = new Point(colVals[0], colVals[1], colVals[2], page);
        root.insert(e);
    }

    public Vector<Point> search(Point e) {
        // TODO
        return null;
    }
}
