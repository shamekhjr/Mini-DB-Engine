import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class OctTree implements Serializable {
    OctTreeNode root;
    int maxEntries;
    String[] colNames;
    boolean onPrimaryKey;

    public OctTree(String col1Name, String col2Name, String col3Name, Comparable minC1, Comparable minC2, Comparable minC3, Comparable maxC1, Comparable maxC2, Comparable maxC3) throws DBAppException {
        this.maxEntries = loadMaxEntries();
        root = new OctTreeNode(minC1, minC2, minC3, maxC1, maxC2, maxC3, maxEntries);
        this.colNames = new String[] {col1Name, col2Name, col3Name};
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
        Entry e = new Entry(colVals[0], colVals[1], colVals[2], page);
        root.insert(e);
    }

    public Vector<Entry> search(Entry e) {
        // TODO
        return null;
    }


}
