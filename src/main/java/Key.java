import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Key <T extends Serializable, M extends Serializable, N extends Serializable> implements Serializable{
    public Vector<Pair<Hashtable<String, Object>, Integer>> vecRowNumberPerPage; // pair <row reference, page number> for duplicate points
    public String colName1, colName2, colName3;
    public T valueCol1;
    public M valueCol2;
    public N valueCol3;

    /*
        Key: is the representation of exact three values which the Octree is built on
     */
    public Key(String colName1, T valueCol1, String colName2, M valueCol2, String colName3, N valueCol3) {
        this.valueCol1 = valueCol1;
        this.valueCol2 = valueCol2;
        this.valueCol3 = valueCol3;
    }

    /*
        1. Check whether a point to be inserted into the key is already inserted.
     */
    public void insertRow (Hashtable<String, Object> htblColNameValue, int pageNumber) {
        boolean found = false;
        for (Pair<Hashtable<String, Object>,Integer> pair : vecRowNumberPerPage) {
            if (pair.val1.equals(htblColNameValue) && pair.val2 == pageNumber) {
                found = true;
            }
        }

        if (!found) {
            vecRowNumberPerPage.add(new Pair<Hashtable<String,Object>,Integer>(htblColNameValue,pageNumber));
        }
    }
}
