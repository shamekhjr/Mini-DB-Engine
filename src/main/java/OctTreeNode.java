import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

public class OctTreeNode  implements Serializable {
    public Vector<Point> points;
    public OctTreeNode[] children;
    String[][] colNamesDatatypes;
    public Hashtable<String, Pair<Comparable, Comparable>> hMinMaxPerColumn;
    public int maxEntries;
    boolean isLeaf; //TODO ALWAYS UPDATE THIS

   public OctTreeNode(String[][] colNamesDatatypes, Hashtable<String, Pair<Comparable, Comparable>> hMinMaxPerColumn, int maxEntries) {
        this.colNamesDatatypes = colNamesDatatypes;
        this.maxEntries = maxEntries;
        this.points = new Vector<>(maxEntries);
        this.children = new OctTreeNode[8];
        this.isLeaf = true;
        this.hMinMaxPerColumn = hMinMaxPerColumn;
   }

   public boolean isFull() {
       return points.size() == maxEntries;
   }

   public boolean isEmpty() {
       return points.size() == 0;
   }

   public boolean wraps(Point p) {
       // TODO accept less than 3 cols
//        return  (p.col1.compareTo(hMinMaxPerColumn.get(colNamesDatatypes[0][0])) >= 0 && p.col1.compareTo(maxC1) <= 0) &&
//                (p.col2.compareTo(minC2) >= 0 && p.col2.compareTo(maxC2) <= 0) &&
//                (p.col3.compareTo(minC3) >= 0 && p.col3.compareTo(maxC3) <= 0);
       return true;
   }

   public void insert(Point p) { // finds the not full wrapping OctTreeNode and inserts, insert/subdivide-&-insert
       if(!isLeaf) {
           // find child that wraps e
           // insert in child
       } else {
           boolean duplicates = false;
           if (this.wraps(p)) {
               if (duplicates) { // check if duplicates

               } else if (points.size() < maxEntries) { // check if space
                   points.add(p);
               } else {
                   // create children
                   // TODO
                   subdivide();
                   distribute();
                   this.isLeaf = false;
                   insert(p);
                   // insert in division
               }
           }
       }
   }

   public boolean delete(Point p) {
       if (this.wraps(p)) {
           for (Point entry: points) {
               if (entry.equals(p)) {
                     points.remove(entry);
               }
           }
           return true;
       } else {
           // find child that wraps e
       }
       return false;
   }

   public void subdivide() {

   }

   public void distribute() {

   }

   public void insertInChildren(Point p) {

   }

    public Vector<OctTreeNode> search (Hashtable<String, Object> htblColNameValue) {
        Comparable valColumn1 =  (Comparable) htblColNameValue.get(colNamesDatatypes[0][0]);
        Comparable valColumn2 =  (Comparable) htblColNameValue.get(colNamesDatatypes[1][0]);
        Comparable valColumn3 =  (Comparable) htblColNameValue.get(colNamesDatatypes[2][0]);

        if (htblColNameValue.size() == 3) {
            return searchThreeColumns(valColumn1,valColumn2,valColumn3);
        }
        else if (htblColNameValue.size() == 2) {
            if (valColumn1 == null) {
                return searchTwoColumns(new Pair(colNamesDatatypes[1][0], valColumn2), new Pair(colNamesDatatypes[2][0], valColumn3));
            }
            else  if (valColumn2 == null) {
                return searchTwoColumns(new Pair(colNamesDatatypes[0][0], valColumn1), new Pair(colNamesDatatypes[1][0], valColumn3));
            }
            else {
                return searchTwoColumns(new Pair(colNamesDatatypes[0][0], valColumn1), new Pair(colNamesDatatypes[1][0], valColumn2));
            }
        }
        else if (htblColNameValue.size() ==1) {
            if (valColumn1 != null) {
                return searchOneColumn(new Pair(colNamesDatatypes[0][0], valColumn1));
            }
            else if (valColumn2 != null) {
                return searchOneColumn(new Pair(colNamesDatatypes[1][0], valColumn2));
            }
            else if (valColumn3 != null) {
                return searchOneColumn(new Pair(colNamesDatatypes[2][0], valColumn3));
            }
        }
        return new Vector<OctTreeNode>(); // Empty list of OctTreeNodes
    }

    /*
    If OctTreeNode is a leaf:
       1. Return the current OctTreeNode in a vector
    If OctTreeNode is a non-leaf:
       1. Decide which child OctTreeNode to go next.
    */
    public Vector<OctTreeNode> searchThreeColumns (Comparable valCol1, Comparable valCol2, Comparable valCol3) {
        if (isLeaf) {
            Vector<OctTreeNode> vecResultOctTreeNode = new Vector<OctTreeNode>();
            vecResultOctTreeNode.add(this);
            return vecResultOctTreeNode;
        }
        else {
            OctTreeNode next = null;
            for (int i = 0; i < children.length; i++) {
                OctTreeNode current = children[i];
                Pair<Comparable, Comparable> minMaxCol1 = ((Pair<Comparable, Comparable>)(current.hMinMaxPerColumn.get(colNamesDatatypes[0][0])));
                Pair<Comparable, Comparable> minMaxCol2 = ((Pair<Comparable, Comparable>)(current.hMinMaxPerColumn.get(colNamesDatatypes[1][0])));
                Pair<Comparable, Comparable> minMaxCol3 = ((Pair<Comparable, Comparable>)(current.hMinMaxPerColumn.get(colNamesDatatypes[2][0])));
                boolean cond1 = ((Comparable)minMaxCol1.val1).compareTo(valCol1) <= 0 && ((Comparable)minMaxCol2.val1).compareTo(valCol2) <= 0 && ((Comparable)minMaxCol3.val1).compareTo(valCol3) <= 0;
                boolean cond2 = ((Comparable)minMaxCol1.val2).compareTo(valCol1) >= 0 && ((Comparable)minMaxCol2.val2).compareTo(valCol2) >= 0 && ((Comparable)minMaxCol3.val2).compareTo(valCol3) >= 0;
                if (cond1 && cond2) {
                    next = current;
                }
            }
            if (next == null) {
                return new Vector<OctTreeNode>(); // Empty list of keys
            }
            return next.searchThreeColumns(valCol1,valCol2,valCol3);
        }
    }

    /*
    If OctTreeNode is a leaf:
       1. Return the current OctTreeNode in a vector
    If OctTreeNode is a non-leaf:
       1. Decide which child OctTreeNode(s) to go next.
    */
    public Vector<OctTreeNode> searchTwoColumns (Pair<String, Comparable> col1,Pair<String, Comparable> col2) {
        if (isLeaf) {
            Vector<OctTreeNode> vecResultOctTreeNode = new Vector<OctTreeNode>();
            vecResultOctTreeNode.add(this);
            return vecResultOctTreeNode;
        }
        else {
            Vector<OctTreeNode> vecNextOctTreeNodes = new Vector<>();
            for (int i = 0; i < children.length; i++) {
                OctTreeNode current = children[i];
                Pair<Comparable,Comparable> minMaxCol1 = ((Pair<Comparable, Comparable>)(current.hMinMaxPerColumn.get(col1.val1)));
                Pair<Comparable,Comparable> minMaxCol2 = ((Pair<Comparable, Comparable>)(current.hMinMaxPerColumn.get(col2.val1)));
                boolean cond1 = ((Comparable)minMaxCol1.val1).compareTo(col1.val2) <= 0 && ((Comparable)minMaxCol2.val1).compareTo(col2.val2) <= 0;
                boolean cond2 = ((Comparable)minMaxCol1.val2).compareTo(col1.val2) >= 0 && ((Comparable)minMaxCol2.val2).compareTo(col2.val2) >= 0;
                if (cond1 && cond2) {
                    vecNextOctTreeNodes.add(current);
                }
            }
            Vector<OctTreeNode> vecResultOctTreeNodes = new Vector<OctTreeNode>();
            for (OctTreeNode next : vecNextOctTreeNodes) {
                Vector<OctTreeNode> vecTmpOctTreeNodes = next.searchTwoColumns(col1,col2);
                for(OctTreeNode OctTreeNode : vecTmpOctTreeNodes) {
                    vecResultOctTreeNodes.add(OctTreeNode);
                }
            }
            return vecResultOctTreeNodes;
        }
    }
    /*
    If OctTreeNode is a leaf:
       1. Return the current OctTreeNode in a vector
    If OctTreeNode is a non-leaf:
       1. Decide which child OctTreeNode(s) to go next.
     */
    public Vector<OctTreeNode> searchOneColumn (Pair<String, Comparable> col) {
        if (isLeaf) {
            Vector<OctTreeNode> vecResultOctTreeNode = new Vector<OctTreeNode>();
            vecResultOctTreeNode.add(this);
            return vecResultOctTreeNode;
        }
        else {
            Vector<OctTreeNode> vecNextOctTreeNode = new Vector<>();
            for (int i = 0; i < children.length; i++) {
                OctTreeNode current = children[i];
                Pair<Comparable,Comparable> minMaxCol = ((Pair<Comparable,Comparable>)(current.hMinMaxPerColumn.get(col.val1)));
                boolean cond1 = ((Comparable)minMaxCol.val1).compareTo(col.val2) <= 0;
                boolean cond2 = ((Comparable)minMaxCol.val2).compareTo(col.val2) >= 0;
                if (cond1 && cond2) {
                    vecNextOctTreeNode.add(current);
                }
            }

            Vector<OctTreeNode> vecResultOctTreeNodes = new Vector<OctTreeNode>();
            for (OctTreeNode next : vecNextOctTreeNode) {
                Vector<OctTreeNode> vecTmpKeys = next.searchOneColumn(col);
                for(OctTreeNode OctTreeNode : vecTmpKeys) {
                    vecResultOctTreeNodes.add(OctTreeNode);
                }
            }
            return vecResultOctTreeNodes;
        }
    }
}
