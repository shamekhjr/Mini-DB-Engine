import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

public class OctTreeNode  implements Serializable {
    public Vector<Point> points;
    public OctTreeNode[] children;
    String[][] colNamesDatatypes;
    public Hashtable<String, Pair<Comparable, Comparable>> hMinMaxPerColumn; // Hashtable< Column Name, Pair< Minimum, Maximum> >
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
        return  (p.cols[0].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[0][0]).val1) >= 0 && p.cols[0].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[0][0]).val2) <= 0) &&
                (p.cols[1].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[1][0]).val1) >= 0 && p.cols[1].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[1][0]).val2) <= 0) &&
                (p.cols[2].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[2][0]).val1) >= 0 && p.cols[2].compareTo(hMinMaxPerColumn.get(colNamesDatatypes[2][0]).val2) <= 0);
   }

   public void sethMinMaxPerColumn(Hashtable<String, Pair<Comparable, Comparable>> in) {
       this.hMinMaxPerColumn = in;
   }

   public void insert(Point p) { // finds the not full wrapping OctTreeNode and inserts, insert/subdivide-&-insert
       if(!isLeaf) {
           for (OctTreeNode child: children) { // find child that wraps p
               if (child.wraps(p)) {
                   child.insert(p);
                   break;
               }
           }
       } else {
           if (this.wraps(p)) {
               for (Point entry: points) { // check if it is a duplicate
                   if (entry.equals(p)) {
                       entry.duplicates.add(p);
                       return;
                   }
               }
               if (points.size() < maxEntries) { // check if space
                   points.add(p);
               } else { // node is full, split
                   subdivide();
                   distribute();
                   this.isLeaf = false;
                   this.insert(p);
               }
           }
       }
   }

   public void delete(Point p) {
       if (isLeaf) {
               for (Point point: points) {
                   if (point.cols[0].equals(p.cols[0])
                           && point.cols[1].equals(p.cols[1])
                           && point.cols[2].equals(p.cols[2])
                           && point.reference == p.reference) {

                       point.duplicates = null;
                       points.remove(point);
                   }
               }

       } else {
           // find child that wraps e
           for (OctTreeNode child: children) {
               if (child.wraps(p)) {
                   child.delete(p);
               }
           }
       }

   }

   public void subdivide() {
       for (int i = 0; i < colNamesDatatypes.length; i++) {
           children[i] = new OctTreeNode(colNamesDatatypes, null, maxEntries);
       }

       // col1 = x, col2 = y, col3 = z
       int x = 0;
       for (String[] colData: colNamesDatatypes) {
           int finalX = x;
           if (colData[1].equals("java.lang.Integer")) {
               Pair<Comparable, Comparable> minMax = hMinMaxPerColumn.get(colData[0]);
               int min = (int) minMax.val1;
               int max = (int) minMax.val1;

               // get mid from min and max
               int mid = min + (max - min) / 2;
               children[0].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], new Pair<>(min, mid));
               }});
               children[1].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(min, mid));
               }});
               children[2].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[3].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[4].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0 || finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[5].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[6].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
               children[7].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
           }
           else if (colData[1].equals("java.lang.String")) {
               Pair<Comparable, Comparable> minMax = hMinMaxPerColumn.get(colData[0]);
               String Min = (String) minMax.val1;
               String max = (String) minMax.val1;
               Min = concatExtra(Min, max);

               // get mid from min and max
               String mid = getMidStrings(Min, max, max.length());
               String min = Min;
               children[0].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], new Pair<>(min, mid));
               }});
               children[1].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(min, mid));
               }});
               children[2].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[3].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[4].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0 || finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[5].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[6].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
               children[7].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
           }
           else if (colData[1].equals("java.lang.Double")) {
               Pair<Comparable, Comparable> minMax = hMinMaxPerColumn.get(colData[0]);
               double min = (double) minMax.val1;
               double max = (double) minMax.val1;

               // get mid from min and max
               double mid = min + (max - min) / 2.0;
               children[0].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], new Pair<>(min, mid));
               }});
               children[1].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(min, mid));
               }});
               children[2].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[3].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[4].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0 || finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[5].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[6].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
               children[7].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});

           }
           else if (colData[1].equals("java.util.Date")) {
               Pair<Comparable, Comparable> minMax = hMinMaxPerColumn.get(colData[0]);
               Date min = (Date) minMax.val1;
               Date max = (Date) minMax.val1;

               // get mid from min and max
               Date mid = new Date((min.getTime() + max.getTime()) / 2);
               children[0].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], new Pair<>(min, mid));
               }});
               children[1].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(min, mid));
               }});
               children[2].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[3].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(min, mid));
               }});
               children[4].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0 || finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[5].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(min, mid) : new Pair<>(mid, max));
               }});
               children[6].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(min, mid) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
               children[7].sethMinMaxPerColumn(new Hashtable<String, Pair<Comparable,Comparable>>() {{
                   put(colData[0], (finalX == 0) ? new Pair<>(mid, max) : (finalX == 1) ? new Pair<>(mid, max) : new Pair<>(mid, max));
               }});
           }
           x++;
       }
   }

   public String concatExtra(String s1, String s2) {
       return (s1.length() < s2.length()) ? s1 + s2.substring(s1.length()): s1;
   }

   public String getMidStrings(String S, String T, int N) {
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

       StringBuilder sb = new StringBuilder();
       for (int i = 1; i <= N; i++) {
           sb.append((char)(a1[i] + 97));
       }
         return sb.toString();

   }

   //check the subdivide function for dissecting ranges of a node in order for its children to be assigned to them?

   public void distribute() {
       //assigning points to children according to their ranges
       for (Point p: points) {
           for (OctTreeNode child: children) {
               if (child.wraps(p)) {
                   child.insert(p);
               }
           }
       }
   }

   public void insertInChildren(Point p) {

   }

    // Deprecated
   public Vector<Integer> eliminateDuplicatePageNumbers (Vector<Integer> vecPageNumbers) {
       Vector<Integer> resultPageNumbers = new Vector<>();
       for (Integer i : vecPageNumbers) {
           if (!resultPageNumbers.contains(i)) {
               resultPageNumbers.add(i);
           }
       }
       return resultPageNumbers;
   }

   /*
       Use the Octree search:
        "if the 3 columns an octree was created on appear in sql term and they are Anded together, then use octree"
         -- Wael Aboulsaadat

         AKA only if the 3 columns are used SIMULTANEOUSLY in the query, then use the octree.
    */
   public Vector<OctTreeNode> searchNode (SQLTerm[] arrSQLTerms) {
        if (isLeaf) {
            Vector<OctTreeNode> vecResultOctTreeNode = new Vector<>();
            vecResultOctTreeNode.add(this);
            return vecResultOctTreeNode;
        }
        else {
            Vector<OctTreeNode> vecNextOctTreeNode = new Vector<>();
            // For each SQLTerm, there will be number of children nodes with range that satisfies it.
            // Initially, we want to traverse all children.
            boolean[] nextChildren = new boolean[8]; // nextChildren[i] == true -> we are going to traverse the subtree at children[i].
            for (int i = 0; i < nextChildren.length; i++) {
                nextChildren[i] = true;
            }

            for (int i = 0; i < arrSQLTerms.length; i++) {
                SQLTerm sqlTerm = arrSQLTerms[i];
                for (int j = 0; j < children.length; j++) {
                    OctTreeNode current = children[j];
                    Pair<Comparable, Comparable> minMaxCol = ((Pair<Comparable, Comparable>) (current.hMinMaxPerColumn.get(sqlTerm._strColumnName)));
                    boolean condition;

                    if (sqlTerm._strOperator.equals("=")) { // Node(Min, Max) = SQLTerm._objValue;
                        // condition = (Min <= SQLTerm._objValue && Max >= SQLTerm._objValue);
                        boolean cond1 = ((Comparable) minMaxCol.val1).compareTo((Comparable) (sqlTerm._objValue)) <= 0;
                        boolean cond2 = ((Comparable) minMaxCol.val2).compareTo((Comparable) (sqlTerm._objValue)) >= 0;
                        condition = cond1 && cond2;
                    } else if (sqlTerm._strOperator.equals(">")) { // Node(Max) > SQLTerm._objValue;
                        // condition = (Max > SQLTerm._objValue);
                        condition = ((Comparable) minMaxCol.val2).compareTo((Comparable) (sqlTerm._objValue)) > 0;
                    } else if (sqlTerm._strOperator.equals("<")) { // Node(Min) < SQLTerm._objValue;
                        // condition = (Min < SQLTerm._objValue);
                        condition = ((Comparable) minMaxCol.val1).compareTo((Comparable) (sqlTerm._objValue)) < 0;
                    } else if (sqlTerm._strOperator.equals(">=")) { // Node(Max) >= SQLTerm._objValue;
                        // condition = (Max >= SQLTerm._objValue);
                        condition = ((Comparable) minMaxCol.val2).compareTo((Comparable) (sqlTerm._objValue)) >= 0;
                    } else if (sqlTerm._strOperator.equals("<=")) { // Node(Min) <= SQLTerm._objValue;
                        // condition = (Min <= SQLTerm._objValue);
                        condition = ((Comparable) minMaxCol.val1).compareTo((Comparable) (sqlTerm._objValue)) <= 0;
                    } else { // Node(Max) != SQLTerm._objValue;
                        // condition = ALWAYS TRUE;
                        // Note: we check for not equal at the leaf nodes as we need to compare each Point with the SQLTerm.objValue
                        condition = true;
                    }

                    // If (the condition to go to the children node is false) then { nextChildren[j] = false }
                    if (!condition) {
                        nextChildren[j] = false;
                    }
                }
            }

            for (int i = 0; i < children.length; i++) {
                if (nextChildren[i]) {
                    vecNextOctTreeNode.add(children[i]);
                }
            }

            Vector<OctTreeNode> vecResultOctTreeNodes = new Vector<>();
            for (OctTreeNode next : vecNextOctTreeNode) {
                Vector<OctTreeNode> vecTmpNodes = next.searchNode(arrSQLTerms);
                for(OctTreeNode OctTreeNode : vecTmpNodes) {
                    vecResultOctTreeNodes.add(OctTreeNode);
                }
            }
            return vecResultOctTreeNodes;
        }
   }

    public Vector<OctTreeNode> searchExactQueries (Hashtable<String, Object> htblColNameValue) {
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
        else if (htblColNameValue.size() == 1) {
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

    public Point searchPkAttributeAndDelete(Comparable pk) {
        if (isLeaf) {
            for (int i = 0; i < points.size(); i++) {
                if (points.get(i).pkValue.compareTo(pk) == 0) {
                    Point p = points.remove(i);
                    return p;
                }

                for (int j = 0; j < points.get(i).duplicates.size(); j++) {
                    if (points.get(i).duplicates.get(j).pkValue.compareTo(pk) == 0) {
                        Point p = points.get(i).duplicates.remove(j);
                        return p;
                    }
                }
            }
            return null;
        }
        else {
            for (int i = 0; i < children.length; i++) {
                OctTreeNode current = children[i];
                Point res =  current.searchPkAttributeAndDelete(pk);
                if (res != null) {
                    return res;
                }
            }
            return null;
        }
    }

    public void updateRefNum(int oldIndex, int newIndex) {
        if (isLeaf) {
            for (int i = 0; i < points.size(); i++) {
                if (points.get(i).reference == oldIndex) {
                    points.get(i).reference = newIndex;
                }
                for (int j = 0; j < points.get(i).duplicates.size(); j++) {
                    if (points.get(i).duplicates.get(j).reference == oldIndex) {
                        points.get(i).duplicates.get(j).reference = newIndex;
                    }
                }
            }
        }
        else {
            for (int i = 0; i < children.length; i++) {
                OctTreeNode current = children[i];
                current.updateRefNum(oldIndex, newIndex);
            }
        }
    }
}
