import java.io.Serializable;
import java.util.Vector;

public class OctTreeNode  implements Serializable {
    public Comparable minC1, minC2, minC3, maxC1, maxC2, maxC3;
    public Vector<Point> points;
    public OctTreeNode[] children;
    String[] colNames;
    public int maxEntries;

   public OctTreeNode(Comparable minC1, Comparable minC2, Comparable minC3, Comparable maxC1, Comparable maxC2, Comparable maxC3, int maxEntries) {
        this.minC1 = minC1;
        this.minC2 = minC2;
        this.minC3 = minC3;
        this.maxC1 = maxC1;
        this.maxC2 = maxC2;
        this.maxC3 = maxC3;
        this.maxEntries = maxEntries;
        this.points = new Vector<>(maxEntries);
        this.children = new OctTreeNode[8];
   }

   public boolean isFull() {
        return points.size() == maxEntries;
   }

   public boolean isEmpty() {
        return points.size() == 0;
   }

   public boolean wraps(Point entry) {
        return (entry.col1.compareTo(minC1) >= 0 && entry.col1.compareTo(maxC1) <= 0) &&
                (entry.col2.compareTo(minC2) >= 0 && entry.col2.compareTo(maxC2) <= 0) &&
                (entry.col3.compareTo(minC3) >= 0 && entry.col3.compareTo(maxC3) <= 0);
   }

   public void insert(Point p) { // finds the not full wrapping node and inserts, insert/subdivide-&-insert
       if (this.wraps(p) && points.size() < maxEntries) {
           points.add(p);
       } else if (true) { // has children
              // find child that wraps e
              // insert in child
       } else {
           // TODO
           subdivide();
           distribute();
           insertInChildren(p);
           // insert in division
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
}
