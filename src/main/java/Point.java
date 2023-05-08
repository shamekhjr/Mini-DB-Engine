import java.io.Serializable;
import java.util.Vector;

public class Point implements Serializable {
    Comparable col1;
    Comparable col2;
    Comparable col3;
    int reference; // page num only
    Vector<Point> duplicates;
    Object pkValue; // useful when updating

    public Point(Comparable col1, Comparable col2, Comparable col3, int reference) {
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
        this.reference = reference;
        this.duplicates = new Vector<>();
    }

    public boolean equals(Object o) {
        Point e = (Point) o;
        return this.col1.equals(e.col1) && this.col2.equals(e.col2) && this.col3.equals(e.col3);
    }
}
