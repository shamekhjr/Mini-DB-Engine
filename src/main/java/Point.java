import java.io.Serializable;
import java.util.Vector;

public class Point implements Serializable {
    Comparable[] cols;
    int reference; // page num only
    Vector<Point> duplicates;
    Comparable pkValue; // useful when updating

    public Point(Comparable col1, Comparable col2, Comparable col3, int reference, Comparable pkValue) {
        this.cols = new Comparable[3];
        this.cols[0] = col1;
        this.cols[1] = col2;
        this.cols[2] = col3;
        this.reference = reference;
        this.duplicates = new Vector<>();
        this.pkValue = pkValue;
    }

    public boolean equals(Object o) {
        Point e = (Point) o;
        return this.cols[0].equals(e.cols[0]) && this.cols[1].equals(e.cols[1]) && this.cols[2].equals(e.cols[2]);
    }
}
