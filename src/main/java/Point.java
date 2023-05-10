import java.io.Serializable;
import java.util.Vector;

public class Point implements Serializable {
    Comparable[] cols;
    int reference; // page num only // DEPATE
    Vector<Integer> references = new Vector<>(); // a Point could contain multiple page numbers if there is duplicates
    Vector<Point> duplicates;
    Comparable pkValue; // useful when updating

    public Point(Comparable col1, Comparable col2, Comparable col3, int reference, Comparable pkValue) {
        this.cols[0] = col1;
        this.cols[1] = col2;
        this.cols[2] = col3;
        reference = reference;
        references.add(reference);
        this.duplicates = new Vector<>();
        this.pkValue = pkValue;
    }

    public boolean equals(Object o) {
        Point e = (Point) o;
        return this.cols[0].equals(e.cols[0]) && this.cols[1].equals(e.cols[1]) && this.cols[2].equals(e.cols[2]);
    }
}
