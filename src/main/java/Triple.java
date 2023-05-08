import java.io.Serializable;

public class Triple<T extends Serializable, M extends Serializable, N extends Serializable> implements Serializable {
    public T val1;
    public M val2;
    public N val3;

    public Triple(T val1, M val2, N val3) {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }
}
