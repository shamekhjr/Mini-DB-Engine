import java.io.Serializable;
import java.util.Date;

public class rangePair<M extends Serializable, N extends Serializable> implements Serializable {
    public M min;
    public N max;

    public rangePair(M min, N max) {
        this.min = min;
        this.max = max;
    }
}