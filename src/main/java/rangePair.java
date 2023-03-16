public class rangePair<Mn, Mx> {
    public Mn min;
    public Mx max;

    public rangePair(Mn min, Mx max) {
        this.min = min;
        this.max = max;
    }

    public void updateMinMax(Mn min, Mx Max) {
        this.min = min;
        this.max = max;
    }
}