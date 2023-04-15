public class Null {
    public static  Null NULL;

    public static Null getInstance(){
        if(NULL == null){
            NULL = new Null();
        }
        return NULL;
    }
    @Override
    public boolean equals(Object obj) {
        return obj.getClass().equals(Null.class);
    }

    @Override
    public String toString() {
        return "Null";
    }
}
