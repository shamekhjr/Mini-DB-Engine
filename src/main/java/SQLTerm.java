public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
        _strTableName = strTableName;
        _strColumnName = strColumnName;
        _strOperator = strOperator;
        _objValue = objValue;
    }

    //empty constructor
    public SQLTerm() {

    }


}
