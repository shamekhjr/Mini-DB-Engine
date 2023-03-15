import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Hashtable;
import java.io.FileInputStream;

public class Page {
    private Vector <Hashtable<String, Object>> records; // contain list of hashtable which represent records


    public Page()
    {
        Vector<Hashtable<String, Object>> records = new Vector<>();
    }

    public boolean isFull ()
    {
        String _filename = "DBApp.config";
        Properties _configProperties = new Properties();

        try (FileInputStream fis = new FileInputStream(_filename))
        {
            _configProperties.load(fis);
        }
        catch (FileNotFoundException ex)
        {
            /* do something */
        }
        catch (IOException e)
        {
            /* do something */
        }

        int n = Integer.parseInt(_configProperties.getProperty("DBApp.MaximumRowsCountinTablePage"));

        return n == records.size();
    }

    public boolean isEmpty ()
    {
        return records.isEmpty();
    }

    public void updatePage (String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
    {

    }

    public void deletePage(Hashtable<String, Object> htblColNameValue)
    {

    }
}
