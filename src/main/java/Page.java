import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Hashtable;

public class Page implements Serializable {
    @Serial
    private static final long serialVersionUID = 987662595421l;
    public Vector<Hashtable<String, Object>> vRecords; // contain list of hashtable which represent records
    public String sTableName;
    public String sClusteringKey;
    public int index;
    public int maxRecords;



    public Page(String sTable, String sClusteringKey, int index, Boolean load)
    {

        this.sTableName = sTable;
        this.index = index;
        this.sClusteringKey = sClusteringKey;
        String _filename = "src/main/resources/DBApp.config"; // File that contain configuration
        Properties configProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(_filename))
        {
            configProperties.load(fis);
        }
        catch (FileNotFoundException ex)
        {
            ex.printStackTrace();
        }
        catch (IOException e)  {
            e.printStackTrace();
        }
        this.maxRecords = Integer.parseInt(configProperties.getProperty("MaximumRowsCountinTablePage"));
        this.vRecords = new Vector<>(maxRecords);
        // load page
        if (load) {
            try {
                FileInputStream fis = new FileInputStream("src/main/resources/data/"+this.sTableName+"/"+this.sTableName + "_page" + index + ".class");
                ObjectInputStream ois = new ObjectInputStream(fis);
                this.vRecords = (Vector<Hashtable<String, Object>>) ois.readObject();
                ois.close();
                fis.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public int size() {
        return vRecords.size();
    }

    public boolean isFull ()
    {
        return (maxRecords == vRecords.size());
    }

    public boolean isEmpty ()
    {
        return vRecords.isEmpty();
    }

    public void sortedInsert(Hashtable<String,Object> hInsertRow, String sClusteringKey) {
        // binary search the position to insert into
        int lo = 0;
        int hi = vRecords.size() - 1;

        // check minMax first
        if (((Comparable)hInsertRow.get(sClusteringKey)).compareTo(vRecords.get(lo).get(sClusteringKey)) <= 0) {
            vRecords.add(lo, hInsertRow);
            return;
        } else if (((Comparable)hInsertRow.get(sClusteringKey)).compareTo(vRecords.get(hi).get(sClusteringKey)) >= 0) {
            vRecords.add(hInsertRow);
            return;
        }
        //System.out.println(hi);
        int mid = (lo + hi) / 2;
        while (lo < hi - 1) {
            Hashtable<String, Object> hMidRow = vRecords.get(mid);
            if (hMidRow.get(sClusteringKey).equals(hInsertRow.get(sClusteringKey))) {
                vRecords.add(mid, hInsertRow);
                return;
            } else if (((Comparable)hMidRow.get(sClusteringKey)).compareTo((Comparable)hInsertRow.get(sClusteringKey)) < 0) {
                lo = mid;
            } else {
                hi = mid;
            }
            mid = (lo + hi) / 2;
        }
        if (((Comparable)hInsertRow.get(sClusteringKey)).compareTo(vRecords.get(hi).get(sClusteringKey)) > 0) vRecords.add(hi + 1, hInsertRow);
        else if (((Comparable)hInsertRow.get(sClusteringKey)).compareTo(vRecords.get(lo).get(sClusteringKey)) < 0) vRecords.add(lo, hInsertRow);
        else vRecords.add(hi, hInsertRow);
    }

    public void updatePage(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws CsvValidationException, IOException, ParseException, DBAppException {

        // Step 1: Search for the targeted record based on the Clustering key.
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);

        Vector<Integer> tempvRecords = this.searchPage(htblColNameValue);
        int size = tempvRecords.size();

        if (size == 0) {
            throw new DBAppException("Primary key does not exist");
        }

        for (int i : tempvRecords) {
            // Get the index of the record in the page
            Hashtable<String, Object> hTemp = vRecords.get(i);
            for (String key : htblColNameValue.keySet())
            {
                    hTemp.put(key, htblColNameValue.get(key));
            }
        }
    }

    public void deleteRecord(Hashtable<String, Object> htblColNameValue)
    {
        //this.deserializePage();
        // [Old] Vector<Hashtable<String, Object>> tempvRecords = this.searchPage(htblColNameValue);
        Vector<Integer> tempvRecords = this.searchPage(htblColNameValue);
        int sizeOfPage = tempvRecords.size();

        for (int i = 0; i < sizeOfPage; i++) {
            // [Old]
            /*Hashtable<String, Object> temphtblColNameValue = tempvRecords.get(i);
            vRecords.removeElement(temphtblColNameValue);*/
            int recIndex = tempvRecords.get(i);
            vRecords.remove(recIndex);
        }
    }

    public Vector<Integer> searchPage(Hashtable<String, Object> hCondition) {
        //this.deserializePage();

        Vector<Integer> retVRecords = new Vector<>();
        Vector<Hashtable<String, Object>> rvRecords = new Vector<>(); // the result of search could be a list of records
        int sizeOfPage = vRecords.size();

        // if the search is based on the Clustering Key then for sure their will be only one record
        // As the Clustering Key is the primary key which contain no duplicate
        if (hCondition.keySet().contains(sClusteringKey)) { // Binary Search on the Clustering Key (Recall: Clustering key is the Primary key)
            int left = 0, right = sizeOfPage - 1;
            int mid = -1;
            while (left <= right) {
                mid = left + (right - left) / 2;
                if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) < 0) {
                    left = mid + 1;
                } else if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) > 0) {
                    right = mid - 1;
                } else {
                    break;
                }
            }

            if (left <= right)
            {
                Hashtable<String, Object> rhtblColNameValue = vRecords.get(mid);
                rvRecords.add(rhtblColNameValue);
                retVRecords.add(mid);
            }
        }
        else { // If the search is not based on the clustering key then their could be multiple records as output due to duplicate values in column
            for (int i = 0; i < sizeOfPage; i++) {
                Hashtable<String, Object> temphtblColNameValue = vRecords.get(i);

                boolean bAllSatisfied = true;
                for (String key : temphtblColNameValue.keySet()) { // Problem: Set are not thread safe :(
                    if (!(temphtblColNameValue.get(key).equals(hCondition.get(key)))) {
                        bAllSatisfied = false;
                    }
                }
                if (bAllSatisfied) {
                    rvRecords.add(temphtblColNameValue);
                    retVRecords.add(i);
                }
            }
        }
        this.serializePage();
        // [Old] return rvRecords; Return of the result of search is a Vector of HashTable (References to the Object)
        return retVRecords; // Return vector of index so that we can access the records
    }
    public void serializePage () {
        try {
            FileOutputStream fos = new FileOutputStream("src/main/resources/data/"+this.sTableName +"/"+this.sTableName+"_page"+ index + ".class");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(vRecords);
            oos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // deprecated
    public void deserializePage() {
        try {
            FileInputStream fis = new FileInputStream(this.sTableName+"_page"+ index + ".class");
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.vRecords = (Vector<Hashtable<String, Object>>) ois.readObject();
            ois.close();
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void deletePage() {
        File myObj = new File("src/main/resources/data/"+this.sTableName+"/"+this.sTableName+"_page"+ index + ".class");
        myObj.delete();

        // encourage the garbage collector to free the memory
        this.vRecords = null;
        myObj = null;
        System.gc();
    }

    public Vector<Pair<Integer,Hashtable<String,Object>>> searchFromPage(Hashtable<String, Object> hCondition) {
        //this.deserializePage();

        Vector<Pair<Integer, Hashtable<String, Object>>> retVRecords = new Vector<>();
        int sizeOfPage = vRecords.size();

        // if the search is based on the Clustering Key then for sure their will be only one record
        // As the Clustering Key is the primary key which contain no duplicate
        if (hCondition.keySet().contains(sClusteringKey)) { // Binary Search on the Clustering Key (Recall: Clustering key is the Primary key)
            int left = 0, right = sizeOfPage - 1;
            int mid = -1;
            while (left <= right) {
                mid = left + (right - left) / 2;
                if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) < 0) {
                    left = mid + 1;
                } else if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) > 0) {
                    right = mid - 1;
                } else {
                    break;
                }
            }

            if (left <= right)
            {
                Hashtable<String, Object> rhtblColNameValue = vRecords.get(mid);
                retVRecords.add(new Pair<Integer, Hashtable<String, Object>>(mid,rhtblColNameValue));
            }
        }
        else { // If the search is not based on the clustering key then their could be multiple records as output due to duplicate values in column
            for (int i = 0; i < sizeOfPage; i++) {
                Hashtable<String, Object> temphtblColNameValue = vRecords.get(i);

                boolean bAllSatisfied = true;
                for (String key : temphtblColNameValue.keySet()) { // Problem: Set are not thread safe :(
                    if (!(temphtblColNameValue.get(key).equals(hCondition.get(key)))) {
                        bAllSatisfied = false;
                    }
                }
                if (bAllSatisfied) {
                    retVRecords.add(new Pair<Integer, Hashtable<String, Object>>(i,temphtblColNameValue));
                }
            }
        }
        this.serializePage();
        return retVRecords; // Return vector of index so that we can access the records
    }
}


