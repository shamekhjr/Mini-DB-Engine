import java.io.*;
import java.util.*;
import java.util.Hashtable;

public class Page implements Serializable {
    public Vector<Hashtable<String, Object>> vRecords; // contain list of hashtable which represent records
    public String sTableName;
    public String sClusteringKey;
    public int index;



    public Page(String sTable, int index, Boolean load)
    {
        this.vRecords = new Vector<>();
        this.sTableName = sTable;
        this.index = index;
        // load page
        if (load) {
            try {
                FileInputStream fis = new FileInputStream(this.sTableName + "_page" + index + ".class");
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

        int n = Integer.parseInt(configProperties.getProperty("MaximumRowsCountinTablePage"));

        return (n == vRecords.size());
    }

    public boolean isEmpty ()
    {
        return vRecords.isEmpty();
    }

    public void sortedInsert(Hashtable<String,Object> hInsertRow, String sClusteringKey) {
        // binary search the position to insert into
        int lo = 0;
        int hi = vRecords.size() - 1;
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

    public void updatePage(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
    {
        // Problem: What if we updated a record which need to inserted in another page ?
        // If the record need to be inserted in another page, then we need:
            // 1. Delete the record.
            // 2. Update the record.
            // 3. Insert the record.
    }

    public void deleteRecord(Hashtable<String, Object> htblColNameValue)
    {
        //this.deserializePage();
        Vector<Hashtable<String, Object>> tempvRecords = this.searchPage(htblColNameValue);
        int sizeOfPage = tempvRecords.size();

        for (int i = 0; i < sizeOfPage; i++) {
            Hashtable<String, Object> temphtblColNameValue = tempvRecords.get(i);
            vRecords.removeElement(temphtblColNameValue); // Problem: O(N) ?
        }
    }

    public Vector<Hashtable<String, Object>> searchPage(Hashtable<String, Object> hCondition) {
        //this.deserializePage();

        Vector<Hashtable<String, Object>> rvRecords = new Vector<>(); // the result of search could be a list of records
        int sizeOfPage = vRecords.size();

        // if the search is based on the Clustering Key then for sure their will be only one record
        // As the Clustering Key is the primary key which contain no duplicate
        if (hCondition.keySet().contains(sClusteringKey)) { // Binary Search on the Clustering Key (Recall: Clustering key is the Primary key)
            int left = 0, right = sizeOfPage - 1;
            int mid = -1;
            while (left <= right) {
                mid = (left + right) / 2;
                if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) > 0) {
                    left = mid + 1;
                } else if (((Comparable) vRecords.get(mid).get(sClusteringKey)).compareTo(((Comparable) hCondition.get(sClusteringKey))) < 0) {
                    right = mid;
                } else {
                    break;
                }
            }

            if (left <= right)
            {
                Hashtable<String, Object> rhtblColNameValue = vRecords.get(mid);
                rvRecords.add(rhtblColNameValue);
            }
        }
        else { // If the search is not based on the clustering key then their could be multiple records as output due to duplicate values in column
            for (int i = 0; i < sizeOfPage; i++) {
                Hashtable<String, Object> temphtblColNameValue = vRecords.get(i);
                Set<String> tempSet = temphtblColNameValue.keySet();
                boolean bAllSatisfied = true;
                for (String key : tempSet) {
                    if (!(temphtblColNameValue.get(key).equals(hCondition.get(key)))) {
                        bAllSatisfied = false;
                    }
                }
                if (bAllSatisfied) {
                    rvRecords.add(temphtblColNameValue);
                }
            }
        }
        this.serializePage();
        return rvRecords; // Return of the result of search is a Vector of HashTable (References to the Object)
    }
    public void serializePage () {
        try {
            FileOutputStream fos = new FileOutputStream(this.sTableName+"_page"+ index + ".class");
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

    // delete page
    public void deletePage() {
        //TODO search for delete code
    }
}
