package org.garret.bench;

import java.io.PrintStream;
import com.db4o.*;
import com.db4o.config.*;
import com.db4o.query.Query;

/*
 * Record used in this example contains just two fields (columns)
 * We don't need to inherit from Persistent like in Perst
 */
class RecordDb4o { 
    String strKey;
    long   intKey;
};


public class Db4oTest extends Test { 
    final static int nRecords = Benchmark.TEST_ITERATIONS;
    
    String databaseName;
    PrintStream out;
    
    Db4oTest(String databaseName, PrintStream out) {
    	this.databaseName = databaseName;
    	this.out = out;
    }
    
    public String getName() { 
    	return "db4o";
    }
    
	private RecordDb4o getRecordByName(String name, ObjectContainer c) {
    	Query query = c.query();
    	query.constrain(RecordDb4o.class);
    	query.descend("strKey").constrain(name);
    	ObjectSet<RecordDb4o> result = query.execute();
    	if(result.hasNext())
    		return result.next();
    	return null;
    }
    
	private RecordDb4o getRecordByNumber(long number, ObjectContainer c) {
    	Query query = c.query();
    	query.constrain(RecordDb4o.class);
    	query.descend("intKey").constrain(number);
    	ObjectSet<RecordDb4o> result = query.execute();
    	if(result.hasNext())
    		return result.next();
    	return null;
    }
    
	private ObjectSet<RecordDb4o> getRecordsOrderedByName(ObjectContainer c){
    	Query query = c.query();
    	query.constrain(RecordDb4o.class);
    	query.descend("strKey").orderAscending();
    	return query.execute();
    }
    
	private ObjectSet<RecordDb4o> getRecordsOrderedByNumber(ObjectContainer c){
    	Query query = c.query();
    	query.constrain(RecordDb4o.class);
    	query.descend("intKey").orderAscending();
    	return query.execute();
    }
    
	public void run() {
        // Configure db4o database
        EmbeddedConfiguration config = Db4oEmbedded.newConfiguration();
        config.common().objectClass(RecordDb4o.class).objectField("intKey").indexed(true);
        config.common().objectClass(RecordDb4o.class).objectField("strKey").indexed(true);
        config.file().lockDatabaseFile(false);
        //config.idSystem().useInMemorySystem();
        //config.idSystem().usePointerBasedSystem();
        //config.idSystem().useSingleBTreeSystem();
        //config.common().bTreeNodeSize(6400);
        
        //Open database
        ObjectContainer oc = Db4oEmbedded.openFile(config, databaseName);
        
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;   
        // Insert data in the database
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            oc.store(rec);
        }
        oc.commit(); // Commit current transaction
        out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        // This loop perform index searches using both indices
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            RecordDb4o rec1 = this.getRecordByNumber(key, oc);
            RecordDb4o rec2 = this.getRecordByName(Long.toString(key), oc);
            assert(rec1 != null && rec1 == rec2);
        }
        out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        key = Long.MIN_VALUE;
        i = 0;
        // Perform iteration through all records using intKey index (records are sorted by intKey)
        ObjectSet<RecordDb4o> result = this.getRecordsOrderedByNumber(oc);
        while(result.hasNext()){
        	RecordDb4o rec = result.next();
        	assert(rec.intKey >= key);
            key = rec.intKey;
            i += 1;
        }
        assert(i == nRecords);
        String strKey = "";
        i = 0;
        // Perform iteration through all records using strKey index (records are sorted by strKey)
        result = this.getRecordsOrderedByName(oc);
        while(result.hasNext()){
        	RecordDb4o rec = result.next();
        	assert(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
            i += 1;
        }
        
        assert(i == nRecords);
        out.println("Elapsed time for iterating through " + (nRecords*2) + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        // Locate and remove all records
        for (i = 0; i < nRecords; i++) { 
        	key = (3141592621L*key + 2718281829L) % 1000000007L;
        	RecordDb4o rec = this.getRecordByNumber(key, oc);
        	if (rec != null)
        		oc.delete(rec);
        }
        // Check that no records are left in the database
        assert(oc.queryByExample(RecordDb4o.class).size() == 0);
        out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
                        
        // Close the database
        oc.close();
        out.flush();
        // Notify about test completion
        done();
    }
}
