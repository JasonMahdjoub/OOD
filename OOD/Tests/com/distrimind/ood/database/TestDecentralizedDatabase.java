/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
 */
package com.distrimind.ood.database;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.distrimind.ood.database.BigDatabaseEventToSend;
import com.distrimind.ood.database.DatabaseConfiguration;
import com.distrimind.ood.database.DatabaseCreationCallable;
import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseEventToSend;
import com.distrimind.ood.database.DatabaseEventType;
import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SynchronizedTransaction;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.TableEvent;
import com.distrimind.ood.database.TransactionIsolation;
import com.distrimind.ood.database.decentralizeddatabase.TableAlone;
import com.distrimind.ood.database.decentralizeddatabase.TablePointed;
import com.distrimind.ood.database.decentralizeddatabase.TablePointing;
import com.distrimind.ood.database.decentralizeddatabase.UndecentralizableTableA1;
import com.distrimind.ood.database.decentralizeddatabase.UndecentralizableTableB1;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public abstract class TestDecentralizedDatabase
{
    public static class DistantDatabaseEvent
    {
	private final byte[] eventToSend;
	private final byte[] joindedData;
	private final AbstractDecentralizedID hostDest;
	
	DistantDatabaseEvent(DatabaseWrapper wrapper, DatabaseEventToSend eventToSend) throws DatabaseException, IOException
	{
	    try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
	    {
		try(ObjectOutputStream oos=new ObjectOutputStream(baos))
		{
		    oos.writeObject(eventToSend);
		}
	    
		baos.flush();
		
		this.eventToSend=baos.toByteArray();
	    }
	    if (eventToSend instanceof BigDatabaseEventToSend)
	    {
		BigDatabaseEventToSend b=(BigDatabaseEventToSend)eventToSend;
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    b.exportToOutputStream(wrapper, baos);
		    baos.flush();
		    this.joindedData=baos.toByteArray();
		}
	    }
	    else
		this.joindedData=null;
	    hostDest=eventToSend.getHostDestination();
	}
	
	public DatabaseEventToSend getDatabaseEventToSend() throws IOException, ClassNotFoundException
	{
	    try(ByteArrayInputStream bais=new ByteArrayInputStream(eventToSend))
	    {
		try(ObjectInputStream ois=new ObjectInputStream(bais))
		{
		    return (DatabaseEventToSend)ois.readObject();
		}
	    }
	}
	
	public InputStream getInputStream() 
	{
	    return new ByteArrayInputStream(joindedData);
	}
	
	public AbstractDecentralizedID getHostDestination()
	{
	    return hostDest;
	}
    }
    
    public class Database implements AutoCloseable, DatabaseWrapper.DatabaseNotifier
    {
	private final DatabaseWrapper dbwrapper;
	private boolean connected;
	private final AbstractDecentralizedID hostID;
	private final ArrayList<DatabaseEvent> localEvents;
	private final List<DistantDatabaseEvent> eventsReceivedStack;
	private TableAlone tableAlone;
	private TablePointed tablePointed;
	private TablePointing tablePointing;
	private UndecentralizableTableA1 undecentralizableTableA1;
	private UndecentralizableTableB1 undecentralizableTableB1;
	private boolean newDatabaseEventDetected=false;
	private boolean replaceWhenDirectCollisionDetected=false;
	private boolean replaceWhenIndirectCollisionDetected=false;
	private DirectCollisionDetected directCollisionDetected=null;
	private IndirectCollisionDetected indirectCollisionDetected=null;
	private TablePointed.Record recordPointed=null;
	private TablePointing.Record recordPointingNull=null, recordPointingNotNull=null;
	private TableAlone.Record recordAlone=null;
	private final List<Map<String, Object>> recordsToRemoveNotFound, recordsToUpdateNotFound;
	
	
	Database(DatabaseWrapper dbwrapper) throws DatabaseException
	{
	    this.dbwrapper=dbwrapper;
	    connected=false;
	    hostID=new DecentralizedIDGenerator();
	    localEvents=new ArrayList<>();
	    eventsReceivedStack=Collections.synchronizedList(new LinkedList<DistantDatabaseEvent>());
	    dbwrapper.loadDatabase(new DatabaseConfiguration(TableAlone.class.getPackage()), true);
	    tableAlone=(TableAlone)dbwrapper.getTableInstance(TableAlone.class);
	    tablePointed=(TablePointed)dbwrapper.getTableInstance(TablePointed.class);
	    tablePointing=(TablePointing)dbwrapper.getTableInstance(TablePointing.class);
	    undecentralizableTableA1=(UndecentralizableTableA1)dbwrapper.getTableInstance(UndecentralizableTableA1.class);
	    undecentralizableTableB1=(UndecentralizableTableB1)dbwrapper.getTableInstance(UndecentralizableTableB1.class);
	    recordsToRemoveNotFound=Collections.synchronizedList(new ArrayList<Map<String, Object>>());
	    recordsToUpdateNotFound=Collections.synchronizedList(new ArrayList<Map<String, Object>>());
	}

	public TableAlone getTableAlone()
	{
	    return tableAlone;
	}

	public TablePointed getTablePointed()
	{
	    return tablePointed;
	}

	public TablePointing getTablePointing()
	{
	    return tablePointing;
	}

	public UndecentralizableTableA1 getUndecentralizableTableA1()
	{
	    return undecentralizableTableA1;
	}

	public UndecentralizableTableB1 getUndecentralizableTableB1()
	{
	    return undecentralizableTableB1;
	}

	public boolean isConnected()
	{
	    return connected;
	}

	public void setConnected(boolean _connected)
	{
	    connected = _connected;
	}

	public DatabaseWrapper getDbwrapper()
	{
	    return dbwrapper;
	}
	
	@Override
	public void close()
	{
	    dbwrapper.close();
	}

	public AbstractDecentralizedID getHostID()
	{
	    return hostID;
	}
	public ArrayList<DatabaseEvent> getLocalEvents()
	{
	    return localEvents;
	}
	public void clearPendingEvents()
	{
	    localEvents.clear();
	    this.eventsReceivedStack.clear();
	    newDatabaseEventDetected=false;
	    indirectCollisionDetected=null;
	    directCollisionDetected=null;
	}
	
	public boolean isNewDatabaseEventDetected()
	{
	    return newDatabaseEventDetected;
	}
	
	public void setNewDatabaseEventDetected(boolean newDatabaseEventDetected)
	{
	    this.newDatabaseEventDetected=newDatabaseEventDetected;
	}
	
	public List<DistantDatabaseEvent> getReceivedDatabaseEvents()
	{
	    return this.eventsReceivedStack;
	}

	@Override
	public void newDatabaseEventDetected(DatabaseWrapper _wrapper)
	{
	    newDatabaseEventDetected=true;
	}



	@Override
	public boolean directCollisionDetected(AbstractDecentralizedID _distantPeerID, DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues) 
	{
	    directCollisionDetected=new DirectCollisionDetected(_distantPeerID, _type, _concernedTable, _keys, _newValues, _actualValues);
	    return replaceWhenDirectCollisionDetected;
	}

	@Override
	public boolean indirectCollisionDetected(AbstractDecentralizedID _distantPeerID, DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues, AbstractDecentralizedID _distantPeerInCollisionWithDataToSynchronize) 
	{
	    indirectCollisionDetected=new IndirectCollisionDetected(_distantPeerID, _type, _concernedTable, _keys, _newValues, _actualValues, _distantPeerInCollisionWithDataToSynchronize);
	    return replaceWhenIndirectCollisionDetected;
	}
	
	
	public boolean isReplaceWhenDirectCollisionDetected()
	{
	    return replaceWhenDirectCollisionDetected;
	}

	public void setReplaceWhenDirectCollisionDetected(boolean _replaceWhenDirectCollisionDetected)
	{
	    replaceWhenDirectCollisionDetected = _replaceWhenDirectCollisionDetected;
	}

	public boolean isReplaceWhenIndirectCollisionDetected()
	{
	    return replaceWhenIndirectCollisionDetected;
	}

	public void setReplaceWhenIndirectCollisionDetected(boolean _replaceWhenIndirectCollisionDetected)
	{
	    replaceWhenIndirectCollisionDetected = _replaceWhenIndirectCollisionDetected;
	}

	public DirectCollisionDetected getDirectCollisionDetected()
	{
	    return directCollisionDetected;
	}

	public IndirectCollisionDetected getIndirectCollisionDetected()
	{
	    return indirectCollisionDetected;
	}

	public TablePointed.Record getRecordPointed()
	{
	    return recordPointed;
	}

	public void setRecordPointed(TablePointed.Record _recordPointed)
	{
	    recordPointed = _recordPointed;
	}

	public TablePointing.Record getRecordPointingNull()
	{
	    return recordPointingNull;
	}
	public TablePointing.Record getRecordPointingNotNull()
	{
	    return recordPointingNotNull;
	}

	public void setRecordPointingNull(TablePointing.Record _recordPointing)
	{
	    recordPointingNull = _recordPointing;
	}
	public void setRecordPointingNotNull(TablePointing.Record _recordPointing)
	{
	    recordPointingNotNull = _recordPointing;
	}

	public TableAlone.Record getRecordAlone()
	{
	    return recordAlone;
	}

	public void setRecordAlone(TableAlone.Record _recordAlone)
	{
	    recordAlone = _recordAlone;
	}

	@Override
	public void recordToRemoveNotFound(Table<?> _concernedTable, Map<String, Object> _primary_keys)
	{
	    recordsToRemoveNotFound.add(_primary_keys);
	}

	@Override
	public void recordToUpdateNotFound(Table<?> _concernedTable, Map<String, Object> _primary_keys)
	{
	    recordsToUpdateNotFound.add(_primary_keys);
	}

	public List<Map<String, Object>> getRecordsToRemoveNotFound()
	{
	    return recordsToRemoveNotFound;
	}

	public List<Map<String, Object>> getRecordsToUpdateNotFound()
	{
	    return recordsToUpdateNotFound;
	}
	
    }
    public static class DirectCollisionDetected
    {
	final AbstractDecentralizedID distantPeerID;
	final DatabaseEventType type;
	final Table<?> concernedTable;
	final HashMap<String, Object> keys;
	final DatabaseRecord newValues;
	final DatabaseRecord actualValues;
	public DirectCollisionDetected(AbstractDecentralizedID _distantPeerID, DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues)
	{
	    super();
	    distantPeerID = _distantPeerID;
	    type = _type;
	    concernedTable = _concernedTable;
	    keys = _keys;
	    newValues = _newValues;
	    actualValues = _actualValues;
	}
	
    }
    
    public static class IndirectCollisionDetected
    {
	final AbstractDecentralizedID distantPeerID;
	final DatabaseEventType type;
	final Table<?> concernedTable;
	final HashMap<String, Object> keys;
	final DatabaseRecord newValues;
	final DatabaseRecord actualValues;
	final AbstractDecentralizedID distantPeerInCollisionWithDataToSynchronize;
	public IndirectCollisionDetected(AbstractDecentralizedID _distantPeerID, DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues, AbstractDecentralizedID _distantPeerInCollisionWithDataToSynchronize)
	{
	    super();
	    distantPeerID = _distantPeerID;
	    type = _type;
	    concernedTable = _concernedTable;
	    keys = _keys;
	    newValues = _newValues;
	    actualValues = _actualValues;
	    distantPeerInCollisionWithDataToSynchronize = _distantPeerInCollisionWithDataToSynchronize;
	}
    }
    
    private volatile Database db1=null, db2=null, db3=null, db4=null;
    private final ArrayList<Database> listDatabase=new ArrayList<>(3);
    
    
    public abstract DatabaseWrapper getDatabaseWrapperInstance1() throws IllegalArgumentException, DatabaseException;
    public abstract DatabaseWrapper getDatabaseWrapperInstance2() throws IllegalArgumentException, DatabaseException;
    public abstract DatabaseWrapper getDatabaseWrapperInstance3() throws IllegalArgumentException, DatabaseException;
    public abstract DatabaseWrapper getDatabaseWrapperInstance4() throws IllegalArgumentException, DatabaseException;
    public abstract void removeDatabaseFiles1();
    public abstract void removeDatabaseFiles2();
    public abstract void removeDatabaseFiles3();
    public abstract void removeDatabaseFiles4();
    
    
    
    public void loadDatabase(Database db) throws DatabaseException
    {
	db.getDbwrapper().loadDatabase(new DatabaseConfiguration(TableAlone.class.getPackage(), new DatabaseCreationCallable() {
	    
	    @Override
	    public void transfertDatabaseFromOldVersion(DatabaseConfiguration _newDatabaseTables) throws Exception
	    {		
	    }
	    
	    @Override
	    public boolean hasToRemoveOldDatabase() throws Exception
	    {
		return false;
	    }
	    
	    @Override
	    public void afterDatabaseCreation(DatabaseConfiguration _newDatabaseTables) throws Exception
	    {
		
	    }
	}, null), true);
	
    }
    
    @BeforeClass
    public void loadDatabase() throws DatabaseException
    {
	unloadDatabase();
	db1=new Database(getDatabaseWrapperInstance1());
	db2=new Database(getDatabaseWrapperInstance2());
	db3=new Database(getDatabaseWrapperInstance3());
	listDatabase.add(db1);
	listDatabase.add(db2);
	listDatabase.add(db3);
    }

    
    
    
    
    public void unloadDatabase1()
    {
	if (db1!=null)
	{
	    try
	    {
		db1.close();
	    }
	    finally
	    {
		db1=null;
	    }
	}
	removeDatabaseFiles1();
    }
    public void unloadDatabase2()
    {
	if (db2!=null)
	{
	    try
	    {
		db2.close();
	    }
	    finally
	    {
		db2=null;
	    }
	}
	removeDatabaseFiles2();
    }
    public void unloadDatabase3()
    {
	if (db3!=null)
	{
	    try
	    {
		db3.close();
	    }
	    finally
	    {
		db3=null;
	    }
	}
	removeDatabaseFiles3();	
    }
    public void unloadDatabase4()
    {
	if (db4!=null)
	{
	    try
	    {
		db4.close();
		listDatabase.remove(db4);
	    }
	    finally
	    {
		db4=null;
	    }
	}
	removeDatabaseFiles4();
    }
    
    @AfterClass
    public void unloadDatabase()
    {
	try
	{
	    unloadDatabase1();
	}
	finally
	{
	    try
	    {
		unloadDatabase2();
	    }
	    finally
	    {
		try
		{
		    unloadDatabase3();
		}
		finally
		{
		    try
		    {
			unloadDatabase4();
		    }
		    finally
		    {
			listDatabase.clear();
		    }
		}
	    }
	}
    }
    
    @Override
    public void finalize()
    {
	unloadDatabase();
    }
    
    @AfterMethod
    public void cleanPendedEvents()
    {
	for (Database db : listDatabase)
	{
	    synchronized(db)
	    {
		db.clearPendingEvents();
	    }
	}
    }
    private void sendDistantDatabaseEvent(DistantDatabaseEvent event)
    {
	for (Database db : listDatabase)
	{
	    if (db.getHostID().equals(event.getHostDestination()))
	    {
		if (db.isConnected())
		    db.getReceivedDatabaseEvents().add(event);
		else
		    Assert.fail();
		break;
	    }
	}
    }
    
    private boolean checkMessages(Database db) throws ClassNotFoundException, DatabaseException, IOException
    {
	boolean changed=false;
	while (!db.getReceivedDatabaseEvents().isEmpty())
	{
	    changed=true;
	    DistantDatabaseEvent dde=db.getReceivedDatabaseEvents().remove(0);
	    
	    DatabaseEventToSend event=dde.getDatabaseEventToSend();
	    if (event instanceof BigDatabaseEventToSend)
	    {
		try(InputStream is=dde.getInputStream())
		{
		    db.getDbwrapper().getSynchronizer().received((BigDatabaseEventToSend)event, is);
		}
	    }
	    else
	    {
		db.getDbwrapper().getSynchronizer().received(event);
	    }
	}
	return changed;
    }
    private boolean checkMessages() throws ClassNotFoundException, DatabaseException, IOException 
    {
	boolean changed=false;
	for (Database db : listDatabase)
	{
	    synchronized(db)
	    {
		changed|=checkMessages(db);
	    }
	}
	return changed;
    }
    private void exchangeMessages() throws DatabaseException, IOException, ClassNotFoundException
    {
	boolean loop=true;
	while (loop)
	{
	    loop=false;
	    for (Database db : listDatabase)
	    {
		
		synchronized(db)
		{
		    DatabaseEvent e=db.getDbwrapper().getSynchronizer().nextEvent();
		    if (e!=null)
		    {
			
			loop=true;
			if (e instanceof DatabaseEventToSend)
			{
			    DatabaseEventToSend es=(DatabaseEventToSend)e;
			    
			    Assert.assertEquals(es.getHostSource(), db.getHostID());
			    Assert.assertNotEquals(es.getHostDestination(), db.getHostID());
			    if (db.isConnected())
			    {
				sendDistantDatabaseEvent(new DistantDatabaseEvent(db.getDbwrapper(), es));
			    }
			    else
				Assert.fail();//TODO really ?
			}
			else
			{
			    db.getLocalEvents().add(e);
			}
		    }
		}
	    }
	    loop|=checkMessages();
	}
    }
    private void checkAllDatabaseInternalDataUsedForSynchro() throws DatabaseException
    {
	for (Database db : listDatabase)
	    checkDatabaseInternalDataUsedForSynchro(db);
    }
    
    private void checkDatabaseInternalDataUsedForSynchro(Database db) throws DatabaseException
    {
	synchronized(db)
	{
	    Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);
		
	    Assert.assertEquals(db.getDbwrapper().getTransactionsTable().getRecords().size(), 0);
	    Assert.assertEquals(db.getDbwrapper().getDatabaseEventsTable().getRecords().size(), 0);
	    Assert.assertEquals(db.getDbwrapper().getHooksTransactionsTable().getRecords().size(), listDatabase.size());
	    Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
	}

    }
    
    private void connectLocal(Database db) throws DatabaseException
    {
	synchronized(db)
	{
	    if (!db.isConnected())
	    {
		db.getDbwrapper().getSynchronizer().initLocalHostID(db.getHostID());
		db.setConnected(true);
	    }
	}
    }
    private void connectDistant(Database db, Database ... listDatabase) throws DatabaseException
    {
	synchronized(db)
	{
	    if (db.isConnected())
	    {
		
		for (Database otherdb : listDatabase)
		{
		    if (otherdb!=db && otherdb.isConnected())
		    {
			db.getDbwrapper().getSynchronizer().initHook(otherdb.getHostID(), otherdb.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(db.getHostID()));
			//otherdb.getDbwrapper().getSynchronizer().initHook(db.getHostID(), db.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(otherdb.getHostID()));
		    }
		}
	    }
	}
	    
    }
    
    private void connectSelectedDatabase(Database ...listDatabase) throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	{
	    connectLocal(db);
	}
	for (Database db : listDatabase)
	{
	    connectDistant(db, listDatabase);
	}
	exchangeMessages();
    }
    
    private void connectAllDatabase() throws ClassNotFoundException, DatabaseException, IOException
    {
	Database dbs[]=new Database[listDatabase.size()];
	for (int i=0;i<dbs.length;i++)
	    dbs[i]=listDatabase.get(i);
	for (Database db : listDatabase)
	{
	    connectLocal(db);
	}
	for (Database db : listDatabase)
	{
	    connectDistant(db, dbs);
	}
	exchangeMessages();
    }
    
    private void disconnect(Database db, Database ...listDatabase) throws DatabaseException
    {
	synchronized(db)
	{
	    if (db.isConnected())
	    {
		db.setConnected(false);
		db.getDbwrapper().getSynchronizer().deconnectHook(db.getHostID());
		for (Database dbother : listDatabase)
		{
		    if (dbother!=db && dbother.isConnected())
			dbother.getDbwrapper().getSynchronizer().deconnectHook(db.getHostID());
		}
		Assert.assertFalse(db.isConnected());
	    }
	}
    }
    private void disconnectSelectedDatabase(Database ...listDatabase) throws DatabaseException
    {
	for (Database db : listDatabase)
	    disconnect(db, listDatabase);
    }
    private void disconnectAllDatabase() throws DatabaseException
    {
	Database dbs[]=new Database[listDatabase.size()];
	for (int i=0;i<dbs.length;i++)
	    dbs[i]=listDatabase.get(i);
	disconnectSelectedDatabase(dbs);
    }
    
    private TableAlone.Record generatesTableAloneRecord()
    {
	TableAlone.Record ralone=new TableAlone.Record();
	ralone.id=new DecentralizedIDGenerator();
	ralone.value=generateString();
	return ralone;
	
    }
    
    private void addTableAloneRecord(Database db, boolean first) throws DatabaseException
    {
	TableAlone.Record ralone=generatesTableAloneRecord();
	
	db.getTableAlone().addRecord(((Table<TableAlone.Record>)db.getTableAlone()).getMap(ralone, true, true));
	if (first)
	    db.setRecordAlone(ralone);
    }
    
    private void addUndecentralizableTableA1Record(Database db) throws DatabaseException
    {
	UndecentralizableTableA1.Record record=new UndecentralizableTableA1.Record();
	record.value=db.getHostID().toString();
	for (int i=0;i<10;i++)
	{
	    record.value+='a'+((int)(Math.random()*52));
	}
	db.getUndecentralizableTableA1().addRecord(record);
	
    }
    
    private void addUndecentralizableTableB1Record(Database db) throws DatabaseException
    {
	UndecentralizableTableA1.Record record=new UndecentralizableTableA1.Record();
	record.value=db.getHostID().toString();
	for (int i=0;i<10;i++)
	{
	    record.value+='a'+((int)(Math.random()*52));
	}
	record=db.getUndecentralizableTableA1().addRecord(record);
	UndecentralizableTableB1.Record record2=new UndecentralizableTableB1.Record();
	record2.pointing=record;
	record2=db.getUndecentralizableTableB1().addRecord(record2);
    }

    private String generateString()
    {
	String res="";
	for (int i=0;i<10;i++)
	{
	    res+='a'+((int)(Math.random()*52));
	}
	return res;
    }
    
    private TablePointed.Record generatesTablePointedRecord() 
    {
	TablePointed.Record rpointed=new TablePointed.Record();
	rpointed.id=new DecentralizedIDGenerator();
	rpointed.value=generateString();
	return rpointed;
    }

    private TablePointing.Record generatesTablePointingRecord(TablePointed.Record rpointed) 
    {
	TablePointing.Record rpointing1=new TablePointing.Record();
	rpointing1.id=new DecentralizedIDGenerator();
	rpointing1.table2=Math.random()<0.5?null:rpointed;
	return rpointing1;
    }

    private void addTablePointedAndPointingRecords(Database db, boolean first) throws DatabaseException
    {
	TablePointed.Record rpointed=new TablePointed.Record();
	rpointed.id=new DecentralizedIDGenerator();
	rpointed.value=generateString();
	
	rpointed=db.getTablePointed().addRecord(rpointed);

	TablePointing.Record rpointing1=new TablePointing.Record();
	rpointing1.id=new DecentralizedIDGenerator();
	rpointing1.table2=null;
	rpointing1=db.getTablePointing().addRecord(rpointing1);
	TablePointing.Record rpointing2=new TablePointing.Record();
	rpointing2.id=new DecentralizedIDGenerator();
	rpointing2.table2=rpointed;
	rpointing2=db.getTablePointing().addRecord(rpointing2);
	if (first)
	{
	    db.setRecordPointingNull(rpointing1);
	    db.setRecordPointingNotNull(rpointing2);
	}
    }
    private void addElements(Database db) throws DatabaseException
    {
	addTableAloneRecord(db, true);
	addTablePointedAndPointingRecords(db, true);
	addUndecentralizableTableA1Record(db);
	addUndecentralizableTableB1Record(db);
    }
    
    private void addElements() throws DatabaseException
    {
	for (Database db : listDatabase)
	    addElements(db);
    }
    
    @Test
    public void testAddFirstElements() throws DatabaseException
    {
	addElements();
    }
    
    
    @Test(dependsOnMethods={"testAddFirstElements"})
    public void testInit() throws DatabaseException
    {
	for (Database db : listDatabase)
	{
	    db.getDbwrapper().getSynchronizer().setNotifier(db);
	    db.getDbwrapper().setMaxEventsToSynchronizeAtTheTime(5);
	    db.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);
	    db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(), TablePointed.class.getPackage());
	    Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());
	    
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		    db.getDbwrapper().getSynchronizer().addHookForDistantHost(other.getHostID(), false, TablePointed.class.getPackage());
	    }
	}
	
	    
    }
    
    @Test(dependsOnMethods={"testInit"})
    public void testAllConnect() throws ClassNotFoundException, DatabaseException, IOException
    {
	
	connectAllDatabase();
	exchangeMessages();
	for (Database db : listDatabase)
	{
	    for (Database other : listDatabase)
	    {
		Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(other.getHostID()));
	    }
	}
    }
    
    
    /*@Test(dependsOnMethods={"testAllConnect"})
    public void testInitDatabaseNetwork() throws DatabaseException
    {
	for (Database db : listDatabase)
	{
	    
	    db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(), TablePointed.class.getPackage());
	}
    }*/
    
    /*@Test(dependsOnMethods={"testInit"})
    public void testAddSynchroBetweenDatabase() throws DatabaseException, ClassNotFoundException, IOException
    {
	for (Database db : listDatabase)
	{
	    for (Database otherdb : listDatabase)
	    {
		if (otherdb!=db)
		{
		    db.getDbwrapper().getSynchronizer().addHookForDistantHost(otherdb.getHostID(),TablePointed.class.getPackage());
		    otherdb.getDbwrapper().getSynchronizer().addHookForDistantHost(db.getHostID(),TablePointed.class.getPackage());
		}
	    }
	}
	exchangeMessages();
    }*/
    
    private void testSynchronisation(Database db) throws DatabaseException
    {
	
	for (TableAlone.Record r : db.getTableAlone().getRecords())
	{
	    
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		{
		    TableAlone.Record otherR=other.getTableAlone().getRecord("id", r.id);
		    Assert.assertNotNull(otherR);
		    Assert.assertEquals(otherR.value, r.value);
		}
	    }
	}
	for (TablePointed.Record r : db.getTablePointed().getRecords())
	{
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		{
		    TablePointed.Record otherR=other.getTablePointed().getRecord("id", r.id);
		    Assert.assertNotNull(otherR);
		    Assert.assertEquals(otherR.value, r.value);
		}
	    }
	}
	for (TablePointing.Record r : db.getTablePointing().getRecords())
	{
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		{
		    TablePointing.Record otherR=other.getTablePointing().getRecord("id", r.id);
		    Assert.assertNotNull(otherR);
		    if (r.table2==null)
			Assert.assertNull(otherR.table2);
		    else
			Assert.assertEquals(otherR.table2.value, r.table2.value);
		}
	    }
	}
	for (UndecentralizableTableA1.Record r : db.getUndecentralizableTableA1().getRecords())
	{
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		{
		    UndecentralizableTableA1.Record otherR=other.getUndecentralizableTableA1().getRecord("id", new Integer(r.id));
		    if (otherR!=null)
		    {
			Assert.assertNotEquals(otherR.value, r.value);
		    }
		}
	    }
	}
	for (UndecentralizableTableB1.Record r : db.getUndecentralizableTableB1().getRecords())
	{
	    for (Database other : listDatabase)
	    {
		if (other!=db)
		{
		    UndecentralizableTableB1.Record otherR=other.getUndecentralizableTableB1().getRecord("id", r.id);
		    if (otherR!=null)
		    {
			if (r.pointing==null)
			    Assert.assertNull(otherR.pointing);
			else
			    Assert.assertNotEquals(otherR.pointing.value, r.pointing.value);
		    }
		}
	    }
	}
    }
    
    private void testSynchronisation() throws DatabaseException
    {
	for(Database db : listDatabase)
	    testSynchronisation(db);
    }
    
    @Test(dependsOnMethods={"testAllConnect"})
    public void testOldElementsAddedBeforeAddingSynchroSynchronized() throws DatabaseException, ClassNotFoundException, IOException
    {
	exchangeMessages();
	testSynchronisation();
	disconnectAllDatabase();
	checkAllDatabaseInternalDataUsedForSynchro();

    }
    
    
     
    private ArrayList<TableEvent<DatabaseRecord>> proviveTableEvents(int number)
    {
	ArrayList<TableEvent<DatabaseRecord>> res=new ArrayList<>();
	ArrayList<TablePointed.Record> pointedRecord=new ArrayList<>();
	ArrayList<DatabaseRecord> livingRecords=new ArrayList<>();
	
	while (number>0)
	{
	    TableEvent<DatabaseRecord> te=null;
	
	    switch((int)(Math.random()*4))
	    {
		
	    
		case 0:
		{
	    
		    DatabaseRecord record=null;
		    switch((int)(Math.random()*3))
		    {
			case 0:
			    record=generatesTableAloneRecord();
			    break;
			case 1:
			    record=generatesTablePointedRecord();
			    break;
			case 2:
			    if (pointedRecord.isEmpty())
				record=generatesTablePointedRecord();
			    else
				record=generatesTablePointingRecord(pointedRecord.get((int)(Math.random()*pointedRecord.size())));
			    break;
		    }
		    te=new TableEvent<DatabaseRecord>(-1, DatabaseEventType.ADD, null, record, null);
		    livingRecords.add(record);
		    
		}
		break;
		case 1:
		{
		    if (livingRecords.isEmpty())
			continue;
		    DatabaseRecord record=livingRecords.get((int) (Math.random()*livingRecords.size()));
		    te=new TableEvent<DatabaseRecord>(-1, DatabaseEventType.REMOVE, record, null, null);
		    Assert.assertEquals(livingRecords.remove(record), true);
		    pointedRecord.remove(record);
		}
		break;
		case 2:
		{
		    if (livingRecords.isEmpty())
			continue;
		    DatabaseRecord record=livingRecords.get((int) (Math.random()*livingRecords.size()));
		    te=new TableEvent<DatabaseRecord>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, record, null, null);
		    Assert.assertEquals(livingRecords.remove(record), true);
		    pointedRecord.remove(record);
		}
		break;
		case 3:
		{
		    if (livingRecords.isEmpty())
			continue;
		    DatabaseRecord record=livingRecords.get((int) (Math.random()*livingRecords.size()));
		    DatabaseRecord recordNew=null;
		    if (record instanceof TableAlone.Record)
		    {
			TableAlone.Record r=generatesTableAloneRecord();
			r.id=((TableAlone.Record)record).id;
			recordNew=r;
		    }
		    else if (record instanceof TablePointed.Record)
		    {
			TablePointed.Record r=generatesTablePointedRecord();
			r.id=((TablePointed.Record)record).id;
			recordNew=r;
		    }
		    else if (record instanceof TablePointing.Record)
		    {
			TablePointing.Record r=new TablePointing.Record();
			r.id=((TablePointing.Record)record).id;
			if (pointedRecord.isEmpty())
			    continue;
			r.table2=pointedRecord.get((int)(Math.random()*pointedRecord.size()));
			recordNew=r;
		    }
		    te=new TableEvent<DatabaseRecord>(-1, DatabaseEventType.UPDATE, record, recordNew, null);
		}
		break;
		     
	     }
	     if (te!=null)
	     {
		 res.add(te);
		 --number;
	     }
	}
	
	return res;
    }
    
    @DataProvider(name = "provideDataForSynchroBetweenTwoPeers", parallel = false)
    public Object[][] provideDataForSynchroBetweenTwoPeers()
    {
	int numberEvents=40;
	Object[][] res=new Object[numberEvents*2*2*2][];
	int index=0;
	for (boolean exceptionDuringTransaction : new boolean[]{false, true})
	{
	    for (boolean generateDirectConflict : new boolean[]{true, false})
	    {
		for (boolean peersInitiallyConnected : new boolean[]{true, false})
		{
		    for (TableEvent<DatabaseRecord> te : proviveTableEvents(numberEvents))
		    {
			res[index++]=new Object[]{new Boolean(exceptionDuringTransaction), new Boolean(generateDirectConflict), new Boolean(peersInitiallyConnected), te};
		    }
		}
	    }
	    
	}
	return res;
    }
    private void proceedEvent(final Database db, final boolean exceptionDuringTransaction, final List<TableEvent<DatabaseRecord>> events) throws DatabaseException
    {
	proceedEvent(db, exceptionDuringTransaction, events, false);
    }
    private void proceedEvent(final Database db, final boolean exceptionDuringTransaction, final List<TableEvent<DatabaseRecord>> events, final boolean manualKeys) throws DatabaseException
    {
	db.getDbwrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

	    @Override
	    public Void run() throws Exception
	    {
		int indexException=exceptionDuringTransaction?((int)(Math.random()*events.size())):-1;
		for (int i=0;i<events.size();i++)
		{
		    TableEvent<DatabaseRecord> te=events.get(i);
		    proceedEvent(te.getTable(db.getDbwrapper()), te, indexException==i, manualKeys);
		}
		return null;
	    }

	    @Override
	    public TransactionIsolation getTransactionIsolation()
	    {
		return TransactionIsolation.TRANSACTION_SERIALIZABLE;
	    }

	    @Override
	    public boolean doesWriteData()
	    {
		return true;
	    }
	});
	
    }
    
    protected void proceedEvent(final Table<DatabaseRecord> table, final TableEvent<DatabaseRecord> event, boolean exceptionDuringTransaction, boolean manualKeys) throws Exception
    {
		switch(event.getType())
		{
		    
		    case ADD:
			
			table.addRecord(table.getMap(event.getNewDatabaseRecord(), true, true));
			break;
		    case REMOVE:
			table.removeRecord(event.getOldDatabaseRecord());
			break;
		    case REMOVE_WITH_CASCADE:
			table.removeRecordWithCascade(event.getOldDatabaseRecord());
			break;
		    case UPDATE:
			table.updateRecord(event.getNewDatabaseRecord());
			break;
		}
		if (exceptionDuringTransaction)
		    throw new Exception();
		
    }
    private void testEventSynchronized(Database db, List<TableEvent<DatabaseRecord>> levents, boolean synchronizedOk) throws DatabaseException
    {
	for (TableEvent<DatabaseRecord> te : levents)
	    testEventSynchronized(db, te, synchronizedOk);
    }
    
    private Map<String, Object> getMapPrimaryKeys(Table<DatabaseRecord> table, DatabaseRecord record) throws DatabaseException
    {
	Map<String, Object> res=new HashMap<>();
	for (FieldAccessor fa : table.getPrimaryKeysFieldAccessors())
	{
	    res.put(fa.getFieldName(), fa.getValue(record));
	}
	return res;
    }
    
    private void testEventSynchronized(Database db, TableEvent<DatabaseRecord> event, boolean synchronizedOk) throws DatabaseException
    {
	
	if (event.getType()==DatabaseEventType.ADD || event.getType()==DatabaseEventType.UPDATE)
	{
		Table<DatabaseRecord> table=event.getTable(db.getDbwrapper());
		
		
		DatabaseRecord dr=table.getRecord(getMapPrimaryKeys(table, event.getNewDatabaseRecord()));
		if (synchronizedOk)
		    Assert.assertNotNull(dr, event.getType().name());
		Assert.assertEquals(table.equalsAllFields(dr, event.getNewDatabaseRecord()), synchronizedOk, "Concerned event : "+event);
	}
	else if (event.getType()==DatabaseEventType.REMOVE || event.getType()==DatabaseEventType.REMOVE_WITH_CASCADE)
	{
		Table<DatabaseRecord> table=event.getTable(db.getDbwrapper());
		DatabaseRecord dr=table.getRecord(getMapPrimaryKeys(table, event.getOldDatabaseRecord()));
		if (synchronizedOk)
		    Assert.assertNull(dr);
	}
	else
	    throw new IllegalAccessError();
    }
    
    private void testDirectCollision(Database db, TableEvent<DatabaseRecord> event, DirectCollisionDetected collision) throws DatabaseException
    {
	Table<DatabaseRecord> table=event.getTable(db.getDbwrapper());
	Assert.assertNotNull(collision);
	Assert.assertEquals(collision.type, event.getType());
	Assert.assertEquals(collision.concernedTable.getName(), table.getName());
	Assert.assertNotEquals(collision.distantPeerID, db.getHostID());
	switch(event.getType())
	{
	    case ADD:
		Assert.assertNotNull(collision.actualValues);
		Assert.assertNull(event.getOldDatabaseRecord());
		Assert.assertTrue(table.equals(event.getNewDatabaseRecord(), collision.actualValues));
		Assert.assertTrue(table.equalsAllFields(event.getNewDatabaseRecord(), collision.newValues));
		break;
	    case REMOVE:
		
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNull(event.getNewDatabaseRecord());
		Assert.assertNull(collision.newValues);
		if (collision.actualValues!=null)
		{
		    Assert.assertTrue(table.equals(event.getOldDatabaseRecord(), collision.actualValues));
		}
		break;
	    case REMOVE_WITH_CASCADE:
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNull(event.getNewDatabaseRecord());
		Assert.assertNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    case UPDATE:
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNotNull(event.getNewDatabaseRecord());
		Assert.assertNotNull(collision.newValues);
		//Assert.assertNull(collision.actualValues);
		break;
	    
	}
    }
    private void testIndirectCollision(Database db, TableEvent<DatabaseRecord> event, IndirectCollisionDetected collision) throws DatabaseException
    {
	Table<DatabaseRecord> table=event.getTable(db.getDbwrapper());
	Assert.assertNotNull(collision);
	Assert.assertEquals(collision.type, event.getType());
	Assert.assertEquals(collision.concernedTable, table);
	Assert.assertEquals(collision.distantPeerInCollisionWithDataToSynchronize, db.getHostID());
	
	switch(event.getType())
	{
	    case ADD:
		Assert.assertNotNull(collision.actualValues);
		Assert.assertNull(event.getOldDatabaseRecord());
		Assert.assertTrue(table.equals(event.getNewDatabaseRecord(), collision.actualValues));
		Assert.assertTrue(table.equalsAllFields(event.getNewDatabaseRecord(), collision.newValues));
		break;
	    case REMOVE:
		
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNull(event.getNewDatabaseRecord());
		Assert.assertNull(collision.newValues);
		if (collision.actualValues!=null)
		{
		    Assert.assertTrue(table.equals(event.getOldDatabaseRecord(), collision.actualValues));
		}
		break;
	    case REMOVE_WITH_CASCADE:
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNull(event.getNewDatabaseRecord());
		Assert.assertNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    case UPDATE:
		Assert.assertNotNull(event.getOldDatabaseRecord());
		Assert.assertNotNull(event.getNewDatabaseRecord());
		Assert.assertNotNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    
	}
    }
    
    private DatabaseRecord clone(DatabaseRecord record)
    {
	if (record==null)
	    return null;
	
	if (record instanceof TableAlone.Record)
	{
	    TableAlone.Record r=(TableAlone.Record)record;
	    return r.clone();
	}
	else if (record instanceof TablePointing.Record)
	{
	    TablePointing.Record r=(TablePointing.Record)record;
	    return r.clone();
	}
	else if (record instanceof TablePointed.Record)
	{
	    TablePointed.Record r=(TablePointed.Record)record;
	    return r.clone();
	}
	else if (record instanceof UndecentralizableTableA1.Record)
	{
	    UndecentralizableTableA1.Record r=(UndecentralizableTableA1.Record)record;
	    return r.clone();
	}
	else if (record instanceof UndecentralizableTableB1.Record)
	{
	    UndecentralizableTableB1.Record r=(UndecentralizableTableB1.Record)record;
	    return r.clone();
	}
	else
	    throw new IllegalAccessError("Unkown type "+record.getClass());
    }
    private TableEvent<DatabaseRecord> clone(TableEvent<DatabaseRecord> event)
    {
	return new TableEvent<DatabaseRecord>(event.getID(), event.getType(), clone(event.getOldDatabaseRecord()), clone(event.getNewDatabaseRecord()), event.getHostsDestination());
    }
    private List<TableEvent<DatabaseRecord>> clone(List<TableEvent<DatabaseRecord>> events)
    {
	ArrayList<TableEvent<DatabaseRecord>> res=new ArrayList<>(events.size());
	for (TableEvent<DatabaseRecord> te : events)
	    res.add(clone(te));
	return res;
    }
    
    private void testSynchroBetweenPeers(int peersNumber, boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
    {
	synchronized(TestDecentralizedDatabase.class)
	{
	if (peersNumber<2 || peersNumber>listDatabase.size())
	    throw new IllegalArgumentException();
	List<TableEvent<DatabaseRecord>> levents=Arrays.asList(event);
	ArrayList<Database> l=new ArrayList<>(listDatabase.size());
	for (int i=0;i<peersNumber;i++)
	    l.add(listDatabase.get(i));
	Database[] concernedDatabase=new Database[l.size()];
	for (int i=0;i<l.size();i++)
	    concernedDatabase[i]=l.get(i);
	
	    if (exceptionDuringTransaction)
	    {
		    if (peersInitiallyConnected)
		    {
			connectSelectedDatabase(concernedDatabase);
			exchangeMessages();
			for (int i=1;i<peersNumber;i++)
			{
			    Database db=concernedDatabase[i];
			    db.setNewDatabaseEventDetected(false);
			}
		    }
		
		    Database db=concernedDatabase[0];
		    try
		    {
			proceedEvent(db, true, levents);
			Assert.fail();
		    }
		    catch(Exception e)
		    {
			
		    }

		    if (!peersInitiallyConnected)
			connectSelectedDatabase(concernedDatabase);
		   
		    exchangeMessages();

		    for (int i=1;i<peersNumber;i++)
		    {
			 db=concernedDatabase[i];
			 if (peersInitiallyConnected)
			     Assert.assertFalse(db.isNewDatabaseEventDetected(), ""+db.getLocalEvents());
			 testEventSynchronized(db, event, false);
		    }
			
		    disconnectSelectedDatabase(concernedDatabase);
	    }
	    else
	    {
		
		if (generateDirectConflict) 
		{
		    int i=0;
		    for (Database db : concernedDatabase)
		    {
			if (i++==0)
			    db.setReplaceWhenDirectCollisionDetected(false);
			else
			    db.setReplaceWhenDirectCollisionDetected(true);
			proceedEvent(db, false, clone(levents), true);
			
		    }
		    connectSelectedDatabase(concernedDatabase);
		    exchangeMessages();
		    i=0;
		    for (Database db : concernedDatabase)
		    {
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			
			DirectCollisionDetected dcollision=db.getDirectCollisionDetected();
			Assert.assertNull(db.getIndirectCollisionDetected());

			Assert.assertNotNull(dcollision, "i="+(i));
			testDirectCollision(db, event, dcollision);
			Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
			Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
			++i;
		    }
		    
		    
		    
		}
		else
		{
		    if (peersInitiallyConnected)
		    {
			connectSelectedDatabase(concernedDatabase);
			for (Database db : concernedDatabase)
			    db.setNewDatabaseEventDetected(false);
		    }
		
		    Database db=concernedDatabase[0];
		    proceedEvent(db, false, levents);

		    if (!peersInitiallyConnected)
			connectSelectedDatabase(concernedDatabase);
		   
		    exchangeMessages();
		    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
		    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());

		    for (int i=1;i<concernedDatabase.length;i++)
		    {
			 db=concernedDatabase[i];
			 Assert.assertNull(db.getDirectCollisionDetected());
			 Assert.assertNull(db.getIndirectCollisionDetected());
			 Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
			 Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
			 //System.exit(0);
			 Assert.assertTrue(db.isNewDatabaseEventDetected());
			 testEventSynchronized(db, event, true);
			 
		    }
		    
		
		}
		disconnectSelectedDatabase(concernedDatabase);
		for (int i=peersNumber;i<listDatabase.size();i++)
		{
		    Database db=listDatabase.get(i);
		    testEventSynchronized(db, event, false);
		}
		
		
		connectAllDatabase();
		exchangeMessages();
		
		for (int i=peersNumber;i<listDatabase.size();i++)
		{
		    Database db=listDatabase.get(i);
		    
		    Assert.assertNull(db.getDirectCollisionDetected());
		    Assert.assertNull(db.getIndirectCollisionDetected());
		    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
		    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
		    Assert.assertTrue(db.isNewDatabaseEventDetected());
		    testEventSynchronized(db, event, true);
		    
		}
		
		disconnectAllDatabase();
	    }
	    testSynchronisation();
	    checkAllDatabaseInternalDataUsedForSynchro();
	}
    }
    
    private void testTransactionBetweenPeers(int peersNumber, boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents, boolean threadTest) throws ClassNotFoundException, DatabaseException, IOException
    {
	if (peersNumber<2 || peersNumber>listDatabase.size())
	    throw new IllegalArgumentException();
	ArrayList<Database> l=new ArrayList<>(listDatabase.size());
	for (int i=0;i<peersNumber;i++)
	    l.add(listDatabase.get(i));
	Database[] concernedDatabase=(Database[])l.toArray();
	
	if (peersInitiallyConnected)
	    connectSelectedDatabase(concernedDatabase);
		
	Database db=concernedDatabase[0];
	
	proceedEvent(db, false, levents);

	if (!peersInitiallyConnected)
	    connectSelectedDatabase(concernedDatabase);
		   
	exchangeMessages();
	Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());

	for (int i=1;i<peersNumber;i++)
	{
	    db=concernedDatabase[i];
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    if (!threadTest)
		testEventSynchronized(db, levents, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
			 
	}
		
	for (int i=peersNumber;i<listDatabase.size();i++)
	{
	    db=listDatabase.get(i);
	    
	    if (!threadTest)
		testEventSynchronized(db, levents, false);
	}
	    
	connectAllDatabase();
	exchangeMessages();
	    
	for (int i=2;i<listDatabase.size();i++)
	{
	    db=listDatabase.get(i);
	    
		    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    if (!threadTest)
		testEventSynchronized(db, levents, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
		    
	}
		
		
	disconnectAllDatabase();
	checkAllDatabaseInternalDataUsedForSynchro();
	    
    }
    
    @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods={"testOldElementsAddedBeforeAddingSynchroSynchronized"})
    public void testSynchroBetweenTwoPeers(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
    {
	testSynchroBetweenPeers(2, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
    }

    @Test(dependsOnMethods={"testSynchroBetweenTwoPeers"})
    public void testSynchroAfterTestsBetweenTwoPeers() throws DatabaseException
    {
	testSynchronisation();
    }

    @DataProvider(name = "provideDataSynchroBetweenThreePeers", parallel = false)
    public Object[][] provideDataSynchroBetweenThreePeers()
    {
	return provideDataForSynchroBetweenTwoPeers();
    }
    
    @Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods={"testSynchroBetweenTwoPeers"})
    public void testSynchroBetweenThreePeers(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
    {
	testSynchroBetweenPeers(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
    }
    
    @Test(dependsOnMethods={"testSynchroBetweenThreePeers"})
    public void testSynchroAfterTestsBetweenThreePeers() throws DatabaseException
    {
	testSynchronisation();
    }
    
    
    
    @DataProvider(name = "provideDataForIndirectSynchro", parallel = false)
    public Object[][] provideDataForIndirectSynchro()
    {
	return provideDataForSynchroBetweenTwoPeers();
    }
    
    //@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods={"testSynchroAfterTestsBetweenThreePeers"})
    @Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods={"testOldElementsAddedBeforeAddingSynchroSynchronized"})
    public void testIndirectSynchro(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws DatabaseException, ClassNotFoundException, IOException
    {
	List<TableEvent<DatabaseRecord>> levents=Arrays.asList(event);
	final Database[] indirectDatabase=new Database[]{listDatabase.get(0), listDatabase.get(2)};
	final Database[] segmentA=new Database[]{listDatabase.get(0), listDatabase.get(1)};
	final Database[] segmentB=new Database[]{listDatabase.get(1), listDatabase.get(2)};
	if (generateDirectConflict) 
	{
	    int i=0;
	    for (Database db : indirectDatabase)//TODO test with opposite direction 
	    {
		if (i++==0)
		    db.setReplaceWhenDirectCollisionDetected(false);
		else
		    db.setReplaceWhenDirectCollisionDetected(true);
		
		proceedEvent(db, false, levents);
	    }
	    connectSelectedDatabase(segmentA);
	    exchangeMessages();
	    
	    Database db=listDatabase.get(1);
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    disconnectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	    exchangeMessages();
	    
	    db=listDatabase.get(2);
	    
	    IndirectCollisionDetected idcollision=db.getIndirectCollisionDetected();
	    Assert.assertNull(db.getDirectCollisionDetected());
	    testIndirectCollision(db, event, idcollision);
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
		 
	    disconnectSelectedDatabase(segmentB);
	    connectSelectedDatabase(segmentA);
	    exchangeMessages();
	    
	    db=listDatabase.get(0);
	    
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    idcollision=db.getIndirectCollisionDetected();
	    testIndirectCollision(db, event, idcollision);
	    Assert.assertFalse(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    disconnectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	    exchangeMessages();

	    db=listDatabase.get(2);
	    
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    db=listDatabase.get(1);
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    testEventSynchronized(db, event, true);
	    
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    connectSelectedDatabase(segmentB);
	}
	else
	{
	    if (peersInitiallyConnected)
		connectSelectedDatabase(segmentA);
	
	    Database db=listDatabase.get(0);
	    proceedEvent(db, false, levents);

	    if (!peersInitiallyConnected)
		connectSelectedDatabase(segmentA);
	   
	    exchangeMessages();
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());

	    db=listDatabase.get(1);	
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    disconnectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	    exchangeMessages();
	    
	    db=listDatabase.get(2);	
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    disconnectSelectedDatabase(segmentB);
	}

	connectAllDatabase();
	exchangeMessages();
	disconnectAllDatabase();
    
	
    }
    
    @Test(dependsOnMethods={"testIndirectSynchro"})
    public void testSynchroAfterIndirectTestsBetweenPeers() throws DatabaseException
    {
	testSynchronisation();
    }
    
    @DataProvider(name = "provideDataForIndirectSynchroWithIndirectConnection", parallel = false)
    public Object[][] provideDataForIndirectSynchroWithIndirectConnection()
    {
	return provideDataForSynchroBetweenTwoPeers();
    }
    
    @Test(dataProvider = "testSynchroAfterIndirectTestsBetweenPeers", dependsOnMethods={"testIndirectSynchro"})
    public void testIndirectSynchroWithIndirectConnection(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
    {
	List<TableEvent<DatabaseRecord>> levents=Arrays.asList(event); 
	final Database[] indirectDatabase=new Database[]{listDatabase.get(0), listDatabase.get(2)};
	final Database[] segmentA=new Database[]{listDatabase.get(0), listDatabase.get(1)};
	final Database[] segmentB=new Database[]{listDatabase.get(1), listDatabase.get(2)};
	if (generateDirectConflict) 
	{
	    int i=0;
	    for (Database db : indirectDatabase)
	    {
		if (i++==0)
		    db.setReplaceWhenDirectCollisionDetected(true);
		else
		    db.setReplaceWhenDirectCollisionDetected(false);
		proceedEvent(db, false, levents);
	    }
	    connectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	    exchangeMessages();
	    
	    
	    Database db=listDatabase.get(0);
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertFalse(db.isNewDatabaseEventDetected());
	    testIndirectCollision(db, event, db.getIndirectCollisionDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    db=listDatabase.get(1);
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    db=listDatabase.get(2);
	    
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertFalse(db.isNewDatabaseEventDetected());
	    testIndirectCollision(db, event, db.getIndirectCollisionDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	}
	else
	{
	    if (peersInitiallyConnected)
	    {
		connectSelectedDatabase(segmentA);
		connectSelectedDatabase(segmentB);
	    }
	
	    Database db=listDatabase.get(0);
	    proceedEvent(db, false, levents);

	    if (!peersInitiallyConnected)
	    {
		connectSelectedDatabase(segmentA);
		connectSelectedDatabase(segmentB);
	    }
	   
	    exchangeMessages();
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());

	    db=listDatabase.get(1);	
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    db=listDatabase.get(2);	
	    Assert.assertNull(db.getDirectCollisionDetected());
	    Assert.assertNull(db.getIndirectCollisionDetected());
	    Assert.assertTrue(db.isNewDatabaseEventDetected());
	    testEventSynchronized(db, event, true);
	    Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	    Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	    disconnectSelectedDatabase(segmentA);
	    disconnectSelectedDatabase(segmentB);
	}

	connectAllDatabase();
	exchangeMessages();
	disconnectAllDatabase();
	
    }
    
    @Test(dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testSynchroAfterPostIndirectTestsBetweenPeers() throws DatabaseException
    {
	testSynchronisation();
    }
    
 
    
    
    @DataProvider(name = "provideDataForTransactionBetweenTwoPeers", parallel = false)
    public Object[][] provideDataForTransactionBetweenTwoPeers()
    {
	int numberTransactions=40;
	ArrayList<Object[]> res=new ArrayList<>();
	for (boolean peersInitiallyConnected : new boolean[]{true, false})
	{
	    for (int i=0;i<numberTransactions;i++)
	    {
		res.add(new Object[]{new Boolean(peersInitiallyConnected), proviveTableEvents((int)(5.0+Math.random()*10.0))});

	    }
	}
	
	return (Object[][])res.toArray();
    }
    
    @Test(dataProvider = "testSynchroAfterPostIndirectTestsBetweenPeers", dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testTransactionBetweenTwoPeers(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
    {
	testTransactionBetweenPeers(2, peersInitiallyConnected, levents, false);
    }
    
    @DataProvider(name = "provideDataForTransactionBetweenThreePeers", parallel = false)
    public Object[][] provideDataForTransactionBetweenThreePeers()
    {
	return provideDataForTransactionBetweenTwoPeers();
    }

    @Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods={"testTransactionBetweenTwoPeers"})
    public void testTransactionBetweenThreePeers(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
    {
	testTransactionBetweenPeers(3, peersInitiallyConnected, levents, false);
    }
    
    @DataProvider(name = "provideDataForTransactionSynchros", parallel = true)
    public Object[][] provideDataForTransactionSynchros()
    {
	return provideDataForTransactionBetweenTwoPeers();
    }
    
    @Test(dataProvider = "provideDataForTransactionSynchros", dependsOnMethods={"testTransactionBetweenThreePeers"}, invocationCount=100)
    public void testTransactionSynchros(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
    {
	testTransactionBetweenPeers(3, peersInitiallyConnected, levents, true);
    }
    
    @DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection", parallel = false)
    public Object[][] provideDataForTransactionSynchrosWithIndirectConnection()
    {
	return provideDataForTransactionBetweenTwoPeers();
    }
    @DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", parallel = true)
    public Object[][] provideDataForTransactionSynchrosWithIndirectConnectionThreaded()
    {
	return provideDataForTransactionSynchrosWithIndirectConnection();
    }
    
    private void testTransactionSynchrosWithIndirectConnection(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents, boolean multiThread) throws ClassNotFoundException, DatabaseException, IOException
    {
	final Database[] segmentA=new Database[]{listDatabase.get(0), listDatabase.get(1)};
	final Database[] segmentB=new Database[]{listDatabase.get(1), listDatabase.get(2)};
	    
	if (peersInitiallyConnected)
	{
	    connectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	}
	
	Database db=listDatabase.get(0);
	proceedEvent(db, false, levents);

	if (!peersInitiallyConnected)
	{
	    connectSelectedDatabase(segmentA);
	    connectSelectedDatabase(segmentB);
	}
	   
	exchangeMessages();
	Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());

	db=listDatabase.get(1);	
	Assert.assertNull(db.getDirectCollisionDetected());
	Assert.assertNull(db.getIndirectCollisionDetected());
	Assert.assertTrue(db.isNewDatabaseEventDetected());
	if (!multiThread)
	    testEventSynchronized(db, levents, true);
	Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	db=listDatabase.get(2);	
	Assert.assertNull(db.getDirectCollisionDetected());
	Assert.assertNull(db.getIndirectCollisionDetected());
	Assert.assertTrue(db.isNewDatabaseEventDetected());
	if (!multiThread)
	    testEventSynchronized(db, levents, true);
	Assert.assertTrue(db.getRecordsToRemoveNotFound().isEmpty());
	Assert.assertTrue(db.getRecordsToUpdateNotFound().isEmpty());
	    
	disconnectSelectedDatabase(segmentA);
	disconnectSelectedDatabase(segmentB);

	connectAllDatabase();
	exchangeMessages();
	disconnectAllDatabase();
	
    }

    @Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods={"testTransactionSynchros"})
    public void testTransactionSynchrosWithIndirectConnection(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
    {
	testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents, false);
    }

    @Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods={"testTransactionSynchrosWithIndirectConnection"})
    public void testTransactionSynchrosWithIndirectConnectionThreaded(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
    {
	testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents, false);
    }
    
    @Test(dependsOnMethods={"testTransactionSynchrosWithIndirectConnectionThreaded"})
    public void testSynchroTransactionTests() throws DatabaseException
    {
	testSynchronisation();
    }
    
    @Test(dependsOnMethods={"testTransactionSynchrosWithIndirectConnection", "testTransactionSynchros", "testTransactionBetweenThreePeers", "testTransactionBetweenTwoPeers", "testTransactionSynchrosWithIndirectConnectionThreaded", "testSynchroTransactionTests"})
    public void addNewPeer() throws DatabaseException, ClassNotFoundException, IOException
    {
	connectAllDatabase();
	testSynchronisation();
	disconnectAllDatabase();
	
	db4=new Database(getDatabaseWrapperInstance4());
	listDatabase.add(db4);
	loadDatabase(db4);
	
	 db4.getDbwrapper().getSynchronizer().setNotifier(db4);
	 db4.getDbwrapper().setMaxEventsToSynchronizeAtTheTime(5);
	 db4.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);
	 db4.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db4.getHostID(), TablePointed.class.getPackage());
	 Assert.assertTrue(db4.getDbwrapper().getSynchronizer().isInitialized());
	    
	 for (Database other : listDatabase)
	 {
	     if (other!=db4)
	     {
		 db4.getDbwrapper().getSynchronizer().addHookForDistantHost(other.getHostID(), false, TablePointed.class.getPackage());
		 other.getDbwrapper().getSynchronizer().addHookForDistantHost(db4.getHostID(), false, TablePointed.class.getPackage());
	     }
	 }
	
	testAllConnect();
	testSynchronisation();
	disconnectAllDatabase();
    }
    
    

    
	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods={"addNewPeer"})
	public void testSynchroBetweenTwoPeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
    		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}


	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods={"testSynchroBetweenTwoPeers2"})
	public void testSynchroBetweenThreePeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}


	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods={"testSynchroBetweenThreePeers2"})
	public void testIndirectSynchro2(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws DatabaseException, ClassNotFoundException, IOException
	{
	    testIndirectSynchro(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods={"testIndirectSynchro2"})
	public void testIndirectSynchroWithIndirectConnection2(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testIndirectSynchroWithIndirectConnection(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods={"testIndirectSynchroWithIndirectConnection2"})
	public void testTransactionBetweenTwoPeers2(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}


	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods={"testTransactionBetweenTwoPeers2"})
	public void testTransactionBetweenThreePeers2(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionSynchros", dependsOnMethods={"testTransactionBetweenThreePeers2"}, invocationCount=100)
	public void testTransactionSynchros2(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchros(peersInitiallyConnected, levents);
	}


	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods={"testTransactionSynchros2"})
	public void testTransactionSynchrosWithIndirectConnection2(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods={"testTransactionSynchrosWithIndirectConnection2"})
	public void testTransactionSynchrosWithIndirectConnectionThreaded2(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchrosWithIndirectConnectionThreaded(peersInitiallyConnected, levents);
	}
    
    
	    @Test(dependsOnMethods={"testTransactionSynchrosWithIndirectConnectionThreaded2"})
	    public void testSynchroTransactionTests2() throws DatabaseException
	    {
		testSynchronisation();
	    }
    
    
    @Test(dependsOnMethods={"testSynchroTransactionTests2"})
    public void removeNewPeer() throws DatabaseException, ClassNotFoundException, IOException
    {
	
	for (Database other : listDatabase)
	{
	    if (db4!=other)
	    {
		other.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());
		db4.getDbwrapper().getSynchronizer().removeHook(other.getHostID(), TableAlone.class.getPackage());
	    }
	}
	db4.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());
	
	unloadDatabase4();
	
	testAllConnect();
	testSynchronisation();
	disconnectAllDatabase();
	
    }

    
	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods={"removeNewPeer"})
	public void testSynchroBetweenTwoPeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}


	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods={"testSynchroBetweenTwoPeers3"})
	public void testSynchroBetweenThreePeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}


	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods={"testSynchroBetweenThreePeers3"})
	public void testIndirectSynchro3(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws DatabaseException, ClassNotFoundException, IOException
	{
	    testIndirectSynchro(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods={"testIndirectSynchro3"})
	public void testIndirectSynchroWithIndirectConnection3(boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testIndirectSynchroWithIndirectConnection(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods={"testIndirectSynchroWithIndirectConnection2"})
	public void testTransactionBetweenTwoPeers3(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}


	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods={"testTransactionBetweenTwoPeers3"})
	public void testTransactionBetweenThreePeers3(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionSynchros", dependsOnMethods={"testTransactionBetweenThreePeers3"}, invocationCount=100)
	public void testTransactionSynchros3(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchros(peersInitiallyConnected, levents);
	}


	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods={"testTransactionSynchros3"})
	public void testTransactionSynchrosWithIndirectConnection3(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods={"testTransactionSynchrosWithIndirectConnection3"})
	public void testTransactionSynchrosWithIndirectConnectionThreaded3(boolean peersInitiallyConnected, List<TableEvent<DatabaseRecord>> levents) throws ClassNotFoundException, DatabaseException, IOException
	{
	    testTransactionSynchrosWithIndirectConnectionThreaded(peersInitiallyConnected, levents);
	}
    
	    @Test(dependsOnMethods={"testTransactionSynchrosWithIndirectConnectionThreaded3"})
	    public void testSynchroTransactionTests3() throws DatabaseException
	    {
		testSynchronisation();
	    }
	
}

