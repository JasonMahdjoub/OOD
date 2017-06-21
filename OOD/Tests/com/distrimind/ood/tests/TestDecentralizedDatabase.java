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
package com.distrimind.ood.tests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
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
import com.distrimind.ood.database.TransactionIsolation;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.tests.decentralizeddatabase.TableAlone;
import com.distrimind.ood.tests.decentralizeddatabase.TablePointed;
import com.distrimind.ood.tests.decentralizeddatabase.TablePointing;
import com.distrimind.ood.tests.decentralizeddatabase.UndecentralizableTableA1;
import com.distrimind.ood.tests.decentralizeddatabase.UndecentralizableTableB1;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.properties.DefaultXMLObjectParser.test;

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
	    
		this.eventToSend=baos.toByteArray();
	    }
	    if (eventToSend instanceof BigDatabaseEventToSend)
	    {
		BigDatabaseEventToSend b=(BigDatabaseEventToSend)eventToSend;
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    b.exportToOutputStream(wrapper, baos);
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
	
	public List<DistantDatabaseEvent> getReceivedDatabaseEvents()
	{
	    return this.eventsReceivedStack;
	}

	@Override
	public void newDatabaseEventDetected(DatabaseWrapper _wrapper)
	{
	    System.out.println("New database event detected ");
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
    
    private volatile Database db1=null, db2=null, db3=null;
    private final ArrayList<Database> listDatabase=new ArrayList<>(3);
    
    
    public abstract DatabaseWrapper getDatabaseWrapperInstance1();
    public abstract DatabaseWrapper getDatabaseWrapperInstance2();
    public abstract DatabaseWrapper getDatabaseWrapperInstance3();
    public abstract void removeDatabaseFiles1();
    public abstract void removeDatabaseFiles2();
    public abstract void removeDatabaseFiles3();
    
    
    
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
	for (Database db : listDatabase)
	{
	    loadDatabase(db);
	    //initDatabase(db);
	}
    }

    
    
    
    
    public void unloadDatabase1()
    {
	if (db1!=null)
	{
	    try
	    {
		db1.close();
		removeDatabaseFiles1();
	    }
	    finally
	    {
		db1=null;
	    }
	}
    }
    public void unloadDatabase2()
    {
	if (db2!=null)
	{
	    try
	    {
		db2.close();
		removeDatabaseFiles2();
	    }
	    finally
	    {
		db2=null;
	    }
	}
    }
    public void unloadDatabase3()
    {
	if (db3!=null)
	{
	    try
	    {
		db3.close();
		removeDatabaseFiles3();
	    }
	    finally
	    {
		db3=null;
	    }
	}
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
		    listDatabase.clear();
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
		db.getReceivedDatabaseEvents().add(event);
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
		db.getDbwrapper().getSynchronizer().received((BigDatabaseEventToSend)event, dde.getInputStream());
	    }
	    else
		db.getDbwrapper().getSynchronizer().received(event);
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
	    loop=checkMessages();
	}
    }
    
    private void connect(Database db, Database ... listDatabase) throws DatabaseException, ClassNotFoundException, IOException
    {
	boolean exchange=false;
	synchronized(db)
	{
	    if (!db.isConnected())
	    {
		db.getDbwrapper().getSynchronizer().initLocalHostID(db.getHostID());
		for (Database otherdb : listDatabase)
		{
		    if (otherdb!=db && otherdb.isConnected())
		    {
			db.getDbwrapper().getSynchronizer().initHook(otherdb.getHostID(), otherdb.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(db.getHostID()));
			otherdb.getDbwrapper().getSynchronizer().initHook(db.getHostID(), db.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(otherdb.getHostID()));
		    }
		}
		db.setConnected(true);
		exchange=true;
		
	    }
	}
	if (exchange)
	    exchangeMessages();
    }
    
    private void connectSelectedDatabase(Database ...listDatabase) throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	    connect(db, listDatabase);
    }
    
    private void connectAllDatabase() throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	{
	    connect(db, (Database[])listDatabase.toArray());
	}
    }
    
    private void disconnect(Database db, Database ...listDatabase) throws DatabaseException, ClassNotFoundException, IOException
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
		exchangeMessages();
	    }
	}
    }
    private void disconnectSelectedDatabase(Database ...listDatabase) throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	    disconnect(db, listDatabase);
    }
    private void disconnectAllDatabase() throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	{
	    disconnect(db, (Database[])listDatabase.toArray());
	}
    }
    
    private void addTableAloneRecord(Database db, boolean first) throws DatabaseException
    {
	TableAlone.Record ralone=new TableAlone.Record();
	ralone.value="";
	for (int i=0;i<10;i++)
	{
	    ralone.value+='a'+(Math.random()*52);
	}
	db.getTableAlone().addRecord(ralone);
	if (first)
	    db.setRecordAlone(ralone);
    }
    private void addUndecentralizableTableA1Record(Database db) throws DatabaseException
    {
	UndecentralizableTableA1.Record record=new UndecentralizableTableA1.Record();
	record.value=db.getHostID().toString();
	for (int i=0;i<10;i++)
	{
	    record.value+='a'+(Math.random()*52);
	}
	db.getUndecentralizableTableA1().addRecord(record);
	
    }
    
    private void addUndecentralizableTableB1Record(Database db) throws DatabaseException
    {
	UndecentralizableTableA1.Record record=new UndecentralizableTableA1.Record();
	record.value=db.getHostID().toString();
	for (int i=0;i<10;i++)
	{
	    record.value+='a'+(Math.random()*52);
	}
	db.getUndecentralizableTableA1().addRecord(record);
	UndecentralizableTableB1.Record record2=new UndecentralizableTableB1.Record();
	record2.pointing=record;
	db.getUndecentralizableTableB1().addRecord(record2);
    }
    private void addTablePointedAndPointingRecords(Database db, boolean first) throws DatabaseException
    {
	TablePointed.Record rpointed=new TablePointed.Record();
	rpointed.value="";
	for (int i=0;i<10;i++)
	{
	    rpointed.value+='a'+(Math.random()*52);
	}
	db.getTablePointed().addRecord(rpointed);

	TablePointing.Record rpointing1=new TablePointing.Record();
	rpointing1.table2=null;
	db.getTablePointing().addRecord(rpointing1);
	TablePointing.Record rpointing2=new TablePointing.Record();
	rpointing1.table2=rpointed;
	db.getTablePointing().addRecord(rpointing2);
	if (first)
	{
	    db.setRecordPointingNull(rpointing1);
	    db.setRecordPointingNotNull(rpointing1);
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
    
    @Test(dependsOnMethods={"testInit"})
    public void testOldElementsAddedBeforeAddingSynchroSynchronized() throws DatabaseException, ClassNotFoundException, IOException
    {
	exchangeMessages();
	testSynchronisation();
	disconnectAllDatabase();
    }
    
    @DataProvider(name = "provideDataForSynchroBetweenTwoPeers", parallel = false)
    public Object[][] provideDataForSynchroBetweenTwoPeers()
    {
	return null;//TODO complete
    }
    
    private void proceedEvent(final Table<DatabaseRecord> table, final DatabaseEventType det, final DatabaseRecord oldRecord, final DatabaseRecord newRecord, final boolean exceptionDuringTransaction) throws DatabaseException
    {
	table.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

	    @Override
	    public Void run() throws Exception
	    {
		switch(det)
		{
		    case ADD:
			table.addRecord(newRecord);
			break;
		    case REMOVE:
			table.removeRecord(oldRecord);
			break;
		    case REMOVE_WITH_CASCADE:
			table.removeRecordWithCascade(oldRecord);
			break;
		    case UPDATE:
			table.updateRecord(oldRecord);
			break;
		}
		if (exceptionDuringTransaction)
		    throw new Exception();
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
    private void testEventSynchronized(Table<DatabaseRecord> table, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord, boolean synchronizedOk) throws DatabaseException
    {
	switch(det)
	{
	    case ADD:case UPDATE:
	    {
		DatabaseRecord dr=table.getRecord(newRecord);
		Assert.assertEquals(table.equalsAllFields(dr, newRecord), synchronizedOk);
		break;
	    }
	    case REMOVE:case REMOVE_WITH_CASCADE:
	    {
		DatabaseRecord dr=table.getRecord(oldRecord);
		if (synchronizedOk)
		    Assert.assertNull(dr);
		break;
	    }
	}
    }
    
    private void testDirectCollision(Database db, Table<DatabaseRecord> table, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord, DirectCollisionDetected collision) throws DatabaseException
    {
	Assert.assertNotNull(collision);
	Assert.assertEquals(collision.type, det);
	Assert.assertEquals(collision.concernedTable, table);
	Assert.assertEquals(collision.distantPeerID, db.getHostID());
	switch(det)
	{
	    case ADD:
		Assert.assertNotNull(collision.actualValues);
		Assert.assertNull(oldRecord);
		Assert.assertTrue(table.equals(newRecord, collision.actualValues));
		Assert.assertTrue(table.equalsAllFields(newRecord, collision.newValues));
		break;
	    case REMOVE:
		
		Assert.assertNotNull(oldRecord);
		Assert.assertNull(newRecord);
		Assert.assertNull(collision.newValues);
		if (collision.actualValues!=null)
		{
		    Assert.assertTrue(table.equals(oldRecord, collision.actualValues));
		}
		break;
	    case REMOVE_WITH_CASCADE:
		Assert.assertNotNull(oldRecord);
		Assert.assertNull(newRecord);
		Assert.assertNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    case UPDATE:
		Assert.assertNotNull(oldRecord);
		Assert.assertNotNull(newRecord);
		Assert.assertNotNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    
	}
    }
    private void testIndirectCollision(Database db, Table<DatabaseRecord> table, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord, IndirectCollisionDetected collision) throws DatabaseException
    {
	Assert.assertNotNull(collision);
	Assert.assertEquals(collision.type, det);
	Assert.assertEquals(collision.concernedTable, table);
	Assert.assertEquals(collision.distantPeerInCollisionWithDataToSynchronize, db.getHostID());
	
	switch(det)
	{
	    case ADD:
		Assert.assertNotNull(collision.actualValues);
		Assert.assertNull(oldRecord);
		Assert.assertTrue(table.equals(newRecord, collision.actualValues));
		Assert.assertTrue(table.equalsAllFields(newRecord, collision.newValues));
		break;
	    case REMOVE:
		
		Assert.assertNotNull(oldRecord);
		Assert.assertNull(newRecord);
		Assert.assertNull(collision.newValues);
		if (collision.actualValues!=null)
		{
		    Assert.assertTrue(table.equals(oldRecord, collision.actualValues));
		}
		break;
	    case REMOVE_WITH_CASCADE:
		Assert.assertNotNull(oldRecord);
		Assert.assertNull(newRecord);
		Assert.assertNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    case UPDATE:
		Assert.assertNotNull(oldRecord);
		Assert.assertNotNull(newRecord);
		Assert.assertNotNull(collision.newValues);
		Assert.assertNull(collision.actualValues);
		break;
	    
	}
    }
    
    private void testSynchroBetweenPeers(int peersNumber, Class<? extends Table<?>> tableClass, boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean replaceOldValueDuringConflict, boolean peersInitiallyConnected, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord) throws ClassNotFoundException, DatabaseException, IOException
    {
	if (peersNumber<2 || peersNumber>listDatabase.size())
	    throw new IllegalArgumentException();
	ArrayList<Database> l=new ArrayList<>(listDatabase.size());
	for (int i=0;i<peersNumber;i++)
	    l.add(listDatabase.get(i));
	Database[] concernedDatabase=(Database[])l.toArray();
	
	    if (exceptionDuringTransaction)
	    {
		    if (peersInitiallyConnected)
			connectSelectedDatabase(concernedDatabase);
		
		    Database db=concernedDatabase[0];
		    @SuppressWarnings("unchecked")
		    Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
		    try
		    {
			proceedEvent(table, det, oldRecord, newRecord, true);
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
			 @SuppressWarnings("unchecked")
			 Table<DatabaseRecord> tableother=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
			 Assert.assertFalse(db.isNewDatabaseEventDetected());
			 testEventSynchronized(tableother, det, oldRecord, newRecord, false);
		    }
			
		    
		    disconnectSelectedDatabase(concernedDatabase);
	    }
	    else
	    {
	    
		if (generateDirectConflict) 
		{
		    //TODO reconsider conflict when a third party is concerned
		    //TODO reconsider conflict when two sides make different choices of conflict resolution 
		    for (Database db : concernedDatabase)
		    {
			db.setReplaceWhenDirectCollisionDetected(replaceOldValueDuringConflict);
			@SuppressWarnings("unchecked")
			Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
			proceedEvent(table, det, oldRecord, newRecord, false);
		    }
		    connectSelectedDatabase(concernedDatabase);
		    exchangeMessages();
		    for (Database db : concernedDatabase)
		    {
			@SuppressWarnings("unchecked")
			Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
			DirectCollisionDetected dcollision=db.getDirectCollisionDetected();
			Assert.assertNull(db.getIndirectCollisionDetected());
			testDirectCollision(db, table, det, oldRecord, newRecord, dcollision);
			if (replaceOldValueDuringConflict)
			    Assert.assertTrue(db.isNewDatabaseEventDetected());
			else
			    Assert.assertFalse(db.isNewDatabaseEventDetected());
		    }
		    
		
		}
		else
		{
		    if (peersInitiallyConnected)
			connectSelectedDatabase(concernedDatabase);
		
		    Database db=concernedDatabase[0];
		    @SuppressWarnings("unchecked")
		    Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
		    proceedEvent(table, det, oldRecord, newRecord, false);

		    if (!peersInitiallyConnected)
			connectSelectedDatabase(concernedDatabase);
		   
		    exchangeMessages();

		    for (int i=1;i<peersNumber;i++)
		    {
			 db=concernedDatabase[i];
			 @SuppressWarnings("unchecked")
			 Table<DatabaseRecord> tableother=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
			 Assert.assertNull(db.getDirectCollisionDetected());
			 Assert.assertNull(db.getIndirectCollisionDetected());
			 Assert.assertTrue(db.isNewDatabaseEventDetected());
			 testEventSynchronized(tableother, det, oldRecord, newRecord, true);
			 
		    }
		
		}

		for (int i=peersNumber;i<listDatabase.size();i++)
		{
		    Database db=listDatabase.get(i);
		    @SuppressWarnings("unchecked")
		    Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
		    testEventSynchronized(table, det, oldRecord, newRecord, false);
		}
	    
		connectAllDatabase();
		exchangeMessages();
	    
		for (int i=2;i<listDatabase.size();i++)
		{
		    Database db=listDatabase.get(i);
		    @SuppressWarnings("unchecked")
		    Table<DatabaseRecord> table=(Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(tableClass);
		    
		    Assert.assertNull(db.getDirectCollisionDetected());
		    Assert.assertNull(db.getIndirectCollisionDetected());
		    Assert.assertTrue(db.isNewDatabaseEventDetected());
		    testEventSynchronized(table, det, oldRecord, newRecord, true);
		}
		
		
		disconnectAllDatabase();
	    }
	
    }
    
    
    @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods={"testOldElementsAddedBeforeAddingSynchroSynchronized"})
    public void testSynchroBetweenTwoPeers(Class<? extends Table<?>> tableClass, boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean replaceOldValueDuringConflict, boolean peersInitiallyConnected, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord) throws ClassNotFoundException, DatabaseException, IOException
    {
	testSynchroBetweenPeers(2, tableClass, exceptionDuringTransaction, generateDirectConflict, replaceOldValueDuringConflict, peersInitiallyConnected, det, oldRecord, newRecord);
    }

    @DataProvider(name = "provideDataSynchroBetweenThreePeers", parallel = false)
    public Object[][] provideDataSynchroBetweenThreePeers()
    {
	return provideDataForSynchroBetweenTwoPeers();
    }
    
    @Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods={"testSynchroBetweenTwoPeers"})
    public void testSynchroBetweenThreePeers(Class<? extends Table<?>> tableClass, boolean exceptionDuringTransaction, boolean generateDirectConflict, boolean replaceOldValueDuringConflict, boolean peersInitiallyConnected, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord) throws ClassNotFoundException, DatabaseException, IOException
    {
	testSynchroBetweenPeers(3, tableClass, exceptionDuringTransaction, generateDirectConflict, replaceOldValueDuringConflict, peersInitiallyConnected, det, oldRecord, newRecord);
    }
    
    
    @DataProvider(name = "provideDataForIndirectSynchro", parallel = false)
    public Object[][] provideDataForIndirectSynchro()
    {
	return null;//TODO complete
    }
    
    @Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods={"testSynchroBetweenThreePeers"})
    public void testIndirectSynchro(boolean normalTransaction, boolean generateConflict, boolean peersInitiallyConnected, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord, boolean expectedSynchronized)
    {
	
    }
    @DataProvider(name = "provideDataForIndirectSynchroWithIndirectConnection", parallel = false)
    public Object[][] provideDataForIndirectSynchroWithIndirectConnection()
    {
	return null;//TODO complete
    }
    
    @Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods={"testIndirectSynchro"})
    public void testIndirectSynchroWithIndirectConnection(boolean normalTransaction, boolean generateConflict, boolean peersInitiallyConnected, DatabaseEventType det, DatabaseRecord oldRecord, DatabaseRecord newRecord, boolean expectedSynchronized)
    {
	
    }
    
    
    @DataProvider(name = "provideDataForTransactionBetweenTwoPeers", parallel = true)
    public Object[][] provideDataForTransactionBetweenTwoPeers()
    {
	return null;//TODO complete
    }
    
    @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testTransactionBetweenTwoPeers(boolean normalTransaction, boolean generateOneConflict, boolean peersInitiallyConnected, int transactionNumber)
    {
	
    }
    
    @DataProvider(name = "provideDataForSynchroBetweenTwoPeers", parallel = true)
    public Object[][] provideDataForTransactionBetweenThreePeers()
    {
	return null;//TODO complete
    }

    @Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testTransactionBetweenThreePeers(boolean normalTransaction, boolean generateOneConflict, boolean peersInitiallyConnected, int transactionNumber)
    {
	
    }
    
    @DataProvider(name = "provideDataForTransactionSynchros", parallel = true)
    public Object[][] provideDataForTransactionSynchros()
    {
	return null;//TODO complete
    }
    
    @Test(dataProvider = "provideDataForTransactionSynchros", dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testTransactionSynchros(boolean normalTransaction, boolean generateOneConflict, boolean peersInitiallyConnected, int transactionNumber)
    {
	
    }
    
    @DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection", parallel = true)
    public Object[][] provideDataForTransactionSynchrosWithIndirectConnection()
    {
	return null;//TODO complete
    }

    @Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods={"testIndirectSynchroWithIndirectConnection"})
    public void testTransactionSynchrosWithIndirectConnection(boolean normalTransaction, boolean generateOneConflict, boolean peersInitiallyConnected, int transactionNumber)
    {
	
    }
    
    @Test(dependsOnMethods={"testTransactionSynchrosWithIndirectConnection", "testTransactionSynchros", "testTransactionBetweenThreePeers", "testTransactionBetweenTwoPeers"})
    public void addNewPeer()
    {
	
    }
    
    //TODO redo all tests
    
    @Test
    public void removeNewPeer()
    {
	
    }
    
}

