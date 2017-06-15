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
import java.util.LinkedList;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import com.distrimind.ood.database.BigDatabaseEventToSend;
import com.distrimind.ood.database.DatabaseConfiguration;
import com.distrimind.ood.database.DatabaseCreationCallable;
import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseEventToSend;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.tests.decentralizeddatabase.TableAlone;
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
    
    public class Database implements AutoCloseable
    {
	private final DatabaseWrapper dbwrapper;
	private boolean connected;
	private final AbstractDecentralizedID hostID;
	private final ArrayList<DatabaseEvent> localEvents;
	private final LinkedList<DistantDatabaseEvent> eventsReceivedStack;
	
	Database(DatabaseWrapper dbwrapper)
	{
	    this.dbwrapper=dbwrapper;
	    connected=false;
	    hostID=new DecentralizedIDGenerator();
	    localEvents=new ArrayList<>();
	    eventsReceivedStack=new LinkedList<>();
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
	}
	
	public LinkedList<DistantDatabaseEvent> getReceivedDatabaseEvents()
	{
	    return this.eventsReceivedStack;
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
	    initDatabase(db);
	}
    }
    
    private void initDatabase(Database db) throws DatabaseException
    {
	db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(), TableAlone.class.getPackage());
	for (Database otherdb : listDatabase)
	{
	    if (otherdb!=db)
	    {
		db.getDbwrapper().getSynchronizer().addHookForDistantHost(otherdb.getHostID(), TableAlone.class.getPackage());
	    }
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
	    db.clearPendingEvents();
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
	    DistantDatabaseEvent dde=db.getReceivedDatabaseEvents().removeFirst();
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
	    changed|=checkMessages(db);
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
	    loop=checkMessages();
	}
    }
    
    private void connect(Database db) throws DatabaseException, ClassNotFoundException, IOException
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
	    
	    exchangeMessages();
	    
	    
	}
    }
    
    private void connectAllDatabase() throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	{
	    connect(db);
	}
    }
    
    private void disconnect(Database db) throws DatabaseException, ClassNotFoundException, IOException
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
    private void disconnectAllDatabase() throws ClassNotFoundException, DatabaseException, IOException
    {
	for (Database db : listDatabase)
	{
	    disconnect(db);
	}
    }
    
}

