
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
import java.io.EOFException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.DatabaseWrapper.DatabaseNotifier;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.AbstractDecentralizedID;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
class DatabaseTransactionsPerHostTable extends Table<DatabaseTransactionsPerHostTable.Record>
{
    
    
    private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable=null;
    private volatile DatabaseHooksTable databaseHooksTable=null;
    private volatile DatabaseEventsTable databaseEventsTable=null;
    private volatile DatabaseDistantTransactionEvent databaseDistantTransactionEventTable=null;
    private volatile IDTable idTable=null;
    
    
    protected DatabaseTransactionsPerHostTable() throws DatabaseException
    {
	super();
    }
    
    DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException
    {
	if (databaseTransactionEventsTable==null)
	    databaseTransactionEventsTable=(DatabaseTransactionEventsTable)getDatabaseWrapper().getTableInstance(DatabaseTransactionEventsTable.class);
	return databaseTransactionEventsTable; 
    }
    DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException
    {
	if (databaseEventsTable==null)
	    databaseEventsTable=(DatabaseEventsTable)getDatabaseWrapper().getTableInstance(DatabaseEventsTable.class);
	return databaseEventsTable; 
    }

    DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException
    {
	if (databaseHooksTable==null)
	    databaseHooksTable=(DatabaseHooksTable)getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
	return databaseHooksTable; 
    }
    DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException
    {
	if (databaseDistantTransactionEventTable==null)
	    databaseDistantTransactionEventTable=(DatabaseDistantTransactionEvent)getDatabaseWrapper().getTableInstance(DatabaseDistantTransactionEvent.class);
	return databaseDistantTransactionEventTable; 
    }

    IDTable getIDTable() throws DatabaseException
    {
	if (idTable==null)
	    idTable=(IDTable)getDatabaseWrapper().getTableInstance(IDTable.class);
	return idTable; 
	   
    }

    static class Record extends DatabaseRecord
    {
	@PrimaryKey @ForeignKey
	private DatabaseTransactionEventsTable.Record transaction;
	@PrimaryKey @ForeignKey
	private DatabaseHooksTable.Record hook;
	
	void set(DatabaseTransactionEventsTable.Record _transaction, DatabaseHooksTable.Record _hook)
	{
	    transaction = _transaction;
	    hook = _hook;
	}

	DatabaseTransactionEventsTable.Record getTransaction()
	{
	    return transaction;
	}
	
	DatabaseHooksTable.Record getHook()
	{
	    return hook;
	}
    }
    
    
    void removeTransactions(final DatabaseHooksTable.Record hook, final Package...removedPackages) throws DatabaseException
    {
	if (hook==null)
	    throw new NullPointerException("hook");
	getDatabaseWrapper().runTransaction(new Transaction() {
	    
	    @Override
	    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	    {
		
		DatabaseTransactionsPerHostTable.this.removeRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {
		    
		    @Override
		    public boolean nextRecord(com.distrimind.ood.database.DatabaseTransactionsPerHostTable.Record _record) 
		    {
			return _record.getHook().getID()==hook.getID() && _record.getTransaction().isConcernedByOneOf(removedPackages);
		    }
		});
		getDatabaseTransactionEventsTable().removeUnusedTransactions();
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
    
    
    
    void validateTransactions(final DatabaseHooksTable.Record hook, final long lastID) throws DatabaseException
    {
	if (hook==null)
	    throw new NullPointerException("hook");

	if (hook.getLastValidatedTransaction()<lastID)
	{
	    getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

		@Override
		public Void run() throws Exception
		{
		    hook.setLastValidatedTransaction(lastID);
		    getDatabaseHooksTable().updateRecord(hook);
		    removeRecords("transaction.id<%lastID", "lastID", new Long(lastID));
		    
		    getDatabaseTransactionEventsTable().removeTransactionsFromLastID();
		    
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
    }   
    
    
    private HashSet<DatabaseTransactionEventsTable.Record> detectCollisionAndRemoveObsoleteEvents(final AbstractDecentralizedID comingFrom, final String concernedTable, final byte[] keys) throws DatabaseException
    {
	final HashSet<DatabaseTransactionEventsTable.Record> toRemove=new HashSet<>();
	final AtomicBoolean collisionDetected=new AtomicBoolean(false);
	getDatabaseEventsTable().getRecords(new Filter<DatabaseEventsTable.Record>() {
	    
	    @Override
	    public boolean nextRecord(com.distrimind.ood.database.DatabaseEventsTable.Record _record) throws DatabaseException
	    {
		//if (_record.getConcernedTable().equals(concernedTable.getName()) && Arrays.equals(_record.getConcernedSerializedPrimaryKey(), keys))
		{
		    for (DatabaseTransactionsPerHostTable.Record rph : DatabaseTransactionsPerHostTable.this.getRecordsWithAllFields(new Object[]{"transaction", _record.getTransaction()}))
		    {
			if (rph.getHook().getHostID().equals(comingFrom))
			{
			    collisionDetected.set(true);
			    this.stopTableParsing();
			}
			else
			    toRemove.add(rph.getTransaction());
			    
		    }
		}
		return false;
	    }
	}, "concernedTable==%concernedTable AND concernedSerializedPrimaryKey==%concernedSerializedPrimaryKey", "concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);
	if (collisionDetected.get())
	    return null;
	else 
	    return toRemove;
	    /*if (toRemove.size()>0)
	{
	    //getDatabaseTransactionEventsTable().removeRecordsWithCascade(toRemove);
	    return toRemove;
	}
	return null;*/
    }
    
    void removeObsoleteEvents(final HashSet<DatabaseTransactionEventsTable.Record> toRemove) throws DatabaseException
    {
	getDatabaseTransactionEventsTable().removeRecordsWithCascade(toRemove);
    }
    
    void alterDatabase(final AbstractDecentralizedID comingFrom, final InputStream inputStream) throws DatabaseException
    {
	alterDatabase(comingFrom, comingFrom, inputStream, getDatabaseWrapper().getSynchronizer().getNotifier(), false);
    }
    
    private void alterDatabase(final AbstractDecentralizedID directPeerID, final AbstractDecentralizedID comingFrom, final InputStream inputStream, final DatabaseNotifier notifier, boolean indirectTransactionRead) throws DatabaseException
    {
	if (comingFrom==null)
	    throw new NullPointerException("comingFrom");
	if (inputStream==null)
	    throw new NullPointerException("inputStream");
	if (directPeerID==null)
	    throw new NullPointerException("directPeerID");
	if (directPeerID!=comingFrom && !indirectTransactionRead)
	    throw new IllegalAccessError();
	
	ArrayList<DatabaseHooksTable.Record> hooks=getDatabaseHooksTable().getRecords("hostID", comingFrom);
	if (hooks.isEmpty())
	    throw new SerializationDatabaseException("The give host id is not valid : "+comingFrom);
	else if (hooks.size()>1)
	    throw new IllegalAccessError();
	
	final AtomicReference<DatabaseHooksTable.Record> fromHook=new AtomicReference<>(hooks.get(0));
	try(ObjectInputStream ois=new ObjectInputStream(inputStream))
	{
	    byte next=ois.readByte();
	    while(next!=EXPORT_FINISHED)
	    {
		if (next==EXPORT_INDIRECT_TRANSACTION)
		{
		    if (indirectTransactionRead)
			throw new SerializationDatabaseException("Unexpected exception !");
		    DatabaseDistantTransactionEvent.Record indirectTransactionEvent=getDatabaseDistantTransactionEvent().unserializeDistantTransactionEvent(ois);
		    alterDatabase(directPeerID, indirectTransactionEvent.getHook().getHostID(), new ByteArrayInputStream(indirectTransactionEvent.getTransaction()), notifier, true);
		}
		else if (next==EXPORT_DIRECT_TRANSACTION)
		{
		    try(ByteArrayOutputStream baos=new ByteArrayOutputStream(); ObjectOutputStream oos=new ObjectOutputStream(baos))
		    {
			oos.writeByte(EXPORT_DIRECT_TRANSACTION);
			final DatabaseTransactionEventsTable.Record dte=getDatabaseTransactionEventsTable().unserialize(ois, true, false);
			next=ois.readByte();
			final DatabaseTransactionEvent localDTE=new DatabaseTransactionEvent();
			boolean transactionNotEmpty=false;
			boolean validatedTransaction=true;
			final HashSet<DatabaseTransactionEventsTable.Record> toRemove=new HashSet<>();
			
			while (next==EXPORT_DIRECT_TRANSACTION_EVENT)
			{
			    transactionNotEmpty=true;
			    oos.writeByte(next);
			    byte typeb=ois.readByte();
			    oos.writeByte(typeb);
			    int size=ois.readInt();
			    oos.writeInt(size);
			    if (size>Table.maxTableNameSizeBytes)
				throw new SerializationDatabaseException("Table name too big");
			    char[] chrs=new char[size];
			    for (int i=0;i<size;i++)
				chrs[i]=ois.readChar();
			    String tableName=String.valueOf(chrs);
			    oos.writeChars(tableName);
			    Table<?> t=null;
			    try
			    {
				t=getDatabaseWrapper().getTableInstance(tableName);
			    }
			    catch(Exception e)
			    {
				throw new SerializationDatabaseException("", e);
			    }
			    size=ois.readInt();
			    oos.writeInt(size);
			    if (size>Table.maxPrimaryKeysSizeBytes)
				throw new SerializationDatabaseException("Table name too big");
			    byte spks[]=new byte[size];
			    if (ois.read(spks)!=size)
				throw new SerializationDatabaseException("Impossible to read the expected bytes number : "+size);
			    oos.write(spks);
			    DatabaseEventType type=DatabaseEventType.getEnum(typeb);
			    if (type==null)
				throw new SerializationDatabaseException("Impossible to decode database event type : "+typeb);
			    DatabaseRecord drNew=null, drOld=null;
			    HashMap<String, Object> mapKeys=new HashMap<>();
			    t.unserializePrimaryKeys(mapKeys, spks);
			    if (type.needsNewValue())
			    {
				drNew=t.unserialize(ois, false, true);
				t.unserializePrimaryKeys(drNew, spks);
				t.serialize(drNew, oos, false, true);
			    }
			    if (type.hasOldValue()) 
			    {
				drOld=t.getRecord(mapKeys);
			    }
			    HashSet<DatabaseTransactionEventsTable.Record> r=detectCollisionAndRemoveObsoleteEvents(fromHook.get().getHostID(), tableName, spks);
			    if (r==null)
			    {
				validatedTransaction=false;
				if (notifier!=null)
				{
				    if (indirectTransactionRead)
				    {
					notifier.indirectCollisionDetected(directPeerID, type, t, mapKeys, drNew, drOld, fromHook.get().getHostID());
				    }
				    else
					notifier.directCollisionDetected(directPeerID, type, t, mapKeys, drNew, drOld);
				}
			    }
			    else
				toRemove.addAll(r);
			    if (validatedTransaction)
			    {
				switch(type)
				{
				    case ADD:
				    {
					localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew));
				    }
				    break;
				    case REMOVE:case REMOVE_WITH_CASCADE:
				    {
					if (drOld==null)
					{
					    if (notifier!=null)
						notifier.recordToRemoveNotFound(t, mapKeys);
					    validatedTransaction=false;
					}
					else
					{
					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew));
					}
				    
				    }
				    break;
				    case UPDATE:
					if (drOld==null)
					{
					    if (notifier!=null)
						notifier.recordToUpdateNotFound(t, mapKeys);
					    validatedTransaction=false;
					}
					else
					{
					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew));
					}
					break;
				}
			    }
			    next=ois.readByte();
			}
			if (transactionNotEmpty)
			{
			    if (validatedTransaction)
			    {
				getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

				    @Override
				    public Void run() throws Exception
				    {
					removeObsoleteEvents(toRemove);
					oos.flush();
					DatabaseDistantTransactionEvent.Record ddte=new DatabaseDistantTransactionEvent.Record(dte.getID(), getIDTable().getLastTransactionID(), fromHook.get(), baos.toByteArray());
					if (getDatabaseHooksTable().isConcernedByIndirectTransaction(ddte))
					{
					    getDatabaseDistantTransactionEvent().addRecord(ddte);
					}
					for (TableEvent<?> te : localDTE.getEvents())
					{
					    switch(te.getType())
					    {
						case ADD:
						    te.getTable(getDatabaseWrapper()).addRecord(te.getNewDatabaseRecord());
						    break;
						case REMOVE:
						    te.getTable(getDatabaseWrapper()).removeUntypedRecord(te.getOldDatabaseRecord());
						    break;
						case REMOVE_WITH_CASCADE:
						    te.getTable(getDatabaseWrapper()).removeUntypedRecordWithCascade(te.getOldDatabaseRecord());
						    break;
						case UPDATE:
						    te.getTable(getDatabaseWrapper()).updateUntypedRecord(te.getNewDatabaseRecord());
						    break;
					    }
					}
					fromHook.set(getDatabaseHooksTable().getRecord("id", new Integer(fromHook.get().getID())));
					fromHook.get().setLastValidatedDistantTransaction(dte.getID());
					getDatabaseHooksTable().updateRecord(fromHook.get());
					
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
				getDatabaseWrapper().getSynchronizer().addNewDatabaseEvent(localDTE);
			    }
			}
			else
			    throw new SerializationDatabaseException("The transaction should not be empty");
		    }
		}
		else
		{
		    throw new SerializationDatabaseException("Unexpected code : "+next);
		}
	    }
	}
	catch(EOFException e)
	{
	    throw new SerializationDatabaseException("Unexpected EOF", e);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
	
    }
    
    
    
    int exportTransactions(OutputStream outputStream, final int hookID, final int maxEventsRecords) throws DatabaseException
    {
	
	
	
	final AtomicInteger number=new AtomicInteger(0);
	//TODO export first indirect data
	
	final AtomicLong nearNextLocalID=new AtomicLong();
    	final DatabaseHooksTable.Record hook=getDatabaseHooksTable().getRecord("id", new Integer(hookID));
	    
    	if (hook==null)
    	    return 0;
	long currentTransactionID=hook.getLastValidatedTransaction();
    	number.set(getDatabaseDistantTransactionEvent().exportTransactions(outputStream, hook, maxEventsRecords, currentTransactionID, nearNextLocalID));
	if (number.get()>=maxEventsRecords)
	    return number.get();
    	
	try(ObjectOutputStream oos=new ObjectOutputStream(outputStream))
	    {
	    	do
	    	{
	    	    Collection<DatabaseTransactionsPerHostTable.Record> records=getOrderedRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {
		    
	    		@Override
	    		public boolean nextRecord(Record _record) 
	    		{
	    		    if (number.incrementAndGet()>=maxEventsRecords)
	    		    {
	    			this.stopTableParsing();
	    			return false;
	    		    }
	    		    return true;
	    		}
	    	    }, "transaction.id<=%nearNextLocalID", new Object[]{"nearNextLocalID", new Long(nearNextLocalID.get())}, true, "transaction.id");
	    	    currentTransactionID=nearNextLocalID.get();
	    	    
	    	    
	    	    for (Record r : records)
	    	    {
	    		oos.writeByte(EXPORT_DIRECT_TRANSACTION);
	    		getDatabaseTransactionEventsTable().serialize(r.getTransaction(), oos, true, false);
	    		getDatabaseEventsTable().getRecords(new Filter<DatabaseEventsTable.Record>(){

			    @Override
			    public boolean nextRecord(com.distrimind.ood.database.DatabaseEventsTable.Record _record) throws DatabaseException
			    {
				try
				{
				    oos.writeByte(EXPORT_DIRECT_TRANSACTION_EVENT);
				    oos.writeByte(_record.getType());
				    oos.writeInt(_record.getConcernedTable().length());
				    oos.writeChars(_record.getConcernedTable());
				    oos.writeInt(_record.getConcernedSerializedPrimaryKey().length);
				    oos.write(_record.getConcernedSerializedPrimaryKey());
				    if (DatabaseEventType.getEnum(_record.getType()).needsNewValue())
				    {
					Table<?> t=getDatabaseWrapper().getTableInstance(_record.getConcernedTable());
					HashMap<String, Object> pks=new HashMap<>();
					t.unserializePrimaryKeys(pks, _record.getConcernedSerializedPrimaryKey());
					DatabaseRecord cr=t.getRecord(pks);
					if (cr==null)
					    throw new DatabaseException("Unexpected exception !");
					t.serialize(cr, oos, false, true);
				    }
				
				    return false;
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
			    }
	    		    
	    		}, "transaction=%transaction", "transaction", r.getTransaction());
	    		
	    	    }
	    	    if (number.get()<maxEventsRecords)
	    		number.set(number.get()+getDatabaseDistantTransactionEvent().exportTransactions(outputStream, hook, maxEventsRecords-number.get(), currentTransactionID, nearNextLocalID));
	    	}
	    	while(number.get()<maxEventsRecords && nearNextLocalID.get()!=currentTransactionID);
		    

		oos.writeByte(EXPORT_FINISHED);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    
	    return number.get();
	
    }
    
    static final byte EXPORT_FINISHED=0;
    static final byte EXPORT_DIRECT_TRANSACTION=1;
    static final byte EXPORT_INDIRECT_TRANSACTION=2;
    static final byte EXPORT_DIRECT_TRANSACTION_EVENT=4;
}
