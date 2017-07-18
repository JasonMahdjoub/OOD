
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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.DatabaseDistantTransactionEvent.Record;
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
final class DatabaseTransactionsPerHostTable extends Table<DatabaseTransactionsPerHostTable.Record>
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
		    removeRecords("transaction.id<=%lastID AND hook=%hook", "lastID", new Long(lastID), "hook", hook);
		    final AtomicLong actualLastID=new AtomicLong(Long.MAX_VALUE);
		    getRecords(new Filter<DatabaseTransactionsPerHostTable.Record>(){

			@Override
			public boolean nextRecord(Record _record) 
			{
			    if (_record.getTransaction().getID()<actualLastID.get())
				actualLastID.set(_record.getTransaction().getID()-1);
			    if (actualLastID.get()==lastID)
				this.stopTableParsing();
			    return false;
			}
			
		    }, "hook=%hook", "hook", hook);
		    if (actualLastID.get()==Long.MAX_VALUE)
			actualLastID.set(getIDTable().getLastTransactionID());
		    hook.setLastValidatedTransaction(actualLastID.get());
		    getDatabaseHooksTable().updateRecord(hook);
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
    
    
    protected boolean detectCollisionAndGetObsoleteEventsToRemove(final AbstractDecentralizedID comingFrom, final String concernedTable, final byte[] keys, final boolean force, final Set<DatabaseTransactionEventsTable.Record> toRemove, final Set<AbstractDecentralizedID> resendTo) throws DatabaseException
    {
	
	final AtomicBoolean collisionDetected=new AtomicBoolean(false);
	getDatabaseEventsTable().getRecords(new Filter<DatabaseEventsTable.Record>() {
	    
	    @Override
	    public boolean nextRecord(com.distrimind.ood.database.DatabaseEventsTable.Record _record) throws DatabaseException
	    {

		//if (_record.getConcernedTable().equals(concernedTable.getName()) && Arrays.equals(_record.getConcernedSerializedPrimaryKey(), keys))
		{
		    for (DatabaseTransactionsPerHostTable.Record rph : DatabaseTransactionsPerHostTable.this.getRecordsWithAllFields(new Object[]{"transaction", _record.getTransaction()}))
		    {
			toRemove.add(rph.getTransaction());
			if (!force)
			{
			    if (rph.getHook().getHostID().equals(comingFrom))
				collisionDetected.set(true);
			    else if (!rph.getHook().concernsLocalDatabaseHost())
				resendTo.add(rph.getHook().getHostID());
			}
			
			    
			/*else
			    toRemove.add(rph.getTransaction());*/
			    
		    }
		}
		return false;
	    }
	}, "concernedTable==%concernedTable AND concernedSerializedPrimaryKey==%concernedSerializedPrimaryKey", "concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);
	
	return collisionDetected.get();
	    /*if (toRemove.size()>0)
	{
	    //getDatabaseTransactionEventsTable().removeRecordsWithCascade(toRemove);
	    return toRemove;
	}
	return null;*/
    }
    
    /*protected void replaceDataFromDistantPeer(final AbstractDecentralizedID comingFrom, final DatabaseTransactionEvent transaction, final String concernedDatabasePackage, HashSet<DatabaseTransactionEventsTable.Record> toRemove) throws DatabaseException
    {
	removeObsoleteEvents(toRemove);
	if (transaction.getEvents().isEmpty())
	    return;
	
	
	
	final AtomicReference<DatabaseTransactionEventsTable.Record> tr=new AtomicReference<>();
	getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.database.DatabaseHooksTable.Record _record) throws DatabaseException
	    {
		if (!_record.concernsLocalDatabaseHost() && !_record.getHostID().equals(comingFrom))
		{
		    if (tr.get()==null)
			tr.set(getDatabaseTransactionEventsTable().addTransaction(concernedDatabasePackage, transaction));
		    DatabaseTransactionsPerHostTable.Record trh=new DatabaseTransactionsPerHostTable.Record();
		    trh.set(tr.get(), _record);
		    addRecord(trh);
		}
		return false;
	    }
	    
	});
    }*/
    
    void removeObsoleteEvents(final HashSet<DatabaseTransactionEventsTable.Record> toRemove) throws DatabaseException
    {
	getDatabaseTransactionEventsTable().removeRecordsWithCascade(toRemove);
    }
    
    void alterDatabase(final AbstractDecentralizedID comingFrom, final InputStream inputStream) throws DatabaseException
    {
	alterDatabase(comingFrom, comingFrom, inputStream, getDatabaseWrapper().getSynchronizer().getNotifier(), null);
    }
    
    private void alterDatabase(final AbstractDecentralizedID directPeerID, final AbstractDecentralizedID comingFrom, final InputStream inputStream, final DatabaseNotifier notifier, final DatabaseDistantTransactionEvent.Record indirectTransactionEvent) throws DatabaseException
    {
	
    }
    
    private void alterDatabase(final AbstractDecentralizedID directPeerID, final AbstractDecentralizedID comingFrom, final InputStream inputStream, final DatabaseNotifier notifier, final DatabaseDistantTransactionEvent.Record indirectTransactionEvent) throws DatabaseException
    {
	
	if (comingFrom==null)
	    throw new NullPointerException("comingFrom");
	if (inputStream==null)
	    throw new NullPointerException("inputStream");
	if (directPeerID==null)
	    throw new NullPointerException("directPeerID");
	if (directPeerID!=comingFrom && indirectTransactionEvent==null)
	    throw new IllegalAccessError();
	if (comingFrom.equals(getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
	    throw new IllegalArgumentException("The given distant host ID cannot be equals to the local host ID : "+comingFrom);
	
	ArrayList<DatabaseHooksTable.Record> hooks=getDatabaseHooksTable().getRecordsWithAllFields("hostID", comingFrom);
	if (hooks.isEmpty())
	    throw new SerializationDatabaseException("The give host id is not valid : "+comingFrom);
	else if (hooks.size()>1)
	    throw new IllegalAccessError();
	
	final AtomicReference<DatabaseHooksTable.Record> fromHook=new AtomicReference<>(hooks.get(0));
	final AtomicLong lastValidatedTransaction=new AtomicLong(-1);
	
	try(DataInputStream ois=new DataInputStream(inputStream))
	{
	    final AtomicInteger next=new AtomicInteger(ois.readByte());
	    while(next.get()!=EXPORT_FINISHED)
	    {
		if (next.get()==EXPORT_INDIRECT_TRANSACTION)
		{
		    if (indirectTransactionEvent!=null)
			throw new SerializationDatabaseException("Unexpected exception !");
		    ArrayList<DatabaseDistantEventsTable.Record> events=new ArrayList();
		    DatabaseDistantTransactionEvent.Record ite=getDatabaseDistantTransactionEvent().unserializeDistantTransactionEvent(ois, events);
		    next.set(ois.readByte());
		    alterDatabase(directPeerID, ite.getHook().getHostID(), new ByteArrayInputStream(ite.getTransaction()), notifier, ite);
		    
		}
		else if (next.get()==EXPORT_DIRECT_TRANSACTION)
		{
		    try(ByteArrayOutputStream baos=new ByteArrayOutputStream(); DataOutputStream oos=new DataOutputStream(baos))//TODO store transaction into disk if too big
		    {
			if (indirectTransactionEvent==null)
			    oos.writeByte(EXPORT_DIRECT_TRANSACTION);
			final DatabaseTransactionEventsTable.Record dte=getDatabaseTransactionEventsTable().unserialize(ois, true, false);
			if (indirectTransactionEvent==null)
			    getDatabaseTransactionEventsTable().serialize(dte, oos, true, false);
			
			next.set(ois.readByte());
			getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

			    @SuppressWarnings("unchecked")
			    @Override
			    public Void run() throws Exception
			    {
				final DatabaseTransactionEvent localDTE=new DatabaseTransactionEvent();
				boolean transactionNotEmpty=false;
				boolean validatedTransaction=true;
				HashSet<DatabaseTransactionEventsTable.Record> toRemove=new HashSet<>();
				Set<AbstractDecentralizedID> hostsDestination=new HashSet<>();
				//fromHook.set(getDatabaseHooksTable().getRecord("id", new Integer(fromHook.get().getID())));
				
				//boolean transactionToResend=false;

				while (next.get()==EXPORT_DIRECT_TRANSACTION_EVENT)
				{
				    transactionNotEmpty=true;
				    if (indirectTransactionEvent==null)
					oos.writeByte(next.get());
				    byte typeb=ois.readByte();
				    if (indirectTransactionEvent==null)
					oos.writeByte(typeb);
				    int size=ois.readInt();
				    if (indirectTransactionEvent==null)
					oos.writeInt(size);
				    if (size>Table.maxTableNameSizeBytes)
					throw new SerializationDatabaseException("Table name too big");
				    char[] chrs=new char[size];
				    for (int i=0;i<size;i++)
					chrs[i]=ois.readChar();
				    String tableName=String.valueOf(chrs);
				    if (indirectTransactionEvent==null)
				    {
					for (int i=0;i<size;i++)
					    oos.writeChar(chrs[i]);
				    }
				    Table<DatabaseRecord> t=null;
				    try
				    {
					t=(Table<DatabaseRecord>)getDatabaseWrapper().getTableInstance(tableName);
				    }
				    catch(Exception e)
				    {
					throw new SerializationDatabaseException("", e);
				    }
				    size=ois.readInt();
				    if (indirectTransactionEvent==null)
					oos.writeInt(size);
				    if (size>Table.maxPrimaryKeysSizeBytes)
					throw new SerializationDatabaseException("Table name too big");
				    byte spks[]=new byte[size];
				    if (ois.read(spks)!=size)
					throw new SerializationDatabaseException("Impossible to read the expected bytes number : "+size);
				    if (indirectTransactionEvent==null)
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
					if (indirectTransactionEvent==null)
					    t.serialize(drNew, oos, false, true);
				    }

				    if (type.hasOldValue() || dte.isForce()) 
        			    {
        				drOld=t.getRecord(mapKeys);
        			    }
        			    boolean eventForce=false;
        			    if (dte.getID()<=fromHook.get().getLastValidatedDistantTransaction())
        				validatedTransaction=false;
        			    if (validatedTransaction)
        			    {
        				HashSet<DatabaseTransactionEventsTable.Record> r=new HashSet<>();
        				Set<AbstractDecentralizedID> hd=new HashSet<>();
        				boolean collision=detectCollisionAndGetObsoleteEventsToRemove(fromHook.get().getHostID(), tableName, spks, dte.isForce(), r, hd);
        				if (collision)
        				{
        				    if (!type.hasOldValue())
        					drOld=t.getRecord(mapKeys);
        				    if (notifier!=null)
        				    {
        					if (indirectTransactionEvent!=null)
        					{
        					    validatedTransaction&=(eventForce=notifier.indirectCollisionDetected(directPeerID, type, t, mapKeys, drNew, drOld, fromHook.get().getHostID()));
        					}
        					else
        					    validatedTransaction&=(eventForce=notifier.directCollisionDetected(directPeerID, type, t, mapKeys, drNew, drOld));
        				    }
        				    else
        					validatedTransaction=false;
        				    
        				}
        				    
        				if (validatedTransaction)
        				{
        				    toRemove.addAll(r);
        				    hostsDestination.addAll(hd);
        				}
        			    }
        			    
        			    if (validatedTransaction)
        			    {
        				switch(type)
        				{
        				    case ADD:
        				    {
        					if (drOld!=null)
        					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null, null, true, t));
        					else
        					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null));
        				    }
        				    break;
        				    case REMOVE:case REMOVE_WITH_CASCADE:
        				    {
        					if (drOld==null)
        					{
        					    if (!dte.isForce() && !eventForce)
        					    {
        						if (notifier!=null)
        						    notifier.recordToRemoveNotFound(t, mapKeys);
        						validatedTransaction=false;
        					    }
        					    else
        						localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null, mapKeys, false, t));
        					}
        					else
        					{
        					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null));
        					}
        				    
        				    }
        				    break;
        				    case UPDATE:
        					if (drOld==null && !eventForce && !dte.isForce())
        					{
        					    if (notifier!=null)
        						notifier.recordToUpdateNotFound(t, mapKeys);
        					    validatedTransaction=false;
        					}
        					else
        					{
        					    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null));
        					}
        					break;
        				}
        			    }
        			    next.set(ois.readByte());
        			    /*if (indirectTransactionEvent==null)
        				oos.writeByte(next.get());*/
        			}
				if (dte.getID()>fromHook.get().getLastValidatedDistantTransaction())
				{
				    if (lastValidatedTransaction.get()>dte.getID())
					throw new DatabaseException("Transactions must be ordered !");
				    lastValidatedTransaction.set(dte.getID());
				}
				
				if (transactionNotEmpty)
				{
			    	    if (validatedTransaction)
			    	    {
			    		removeObsoleteEvents(toRemove);
			    		
			    		if (indirectTransactionEvent!=null)
			    		{
			    		    hostsDestination=indirectTransactionEvent.getConcernedHosts(hostsDestination);
			    		}
			    		if (hostsDestination.isEmpty())
			    		    hostsDestination=null;
			    		
			    		//replaceDataFromDistantPeer(comingFrom, localDTE, dte.concernedDatabasePackage, toRemove);
					final boolean transactionToResendFinal=dte.isForce()?false:(hostsDestination!=null && !hostsDestination.isEmpty());
				    
					if (indirectTransactionEvent==null)
					{
					    oos.writeByte(EXPORT_FINISHED);
					    oos.flush();
					}
					DatabaseDistantTransactionEvent.Record ddte=null;
					if (indirectTransactionEvent==null)
					    ddte=new DatabaseDistantTransactionEvent.Record(dte.getID(), getIDTable().getLastTransactionID(), fromHook.get(), baos.toByteArray(), false, null);
					else
					{
					    ddte=indirectTransactionEvent;
					    ddte.setLocalID(getIDTable().getLastTransactionID());
					}
					if (getDatabaseHooksTable().isConcernedByIndirectTransaction(ddte) && ddte.addNewHostIDAndTellsIfNewPeersCanBeConcerned(getDatabaseHooksTable(), getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
					{
					    getDatabaseDistantTransactionEvent().addRecord(ddte);
					}
					for (TableEvent<?> tetmp : localDTE.getEvents())
					{
					    TableEvent<DatabaseRecord> te=(TableEvent<DatabaseRecord>)tetmp;
					    switch(te.getType())
					    {
						case ADD:
						    if (te.isOldAlreadyPresent())
						    {
							te.getTable(getDatabaseWrapper()).updateUntypedRecord(te.getNewDatabaseRecord(), false, null);
							if (transactionToResendFinal)
							    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination));
						    }
						    else
						    {
							te.getTable(getDatabaseWrapper()).addUntypedRecord(te.getNewDatabaseRecord(), true, transactionToResendFinal, hostsDestination);
						    }
						    break;
						case REMOVE:
						    if (te.getOldDatabaseRecord()==null)
						    {
							if (transactionToResendFinal)
							    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination, te.getMapKeys(), false, te.getTable(getDatabaseWrapper())));
						    }
						    else
							te.getTable(getDatabaseWrapper()).removeUntypedRecord(te.getOldDatabaseRecord(), transactionToResendFinal, hostsDestination);
						    break;
						case REMOVE_WITH_CASCADE:
						    if (te.getOldDatabaseRecord()==null)
						    {
							if (transactionToResendFinal)
							    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination, te.getMapKeys(), false, te.getTable(getDatabaseWrapper())));
						    }
						    else
						    {
							te.getTable(getDatabaseWrapper()).removeUntypedRecordWithCascade(te.getOldDatabaseRecord(), transactionToResendFinal, hostsDestination);
						    }
						    break;
						case UPDATE:
						    if (te.getOldDatabaseRecord()==null)
						    {
							te.getTable(getDatabaseWrapper()).addUntypedRecord(te.getNewDatabaseRecord(), true, false, null);
							if (transactionToResendFinal)
							    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getNewDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination));
						    }
						    else
							te.getTable(getDatabaseWrapper()).updateUntypedRecord(te.getNewDatabaseRecord(), transactionToResendFinal, hostsDestination);
						    break;
					    }
					}
					
					fromHook.get().setLastValidatedDistantTransaction(dte.getID());
					HashMap<String, Object> hm=new HashMap<>();
					hm.put("lastValidatedDistantTransaction", new Long(dte.getID()));
					getDatabaseHooksTable().updateRecord(fromHook.get(), hm);
					getDatabaseWrapper().getSynchronizer().addNewDatabaseEvent(localDTE);
					
				    }
				}
				else
				    throw new SerializationDatabaseException("The transaction should not be empty");
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
		else
		{
		    throw new SerializationDatabaseException("Unexpected code : "+next.get());
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
	finally
	{
	    if (lastValidatedTransaction.get()!=-1)
	    {
		getDatabaseWrapper().getSynchronizer().addNewDatabaseEvent(new DatabaseWrapper.TransactionConfirmationEvents(getDatabaseHooksTable().getLocalDatabaseHost().getHostID(), comingFrom, lastValidatedTransaction.get()));
	    }
	    
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

	try(DataOutputStream oos=new DataOutputStream(outputStream))
	    {
	    	number.set(getDatabaseDistantTransactionEvent().exportTransactions(oos, hook, maxEventsRecords, currentTransactionID, nearNextLocalID));
	    	try
	    	{
	    	    if (number.get()>=maxEventsRecords)
	    		return number.get();
	    	    do
	    	    {
	    		Collection<DatabaseTransactionsPerHostTable.Record> records=getOrderedRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {
		    
	    		    @Override
	    		    public boolean nextRecord(Record _record) 
	    		    {
	    			if (number.incrementAndGet()>=maxEventsRecords)
	    			{
	    			    this.stopTableParsing();
	    			    return true;
	    			}
	    			return true;
	    		    }
	    		}, "transaction.id<%nearNextLocalID AND transaction.id>%previousNearTransactionID AND hook=%hook", new Object[]{"nearNextLocalID", new Long(nearNextLocalID.get()), "previousNearTransactionID", new Long(currentTransactionID), "hook", hook}, true, "transaction.id");
	    		currentTransactionID=nearNextLocalID.get();
	    	
	    	    
	    		for (Record r : records)
	    		{
	    		    oos.writeByte(EXPORT_DIRECT_TRANSACTION);
	    		    getDatabaseTransactionEventsTable().serialize(r.getTransaction(), oos, true, false);
	    		    getDatabaseEventsTable().getOrderedRecords(new Filter<DatabaseEventsTable.Record>(){

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
	    			
	    		    }, "transaction=%transaction", new Object[]{"transaction", r.getTransaction()}, true, "position");
	    		
	    		}
	    		if (number.get()<maxEventsRecords && currentTransactionID!=Long.MAX_VALUE)
	    		    number.set(number.get()+getDatabaseDistantTransactionEvent().exportTransactions(oos, hook, maxEventsRecords-number.get(), currentTransactionID, nearNextLocalID));
	    	    }
	    	    while(number.get()<maxEventsRecords && nearNextLocalID.get()!=currentTransactionID);
	    	}
	    	finally
	    	{
	    	    oos.writeByte(EXPORT_FINISHED);
	    	}
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    
	    return number.get();
	
    }
    
    static final byte EXPORT_FINISHED=1;
    static final byte EXPORT_DIRECT_TRANSACTION=2;
    static final byte EXPORT_DIRECT_TRANSACTION_EVENT=4;
    static final byte EXPORT_INDIRECT_TRANSACTION=8;
    static final byte EXPORT_INDIRECT_TRANSACTION_EVENT=16;
    static final byte EXPORT_INDIRECT_TRANSACTION_FINISHED=EXPORT_INDIRECT_TRANSACTION|EXPORT_FINISHED;
}
