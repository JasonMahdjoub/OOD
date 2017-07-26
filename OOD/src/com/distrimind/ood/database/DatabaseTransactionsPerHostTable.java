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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.DatabaseWrapper.DatabaseNotifier;
import com.distrimind.ood.database.DatabaseWrapper.SynchonizationAnomalyType;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.ood.database.exceptions.RecordNotFoundDatabaseException;
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
    private volatile DatabaseDistantEventsTable databaseDistantEventsTable=null;
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
    DatabaseDistantEventsTable getDatabaseDistantEventsTable() throws DatabaseException
    {
	if (databaseDistantEventsTable==null)
	    databaseDistantEventsTable=(DatabaseDistantEventsTable)getDatabaseWrapper().getTableInstance(DatabaseDistantEventsTable.class);
	return databaseDistantEventsTable; 
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
		    
		    
		    getDatabaseDistantTransactionEvent().updateRecords(new AlterRecordFilter<DatabaseDistantTransactionEvent.Record> () {

			@Override
			public void nextRecord(com.distrimind.ood.database.DatabaseDistantTransactionEvent.Record _record) throws DatabaseException
			{
			    if (_record.addNewHostIDAndTellsIfNewPeersCanBeConcerned(getDatabaseHooksTable(), hook.getHostID()))
			    {
				update();
			    }
			    else
			    {
				removeWithCascade();
			    }
			}
			
		    }, "localID<=%lastID", "lastID", new Long(actualLastID.get()));
		    
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
    }
    protected AbstractDecentralizedID detectCollisionAndGetObsoleteDistantEventsToRemove(final AbstractDecentralizedID comingFrom, final String concernedTable, final byte[] keys, final boolean force, final Set<AbstractDecentralizedID> resendTo, final Set<DatabaseDistantTransactionEvent.Record> recordsToRemove) throws DatabaseException
    {
	recordsToRemove.clear();
	if (force)
	    return null;
	final AtomicReference<AbstractDecentralizedID> collision=new AtomicReference<>(null);
	getDatabaseDistantEventsTable().getRecords(new Filter<DatabaseDistantEventsTable.Record>() {
	    
	    @Override
	    public boolean nextRecord(com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record) throws DatabaseException
	    {
		if (_record.getTransaction().isConcernedBy(comingFrom))
		{
		    collision.set(_record.getTransaction().getHook().getHostID());
		    recordsToRemove.add(_record.getTransaction());
		}
		return false;
	    }
	}, "concernedTable==%concernedTable AND concernedSerializedPrimaryKey==%concernedSerializedPrimaryKey", "concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);
	if (collision.get()!=null)
	{
	    for (DatabaseHooksTable.Record r : getDatabaseHooksTable().getRecords("concernsDatabaseHost==%local AND hostID!=%comingFrom", "local", new Boolean(false), "comingFrom", comingFrom))
		resendTo.add(r.getHostID());
	    
	    return collision.get();
	}
	else
	{
	    return null;
	}
    }
    protected void removeIndirectTransactionAfterCollisionDetection(final AbstractDecentralizedID comingFrom, final String concernedTable, final byte[] keys) throws DatabaseException
    {
	final AtomicReference<AbstractDecentralizedID> collision=new AtomicReference<>(null);
	getDatabaseDistantEventsTable().removeRecordsWithCascade(new Filter<DatabaseDistantEventsTable.Record>() {
	    
	    @Override
	    public boolean nextRecord(com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record) throws DatabaseException
	    {
		if (_record.getTransaction().isConcernedBy(comingFrom))
		{
		    collision.set(_record.getTransaction().getHook().getHostID());
		    stopTableParsing();
		}
		return false;
	    }
	}, "concernedTable==%concernedTable AND concernedSerializedPrimaryKey==%concernedSerializedPrimaryKey", "concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);
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
    
    void removeObsoleteEvents(final Set<DatabaseTransactionEventsTable.Record> directTransactionToRemove, final Set<DatabaseDistantTransactionEvent.Record> indirectTransactionToRemove) throws DatabaseException
    {
	if (!directTransactionToRemove.isEmpty())
	    getDatabaseTransactionEventsTable().removeRecordsWithCascade(directTransactionToRemove);
	if (!indirectTransactionToRemove.isEmpty())
	    getDatabaseDistantTransactionEvent().removeRecordsWithCascade(indirectTransactionToRemove);
    }
    
    void alterDatabase(final AbstractDecentralizedID comingFrom, final InputStream inputStream) throws DatabaseException
    {
	alterDatabase(comingFrom, comingFrom, inputStream, getDatabaseWrapper().getSynchronizer().getNotifier());
    }
    
    private void alterDatabase(final DatabaseHooksTable.Record directPeer, final AtomicReference<DatabaseHooksTable.Record> fromHook, final InputStream inputStream, final DatabaseNotifier notifier, final DatabaseTransactionEventsTable.AbstractRecord transaction, final DatabaseEventsTable.DatabaseEventsIterator iterator, final AtomicLong lastValidatedTransaction) throws DatabaseException
    {
	getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

	    @SuppressWarnings("unchecked")
	    @Override
	    public Void run() throws Exception
	    {
		boolean indirectTransaction=transaction instanceof DatabaseDistantTransactionEvent.Record;
		boolean validatedTransaction=true;
		boolean transactionNotEmpty=false;
		HashSet<DatabaseTransactionEventsTable.Record> directTransactionsToRemove=new HashSet<>();
		Set<DatabaseDistantTransactionEvent.Record> indirectTransactionsToRemove=new HashSet<>();
		Set<AbstractDecentralizedID> hostsDestination=new HashSet<>();
		DatabaseTransactionEvent localDTE=new DatabaseTransactionEvent();
		ArrayList<DatabaseDistantEventsTable.Record> distantEventsList=new ArrayList<>();
		DatabaseDistantTransactionEvent.Record distantTransaction=null;
		
		if (indirectTransaction)
		{
		    distantTransaction=(DatabaseDistantTransactionEvent.Record)transaction;
		    fromHook.set(distantTransaction.getHook());
		}
		else
		{
		    distantTransaction=new DatabaseDistantTransactionEvent.Record(transaction.getID(), getIDTable().getLastTransactionID(), fromHook.get(), false, null, false);
		    final List<AbstractDecentralizedID> concernedHosts=((DatabaseTransactionEventsTable.Record)transaction).getConcernedHosts();
		    if (transaction.isForce() || concernedHosts.size()>0)
		    {
			final List<AbstractDecentralizedID> l=new ArrayList<>();
			getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

			    @Override
			    public boolean nextRecord(com.distrimind.ood.database.DatabaseHooksTable.Record _record) 
			    {
				if( _record.concernsLocalDatabaseHost() 
					|| _record.getHostID().equals(directPeer.getHostID())
					|| (concernedHosts.size()>0 && !concernedHosts.contains(_record.getHostID())))
				    l.add(_record.getHostID());
				return false;
			    }
			    
			});
			
			distantTransaction.setPeersInformed(l);
		    }
		}
		
		
		while (iterator.hasNext())
		{
		    transactionNotEmpty=true;
		    DatabaseEventsTable.AbstractRecord event=iterator.next();
		    if (indirectTransaction)
			distantEventsList.add((DatabaseDistantEventsTable.Record)event);
		    else
			distantEventsList.add(new DatabaseDistantEventsTable.Record(event, distantTransaction));
		    Table<DatabaseRecord> t=null;
		    try
		    {
			t=(Table<DatabaseRecord>)getDatabaseWrapper().getTableInstance(event.getConcernedTable());
		    }
		    catch(Exception e)
		    {
			throw new SerializationDatabaseException("", e);
		    }
		    
		    DatabaseEventType type=DatabaseEventType.getEnum(event.getType());
		    if (type==null)
			throw new SerializationDatabaseException("Impossible to decode database event type : "+event.getType());
		    DatabaseRecord drNew=null, drOld=null;
		    HashMap<String, Object> mapKeys=new HashMap<>();
		    t.unserializePrimaryKeys(mapKeys, event.getConcernedSerializedPrimaryKey());
		    if (type.needsNewValue())
		    {
			drNew=t.getDefaultRecordConstructor().newInstance();
			t.unserializePrimaryKeys(drNew, event.getConcernedSerializedPrimaryKey());
			t.unserializeFields(drNew, event.getConcernedSerializedNewForeignKey(), false, true, false);
			t.unserializeFields(drNew, event.getConcernedSerializedNewNonKey(), false, false, true);
		    }

		    if (type.hasOldValue() || transaction.isForce()) 
		    {
			drOld=t.getRecord(mapKeys);
		    }
		    boolean eventForce=false;
		    if (transaction.getID()<=fromHook.get().getLastValidatedDistantTransaction())
			validatedTransaction=false;
		    if (validatedTransaction)
		    {
			HashSet<DatabaseTransactionEventsTable.Record> r=new HashSet<>();
			Set<AbstractDecentralizedID> hd=new HashSet<>();
			boolean collision=detectCollisionAndGetObsoleteEventsToRemove(fromHook.get().getHostID(), event.getConcernedTable(), event.getConcernedSerializedPrimaryKey(), transaction.isForce(), r, hd);
			Set<DatabaseDistantTransactionEvent.Record> ir=new HashSet<>();
			AbstractDecentralizedID indirectCollisionWith=null;
			if (!collision)
			{
			    hd.clear();
			    indirectCollisionWith=detectCollisionAndGetObsoleteDistantEventsToRemove(fromHook.get().getHostID(), event.getConcernedTable(), event.getConcernedSerializedPrimaryKey(), transaction.isForce(), hd, ir);
			}
			if (collision || indirectCollisionWith!=null)
			{
			    if (!type.hasOldValue())
				drOld=t.getRecord(mapKeys);
			    if (notifier!=null)
			    {
				/*if (indirectTransaction)
				{
				    validatedTransaction&=(eventForce=notifier.indirectCollisionDetected(directPeerID, type, t, mapKeys, drNew, drOld, fromHook.get().getHostID()));
				}
				else*/
				    validatedTransaction&=(eventForce=notifier.collisionDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, type, t, mapKeys, drNew, drOld));
			    }
			    else
				validatedTransaction=false;
			    
			}
			    
			if (validatedTransaction)
			{
			    directTransactionsToRemove.addAll(r);
			    indirectTransactionsToRemove.addAll(ir);
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
				    if (!transaction.isForce() && !eventForce)
				    {
					if (notifier!=null)
					    notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND, t, mapKeys, drOld);
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
				if (drOld==null && !eventForce && !transaction.isForce())
				{
				    if (notifier!=null)
					notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_UPDATE_NOT_FOUND, t, mapKeys, drNew);
				    validatedTransaction=false;
				}
				else
				{
				    localDTE.addEvent(new TableEvent<>(-1, type, drOld, drNew, null));
				}
				break;
			}
		    }

		}
		if (indirectTransaction)
		{
		    if (distantTransaction.getLocalID()>directPeer.getLastValidatedDistantTransaction())
		    {
			if (lastValidatedTransaction.get()>distantTransaction.getLocalID())
			    throw new DatabaseException("Transactions must be ordered !");
			lastValidatedTransaction.set(distantTransaction.getLocalID());

			HashMap<String, Object> hm=new HashMap<>();
			hm.put("lastValidatedDistantTransaction", new Long(distantTransaction.getLocalID()));
			getDatabaseHooksTable().updateRecord(directPeer, hm);

		    }
		}
		else
		{
		    if (transaction.getID()>directPeer.getLastValidatedDistantTransaction())
		    {
			if (lastValidatedTransaction.get()>transaction.getID())
			    throw new DatabaseException("Transactions must be ordered !");
			lastValidatedTransaction.set(transaction.getID());
		    }
		}
	    	if (fromHook.get().getLastValidatedDistantTransaction()<transaction.getID())
	    	{
	    	    HashMap<String, Object> hm=new HashMap<>();
	    	    hm.put("lastValidatedDistantTransaction", new Long(transaction.getID()));
	    	    getDatabaseHooksTable().updateRecord(fromHook.get(), hm);
	    	}
		
		if (transactionNotEmpty)
		{
		    
	    	    if (validatedTransaction)
	    	    {
	    		removeObsoleteEvents(directTransactionsToRemove, indirectTransactionsToRemove);
	    		
	    		if (indirectTransaction)
	    		{
	    		    hostsDestination=distantTransaction.getConcernedHosts(hostsDestination);
	    		}
	    		if (hostsDestination.isEmpty())
	    		    hostsDestination=null;
	    		
	    		//replaceDataFromDistantPeer(comingFrom, localDTE, dte.concernedDatabasePackage, toRemove);
			final boolean transactionToResendFinal=transaction.isForce()?false:(hostsDestination!=null && !hostsDestination.isEmpty());
		    
			
			if (getDatabaseHooksTable().isConcernedByIndirectTransaction(distantTransaction) && distantTransaction.addNewHostIDAndTellsIfNewPeersCanBeConcerned(getDatabaseHooksTable(), getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
			{
			    distantTransaction.setLocalID(getIDTable().getAndIncrementTransactionID());
			    distantTransaction=getDatabaseDistantTransactionEvent().addRecord(distantTransaction);
			    if (distantEventsList.isEmpty())
				throw new IllegalAccessError();
			    for (DatabaseDistantEventsTable.Record e : distantEventsList)
			    {
				e.setTransaction(distantTransaction);
				getDatabaseDistantEventsTable().addRecord(e);
			    }
			    getDatabaseHooksTable().actualizeLastTransactionID(new ArrayList<AbstractDecentralizedID>(0), distantTransaction.getLocalID(), distantTransaction.getLocalID()+1);
			}
			
			ArrayList<TableEvent<?>> l=localDTE.getEvents();
			for (int i=0;i<l.size();i++)
			{
			    TableEvent<?> tetmp=l.get(i);
			    TableEvent<DatabaseRecord> te=(TableEvent<DatabaseRecord>)tetmp;
			    switch(te.getType())
			    {
				case ADD:
				    if (te.isOldAlreadyPresent())
				    {
					try
					{
					    te.getTable(getDatabaseWrapper()).updateUntypedRecord(te.getNewDatabaseRecord(), false, null);
					    if (transactionToResendFinal)
						getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination));
					}
					catch(ConstraintsNotRespectedDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
					catch(FieldDatabaseException | RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}					
				    }
				    else
				    {
					try
					{
					    te.getTable(getDatabaseWrapper()).addUntypedRecord(te.getNewDatabaseRecord(), true, transactionToResendFinal, hostsDestination);
					}
					catch(ConstraintsNotRespectedDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_ALREADY_PRESENT, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
					catch(FieldDatabaseException | RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}					
				    }
				    break;
				case REMOVE:
				    if (te.getOldDatabaseRecord()==null)
				    {
					if (transactionToResendFinal)
					    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination, te.getMapKeys(), false, te.getTable(getDatabaseWrapper())));
				    }
				    else
				    {
					try
					{
					    te.getTable(getDatabaseWrapper()).removeUntypedRecord(te.getOldDatabaseRecord(), transactionToResendFinal, hostsDestination);
					}
					catch(ConstraintsNotRespectedDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_REMOVE_HAS_DEPENDENCIES, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
					catch(RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
				    }
				    break;
				case REMOVE_WITH_CASCADE:
				    if (te.getOldDatabaseRecord()==null)
				    {
					if (transactionToResendFinal)
					    getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getOldDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination, te.getMapKeys(), false, te.getTable(getDatabaseWrapper())));
				    }
				    else
				    {
					try
					{
					    te.getTable(getDatabaseWrapper()).removeUntypedRecordWithCascade(te.getOldDatabaseRecord(), transactionToResendFinal, hostsDestination);
					}
					catch(RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
				    }
				    break;
				case UPDATE:
				    if (te.getOldDatabaseRecord()==null)
				    {
					try
					{
					    te.getTable(getDatabaseWrapper()).addUntypedRecord(te.getNewDatabaseRecord(), true, false, null);
					    if (transactionToResendFinal)
						getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(te.getTable(getDatabaseWrapper()), new TableEvent<DatabaseRecord>(te.getID(), te.getType(), te.getNewDatabaseRecord(), te.getNewDatabaseRecord(), hostsDestination));
					}
					catch(ConstraintsNotRespectedDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_ALREADY_PRESENT, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
					catch(FieldDatabaseException | RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}					
					
				    }
				    else
				    {
					
					try
					{
					    te.getTable(getDatabaseWrapper()).updateUntypedRecord(te.getNewDatabaseRecord(), transactionToResendFinal, hostsDestination);
					}
					catch(ConstraintsNotRespectedDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
					catch(FieldDatabaseException | RecordNotFoundDatabaseException e)
					{
					    if (notifier!=null)
						notifier.anomalyDetected(fromHook.get().getHostID(), indirectTransaction?directPeer.getHostID():null, SynchonizationAnomalyType.RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS, te.getTable(getDatabaseWrapper()), te.getMapKeys(), te.getNewDatabaseRecord());
					}
				    }
				    break;
			    }
			}
			
			//fromHook.get().setLastValidatedDistantTransaction(transaction.getID());
			
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
    
    private void alterDatabase(final AbstractDecentralizedID directPeerID, final AbstractDecentralizedID comingFrom, final InputStream inputStream, final DatabaseNotifier notifier) throws DatabaseException
    {
	
	if (comingFrom==null)
	    throw new NullPointerException("comingFrom");
	if (inputStream==null)
	    throw new NullPointerException("inputStream");
	if (directPeerID==null)
	    throw new NullPointerException("directPeerID");
	if (comingFrom.equals(getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
	    throw new IllegalArgumentException("The given distant host ID cannot be equals to the local host ID : "+comingFrom);
	
	ArrayList<DatabaseHooksTable.Record> hooks=getDatabaseHooksTable().getRecordsWithAllFields("hostID", comingFrom);
	if (hooks.isEmpty())
	    throw new SerializationDatabaseException("The give host id is not valid : "+comingFrom);
	else if (hooks.size()>1)
	    throw new IllegalAccessError();
	
	final DatabaseHooksTable.Record directPeer=hooks.get(0);
	final AtomicLong lastValidatedTransaction=new AtomicLong(-1);
	
	try(DataInputStream ois=new DataInputStream(inputStream))
	{
	    final AtomicInteger next=new AtomicInteger(ois.readByte());
	    while(next.get()!=EXPORT_FINISHED)
	    {
		if (next.get()==EXPORT_INDIRECT_TRANSACTION)
		{
		    DatabaseDistantTransactionEvent.Record ite=getDatabaseDistantTransactionEvent().unserializeDistantTransactionEvent(ois);
		    alterDatabase(directPeer, new AtomicReference<DatabaseHooksTable.Record>(null), inputStream, notifier, ite, getDatabaseDistantEventsTable().distantEventTableIterator(ois), lastValidatedTransaction);
		}
		else if (next.get()==EXPORT_DIRECT_TRANSACTION)
		{
		    DatabaseTransactionEventsTable.Record dte=getDatabaseTransactionEventsTable().unserialize(ois, true, false);
		    alterDatabase(directPeer, new AtomicReference<DatabaseHooksTable.Record>(directPeer), inputStream, notifier, dte, getDatabaseEventsTable().eventsTableIterator(ois), lastValidatedTransaction);
		}
		next.set(ois.readByte());
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
	    		getOrderedRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {
		    
	    		    @Override
	    		    public boolean nextRecord(Record _record) throws DatabaseException 
	    		    {
	    			try
	    			{
	    			    oos.writeByte(EXPORT_DIRECT_TRANSACTION);
	    			    getDatabaseTransactionEventsTable().serialize(_record.getTransaction(), oos, true, false);
	    			    getDatabaseEventsTable().getOrderedRecords(new Filter<DatabaseEventsTable.Record>(){

	    				@Override
	    				public boolean nextRecord(com.distrimind.ood.database.DatabaseEventsTable.Record _record) throws DatabaseException
	    				{
	    				    _record.export(oos, getDatabaseWrapper());
	    				    return false;
	    				}
		    			
	    			    }, "transaction=%transaction", new Object[]{"transaction", _record.getTransaction()}, true, "position");
	    			    oos.writeByte(EXPORT_DIRECT_TRANSACTION_FINISHED);
	    			    if (number.incrementAndGet()>=maxEventsRecords)
	    			    {
	    				this.stopTableParsing();
	    				return false;
	    			    }
	    			    return false;
	    			}
	    			catch(IOException e)
	    			{
	    			    throw DatabaseException.getDatabaseException(e);
	    			}
	    		    }
	    		}, "transaction.id<%nearNextLocalID AND transaction.id>%previousNearTransactionID AND hook=%hook", new Object[]{"nearNextLocalID", new Long(nearNextLocalID.get()), "previousNearTransactionID", new Long(currentTransactionID), "hook", hook}, true, "transaction.id");
	    		currentTransactionID=nearNextLocalID.get();
	    	
	    	    
	    		
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
    static final byte EXPORT_DIRECT_TRANSACTION_FINISHED=EXPORT_DIRECT_TRANSACTION|EXPORT_FINISHED;
    static final byte EXPORT_INDIRECT_TRANSACTION=8;
    static final byte EXPORT_INDIRECT_TRANSACTION_EVENT=16;
    static final byte EXPORT_INDIRECT_TRANSACTION_FINISHED=EXPORT_INDIRECT_TRANSACTION|EXPORT_FINISHED;
}