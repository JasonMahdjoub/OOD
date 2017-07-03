
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


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseTransactionEventsTable extends Table<DatabaseTransactionEventsTable.Record>
{
    private volatile IDTable transactionIDTable=null;
    private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable=null;
    private volatile DatabaseEventsTable databaseEventsTable=null;
    private volatile DatabaseHooksTable databaseHooksTable=null;
    private volatile IDTable idTable=null;
    
    static class Record extends DatabaseRecord
    {
	@PrimaryKey
	protected long id;
	
	@NotNull
	@Field
	protected String concernedDatabasePackage;
	
	@Field
	private boolean force=false;
	
	
	
	long getID()
	{
	    return id;
	}
	
	boolean isConcernedBy(Package p)
	{
	    return p.getName().equals(concernedDatabasePackage);
	}
	boolean isConcernedByOneOf(Package ...ps)
	{
	    if (ps==null)
		return false;
	    for (Package p : ps)
		if (concernedDatabasePackage.equals(p.getName()))
		    return true;
	    return false;
	}
	
	@Override
	public boolean equals(Object record)
	{
	    if (record==null)
		return false;
	    else if (record instanceof Record)
		return id==((Record)record).id;
	    return false;
	}
	
	@Override
	public int hashCode()
	{
	    return (int)id;
	}

	public boolean isForce()
	{
	    return force;
	}

	public void setForce(boolean _force)
	{
	    force = _force;
	}
	
    }
    
    protected DatabaseTransactionEventsTable() throws DatabaseException
    {
	super();
    }
    
    void removeUnusedTransactions() throws DatabaseException
    {
	
	getDatabaseWrapper().runTransaction(new Transaction() {
	    
	    @Override
	    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	    {
		final DatabaseTransactionsPerHostTable t=getDatabaseTransactionsPerHostTable();
		DatabaseTransactionEventsTable.this.removeRecordsWithCascade(new Filter<DatabaseTransactionEventsTable.Record>() {
		    
		    @Override
		    public boolean nextRecord(com.distrimind.ood.database.DatabaseTransactionEventsTable.Record _record) throws DatabaseException
		    {
			return !t.hasRecordsWithAllFields(new Object[]{"transaction", _record});
		    }
		});
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
    
    
    void removeTransactionsFromLastID() throws DatabaseException
    {
	long globalLast=getDatabaseHooksTable().getGlobalLastValidatedTransactionID();
	long prevGlobalLast=getIDTable().getLastValidatedTransactionID();
	if (prevGlobalLast!=globalLast)
	{
	    getIDTable().setLastValidatedTransactionIDI(globalLast);
	    removeTransactionUntilID(globalLast);
	}		    
	
    }
    
    protected DatabaseTransactionEventsTable.Record addTransaction(final Package databasePackage, final DatabaseTransactionEvent dte, byte eventsType) throws DatabaseException
    {
	DatabaseTransactionEventsTable.Record tr=new DatabaseTransactionEventsTable.Record();
	tr.id=getTransactionIDTable().getAndIncrementTransactionID();
	tr.concernedDatabasePackage=databasePackage.getName();
	tr.setForce(dte.isForce());
	tr=addRecord(tr);
	
	for (TableEvent<?> de : dte.getEvents())
	{
	    
	    DatabaseEventsTable.Record r=new DatabaseEventsTable.Record(tr, de, getDatabaseWrapper(), false);
	    getDatabaseEventsTable().addRecord(r);
	}
	return tr;
	
    }
    protected void addTransactionToSynchronizeTables(final Package databasePackages[], DatabaseHooksTable.Record hook, boolean force) throws DatabaseException
    {
	for (Package databasePackage : databasePackages)
	    addTransactionToSynchronizeTables(databasePackage, hook, force);
    }
    
    @SuppressWarnings("unchecked")
    private void addEventsForTablesToSynchronize(final AtomicReference<DatabaseTransactionEventsTable.Record> transaction, final Package databasePackage, final DatabaseHooksTable.Record hook, Class<? extends Table<?>> tableClass, Set<Class<? extends Table<?>>> tablesDone, final AtomicLong currentEventPos, final long maxEvents, final boolean force) throws DatabaseException
    {
	if (tablesDone.contains(tableClass))
	    return;
	    
	
	
	final Table<DatabaseRecord> table=(Table<DatabaseRecord>) getDatabaseWrapper().getTableInstance(tableClass);
	if (!table.supportSynchronizationWithOtherPeers())
	    return;

	tablesDone.add(tableClass);
	
	for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors())
	{
	    addEventsForTablesToSynchronize(transaction, databasePackage, hook, (Class<? extends Table<?>>)(fa.getPointedTable().getClass()), tablesDone, currentEventPos, maxEvents, force);
	}
	table.getRecords(new Filter<DatabaseRecord>() {

	    @Override
	    public boolean nextRecord(DatabaseRecord _record) throws DatabaseException
	    {
		DatabaseEventsTable.Record event=new DatabaseEventsTable.Record(transaction.get(), new TableEvent<DatabaseRecord>(-1, DatabaseEventType.ADD, null, _record, force), getDatabaseWrapper(), false);
		getDatabaseEventsTable().addRecord(event);
		if (currentEventPos.get()>maxEvents)
		    throw new IllegalAccessError();
		if (currentEventPos.incrementAndGet()==maxEvents)
		{
		    DatabaseTransactionsPerHostTable.Record trhost=new DatabaseTransactionsPerHostTable.Record();
		    trhost.set(transaction.get(), hook);
		    getDatabaseTransactionsPerHostTable().addRecord(trhost);

		    DatabaseTransactionEventsTable.Record tr=new DatabaseTransactionEventsTable.Record();
		    tr.id=getTransactionIDTable().getAndIncrementTransactionID();
		    tr.concernedDatabasePackage=databasePackage.getName();
		    tr.setForce(force);
		    
		    transaction.set(addRecord(tr));
		    currentEventPos.set(0);
		}
		return false;
	    }
	});
    }
    
    protected void addTransactionToSynchronizeTables(final Package databasePackage, DatabaseHooksTable.Record hook, boolean force) throws DatabaseException
    {
	DatabaseTransactionEventsTable.Record tr=new DatabaseTransactionEventsTable.Record();
	tr.id=getTransactionIDTable().getAndIncrementTransactionID();
	tr.concernedDatabasePackage=databasePackage.getName();
	tr.setForce(force);
	AtomicReference<DatabaseTransactionEventsTable.Record> transaction=new AtomicReference<>(addRecord(tr));
	AtomicLong currentEventPos=new AtomicLong(0);
	Set<Class<? extends Table<?>>> tables=getDatabaseWrapper().getDatabaseConfiguration(databasePackage).getTableClasses();
	Set<Class<? extends Table<?>>> tablesDone=new HashSet<>();
	
	for (Class<? extends Table<?>> c : tables)
	{
	    
	    addEventsForTablesToSynchronize(transaction, databasePackage, hook, c, tablesDone,currentEventPos, getDatabaseWrapper().getMaxTransactionEventsKeepedIntoMemory(), force);
	}
	if (currentEventPos.get()>0)
	{
	    DatabaseTransactionsPerHostTable.Record trhost=new DatabaseTransactionsPerHostTable.Record();
	    trhost.set(transaction.get(), hook);
	    getDatabaseTransactionsPerHostTable().addRecord(trhost);
	}
	else
	    removeRecord(transaction.get());
	
	
    }
    protected DatabaseTransactionEventsTable.Record addTransaction(final Package databasePackage, final Iterator<DatabaseEventsTable.Record> eventsIt, byte eventsType, boolean force) throws DatabaseException
    {
	DatabaseTransactionEventsTable.Record tr=new DatabaseTransactionEventsTable.Record();
	tr.id=getTransactionIDTable().getAndIncrementTransactionID();
	tr.concernedDatabasePackage=databasePackage.getName();
	tr.setForce(force);
	tr=addRecord(tr);
	
	while (eventsIt.hasNext())
	{
	    DatabaseEventsTable.Record r=eventsIt.next();
	    if (r.getConcernedTable().startsWith(databasePackage.getName()) && !r.getConcernedTable().substring(databasePackage.getName().length()).contains("."))
	    {
		r.setTransaction(tr);
		getDatabaseEventsTable().addRecord(r);
	    }
	}
	return tr;
	
    }
    
    DatabaseTransactionEventsTable.Record addTransactionIfNecessary(final DatabaseConfiguration configuration, final DatabaseTransactionEvent transaction, final byte eventsType) throws DatabaseException
    {
	return (DatabaseTransactionEventsTable.Record)getDatabaseWrapper().runTransaction(new Transaction() {

	    @Override
	    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	    {
		
		final AtomicReference<DatabaseTransactionEventsTable.Record> res=new AtomicReference<>();

		getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {
		    
		    @Override
		    public boolean nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException
		    {
			if (_record.isConcernedDatabaseByPackage(configuration.getPackage()))
			{
			    if (res.get()==null)
			    {
				res.set(addTransaction(configuration.getPackage(), transaction, eventsType));
			    }
			    DatabaseTransactionsPerHostTable.Record trhost=new DatabaseTransactionsPerHostTable.Record();
			    trhost.set(res.get(), _record);
			    getDatabaseTransactionsPerHostTable().addRecord(trhost);
			}
			return false;
		    }
		});
		return res;
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
    
    
    DatabaseTransactionEventsTable.Record addTransactionIfNecessary(final Package databasePackage, final Iterator<DatabaseEventsTable.Record> eventsIterator, final byte eventsType, final boolean force) throws DatabaseException
    {
	return (DatabaseTransactionEventsTable.Record)getDatabaseWrapper().runTransaction(new Transaction() {

	    @Override
	    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	    {
		final AtomicReference<DatabaseTransactionEventsTable.Record> res=new AtomicReference<>();
		
		getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {
		    
		    @Override
		    public boolean nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException
		    {
			if (_record.isConcernedDatabaseByPackage(databasePackage))
			{
			    if (res.get()==null)
			    {
				res.set(addTransaction(databasePackage, eventsIterator, eventsType, force));
			    }
			    DatabaseTransactionsPerHostTable.Record trhost=new DatabaseTransactionsPerHostTable.Record();
			    trhost.set(res.get(), _record);
			    getDatabaseTransactionsPerHostTable().addRecord(trhost);
			}
			return false;
		    }
		});
		return res;
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
    
    DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException
    {
	if (databaseTransactionsPerHostTable==null)
	    databaseTransactionsPerHostTable=(DatabaseTransactionsPerHostTable)getDatabaseWrapper().getTableInstance(DatabaseTransactionsPerHostTable.class);
	return databaseTransactionsPerHostTable;
	
    }
    
    IDTable getTransactionIDTable() throws DatabaseException
    {
	if (transactionIDTable==null)
	    transactionIDTable=(IDTable)getDatabaseWrapper().getTableInstance(IDTable.class);
	return transactionIDTable;
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
    IDTable getIDTable() throws DatabaseException
    {
	if (idTable==null)
	    idTable=(IDTable)getDatabaseWrapper().getTableInstance(IDTable.class);
	return idTable; 
	   
    }
    
    
    void removeTransactionUntilID(long lastTransactionID) throws DatabaseException
    {
	removeRecords("id<%lastID", "lastID", new Long(lastTransactionID));
	
    }
    
    
}
