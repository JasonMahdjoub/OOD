
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

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
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
final class DatabaseDistantTransactionEvent extends Table<DatabaseDistantTransactionEvent.Record>
{
    public static final int TRANSACTION_MAX_SIZE_BYTES=536870912;
    private volatile IDTable transactionIDTable=null;
    
    protected DatabaseDistantTransactionEvent() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	@PrimaryKey
	protected long id;
	
	@Field(index=true)
	protected long localID;
	
	@ForeignKey
	@PrimaryKey
	protected DatabaseHooksTable.Record hook;
	
	@Field @NotNull
	protected byte[] transaction;
	
	
	public Record()
	{
	    
	}


	public Record(long _id, long localID, com.distrimind.ood.database.DatabaseHooksTable.Record _hook, byte[] _transaction) throws SerializationDatabaseException
	{
	    super();
	    if (_hook==null)
		throw new NullPointerException("_hook");
	    if (_transaction==null)
		throw new NullPointerException("_transaction");
	    id = _id;
	    this.localID=localID;
	    hook = _hook;
	    if (_transaction.length>TRANSACTION_MAX_SIZE_BYTES)
		throw new SerializationDatabaseException("Too big transaction ! ");
	    transaction = _transaction;
	}


	public long getId()
	{
	    return id;
	}


	public void setId(long _id)
	{
	    id = _id;
	}


	public DatabaseHooksTable.Record getHook()
	{
	    return hook;
	}


	public void setHook(DatabaseHooksTable.Record _hook)
	{
	    hook = _hook;
	}


	public byte[] getTransaction()
	{
	    return transaction;
	}


	public void setTransaction(byte[] _transaction) throws SerializationDatabaseException
	{
	    if (_transaction==null)
		throw new NullPointerException("_transaction");
	    if (_transaction.length>TRANSACTION_MAX_SIZE_BYTES)
		throw new SerializationDatabaseException("Too big transaction ! ");
	    transaction = _transaction;
	}


	long getLocalID()
	{
	    return localID;
	}


	void setLocalID(long _localID)
	{
	    localID = _localID;
	}
	
	
    }
    
    //private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable=null;
    private volatile DatabaseHooksTable databaseHooksTable=null;
    //private volatile DatabaseEventsTable databaseEventsTable=null;
    
    
    /*DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException
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
    }*/

    DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException
    {
	if (databaseHooksTable==null)
	    databaseHooksTable=(DatabaseHooksTable)getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
	return databaseHooksTable; 
    }
    /*DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException
    {
	if (databaseDistantTransactionEventTable==null)
	    databaseDistantTransactionEventTable=(DatabaseDistantTransactionEvent)getDatabaseWrapper().getTableInstance(DatabaseDistantTransactionEvent.class);
	return databaseDistantTransactionEventTable; 
    }*/
    
    
    /*long removeTransactions(DatabaseHooksTable.Record hook, long lastTransfertID) throws DatabaseException
    {
	return removeRecords("id<=%id", "id", new Long(lastTransfertID));
    }*/
    
    void cleanDistantTransactions() throws DatabaseException
    {
	removeRecords(new Filter<DatabaseDistantTransactionEvent.Record>(){

	    @Override
	    public boolean nextRecord(Record _record) throws DatabaseException
	    {
		if (!getDatabaseHooksTable().isConcernedByIndirectTransaction(_record))
		    return true;
		return false;
	    }
	    
	});
    }
    DatabaseDistantTransactionEvent.Record unserializeDistantTransactionEvent(final ObjectInputStream ois) throws DatabaseException
    {
	try
	{
	    
	    int size=ois.readInt();
	    if (size>1024)
		throw new SerializationDatabaseException("Invalid data (hook id size est greater to 1024)");
	    byte b[]=new byte[size];
	    if (ois.read(b)!=size)
		throw new SerializationDatabaseException("Impossible to read the expected bytes number : "+size);
	    AbstractDecentralizedID hookID=null;
	    try
	    {
		hookID=AbstractDecentralizedID.instanceOf(b);
	    }
	    catch(Exception e)
	    {
		throw new SerializationDatabaseException("Impossible to get the hook identifier ! ", e); 
	    }
	    ArrayList<DatabaseHooksTable.Record> hooks=getDatabaseHooksTable().getRecords("hostID", hookID);
	    if (hooks.size()==0)
		throw new SerializationDatabaseException("Hook not found from this ID : "+hookID);
	    else if (hooks.size()>1)
		throw new IllegalAccessError();
		
	    long transactionID=ois.readLong();
	    size=ois.readInt();
	    if (size>TRANSACTION_MAX_SIZE_BYTES)
		throw new SerializationDatabaseException("Too big transaction ! ");
	    b=new byte[size];
	    if (ois.read(b)!=size)
		throw new SerializationDatabaseException("Impossible to read the expected bytes number : "+size);
	    DatabaseDistantTransactionEvent.Record res=new DatabaseDistantTransactionEvent.Record(transactionID, getTransactionIDTable().getLastTransactionID(), hooks.get(1),b);
	    return res;
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
    IDTable getTransactionIDTable() throws DatabaseException
    {
	if (transactionIDTable==null)
	    transactionIDTable=(IDTable)getDatabaseWrapper().getTableInstance(IDTable.class);
	return transactionIDTable;
    }
    
    
    int exportTransactions(final OutputStream outputStream, final DatabaseHooksTable.Record hook, final int maxEventsRecords, final long fromTransactionID, final AtomicLong nearNextLocalID) throws DatabaseException
    {
    	nearNextLocalID.set(fromTransactionID);
    	final AtomicInteger number=new AtomicInteger(0);
    	try(final ObjectOutputStream oos=new ObjectOutputStream(outputStream))
    	{
    	    getRecords(new Filter<DatabaseDistantTransactionEvent.Record>() {
	    
    		@Override
    		public boolean nextRecord(Record _record) throws DatabaseException
    		{
    		    try
    		    {
    			if (_record.getLocalID()>fromTransactionID)
    			{
    			    if (nearNextLocalID.get()<_record.getLocalID())
    				nearNextLocalID.set(_record.getLocalID());
    			}
    			else if (_record.getLocalID()==fromTransactionID)
    			{
    			    oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION);
    			    byte b[]=hook.getHostID().getBytes();
    			    oos.writeInt(b.length);
    			    oos.write(b);
    			    oos.writeLong(_record.getId());
    			    byte[] t=_record.getTransaction();
    			    oos.writeInt(t.length);
    			    oos.write(t);
    			    number.incrementAndGet();
    			}
    			return false;
    		    }
    		    catch(Exception e)
    		    {
    			throw DatabaseException.getDatabaseException(e);
    		    }
    		}
    	    }, "hook!=%hook AND localID>=%currentLocalID", "hook", hook, "currentLocalID", new Long(fromTransactionID));
    	    return number.get();
    	}
	catch(Exception e)
    	{
	    throw DatabaseException.getDatabaseException(e);
	}
    	
    }
    
}
