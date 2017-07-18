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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.distrimind.ood.database.DatabaseTransactionEventsTable.AbsractRecord;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.Bits;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseDistantTransactionEvent extends Table<DatabaseDistantTransactionEvent.Record>
{
    
    private volatile IDTable transactionIDTable=null;
    private volatile DatabaseDistantEventsTable databaseDistantEventsTable=null; 
    
    protected DatabaseDistantTransactionEvent() throws DatabaseException
    {
	super();
    }

    static class Record extends AbsractRecord
    {
	@Field(index=true)
	protected long localID;
	
	@ForeignKey
	@PrimaryKey
	protected DatabaseHooksTable.Record hook;
	
	@Field(limit=32768)
	protected byte[] peersInformed;
	
	@Field
	protected boolean peersInformedFull=false;
	
	public Record()
	{
	    
	}


	public Record(long _id, long localID, com.distrimind.ood.database.DatabaseHooksTable.Record _hook, /*byte[] _transaction, */boolean peersInformedFull, byte[] peersInformed, boolean force) 
	{
	    super();
	    if (_hook==null)
		throw new NullPointerException("_hook");
	    /*if (_transaction==null)
		throw new NullPointerException("_transaction");*/
	    setID(_id);
	    this.localID=localID;
	    hook = _hook;
	    /*if (_transaction.length>TRANSACTION_MAX_SIZE_BYTES)
		throw new SerializationDatabaseException("Too big transaction ! ");*/
	    //transaction = _transaction;
	    this.peersInformedFull=peersInformedFull;
	    if (!peersInformedFull)
		this.peersInformed=peersInformed;
	    this.setForce(force);
	}




	public DatabaseHooksTable.Record getHook()
	{
	    return hook;
	}


	public void setHook(DatabaseHooksTable.Record _hook)
	{
	    hook = _hook;
	}


	/*public byte[] getTransaction()
	{
	    return transaction;
	}*/


	/*public void setTransaction(byte[] _transaction) throws SerializationDatabaseException
	{
	    if (_transaction==null)
		throw new NullPointerException("_transaction");
	    if (_transaction.length>TRANSACTION_MAX_SIZE_BYTES)
		throw new SerializationDatabaseException("Too big transaction ! ");
	    transaction = _transaction;
	}*/


	long getLocalID()
	{
	    return localID;
	}


	void setLocalID(long _localID)
	{
	    localID = _localID;
	}
	
	void setPeersInformed(Collection<AbstractDecentralizedID> peers)
	{
	    byte[][] bytes=new byte[peers.size()][];
	    int i=0;
	    int size=2+peers.size()*2;
	    for (AbstractDecentralizedID id : peers)
	    {
		bytes[i]=id.getBytes();
		size+=bytes[i++].length+2;
		if (size>32768)
		{
		    peersInformedFull=true;
		    peersInformed=null;
		    return;
		}
	    }
	    peersInformedFull=false;
	    peersInformed=new byte[size];
	    i=2;
	    Bits.putShort(peersInformed, 0, (short)peers.size());
	    for (byte[] b : bytes)
	    {
		Bits.putShort(peersInformed, i, (short)b.length);
		i+=2;
		System.arraycopy(b, 0, peersInformed, i, b.length);
		i+=b.length;
	    }
	}
	
	List<AbstractDecentralizedID> getPeersInformed() throws SerializationDatabaseException
	{
	    if (peersInformed==null)
		return new ArrayList<>(0);
	    short nbPeers=Bits.getShort(peersInformed, 0);
	    ArrayList<AbstractDecentralizedID> res=new ArrayList<>(nbPeers);
	    int off=2;
	    for (int i=0;i<nbPeers;i++)
	    {
		short size=Bits.getShort(peersInformed, 2);
		if (size>1024)
		    throw new SerializationDatabaseException("Invalid data (hook id size est greater to 1024)");
		
		off+=2;
		res.add(AbstractDecentralizedID.instanceOf(peersInformed, off, size));
		off+=size;
	    }
	    return res;
	}
	
	boolean addNewHostIDAndTellsIfNewPeersCanBeConcerned(DatabaseHooksTable hooks, AbstractDecentralizedID newHostID) throws DatabaseException
	{
	    if (peersInformedFull)
		return false;
	    if (newHostID.equals(hook.getHostID()))
		return false;
	    final List<AbstractDecentralizedID> l=getPeersInformed();
	    if (!l.contains(newHostID))
	    {
		l.add(newHostID);
		setPeersInformed(l);
	    }
	    if (peersInformedFull)
		return false;
	    final AtomicBoolean res=new AtomicBoolean(false);
	    hooks.getRecords(new Filter<DatabaseHooksTable.Record>() {

		@Override
		public boolean nextRecord(com.distrimind.ood.database.DatabaseHooksTable.Record _record) 
		{
		    if (!_record.getHostID().equals(hook.getHostID()) && !l.contains(_record.getHostID()))
		    {
			res.set(true);
			stopTableParsing();
		    }

		    return false;
		}
	    });
	    return res.get();
	}
	
	boolean isConcernedBy(AbstractDecentralizedID newHostID) throws SerializationDatabaseException
	{
	    if (peersInformedFull)
		return false;
	    if (newHostID.equals(hook.getHostID()))
		return false;
	    List<AbstractDecentralizedID> l=getPeersInformed();
	    return !l.contains(newHostID);
	}
	
	Set<AbstractDecentralizedID> getConcernedHosts(Collection<AbstractDecentralizedID> hosts) throws SerializationDatabaseException
	{
	    HashSet<AbstractDecentralizedID> res=new HashSet<>();
	    if (peersInformedFull)
		return res;
	    List<AbstractDecentralizedID> l=getPeersInformed();
	    for (AbstractDecentralizedID id : hosts)
	    {
		if (!id.equals(hook.getHostID()) && !l.contains(id)) 
		    res.add(id);
	    }
	    return res;
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
	removeRecordsWithCascade(new Filter<DatabaseDistantTransactionEvent.Record>(){

	    @Override
	    public boolean nextRecord(Record _record) throws DatabaseException
	    {
		if (!getDatabaseHooksTable().isConcernedByIndirectTransaction(_record))
		    return true;
		return false;
	    }
	    
	});
    }
    
    DatabaseDistantTransactionEvent.Record unserializeDistantTransactionEvent(final DataInputStream ois) throws DatabaseException
    {
	try
	{
	    int size=ois.readShort();
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
	    ArrayList<DatabaseHooksTable.Record> hooks=getDatabaseHooksTable().getRecordsWithAllFields("hostID", hookID);
	    if (hooks.size()==0)
		throw new SerializationDatabaseException("Hook not found from this ID : "+hookID);
	    else if (hooks.size()>1)
		throw new IllegalAccessError();
		
	    long transactionID=ois.readLong();
	    boolean force=ois.readBoolean();
	    
	    boolean peersInformedFull=ois.readBoolean();
	    byte[] peersInformed=null;
	    if (!peersInformedFull)
	    {
		size=ois.readShort();
		if (size>0)
		{
		    peersInformed=new byte[size];
		    if (ois.read(peersInformed)!=size)
			throw new SerializationDatabaseException("Impossible to read the expected bytes number : "+size);
		}
	    }

	    
	    
	    DatabaseDistantTransactionEvent.Record res=new DatabaseDistantTransactionEvent.Record(transactionID, getTransactionIDTable().getLastTransactionID(), hooks.get(0),peersInformedFull, peersInformed, force);
	    try
	    {
		res.getPeersInformed();
	    }
	    catch(Exception e)
	    {
		throw new SerializationDatabaseException("Impossible to interprete informed peers", e);
	    }
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
    
    DatabaseDistantEventsTable getDatabaseDistantEventsTable() throws DatabaseException
    {
	if (databaseDistantEventsTable==null)
	    databaseDistantEventsTable=(DatabaseDistantEventsTable)getDatabaseWrapper().getTableInstance(DatabaseDistantEventsTable.class);
	return databaseDistantEventsTable;
    }
    
    
    int exportTransactions(final DataOutputStream oos, final DatabaseHooksTable.Record hook, final int maxEventsRecords, final long fromTransactionID, final AtomicLong nearNextLocalID) throws DatabaseException
    {
    	nearNextLocalID.set(Long.MAX_VALUE);
    	final AtomicInteger number=new AtomicInteger(0);
    	try
    	{
    	    getRecords(new Filter<DatabaseDistantTransactionEvent.Record>() {
	    
    		@Override
    		public boolean nextRecord(Record _record) throws DatabaseException
    		{
    		    try
    		    {
    			if (_record.getLocalID()>fromTransactionID)
    			{
    			    if (_record.getLocalID()<nearNextLocalID.get())
    				nearNextLocalID.set(_record.getLocalID());
    			}
    			else if (_record.getLocalID()==fromTransactionID)
    			{
    			    if (_record.isConcernedBy(hook.getHostID()))
    			    {
    				oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION);
    				byte b[]=_record.getHook().getHostID().getBytes();
    				oos.writeShort((short)b.length);
    				oos.write(b);
    				oos.writeLong(_record.getID());
    				oos.writeBoolean(_record.isForce());
    				oos.writeBoolean(_record.peersInformedFull);
    				if (!_record.peersInformedFull)
    				{
    				    byte[] t=_record.peersInformed;
    				    if (t==null || t.length==0)
    				    {
    					oos.writeShort((short)0);
    				    }
    				    else
    				    {
    					oos.writeShort((short)t.length);
    					oos.write(t);
    				    }
    				}
    				final AtomicInteger number=new AtomicInteger(0);
    				getDatabaseDistantEventsTable().getOrderedRecords(new Filter<DatabaseDistantEventsTable.Record>(){

				    @Override
				    public boolean nextRecord(com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record) throws DatabaseException
				    {
					_record.export(oos, getDatabaseWrapper());
					number.incrementAndGet();
					return false;
				    }}, "transaction=%transaction", new Object[] {"transaction", _record}, true, "position");
    				if (number.get()==0)
    				    throw new IllegalAccessError();
    				oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_FINISHED);
    				number.incrementAndGet();
    			    }
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