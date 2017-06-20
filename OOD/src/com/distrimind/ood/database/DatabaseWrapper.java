
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.DefaultConstructorAccessPrivilegedAction;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.fieldaccessors.ByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.DefaultByteTabObjectConverter;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.ReadWriteLock;



/**
 * This class represent a SqlJet database.
 * @author Jason Mahdjoub
 * @version 1.5
 * @since OOD 1.4
 */
public abstract class DatabaseWrapper implements AutoCloseable
{
    
    
    //protected Connection sql_connection;
    private volatile boolean closed=false;
    private final String database_name;
    private static final HashMap<String, ReadWriteLock> lockers=new HashMap<String, ReadWriteLock>();
    private static final HashMap<String, Integer> number_of_shared_lockers=new HashMap<String, Integer>();
    final static String ROW_COUNT_TABLES="ROW_COUNT_TABLES__";
    //private final HashMap<Class<? extends Table<?>>, Table<?>> tables_instances=new HashMap<>();
    private final ArrayList<ByteTabObjectConverter> converters;
    
    private volatile HashMap<Package, Database> sql_database=new HashMap<Package, Database>();
    //private final DatabaseMetaData dmd;
    private volatile DatabaseTransactionEventsTable transactionTable=null;
    private volatile DatabaseHooksTable databaseHooksTable=null;
    private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable=null;
    private volatile DatabaseEventsTable databaseEventsTable=null;
    private volatile IDTable transactionIDTable=null;
    private final File transactionsFile;
    private volatile int maxTransactionsEventsKeepedIntoMemory=100; 
    private OutputStream transactionOutputStream=null;
    private ObjectOutputStream transactionObjectOutputStream=null;
    private InputStream transactionInputStream=null;
    private ObjectInputStream transactionObjectInputStream=null;
    int currentTransactionsEventNummber=0;
    int currentTransactionsEventReadNummber=0;
    private final HashSet<Package> transactionPackages=new HashSet<>();
    private byte transactionTypes=0;
    private final DatabaseSynchronizer synchronizer=new DatabaseSynchronizer();
    protected int maxEventsRecords=1000;
    protected Database actualDatabaseLoading=null;
    
    
    public int getMaxEventsRecords()
    {
        return maxEventsRecords;
    }

    public void setMaxEventsRecords(int _maxEventsRecords)
    {
        maxEventsRecords = _maxEventsRecords;
    }


    //private final boolean isWindows;
    private static class Database
    {
	final HashMap<Class<? extends Table<?>>, Table<?>> tables_instances=new HashMap<Class<? extends Table<?>>, Table<?>>();
	private DatabaseTransactionEvent currentTransaction=null;
	private final DatabaseConfiguration configuration;
	public Database(DatabaseConfiguration configuration)
	{
	    if (configuration==null)
		throw new NullPointerException("configuration");
	    this.configuration=configuration;
	}
	
	DatabaseTransactionEvent getCurrentTransaction()
	{
	    if (currentTransaction==null)
		currentTransaction=new DatabaseTransactionEvent();
	    return currentTransaction;
	}
	
	void clearTransaction()
	{
	    currentTransaction=null;
	}

	boolean hasTransaction()
	{
	    return currentTransaction!=null;
	}
	DatabaseConfiguration getConfiguration()
	{
	    return configuration;
	}
	
    }
    
    public void setMaxTransactionsEventsKeepedIntoMemory(int v)
    {
	this.maxTransactionsEventsKeepedIntoMemory=v;
    }
    
    public int getMaxTransactionsEventsKeepedIntoMemory()
    {
	return this.maxTransactionsEventsKeepedIntoMemory;
    }
    
    public void addByteTabObjectConverter(ByteTabObjectConverter converter)
    {
	converters.add(converter);
    }
    
    public ArrayList<ByteTabObjectConverter> getByteTabObjectConverters()
    {
	return converters;
    }
    
    public ByteTabObjectConverter getByteTabObjectConverter(Class<?> object_type)
    {
	for (int i=converters.size()-1;i>=0;i--)
	{
	    ByteTabObjectConverter btoc=converters.get(i);
	    if (btoc.isCompatible(object_type))
		return btoc;
	}
	return null;
    }
    
    
    /**
     * Constructor
     * @param _sql_connection the sql_connection
     * @param _database_name the database name
     * @throws DatabaseException 
     * 
     */
    protected DatabaseWrapper(/*Connection _sql_connection, */String _database_name) throws DatabaseException
    {
	if (_database_name==null)
	    throw new NullPointerException("_database_name");
	/*if (_sql_connection==null)
	    throw new NullPointerException("_sql_connection");*/
	database_name=_database_name;
	//sql_connection=_sql_connection;
	
	//isWindows=OSValidator.isWindows();
	
	locker=getLocker();
	converters=new ArrayList<>();
	converters.add(new DefaultByteTabObjectConverter());
	this.transactionsFile=new File(database_name+".tmp.transactions");
	
    }
    
    public boolean isReadOnly() throws DatabaseException
    {
	    return ((Boolean)runTransaction(new Transaction() {
	        
	        @Override
	        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	        {
	            try
	            {
	        	Connection sql_connection=getConnectionAssociatedWithCurrentThread();
	        	return new Boolean(sql_connection.isReadOnly());
	            }
	            catch(SQLException e)
	            {
	        	throw DatabaseException.getDatabaseException(e);
	            }
	            
	        }
	        
	        @Override
	        public TransactionIsolation getTransactionIsolation()
	        {
	            return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
	        }
	        
	        @Override
	        public boolean doesWriteData()
	        {
	    	return false;
	        }
	    })).booleanValue();
	
    }
    
    public int getNetworkTimeout() throws DatabaseException
    {
	    return ((Integer)runTransaction(new Transaction() {
	        
	        @Override
	        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	        {
	            try
	            {
	        	Connection sql_connection=getConnectionAssociatedWithCurrentThread();
	        	return new Integer(sql_connection.getNetworkTimeout());
	            }
	            catch(SQLException e)
	            {
	        	throw DatabaseException.getDatabaseException(e);
	            }
	            
	        }
	        
	        @Override
	        public TransactionIsolation getTransactionIsolation()
	        {
	            return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
	        }
	        
	        @Override
	        public boolean doesWriteData()
	        {
	    	return false;
	        }
	    })).intValue();

	
    }
    
    
    /**
     * Gets the interface that enable database synchronization between different peers
     * @return the interface that enable database synchronization between different peers
     */
    public DatabaseSynchronizer getSynchronizer()
    {
	return synchronizer;
	
    }
    
    
    public class DatabaseSynchronizer
    {
	private DatabaseNotifier notifier=null;
	private boolean canNotify=true;
	private LinkedList<DatabaseEvent> events=new LinkedList<>();
	private HashMap<AbstractDecentralizedID, DatabaseHooksTable.Record> initializedHooks=new HashMap<>();
	DatabaseSynchronizer()
	{
	}
	
	public boolean isInitialized() throws DatabaseException
	{
	    return getHooksTransactionsTable().getLocalDatabaseHost()!=null;
	}
	
	public boolean isInitialized(AbstractDecentralizedID hostID)
	{
	    return initializedHooks.get(hostID)!=null;
	}
	private void notifyNewEvent()
	{
	    boolean notify=false;
	    synchronized(this)
	    {
		if (canNotify && notifier!=null)
		{
		    notify=true;
		    canNotify=false;
		}
		this.notifyAll();
	    }
	    if (notify)
		notifier.newDatabaseEventDetected(DatabaseWrapper.this);
	}
	public DatabaseEvent nextEvent()
	{
	    synchronized(this)
	    {
		if (events.isEmpty())
		    return null;
		else
		{
		    DatabaseEvent e=events.removeFirst();
		    if (events.isEmpty())
			canNotify=true;
		    return e;
		}
	    }
	}
	DatabaseNotifier getNotifier()
	{
	    synchronized(this)
	    {
		return notifier;
	    }
	}
	
	
	public DatabaseEvent waitNextEvent() throws InterruptedException
	{
	    synchronized(this)
	    {
		DatabaseEvent de=nextEvent();
		for (;de!=null;de=nextEvent())
		{
		    this.wait();
		}
		return de;
	    }
	}
	
	public void setNotifier(DatabaseNotifier notifier)
	{
	    synchronized(this)
	    {
		this.notifier=notifier;
		this.canNotify=true;
	    }
	}
	
	public void initLocalHostID(AbstractDecentralizedID localHostID) throws DatabaseException
	{
	    DatabaseHooksTable.Record local=getHooksTransactionsTable().getLocalDatabaseHost();
	    if (local!=null && !local.getHostID().equals(localHostID))
		throw new DatabaseException("The given local host id is different from the stored local host id !");
	    if (local==null)
	    {
		addHookForLocalDatabaseHost(localHostID);
	    }
	}
	
	public void addHookForLocalDatabaseHost(AbstractDecentralizedID hostID, Package ...databasePackages) throws DatabaseException
	{
	    getHooksTransactionsTable().addHooks(hostID, true, databasePackages);
	}
	
	public void addHookForDistantHost(AbstractDecentralizedID hostID, Package ...databasePackages) throws DatabaseException
	{
	    getHooksTransactionsTable().addHooks(hostID, false, databasePackages);
	}
	
	public void removeHook(AbstractDecentralizedID hostID, Package ...databasePackages) throws DatabaseException
	{
	    getHooksTransactionsTable().removeHooks(hostID, databasePackages);
	}
	
	public long getLastValidatedSynchronization(AbstractDecentralizedID hostID) throws DatabaseException
	{
	    synchronized(this)
	    {
		DatabaseHooksTable.Record r=initializedHooks.get(hostID);
		if (r==null)
		    throw DatabaseException.getDatabaseException(new IllegalArgumentException("The host ID "+hostID+" has not been initialized !"));

		return r.getLastValidatedDistantTransaction();
	    }
	}
	
	public void validateLastSynchronization(AbstractDecentralizedID hostID, long lastTransferedTransactionID) throws DatabaseException
	{
	    if (hostID==null)
		throw new NullPointerException("hostID");
	    if (!isInitialized())
		throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
	    
	    synchronized(this)
	    {
		DatabaseHooksTable.Record r=initializedHooks.get(hostID);
		if (r==null)
		    throw DatabaseException.getDatabaseException(new IllegalArgumentException("The host ID "+hostID+" has not been initialized !"));
		if (r.getLastValidatedTransaction()>lastTransferedTransactionID)
		    throw new DatabaseException("The given transfer ID limit "+lastTransferedTransactionID+" is lower than the stored transfer ID limit "+r.getLastValidatedTransaction());	
		getDatabaseTransactionsPerHostTable().validateTransactions(r, lastTransferedTransactionID);
		long lastID=getTransactionIDTable().getLastTransactionID();
		if (lastID>r.getLastValidatedTransaction())
		    addNewDatabaseEvent(new DatabaseEventsToSynchronize(getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), r, lastID, maxEventsRecords));
		
		Map<AbstractDecentralizedID, Long> lastIds=getHooksTransactionsTable().getLastValidatedDistantTransactions();
		
		for (AbstractDecentralizedID host : lastIds.keySet())
		{
		    if (!host.equals(hostID))
		    {
			Map<AbstractDecentralizedID, Long> map=new HashMap<AbstractDecentralizedID, Long>();
			map.putAll(lastIds);
			map.remove(hostID);
			map.remove(host);
			if (map.size()>0)
			{
			    addNewDatabaseEvent(new DatabaseTransactionsIdentifiersToSynchronize(getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), host, map));
			}
		    }
		}
		
	    }
	}
	
	void addNewDatabaseEvent(DatabaseEvent e)
	{
	    if (e==null)
		throw new NullPointerException("e");
	    
	    synchronized(this)
	    {
		boolean add=true;
		if (e.getClass()==DatabaseEventsToSynchronize.class)
		{
		    DatabaseEventsToSynchronize dets=(DatabaseEventsToSynchronize)e;
		    for (DatabaseEvent detmp : events)
		    {
			if (detmp.getClass()==DatabaseEventsToSynchronize.class && dets.tryToFusion((DatabaseEventsToSynchronize)detmp))
			{
			    add=false;
			    break;
			}
		    }
		}
		if (add)
		{
		    events.add(e);
		    notifyNewEvent();
		}
	    }
	}

	
	public void deconnectHook(final AbstractDecentralizedID hostID) throws DatabaseException
	{
	    if (hostID==null)
		throw new NullPointerException("hostID");
	    
	    synchronized(this)
	    {
		DatabaseHooksTable.Record hook=initializedHooks.remove(hostID);
		if (hook==null)
		    throw DatabaseException.getDatabaseException(new IllegalAccessException("hostID "+hostID+" has not be initialized !"));
		if (hook.concernsLocalDatabaseHost())
		{
		    initializedHooks.clear();
		    this.events.clear();
		}
		else
		{
		    for (Iterator<DatabaseEvent> it=events.iterator();it.hasNext();)
		    {
			DatabaseEvent de=it.next();
			if (de instanceof DatabaseEventToSend)
			{
			    DatabaseEventToSend des=(DatabaseEventToSend)de;
			    if (des.getHostDestination().equals(hostID))
				it.remove();
			}
		    }
		}
	    }
	    
	}
	
	
	
	public void initHook(final AbstractDecentralizedID hostID, final long lastValidatedTransacionID) throws DatabaseException
	{
	    if (hostID==null)
		throw new NullPointerException("hostID");
	    if (!isInitialized())
		throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");

	    synchronized(this)
	    {
		if (initializedHooks.remove(hostID)!=null)
		    throw DatabaseException.getDatabaseException(new IllegalAccessException("hostID "+hostID+" already initialized !"));
	    }
	    DatabaseHooksTable.Record r=runSynchronizedTransaction(new SynchronizedTransaction<DatabaseHooksTable.Record>() {

		@Override
		public DatabaseHooksTable.Record run() throws Exception
		{
		    DatabaseHooksTable.Record r=getHooksTransactionsTable().getRecord(new Object[]{"hostID", hostID});
		    if (r==null)
			throw new NullPointerException("Unknow host "+hostID);
		    if (r.getLastValidatedTransaction()>lastValidatedTransacionID)
			throw DatabaseException.getDatabaseException(new IllegalArgumentException("The host "+hostID+" have a validated trnasaction ID greater than the given transaciton ID : "+lastValidatedTransacionID));
		    
		    return r;
		}

		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
		}

		@Override
		public boolean doesWriteData()
		{
		    return true;
		}
	    });
	    synchronized(this)
	    {
		if (initializedHooks.containsValue(r))
		{
		    throw DatabaseException.getDatabaseException(new IllegalAccessException("hostID "+hostID+" already initialized !"));
		}
		initializedHooks.put(hostID, r);
		validateLastSynchronization(hostID, lastValidatedTransacionID);
	    }
	}
	public void received(DatabaseTransactionsIdentifiersToSynchronize d) throws DatabaseException
	{
	    if (!isInitialized())
		throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
	    
	    getHooksTransactionsTable().validateDistantTransactions(d.getHostSource(), d.getLastDistantTransactionIdentifiers(), true);
	}
	public void received(BigDatabaseEventToSend data, InputStream inputStream) throws DatabaseException
	{
	    data.inportFromInputStream(DatabaseWrapper.this, inputStream);
	}
	public void received(DatabaseEventToSend data) throws DatabaseException
	{
	    if (data instanceof DatabaseTransactionsIdentifiersToSynchronize)
		received((DatabaseTransactionsIdentifiersToSynchronize)data);
	}
    }
    
    public static abstract class DatabaseNotifier
    {
	public abstract void newDatabaseEventDetected(DatabaseWrapper wrapper);
	public abstract void newRecordToAddFailed(Table<?> concernedTable, DatabaseRecord concernedRecord, DatabaseException e);
	public abstract void recordToRemoveNotFound(Table<?> concernedTable, Map<String, Object> primary_keys);
	public abstract void recordToRemoveFailed(Table<?> concernedTable, Map<String, Object> primary_keys, DatabaseException e);
	public abstract void recordToUpdateNotFound(Table<?> concernedTable, Map<String, Object> primary_keys);
	public abstract void recordToUpdateFailed(Table<?> concernedTable, Map<String, Object> primary_keys, DatabaseException e);
	
	/**
	 * This function is called when a direct collision is detected during the synchronization process, when receiving data from a distant peer.
	 * 
	 * @param distantPeerID the concerned distant peer
	 * @param type the database event type 
	 * @param concernedTable the concerned table
	 * @param keys the concerned field keys
	 * @param newValues the new received values
	 * @param actualValues the actual field values
	 * @return true if the new event can replace the actual value and false if the actual value must replace the distant value.
	 * @throws DatabaseException if a problem occurs
	 */
	public abstract boolean directCollisionDetected(AbstractDecentralizedID distantPeerID, DatabaseEventType type, Table<?> concernedTable, HashMap<String, Object> keys, DatabaseRecord newValues, DatabaseRecord actualValues) throws DatabaseException;
	
	/**
	 * This function is called when an indirect collision is detected during the synchronization process, when receiving data from a distant peer.
	 * Indirect collisions occurs when for example the actual peer has to synchronize data with distant peer, data in collision with received received from anther to distant peer to synchronize with the actual peer. 
	 * 
	 * @param distantPeerID the concerned distant peer
	 * @param type the database event type 
	 * @param concernedTable the concerned table
	 * @param keys the concerned field keys
	 * @param newValues the new received values
	 * @param actualValues the actual field values
	 * @param distantPeerInCollisionWithDataToSynchronize the distant peer in collision with the receive data to synchronize.
	 * @return true if the new event can replace the actual value and false if the actual value must replace the distant value.
	 * @throws DatabaseException if a problem occurs
	 */
	public abstract boolean indirectCollisionDetected(AbstractDecentralizedID distantPeerID, DatabaseEventType type, Table<?> concernedTable, HashMap<String, Object> keys, DatabaseRecord oldRecord, DatabaseRecord actualValues, AbstractDecentralizedID distantPeerInCollisionWithDataToSynchronize) throws DatabaseException;
	
    }
    public static class DatabaseTransactionsIdentifiersToSynchronize extends DatabaseEvent implements DatabaseEventToSend
    {
	/**
	 * 
	 */
	private static final long serialVersionUID = -925935481339233901L;
	
	protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
	final Map<AbstractDecentralizedID, Long> lastTransactionFieldsBetweenDistantHosts;
	
	DatabaseTransactionsIdentifiersToSynchronize(AbstractDecentralizedID hostIDSource, AbstractDecentralizedID hostIDDestination, Map<AbstractDecentralizedID, Long> lastTransactionFieldsBetweenDistantHosts)
	{
	    this.hostIDSource=hostIDSource;
	    this.hostIDDestination=hostIDDestination;
	    this.lastTransactionFieldsBetweenDistantHosts=lastTransactionFieldsBetweenDistantHosts;
	}
	
	@Override
	public AbstractDecentralizedID getHostDestination() 
	{
	    return hostIDDestination;
	}
	
	@Override
	public AbstractDecentralizedID getHostSource() 
	{
	    return hostIDSource;
	}
	
	public Map<AbstractDecentralizedID, Long> getLastDistantTransactionIdentifiers()
	{
	    return lastTransactionFieldsBetweenDistantHosts;
	}
	
	
	public AbstractDecentralizedID getConcernedHost()
	{
	    return getHostDestination();
	}
	
    }
    
    public static class DatabaseEventsToSynchronize extends DatabaseEvent implements BigDatabaseEventToSend
    {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7594047077302832978L;
	protected final transient DatabaseHooksTable.Record hook;
	protected final int hookID;
	protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
	private long lastTransactionIDIncluded;
	final int maxEventsRecords;
	
	DatabaseEventsToSynchronize(AbstractDecentralizedID hostIDSource, DatabaseHooksTable.Record hook, long lastTransactionIDIncluded, int maxEventsRecords)
	{
	    this.hook=hook;
	    this.hookID=hook.getID();
	    this.hostIDDestination=hook.getHostID();
	    this.hostIDSource=hostIDSource;
	    this.lastTransactionIDIncluded=lastTransactionIDIncluded;
	    this.maxEventsRecords=maxEventsRecords;
	}
	
	@Override
	public AbstractDecentralizedID getHostDestination() 
	{
	    return hostIDDestination;
	}
	
	@Override
	public AbstractDecentralizedID getHostSource() 
	{
	    return hostIDSource;
	}
	@Override
	public void inportFromInputStream(DatabaseWrapper wrapper, final InputStream inputStream) throws DatabaseException
	{
	    if (wrapper==null)
		throw new NullPointerException("wrapper");
	    wrapper.getDatabaseTransactionsPerHostTable().alterDatabase(getHostSource(), inputStream);
	}
	@Override
	public boolean exportToOutputStream(final DatabaseWrapper wrapper, final OutputStream outputStream) throws DatabaseException
	{
	    if (wrapper==null)
		throw new NullPointerException("wrapper");
	    if (maxEventsRecords==0)
		return false;
	    return ((Boolean)wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {

		@Override
		public Boolean run() throws Exception
		{
		    int number=wrapper.getDatabaseTransactionsPerHostTable().exportTransactions(outputStream, hookID, maxEventsRecords);
		    if (number==0)
		    {
			hook.setLastValidatedTransaction(wrapper.getTransactionIDTable().getLastTransactionID());
			wrapper.getHooksTransactionsTable().updateRecord(hook);
		    }
		    return new Boolean(number>0);
		}

		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return TransactionIsolation.TRANSACTION_READ_COMMITTED;
		}

		@Override
		public boolean doesWriteData()
		{
		    return false;
		}
	    })).booleanValue();
	    
	}
	
	public boolean tryToFusion(DatabaseEventsToSynchronize dest)
	{
	    if (hookID==dest.hookID)
	    {
		lastTransactionIDIncluded=Math.max(lastTransactionIDIncluded, dest.lastTransactionIDIncluded);
		return true;
	    }
	    else
		return false;
	}
	
    }
    
    private ReadWriteLock getLocker()
    {
	    synchronized(DatabaseWrapper.class)
	    {
		String f=database_name;
		ReadWriteLock rwl=lockers.get(f);
		if (rwl==null)
		{
		    rwl=new ReadWriteLock();
		    lockers.put(f, rwl);
		    number_of_shared_lockers.put(f, new Integer(1));
		}
		else
		    number_of_shared_lockers.put(f, new Integer(number_of_shared_lockers.get(f).intValue()+1));
		
		return rwl;
	    }
    }
    
    DatabaseTransactionEventsTable getTransactionsTable() throws DatabaseException
    {
	if (transactionTable==null)
	    transactionTable=(DatabaseTransactionEventsTable)getTableInstance(DatabaseTransactionEventsTable.class);
	return transactionTable;
    }
    
    DatabaseHooksTable getHooksTransactionsTable() throws DatabaseException
    {
	if (databaseHooksTable==null)
	    databaseHooksTable=(DatabaseHooksTable)getTableInstance(DatabaseHooksTable.class);
	return databaseHooksTable;
    }
    DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException
    {
	if (databaseTransactionsPerHostTable==null)
	    databaseTransactionsPerHostTable=(DatabaseTransactionsPerHostTable)getTableInstance(DatabaseTransactionsPerHostTable.class);
	return databaseTransactionsPerHostTable;
    }
    DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException
    {
	if (databaseEventsTable==null)
	    databaseEventsTable=(DatabaseEventsTable)getTableInstance(DatabaseEventsTable.class);
	return databaseEventsTable;
    }

    IDTable getTransactionIDTable() throws DatabaseException
    {
	if (transactionIDTable==null)
	    transactionIDTable=(IDTable)getTableInstance(IDTable.class);
	return transactionIDTable;
    }

    protected abstract Connection reopenConnectionImpl()  throws DatabaseLoadingException;
    final Connection reopenConnection()  throws DatabaseException
    {
	try
	{
	    Connection c=reopenConnectionImpl();
	
	    disableAutoCommit(c);
	    return c;
	}
	catch(SQLException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
    }
    
    /**
     * 
     * @return The Sql connection.
     * @throws SQLException 
     */
    Connection getOpenedSqlConnection(Connection sql_connection) throws DatabaseException
    {
	try
	{
	    if (sql_connection==null || sql_connection.isClosed() || !sql_connection.isValid(5))
	    {
		//this.locker.lockWrite();
		/*try
		{*/
		    if (sql_connection!=null && !sql_connection.isValid(5))
			closeConnection(sql_connection, false);
		    if (sql_connection==null || sql_connection.isClosed())
			sql_connection=reopenConnection();
		/*}
		finally
		{
		    this.locker.unlockWrite();
		}*/
	    }
	    return sql_connection;
	}
	catch(SQLException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    protected abstract String getCachedKeyword();
    
    protected abstract void closeConnection(Connection c, boolean deepClosing) throws SQLException;
    
    public boolean isClosed()
    {
	return closed;
    }
    @Override
    public final void close() 
    {
	if (!closed)
	{
	    
	    //synchronized(this)
	    {
		try
		{
		    
		    synchronized(this.threadPerConnectionInProgress)
		    {
			closed=true;
			for (Iterator<Entry<Thread, Connection>> it= threadPerConnection.entrySet().iterator();it.hasNext();)
			{
			    Entry<Thread, Connection> c=it.next();
			    /*if (threadPerConnectionInProgress.containsKey(c.getKey()))
				continue;*/
			    try
			    {
				if (!c.getValue().isClosed())
				    closeConnection(c.getValue(), true);
			    }
			    catch(SQLException e)
			    {
				e.printStackTrace();
			    }
			    finally
			    {
				it.remove();
			    }
			}
		    }
		}
		finally
		{
		    try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
		    {
			DatabaseWrapper.lockers.remove(database_name);
			int v=DatabaseWrapper.number_of_shared_lockers.get(database_name).intValue()-1;
			if (v==0)
			    DatabaseWrapper.number_of_shared_lockers.remove(database_name);
			else if (v>0)
			    DatabaseWrapper.number_of_shared_lockers.put(database_name, new Integer(v));
			else
			    throw new IllegalAccessError();
			sql_database=new HashMap<>();
		    }
		    
		    System.gc();
		}
	    }
	    /*try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	    {
		closeConnection();
	    }
	    catch(SQLException se)
	    {
		throw DatabaseException.getDatabaseException(se);
	    }
	    finally
	    {
		try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
		{
		    DatabaseWrapper.lockers.remove(database_name);
		    int v=DatabaseWrapper.number_of_shared_lockers.get(database_name).intValue()-1;
		    if (v==0)
			DatabaseWrapper.number_of_shared_lockers.remove(database_name);
		    else if (v>0)
			DatabaseWrapper.number_of_shared_lockers.put(database_name, new Integer(v));
		    else
			throw new IllegalAccessError();
		    sql_database.clear();
		}
		closed=true;
	    }*/
	}
    }
    @Override public void finalize()
    {
	try
	{
	    close();
	}
	catch(Exception e)
	{
	    
	}
    }
    
    @Override public int hashCode()
    {
	return database_name.hashCode();
    }
    
    @Override public boolean equals(Object o)
    {
	return o==this;
	/*if (o==null)
	    return false;
	if (o==this)
	    return true;
	if (o instanceof HSQLDBWrapper)
	{
	    return this.database_name.equals(((HSQLDBWrapper) o).database_name);
	}
	return false;*/
    }
    
    @Override public String toString()
    {
	return database_name;
    }
    
    final ReadWriteLock locker;
    //private final AtomicBoolean transaction_already_running=new AtomicBoolean(false);
    private final Map<Thread, Connection> threadPerConnectionInProgress=new HashMap<>();
    private final Map<Thread, Connection> threadPerConnection=new HashMap<>();
    
    
    private static class ConnectionWrapper
    {
	final Connection connection;
	final boolean newTransaction;
	
	ConnectionWrapper(Connection connection, boolean newTransaction)
	{
	    this.connection=connection;
	    this.newTransaction=newTransaction;
	}
    }
    
    ConnectionWrapper isNewTransactionAndStartIt() throws DatabaseException
    {
	Thread t=Thread.currentThread();
	synchronized(threadPerConnectionInProgress)
	{
	    Connection c=threadPerConnectionInProgress.get(t);
	    
	    if (c==null)
	    {
		c=getOpenedSqlConnection(threadPerConnection.get(t));
		threadPerConnection.put(t, c);
		threadPerConnectionInProgress.put(t, c);

		return new ConnectionWrapper(c, true); 
	    }
	    else
		return new ConnectionWrapper(c, false);
	}
    }
    
    protected Connection getConnectionAssociatedWithCurrentThread() throws DatabaseException
    {
	synchronized(threadPerConnectionInProgress)
	{
	    Connection c=threadPerConnectionInProgress.get(Thread.currentThread());
	    
	    Connection c2=getOpenedSqlConnection(c);
	    if (c!=c2)
		threadPerConnectionInProgress.put(Thread.currentThread(), c2);
	    return c2;
	}
    }
    
    void releaseTransaction() throws DatabaseException
    {
	synchronized(threadPerConnectionInProgress)
	{
	    try
	    {
		Connection c=threadPerConnectionInProgress.remove(Thread.currentThread());
		if (c==null)
		    throw new IllegalAccessError();
		if (closed)
		{
		    if (!c.isClosed())
			closeConnection(c, true);
		}
	    
		for (Iterator<Entry<Thread, Connection>> it=threadPerConnection.entrySet().iterator();it.hasNext();)
		{
		    Entry<Thread, Connection> e=it.next();
		    
		    if (!e.getKey().isAlive())
		    {
			if (!e.getValue().isClosed() && !e.getValue().isClosed())
			    closeConnection(e.getValue(), false);

			it.remove();
		    }
		}
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    
    //private volatile boolean transaction_already_running=false;
    
    private boolean setTransactionIsolation(Connection sql_connection, TransactionIsolation transactionIsolation, boolean write) throws SQLException, DatabaseException
    {
	//Connection sql_connection=getOpenedSqlConnection();
	
	transactionIsolation=getValidTransactionIsolation(transactionIsolation);
	if (transactionIsolation!=null)
	{
	    
	    startTransaction(sql_connection, transactionIsolation, write);
	    return true;
	}
	else return false;
		    
    }
    
    protected TransactionIsolation getValidTransactionIsolation(TransactionIsolation transactionIsolation) throws SQLException, DatabaseException
    {
	
	if (supportTransactions())
	{
	    DatabaseMetaData dmd=getConnectionAssociatedWithCurrentThread().getMetaData();
	    if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode()))
	    {
		return transactionIsolation;
	    }
	    else
	    {
		TransactionIsolation ti=transactionIsolation.getNext();
		while (ti!=null)
		{
		    if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode()))
		    {
			return ti;
		    }
		    ti=ti.getNext();
		}
		ti=transactionIsolation.getPrevious();
		while (ti!=null)
		{
		    if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode()))
		    {
			return ti;
		    }
		    ti=ti.getPrevious();
		}
		
	    }
	}
	return null;
    }

    public boolean supportTransactions() throws DatabaseException
    {
	return ((Boolean)runTransaction(new Transaction() {
	    
	    @Override
	    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	    {
		try
		{
		    return new Boolean(getConnectionAssociatedWithCurrentThread().getMetaData().supportsTransactions());
		}
		catch(SQLException e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
	    }
	    
	    @Override
	    public TransactionIsolation getTransactionIsolation()
	    {
		return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
	    }
	    
	    @Override
	    public boolean doesWriteData()
	    {
		return false;
	    }
	})).booleanValue();
    }
    
    protected void startTransaction(Connection _openedConnection, TransactionIsolation transactionIsolation, boolean write) throws SQLException
    {
	_openedConnection.setTransactionIsolation(transactionIsolation.getCode());
    }
    
    protected void endTransaction(Connection _openedConnection)
    {
	
    }
    
    private void ensureTransactionOutputStreamClosed() throws DatabaseException
    {
	if (this.transactionObjectOutputStream!=null)
	{
	    try
	    {
		transactionObjectOutputStream.close();
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    finally
	    {
		try
		{
		    transactionOutputStream.close();
		}
		catch(Exception e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		finally
		{
		    transactionObjectOutputStream=null;
		    transactionOutputStream=null;
		}
	    }
	}
    }
    
    private void ensureTransactionInputStreamClosed() throws DatabaseException
    {
	if (this.transactionObjectInputStream!=null)
	{
	    try
	    {
		transactionObjectInputStream.close();
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    finally
	    {
		try
		{
		    transactionInputStream.close();
		}
		catch(Exception e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		finally
		{
		    transactionObjectInputStream=null;
		    transactionInputStream=null;
		}
	    }
	}
    }

    private void clearTransactions(boolean commit) throws DatabaseException
    {
	if (currentTransactionsEventNummber>maxTransactionsEventsKeepedIntoMemory)
	{
	    if (commit)
	    {
		for (Package p : transactionPackages)
		{
		    getTransactionsTable().addTransactionIfNecessary(p, getEventsStoredInFileIterator(), transactionTypes);
		}
	    }
	}
	else
	{
	    for (Map.Entry<Package, Database> e : this.sql_database.entrySet())
	    {
		Database d=e.getValue();
		if (commit)
		{
		    if (d.hasTransaction())
			getTransactionsTable().addTransactionIfNecessary(e.getValue().getConfiguration(), d.getCurrentTransaction(), transactionTypes);
		}
		d.clearTransaction();
	    }
	}
	ensureTransactionOutputStreamClosed();
	ensureTransactionInputStreamClosed();
	if (this.transactionsFile.exists())
	    this.transactionsFile.delete();
	transactionPackages.clear();
	currentTransactionsEventNummber=0;
	transactionTypes=0;
    }
    
    private boolean areTransactionsStoredIntoTemporaryFile()
    {
	return transactionObjectOutputStream!=null;
    }
    
    private ObjectOutputStream getTransactionsOutputStream() throws DatabaseException
    {
	if (transactionObjectOutputStream==null)
	{
	    try
	    {
		transactionOutputStream=new FileOutputStream(transactionsFile);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    try
	    {
		transactionObjectOutputStream=new ObjectOutputStream(transactionOutputStream);
	    }
	    catch(Exception e)
	    {
		try
		{
		    transactionOutputStream.close();
		}
		catch(Exception e2)
		{
		    e2.printStackTrace();
		}
		finally
		{
		    transactionOutputStream=null;
		}
		throw DatabaseException.getDatabaseException(e);
	    }
	}
	return transactionObjectOutputStream;
    }
    
    private ObjectInputStream getTransactionsInputStream() throws DatabaseException
    {
	ensureTransactionOutputStreamClosed();
	if (transactionObjectInputStream==null)
	{
	    currentTransactionsEventReadNummber=0;
	    try
	    {
		transactionInputStream=new FileInputStream(transactionsFile);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    try
	    {
		transactionObjectInputStream=new ObjectInputStream(transactionInputStream);
		
	    }
	    catch(Exception e)
	    {
		try
		{
		    transactionInputStream.close();
		}
		catch(Exception e2)
		{
		    e2.printStackTrace();
		}
		finally
		{
		    transactionInputStream=null;
		}
		throw DatabaseException.getDatabaseException(e);
	    }
	}
	return transactionObjectInputStream;
    }
    
    
    private <T extends DatabaseRecord> boolean storeEvent(TableEvent<T> de) throws DatabaseException
    {
	try
	{
	    Table<T> table=de.getTable(this);
	    transactionPackages.add(table.getClass().getPackage());
	    ObjectOutputStream oos=getTransactionsOutputStream();
	    String tableName=table.getClass().getName();
	    oos.writeInt(tableName.length());
	    oos.writeChars(tableName);
	    oos.writeByte(de.getType().getByte());
	    byte pk[];
	    if (de.getOldDatabaseRecord()!=null)
	    {
		pk=table.serializePrimaryKeys(de.getOldDatabaseRecord());
	    }
	    else
	    {
		pk=table.serializePrimaryKeys(de.getNewDatabaseRecord());
	    }
	    oos.writeInt(pk.length);
	    oos.write(pk);
	    /*if (de.getOldDatabaseRecord()==null)
		oos.writeInt(-1);
	    else
	    {
		byte tab[]=table.serializeFieldsNonPK(de.getOldDatabaseRecord());
		oos.writeInt(tab.length);
		oos.write(tab);
	    }*/
	    return true;
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    
    
    private Iterator<DatabaseEventsTable.Record> getEventsStoredInFileIterator() throws DatabaseException
    {
	ensureTransactionInputStreamClosed();
	return new Iterator<DatabaseEventsTable.Record>() {

	    @Override
	    public boolean hasNext()
	    {
		return currentTransactionsEventReadNummber<currentTransactionsEventNummber;
	    }

	    @Override
	    public DatabaseEventsTable.Record next()
	    {
		try
		{
		    return getNextDatabaseEventRecord();
		}
		catch(Exception e)
		{
		    e.printStackTrace();
		    return null;
		}
	    }

	    @Override
	    public void remove()
	    {
		throw new IllegalAccessError();
		
	    }
	};	
    }
    
    DatabaseEventsTable.Record getNextDatabaseEventRecord() throws DatabaseException
    {
	try
	{
	    ObjectInputStream ois=getTransactionsInputStream();
	    int size=ois.readInt();
	    char chars[]=new char[size];
	    for (int i=0;i<size;i++)
		chars[i]=ois.readChar();
	    DatabaseEventsTable.Record r=new DatabaseEventsTable.Record();
	    r.setConcernedTable(String.copyValueOf(chars));
	    
	    r.setType(ois.readByte());
	    
	    size=ois.readInt();
	    byte pk[]=new byte[size];
	    if (ois.read(pk)!=size)
		throw new DatabaseException("Unexpected exception !");
	    
	    r.setConcernedSerializedPrimaryKey(pk);
	    /*size=ois.readInt();
	    if (size!=-1)
	    {
		byte old[]=new byte[size];
		if (ois.read(old)!=size)
		    throw new DatabaseException("Unexpected exception !");
		r.setConcernedSerializedOldNonPK(old);
	    }
	    else
		r.setConcernedSerializedOldNonPK(null);*/
	    this.currentTransactionsEventReadNummber++;
	    return r;
	    
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    <T extends DatabaseRecord> boolean addEvent(Table<T> table, TableEvent<T> de) throws DatabaseException
    {
	if (table==null)
	    throw new NullPointerException("table");
	if (de==null)
	    throw new NullPointerException("de");
	if (!table.supportSynchronizationWithOtherPeers())
	    return false;
	Package p=table.getClass().getPackage();
	if (p.equals(DatabaseWrapper.class.getPackage()))
	    return false;
	if (!getHooksTransactionsTable().supportPackage(p))
	    return false;
	Database d=sql_database.get(p);
	if (d!=null)
	{
	    if (currentTransactionsEventNummber<this.maxTransactionsEventsKeepedIntoMemory)
	    {
		if (d.getCurrentTransaction().addEvent(de))
		{
		    transactionTypes|=de.getType().getByte();
		    currentTransactionsEventNummber++;
		    return true;
		}
		else
		    return false;
	    }
	    else
	    {
		if (!areTransactionsStoredIntoTemporaryFile())
		{
		    for (Database d2 : sql_database.values())
		    {
			if (d2.hasTransaction())
			{
			    for (TableEvent<?> de2 : d2.getCurrentTransaction().getEvents())
				storeEvent(de2);
			    d2.clearTransaction();
			}
		    }
		}
		if (storeEvent(de))
		{
		    transactionTypes|=de.getType().getByte();
		    currentTransactionsEventNummber++;
		    return true;
		}
		else
		    return false;
	    }
	}
	else
	    return false;
    }

    protected abstract void rollback(Connection openedConnection) throws SQLException;
    
    protected abstract void commit(Connection openedConnection) throws SQLException, DatabaseException;
    
    protected abstract boolean supportSavePoint(Connection openedConnection) throws SQLException;

    protected abstract void rollback(Connection openedConnection, String savePointName, Savepoint savePoint) throws SQLException;
    
    protected abstract void disableAutoCommit(Connection openedConnection) throws SQLException;
    
    protected abstract Savepoint savePoint(Connection openedConnection, String savePoint) throws SQLException;
    
    protected abstract void releasePoint(Connection openedConnection, String _savePointName, Savepoint savepoint) throws SQLException;
    
    private volatile long savePoint=0;
    
    
    protected String generateSavePointName() 
    {
	return "Savepoint"+(++savePoint);
	/*HexadecimalEncodingAlgorithm h=new HexadecimalEncodingAlgorithm();
	StringBuffer sb=new StringBuffer("S");
	h.convertToCharacters(new DecentralizedIDGenerator().getBytes(), sb);
	return sb.toString();*/
    }
    
    
    
    
    Object runTransaction(final Transaction _transaction) throws DatabaseException
    {
	Object res=null;
	ConnectionWrapper cw=isNewTransactionAndStartIt();
	if (cw.newTransaction)
	{
	    String savePointName=null;
	    Savepoint savePoint=null;
	    try
	    {
		
		setTransactionIsolation(cw.connection, _transaction.getTransactionIsolation(), _transaction.doesWriteData());
		
		if (_transaction.doesWriteData() && supportSavePoint(cw.connection))
		{
		    savePointName=generateSavePointName();
		    savePoint=savePoint(cw.connection, savePointName);
		}
		res=_transaction.run(this);
		commit(cw.connection);
		if (_transaction.doesWriteData())
		{
		    if (savePoint!=null)
			releasePoint(cw.connection, savePointName, savePoint);
		    clearTransactions(true);
		}
		
		endTransaction(cw.connection);
	    }
	    catch(DatabaseException e)
	    {
		try
		{
		    rollback(cw.connection);
		    if (_transaction.doesWriteData())
		    {
			if (savePoint!=null || savePointName!=null)
			{
			    releasePoint(cw.connection, savePointName, savePoint);
			}
			clearTransactions(false);
		    }
		}
		catch(SQLException se)
		{
		    throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
		}
		throw e;
	    }
	    catch(SQLException e)
	    {
		try
		{
		    rollback(cw.connection);
		    if (_transaction.doesWriteData())
		    {
			if (savePoint!=null || savePointName!=null)
			{
			    releasePoint(cw.connection, savePointName, savePoint);
			}
			clearTransactions(false);
		    }
		}
		catch(SQLException se)
		{
		    throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
		}
		clearTransactions(false);
		throw DatabaseException.getDatabaseException(e);
	    }
	    finally
	    {
		/*try
		{
		    setTransactionIsolation(TransactionIsolation.TRANSACTION_NONE);
		}
		catch(SQLException e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		finally
		{*/
		    releaseTransaction();
		//}
	    }	
	}
	else
	{
	    String savePointName=null;
	    Savepoint savePoint=null;

	    try
	    {
		if (_transaction.doesWriteData() && supportSavePoint(cw.connection))
		{
		    savePointName=generateSavePointName();
		    savePoint=savePoint(cw.connection, savePointName);
		}

		res=_transaction.run(this);
		if (savePoint!=null)
		    releasePoint(cw.connection, savePointName, savePoint);
		
	    }
	    catch(DatabaseException e)
	    {
		try
		{
		    if (_transaction.doesWriteData() && savePoint!=null)
		    {
			rollback(cw.connection, savePointName, savePoint);
			releasePoint(cw.connection, savePointName, savePoint);
		    }
		}
		catch(SQLException se)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		throw e;
	    }
	    catch(SQLException e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
	return res;
	/*Object res=null;
	if (_transaction.doesWriteData())
	{
	    if (!transaction_already_running)
	    {
		transaction_already_running=true;
		try
		{
		    res=_transaction.run(this);
		    sql_connection.commit();
		}
		catch(DatabaseException e)
		{
		    try
		    {
			sql_connection.rollback();
		    }
		    catch(SQLException se)
		    {
			throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
		    }
		    throw e;
		}
		catch(SQLException e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		finally
		{
		    transaction_already_running=false;
		}	
	    }
	    else
	    {
		res=_transaction.run(this);
	    }
	}
	else
	{
	    res=_transaction.run(this);
	}
	return res;*/
    }
    
    
    /**
     * Run a transaction by locking this database with the current thread. During this transaction execution, no transaction will be able to be run thanks to another thread. 
     * @param _transaction the transaction to run
     * @return the result of the transaction
     * @throws Exception if an exception occurs during the transaction running
     * @param <O> a type 
     */
    @SuppressWarnings("unchecked")
    public <O> O runSynchronizedTransaction(final SynchronizedTransaction<O> _transaction) throws DatabaseException
    {
	    return (O)this.runTransaction(new Transaction() {
	    
		@Override
		public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		{
		    try
		    {
			/*if (_transaction.doesWriteData())
			    locker.lockWrite();
			else
			    locker.lockRead();*/
			return (Object)_transaction.run();
		    }
		    catch(Exception e)
		    {
			throw new DatabaseException("",e);
		    }
		    /*finally
		    {
			if (_transaction.doesWriteData())
			    locker.unlockWrite();
			else
			    locker.unlockRead();
		    }*/
		}
		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return _transaction.getTransactionIsolation();
		}
		@Override
		public boolean doesWriteData()
		{
		    return _transaction.doesWriteData();
		}
	    });
    }
    
    
    
    /**
     * According a class name, returns the instance of a table which inherits the class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The returned table is always the same instance.
     * @param _table_name the full class name (with its package)
     * @return the corresponding table.
     * @throws DatabaseException if the class have not be found or if problems occur during the instantiation.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final Table<?> getTableInstance(String _table_name) throws DatabaseException
    {
	synchronized(this)
	{
	    if (_table_name==null)
		throw new NullPointerException("The parameter _table_name is a null pointer !");

	    try
	    {
		Class<?> c=Class.forName(_table_name);
		if (Table.class.isAssignableFrom(c))
		{
		    @SuppressWarnings("unchecked")
		    Class<? extends Table<?>> class_table=(Class<? extends Table<?>>)c;
		    return getTableInstance(class_table);
		}
		else
		    throw new DatabaseException("The class "+_table_name+" does not extends "+Table.class.getName());
	    }
	    catch (ClassNotFoundException e)
	    {
		throw new DatabaseException("Impossible to found the class/table "+_table_name);
	    }
	}	
	/*try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    if (_table_name==null)
		throw new NullPointerException("The parameter _table_name is a null pointer !");

	    try
	    {
		Class<?> c=Class.forName(_table_name);
		if (Table.class.isAssignableFrom(c))
		{
		    @SuppressWarnings("unchecked")
		    Class<? extends Table<?>> class_table=(Class<? extends Table<?>>)c;
		    return getTableInstance(class_table);
		}
		else
		    throw new DatabaseException("The class "+_table_name+" does not extends "+Table.class.getName());
	    }
	    catch (ClassNotFoundException e)
	    {
		throw new DatabaseException("Impossible to found the class/table "+_table_name);
	    }
	}*/
    }
    
    /**
     * According a Class&lsaquo;? extends Table&lsaquo;?&rsaquo;&rsaquo;, returns the instance of a table which inherits the class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The returned table is always the same instance. 
     * @param _class_table the class type
     * @return the corresponding table.
     * @throws DatabaseException if problems occur during the instantiation.
     * @throws NullPointerException if parameters are null pointers.
     * @param <TT> The table type
     */
    public final <TT extends Table<?>> Table<? extends Object> getTableInstance(Class<TT> _class_table) throws DatabaseException 
    {
	synchronized(Table.class)
	{
	    if (_class_table==null)
		throw new NullPointerException("The parameter _class_table is a null pointer !");
	    Database db=this.sql_database.get(_class_table.getPackage());
	    if (db==null)
	    {
		try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
		{
		    if (actualDatabaseLoading!=null && actualDatabaseLoading.getConfiguration().getPackage().equals(_class_table.getPackage()))
			db=actualDatabaseLoading;
		    else
			throw new DatabaseException("The given database was not loaded : "+_class_table.getPackage());
		}
	    }
	    Table<?> founded_table=db.tables_instances.get(_class_table);
	    if (founded_table!=null)
		return founded_table;
	    else
		throw new DatabaseException("Impossible to find the instance of the table "+_class_table.getName()+". It is possible that no SqlConnection was associated to the corresponding table.");
	}
    }
    
    
    /*@SuppressWarnings("unchecked")
    public final <TT extends Table<?>> Table<? extends DatabaseRecord> getTableInstance(Class<TT> _class_table) throws DatabaseException 
    {
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    if (this.closed)
		throw new DatabaseException("The given Database was closed : "+this);
	    if (_class_table==null)
		throw new NullPointerException("The parameter _class_table is a null pointer !");
	    
	    if (this.tables_instances.containsKey(_class_table))
		return (TT)this.tables_instances.get(_class_table);
	    else
	    {
		checkRowCountTable();
		return loadTable(_class_table);
	    }
	}
    }
    @SuppressWarnings("unchecked")
    private <TT extends Table<?>> TT loadTable(Class<TT> _class_table) throws DatabaseException
    {
	try
	{
	    ArrayList<Table<?>> list_tables=new ArrayList<>();
	    LinkedList<Class<? extends Table<?>>> list_classes_to_instanciate=new LinkedList<>();
	    
	    TT res=newInstance(_class_table);
	    this.tables_instances.put(_class_table, res);
	    list_tables.add(res);
	    list_classes_to_instanciate.push(_class_table);
	
	    while (list_classes_to_instanciate.size()>0)
	    {
		Class<? extends Table<?>> c=list_classes_to_instanciate.poll();
		Table<?> t=tables_instances.get(c);
		if (t==null)
		{
		    t=newInstance(c);
		    list_tables.add(t);
		    this.tables_instances.put(c, t);
		}
		for (ForeignKeyFieldAccessor fkfa : t.getForeignKeysFieldAccessors())
		    list_classes_to_instanciate.add((Class<? extends Table<?>>)fkfa.getFieldClassType());
	    }
	    for (Table<?> t : list_tables)
	    {
		t.initializeStep1();
	    }
	    for (Table<?> t : list_tables)
	    {
		t.initializeStep2();
	    }
	    for (Table<?> t : list_tables)
	    {
		t.initializeStep3();
	    }
	    return res;
	}
	catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | PrivilegedActionException e)
	{
	    throw new DatabaseException("Impossible to access to a class ! ", e);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    private void checkRowCountTable() throws DatabaseException
    {
	    runTransaction(new Transaction() {
		    
		@Override
		public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException
		{
		    try
		    {
			boolean table_found=false;
			try (ReadQuerry rq=new ReadQuerry(sql_connection.getSqlConnection(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+ROW_COUNT_TABLES+"'"))
			{
			    if (rq.result_set.next())
				table_found=true;
			}
			if (!table_found)
			{
			    Statement st=sql_connection.getSqlConnection().createStatement();
			    st.executeUpdate("CREATE TABLE "+ROW_COUNT_TABLES+" (TABLE_NAME VARCHAR(512), ROW_COUNT INTEGER)");
			    st.close();
			}
			
			
			return null;
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		}
		@Override
		public boolean doesWriteData()
		{
		    return true;
		}
		
	    });
	
    }*/
    
    /**
     * Remove a database
     * @param configuration the database configuration
     * @throws DatabaseException if a problem occurs
     */
    public final void deleteDatabase(final DatabaseConfiguration configuration) throws DatabaseException
    {
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    
	    runTransaction(new Transaction() {
		    
		@SuppressWarnings("synthetic-access")
		@Override
		public Void run(DatabaseWrapper sql_connection) throws DatabaseException
		{
		    try
		    {
			if (!sql_database.containsKey(configuration.getPackage()))
			    loadDatabase(configuration, false);
			
		    }
		    catch(DatabaseException e)
		    {
			return null;
		    }
		    
		    Database db=sql_database.get(configuration.getPackage());
		    if (db==null)
			throw new IllegalAccessError();
		    
		    ArrayList<Table<?>> list_tables=new ArrayList<>(configuration.getTableClasses().size());
		    for (Class<? extends Table<?>> c : configuration.getTableClasses())
		    {
			Table<?> t=getTableInstance(c);
			list_tables.add(t);
		    }
		    for (Table<?> t : list_tables)
		    {
			t.removeTableFromDatabaseStep1();
		    }
		    for (Table<?> t : list_tables)
		    {
			t.removeTableFromDatabaseStep2();
		    }
		    
		    @SuppressWarnings("unchecked")
		    HashMap<Package, Database> sd=(HashMap<Package, Database>)sql_database.clone();
		    sd.remove(configuration.getPackage());
		    sql_database=sd;
		    
		    return null;
		}
		@Override
		public boolean doesWriteData()
		{
		    return true;
		}



		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return TransactionIsolation.TRANSACTION_SERIALIZABLE;
		}
	    });

	    
	    
	    
	    
	}
    }
    
    /**
     * Associate a Sql database with a given database configuration. Every table/class in the given configuration which inherits to the class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included into the same database.
     * This function must be called before every any operation with the corresponding tables.
     * @param configuration the database configuration
     * @param createDatabaseIfNecessaryAndCheckIt If set to false, and if the database does not exists, generate a DatabaseException. If set to true, and if the database does not exists, create it. Use {@link DatabaseConfiguration#getDatabaseCreationCallable()} if the database is created and if transfer from old database must done.
     * @return the result returned by onCreationDatabaseCallable or null if the database was already created 
     * @throws DatabaseException if the given package is already associated to a database, or if the database cannot be created.
     * @throws NullPointerException if the given parameters are null.
     */
    public final void loadDatabase(final DatabaseConfiguration configuration, final boolean createDatabaseIfNecessaryAndCheckIt) throws DatabaseException
    {
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    if (this.closed)
		throw new DatabaseException("The given Database was closed : "+this);
	    if (configuration==null)
		throw new NullPointerException("tables is a null pointer.");
	    
	    if (sql_database.containsKey(configuration.getPackage()))
		throw new DatabaseException("There is already a database associated to the given HSQLDBWrappe ");
	    try
	    {
	    actualDatabaseLoading=new Database(configuration);
	    
	    
	    runTransaction(new Transaction() {
		    
		@Override
		public Void run(DatabaseWrapper sql_connection) throws DatabaseException
		{
		    try
		    {
			
			/*boolean table_found=false;
			try (ReadQuerry rq=new ReadQuerry(sql_connection.getSqlConnection(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+ROW_COUNT_TABLES+"'"))
			{
			    if (rq.result_set.next())
				table_found=true;
			}*/
			boolean allNotFound=true;
			if (!doesTableExists(ROW_COUNT_TABLES))
			{
			    Statement st=getConnectionAssociatedWithCurrentThread().createStatement();
			    st.executeUpdate("CREATE TABLE "+ROW_COUNT_TABLES+" (TABLE_NAME VARCHAR(512), ROW_COUNT INTEGER)"+getSqlComma());
			    st.close();
			}
			
			
			ArrayList<Table<?>> list_tables=new ArrayList<>(configuration.getTableClasses().size());
			for (Class<? extends Table<?>> class_to_load : configuration.getTableClasses())
			{
			    Table<?> t=newInstance(class_to_load);
			    list_tables.add(t);
			    actualDatabaseLoading.tables_instances.put(class_to_load, t);
			}
			
			for (Table<?> t : list_tables)
			{
			    t.initializeStep1(configuration);
			}
			for (Table<?> t : list_tables)
			{
			    allNotFound=!t.initializeStep2(createDatabaseIfNecessaryAndCheckIt) && allNotFound;
			}
			for (Table<?> t : list_tables)
			{
			    t.initializeStep3();
			}
			if (allNotFound)
			{
			    try
			    {
				DatabaseConfiguration oldConfig= configuration.getOldVersionOfDatabaseConfiguration();
				DatabaseCreationCallable callable=configuration.getDatabaseCreationCallable();
				boolean removeOldDatabase=false;
				if (oldConfig!=null && callable!=null)
				{
				    try
				    {
					loadDatabase(oldConfig, false);
					if (callable!=null)
					    callable.transfertDatabaseFromOldVersion(configuration);
					removeOldDatabase=callable.hasToRemoveOldDatabase();
				    }
				    catch(DatabaseException e)
				    {
					oldConfig=null;
				    }
				}
				if (callable!=null)
				{
				    callable.afterDatabaseCreation(configuration);
				    if (removeOldDatabase)
					deleteDatabase(oldConfig);
				}
			    }
			    catch(Exception e)
			    {
				throw DatabaseException.getDatabaseException(e);
			    }
			}
			
			return null;
			
		    }
		    catch (ClassNotFoundException e)
		    {
			throw new DatabaseException("Impossible to access to t)he list of classes contained into the package "+configuration.getPackage().getName(), e);
		    }
		    catch (IOException e)
		    {
			throw new DatabaseException("Impossible to access to the list of classes contained into the package "+configuration.getPackage().getName(), e);	
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		}
		
		@Override
		public boolean doesWriteData()
		{
		    return true;
		}



		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return TransactionIsolation.TRANSACTION_SERIALIZABLE;
		}
		
	    });
	    @SuppressWarnings("unchecked")
	    HashMap<Package, Database> sd=(HashMap<Package, Database>)sql_database.clone();
	    sd.put(configuration.getPackage(), actualDatabaseLoading);
	    sql_database=sd;
	    }
	    finally
	    {
		actualDatabaseLoading=null;
	    }
	}
    }
    
    
    <TT extends Table<?>> TT newInstance(Class<TT> _class_table) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, DatabaseException, PrivilegedActionException
    {
	DefaultConstructorAccessPrivilegedAction<TT> class_privelege=new DefaultConstructorAccessPrivilegedAction<TT>(_class_table); 
	
	Constructor<TT> const_table=(Constructor<TT>)AccessController.doPrivileged(class_privelege);

	TT t=(TT)const_table.newInstance();
	t.initializeStep0(this);
	return t;
    }
    
    protected abstract boolean doesTableExists(String tableName) throws Exception;
    protected abstract ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception;
    protected abstract void checkConstraints(Table<?> table) throws DatabaseException;
    
    
    
    
    public static abstract class TableColumnsResultSet
    {
	protected ResultSet resultSet;
	
	protected TableColumnsResultSet(ResultSet rs)
	{
	    resultSet=rs;
	}
	
	public boolean next() throws SQLException
	{
	    return resultSet.next();
	}
	
	public abstract String getColumnName() throws SQLException;
	public abstract String getTypeName() throws SQLException;
	public abstract int getColumnSize() throws SQLException;
	public abstract boolean isNullable() throws SQLException;
	public abstract boolean isAutoIncrement() throws SQLException;
    }
    
    protected abstract String getSqlComma();
    protected abstract int getVarCharLimit();
    protected abstract boolean isVarBinarySupported();

    protected abstract String getSqlNULL();
    
    protected abstract String getSqlNotNULL();
    
    protected abstract String getByteType();
    protected abstract String getIntType();
    protected abstract String getSerializableType();
    protected abstract String getFloatType();
    protected abstract String getDoubleType();
    protected abstract String getShortType();
    protected abstract String getLongType();
    protected abstract String getBigDecimalType();
    protected abstract String getBigIntegerType();
    protected abstract String getSqlQuerryToGetLastGeneratedID();
    protected abstract String getDropTableIfExistsKeyWord();
    protected abstract String getDropTableCascadeKeyWord();
    
    Collection<Table<?>> getListTables(Package p)
    {
	Database db=this.sql_database.get(p);
	if (db==null)
	{
	    try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	    {
		if (actualDatabaseLoading!=null && actualDatabaseLoading.getConfiguration().getPackage().equals(p))
		    db=actualDatabaseLoading;
	    }
	}
	return db.tables_instances.values();
    }

    protected abstract String getOnUpdateCascadeSqlQuerry();
    protected abstract String getOnDeleteCascadeSqlQuerry();
    protected boolean supportUpdateCascade()
    {
	return !getOnUpdateCascadeSqlQuerry().equals("");
    }
    
    protected abstract Blob getBlob(byte[] bytes) throws SQLException;

    /**
     * Backup the database into the given path. 
     * @param path the path where to save the database.  
     * @throws DatabaseException if a problem occurs
     */
    public abstract void backup(File path) throws DatabaseException;
    
    protected abstract boolean isThreadSafe();
}
