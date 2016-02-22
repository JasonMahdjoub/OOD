/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */



package com.distrimind.ood.database;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.DefaultConstructorAccessPrivilegedAction;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.fieldaccessors.ByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.DefaultByteTabObjectConverter;
import com.distrimind.util.ListClasses;
import com.distrimind.util.ReadWriteLock;



/**
 * This class represent a SqlJet database.
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.4
 */
public abstract class DatabaseWrapper
{
    protected final Connection sql_connection;
    private boolean closed=false;
    private final String database_name;
    private static final HashMap<String, ReadWriteLock> lockers=new HashMap<String, ReadWriteLock>();
    private static final HashMap<String, Integer> number_of_shared_lockers=new HashMap<String, Integer>();
    final static String ROW_COUNT_TABLES="ROW_COUNT_TABLES__";
    //private final HashMap<Class<? extends Table<?>>, Table<?>> tables_instances=new HashMap<>();
    private final ArrayList<ByteTabObjectConverter> converters;
    
    private HashMap<Package, Database> sql_database=new HashMap<Package, Database>();
    
    private static class Database
    {
	public final HashMap<Class<? extends Table<?>>, Table<?>> tables_instances=new HashMap<Class<? extends Table<?>>, Table<?>>();
	
	public Database(Package _package)
	{
	    
	}
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
     * 
     */
    protected DatabaseWrapper(Connection _sql_connection, String _database_name)
    {
	if (_database_name==null)
	    throw new NullPointerException("_database_name");
	if (_sql_connection==null)
	    throw new NullPointerException("_sql_connection");
	database_name=_database_name;
	sql_connection=_sql_connection;
	locker=getLocker();
	converters=new ArrayList<>();
	converters.add(new DefaultByteTabObjectConverter());
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
    

    
    /**
     * 
     * @return The Sql connection.
     */
    public Connection getSqlConnection()
    {
	return sql_connection;
    }
    
    protected abstract String getCachedKeyword();
    
    protected abstract void closeConnection() throws SQLException;
    
    public void close() throws DatabaseException
    {
	if (!closed)
	{
	    try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
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
	    }
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
    private boolean transaction_already_running=false;
    
    
    Object runTransaction(final Transaction _transaction) throws DatabaseException
    {
	Object res=null;
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
	return res;
    }
    
    
    /**
     * Run a transaction by locking this database with the current thread. During this transaction execution, no transaction will be able to be run thanks to another thread. 
     * @param _transaction the transaction to run
     * @return the result of the transaction
     * @throws Exception if an exception occurs during the transaction running
     * @param <O> a type 
     */
    @SuppressWarnings("unchecked")
    public <O> O runSynchronizedTransaction(final SynchronizedTransaction<O> _transaction) throws Exception
    {
	try
	{
	    return (O)this.runTransaction(new Transaction() {
	    
		@Override
		public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		{
		    try
		    {
			locker.lockWrite();
			return (Object)_transaction.run();
		    }
		    catch(Exception e)
		    {
			throw new DatabaseException("",e);
		    }
		    finally
		    {
			locker.unlockWrite();
		    }
		}
	    
		@Override
		public boolean doesWriteData()
		{
		    return true;
		}
	    });
	}
	catch(DatabaseException e)
	{
	    if (e.getCause()!=null && e.getCause() instanceof Exception)
		throw (Exception)e.getCause();
	    else
		throw e;
	}
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
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
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
    }
    
    /**
     * According a Class&lsaquo;? extends Table&lsaquo;?&rsaquo;&rsaquo;, returns the instance of a table which inherits the class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The returned table is always the same instance. 
     * @param _class_table the class type
     * @return the corresponding table.
     * @throws DatabaseException if problems occur during the instantiation.
     * @throws NullPointerException if parameters are null pointers.
     * @param <TT> The table type
     */
    public final <TT extends Table<?>> Table<? extends DatabaseRecord> getTableInstance(Class<TT> _class_table) throws DatabaseException 
    {
	synchronized(Table.class)
	{
	    if (_class_table==null)
		throw new NullPointerException("The parameter _class_table is a null pointer !");
	    Database db=this.sql_database.get(_class_table.getPackage());
	    if (db==null)
		throw new DatabaseException("The given database was not loaded : "+_class_table.getPackage());
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
     * Associate a Sql database with a given package. Every table/class in the given package which inherits to the class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included into the same database.
     * This function must be called before every any operation with the corresponding tables.
     * @param _package the package which correspond to the collection of tables/classes.
     * @throws DatabaseException if the given package is already associated to a database.
     * @throws NullPointerException if the given parameters are null.
     */
    public final void loadDatabase(Package _package) throws DatabaseException
    {
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    if (this.closed)
		throw new DatabaseException("The given Database was closed : "+this);
	    if (_package==null)
		throw new NullPointerException("_package is a null pointer.");
	    if (sql_database.containsKey(_package))
		throw new DatabaseException("There is already a database associated to the given HSQLDBWrapper "+sql_connection);
	    
	    Database db=new Database(_package);
	    sql_database.put(_package, db);
	    
	    runTransaction(new Transaction() {
		    
		@Override
		public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException
		{
		    try
		    {
			
			/*boolean table_found=false;
			try (ReadQuerry rq=new ReadQuerry(sql_connection.getSqlConnection(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+ROW_COUNT_TABLES+"'"))
			{
			    if (rq.result_set.next())
				table_found=true;
			}*/
			if (!doesTableExists(ROW_COUNT_TABLES))
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
	    
	    
	    ArrayList<Class<?>> list_classes;
	    try
	    {
		list_classes = ListClasses.getClasses(_package);
		ArrayList<Table<?>> list_tables=new ArrayList<>();
		for (Class<?> c : list_classes)
		{
		    if (Table.class.isAssignableFrom(c))
		    {
			@SuppressWarnings("unchecked")
			Class<? extends Table<?>> class_to_load=(Class<? extends Table<?>>)c;
			Table<?> t=newInstance(class_to_load);
			list_tables.add(t);
			db.tables_instances.put(class_to_load, t);
		    }
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
	    }
	    catch (ClassNotFoundException e)
	    {
		throw new DatabaseException("Impossible to access to t)he list of classes contained into the package "+_package.getName(), e);
	    }
	    catch (IOException e)
	    {
		throw new DatabaseException("Impossible to access to the list of classes contained into the package "+_package.getName(), e);	
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    
    private <TT extends Table<?>> TT newInstance(Class<TT> _class_table) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, DatabaseException, PrivilegedActionException
    {
	DefaultConstructorAccessPrivilegedAction<TT> class_privelege=new DefaultConstructorAccessPrivilegedAction<TT>(_class_table); 
	
	Constructor<TT> const_table=(Constructor<TT>)AccessController.doPrivileged(class_privelege);

	TT t=(TT)const_table.newInstance();
	t.initializeStep0(this);
	return t;
    }
    
    abstract boolean doesTableExists(String tableName) throws Exception;
    abstract ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception;
    abstract void checkConstraints(Table<?> table) throws DatabaseException;
    
    
    
    
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
    
    abstract String getSqlComma();
    public abstract int getVarCharLimit();
    public abstract boolean isVarBinarySupported();

    abstract String getSqlNULL();
    
    abstract String getSqlNotNULL();
    
    public abstract String getByteType();
    public abstract String getIntType();
    public abstract String getSerializableType();
    public abstract String getFloatType();
    public abstract String getDoubleType();
    public abstract String getShortType();
    public abstract String getLongType();
    public abstract String getBigDecimalType();
    public abstract String getBigIntegerType();
    abstract String getSqlQuerryToGetLastGeneratedID();
    
    Collection<Table<?>> getListTables(Package p)
    {
	return this.sql_database.get(p).tables_instances.values();
    }

    abstract String getOnUpdateCascadeSqlQuerry();
    abstract String getOnDeleteCascadeSqlQuerry();
    public boolean supportUpdateCascade()
    {
	return !getOnUpdateCascadeSqlQuerry().equals("");
    }
    
    public abstract Blob getBlob(byte[] bytes) throws SQLException;
    
}
