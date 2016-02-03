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



package ood.database;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import ood.database.exceptions.DatabaseException;
import ood.database.exceptions.DatabaseIntegrityException;
import ood.utils.ReadWriteLock;


/**
 * This class represent a SqlJet database contained into one file.
 * @author Jason Mahdjoub
 * @version 1.0
 *
 */
public final class HSQLDBWrapper
{
    protected final Connection sql_connection;
    private static boolean hsql_loaded=false;
    private boolean closed=false;
    private final String database_name;
    private static final HashMap<String, ReadWriteLock> lockers=new HashMap<String, ReadWriteLock>();
    private static final HashMap<String, Integer> number_of_shared_lockers=new HashMap<String, Integer>();

    
    private static void ensureHSQLLoading() throws DatabaseLoadingException
    {
	synchronized(HSQLDBWrapper.class)
	{
	    if (!hsql_loaded)
	    {
		try
		{
		    
		    Class.forName("org.hsqldb.jdbc.JDBCDriver" );
		    hsql_loaded=true;
		}
		catch(Exception e)
		{
		    throw new DatabaseLoadingException("Impossible to load HSQLDB ", e);
		}
	    }
	}
    }
    
    
    /**
     * Constructor
     * @param _file_name The file which contains the database. If this file does not exists, it will be automatically created with the correspondent database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws IllegalArgumentException If the given file is a directory.
     * @throws DatabaseLoadingException If a Sql exception exception occurs.
     */
    public HSQLDBWrapper(File _file_name) throws IllegalArgumentException, DatabaseException
    {
	this(_file_name, 100, 10000, 0, 512);
    }
    
    
    /**
     * Constructor
     * @param _file_name The file which contains the database. If this file does not exists, it will be automatically created with the correspondent database.
     * @param _cache_rows indicates the maximum number of rows of cached tables that are held in memory. The value can range between 100- 4 billion. Default value is 100. Table loaded into memory are not concerned.
     * @param _cache_size Indicates the total size (in kilobytes) of rows in the memory cache used with cached tables. The value can range between 100 KB - 4 GB. The default is 10,000, representing 10,000 kilobytes. 
     * @param _result_max_memory_rows This property can be set to specify how many rows of each results or temporary table are stored in memory before the table is written to disk. The default is zero and means data is always stored in memory. If this setting is used, it should be set above 1000.
     * @param _cache_free_count The default indicates 512 unused spaces are kept for later use. The value can range between 0 - 8096. When rows are deleted, the space is recovered and kept for reuse for new rows. If too many rows are deleted, the smaller recovered spaces are lost and the largest ones are retained for later use. Normally there is no need to set this property.
     * @throws NullPointerException if parameters are null pointers.
     * @throws IllegalArgumentException If the given file is a directory.
     * @throws DatabaseLoadingException If a Sql exception exception occurs.
     */
    public HSQLDBWrapper(File _file_name, int _cache_rows, int _cache_size, int _result_max_memory_rows, int _cache_free_count) throws IllegalArgumentException, DatabaseException
    {
	if (_file_name==null)
	    throw new NullPointerException("The parameter _file_name is a null pointer !");
	if (_file_name.isDirectory())
	    throw new IllegalArgumentException("The given file name is a directory !");
	ensureHSQLLoading();
	try
	{
	    database_name="Database from file : "+getHSQLDBDataFileName(_file_name)+".data";
	    sql_connection=DriverManager.getConnection("jdbc:hsqldb:file:"+getHSQLDBDataFileName(_file_name)+";hsqldb.cache_rows="+_cache_rows+";hsqldb.cache_size="+_cache_size+";hsqldb.result_max_memory_rows="+_result_max_memory_rows+";hsqldb.cache_free_count="+_cache_free_count+";shutdown=true", "SA", "");
	    sql_connection.setAutoCommit(false);
	    locker=getLocker();
	}
	catch (Exception e)
	{
	    throw new DatabaseLoadingException("Impossible to create the database into the file "+_file_name, e);
	}
    }
    
    private ReadWriteLock getLocker()
    {
	    synchronized(HSQLDBWrapper.class)
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
    
    private static String getHSQLDBDataFileName(File _file_name)
    {
	String s=_file_name.getAbsolutePath();
	if (s.toLowerCase().endsWith(".data"))
	    return s.substring(0, s.length()-5);
	else
	    return s; 
    }
    
    /**
     * Delete all the files associated to the .data database file.
     * @param _file_name the .data database file, or the base file name (the file without the .data extension).
     */
    public static void deleteDatabaseFiles(File _file_name)
    {
	String file_base=getHSQLDBDataFileName(_file_name);
	File f=new File(file_base+".data");
	if (f.exists())
	    f.delete();
	f=new File(file_base+".log");
	if (f.exists())
	    f.delete();
	f=new File(file_base+".properties");
	if (f.exists())
	    f.delete();
	f=new File(file_base+".script");
	if (f.exists())
	    f.delete();
	f=new File(file_base+".tmp");
	if (f.exists())
	    f.delete();
    }
    /**
     * Constructor
     * @param _url The url pointing to the server which contains the database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws IllegalArgumentException If the given file is a directory.
     * @throws DatabaseLoadingException If a Sql exception exception occurs.
     */
    public HSQLDBWrapper(URL _url) throws IllegalArgumentException, DatabaseException
    {
	if (_url==null)
	    throw new NullPointerException("The parameter _url is a null pointer !");
	ensureHSQLLoading();
	try
	{
	    database_name="Database from URL : "+_url.getPath();
	    sql_connection=DriverManager.getConnection("jdbc:hsqldb:"+_url.getPath()+";hsqldb.cache_rows=0;shutdown=true", "SA", "");
	    sql_connection.setAutoCommit(false);
	    locker=getLocker();
	}
	catch (Exception e)
	{
	    throw new DatabaseLoadingException("Impossible to create the database into the file "+_url, e);
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
    
    private void close() throws DatabaseException
    {
	if (!closed)
	{
	    try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	    {
		Statement s=sql_connection.createStatement();
		s.executeQuery("SHUTDOWN");
		s.close();
		sql_connection.close();
	    }
	    catch(SQLException se)
	    {
		throw DatabaseException.getDatabaseException(se);
	    }
	    finally
	    {
		synchronized(HSQLDBWrapper.class)
		{
		    HSQLDBWrapper.lockers.remove(database_name);
		    int v=HSQLDBWrapper.number_of_shared_lockers.get(database_name).intValue()-1;
		    if (v==0)
			HSQLDBWrapper.number_of_shared_lockers.remove(database_name);
		    else if (v>0)
			HSQLDBWrapper.number_of_shared_lockers.put(database_name, new Integer(v));
		    else
			throw new IllegalAccessError();
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
		public Object run(HSQLDBWrapper _sql_connection) throws DatabaseException
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
     * Closes the database files, rewrites the script file, deletes the log file and opens the database.
     * @param _defrag If true is specified, this command also shrinks the .data file to its minimal size.
     * @throws DatabaseException if a SQL exception occurs.
     */
    public void checkPoint(boolean _defrag) throws DatabaseException
    {
	try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock())
	{
	    try
	    {
		Statement st=sql_connection.createStatement();
		st.execute("CHECKPOINT"+(_defrag?" DEFRAG":""));
		st.close();
	    }
	    catch(SQLException e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
}
