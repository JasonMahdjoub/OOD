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

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.ReadQuerry;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.ReadWriteLock;

/**
 * Sql connection wrapper for HSQLDB
 * @author Jason Mahdjoub
 * @version 1.1
 */
public class HSQLDBWrapper extends DatabaseWrapper
{
    private static boolean hsql_loaded=false;
    
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
	super(getConnection(_file_name, _cache_rows, _cache_size, _result_max_memory_rows, _cache_free_count), "Database from file : "+getHSQLDBDataFileName(_file_name)+".data");
    }
    
    private static Connection getConnection(File _file_name, int _cache_rows, int _cache_size, int _result_max_memory_rows, int _cache_free_count) throws DatabaseLoadingException
    {
	if (_file_name==null)
	    throw new NullPointerException("The parameter _file_name is a null pointer !");
	if (_file_name.isDirectory())
	    throw new IllegalArgumentException("The given file name is a directory !");
	ensureHSQLLoading();
	try
	{
	    Connection c=DriverManager.getConnection("jdbc:hsqldb:file:"+getHSQLDBDataFileName(_file_name)+";hsqldb.cache_rows="+_cache_rows+";hsqldb.cache_size="+_cache_size+";hsqldb.result_max_memory_rows="+_result_max_memory_rows+";hsqldb.cache_free_count="+_cache_free_count+";shutdown=true", "SA", "");
	    c.setAutoCommit(false);
	    return c;
	}
	catch(Exception e)
	{
	    throw new DatabaseLoadingException("Impossible to create the database into the file "+_file_name, e);
	}
    }
    private static Connection getConnection(URL _url) throws DatabaseLoadingException
    {
	if (_url==null)
	    throw new NullPointerException("The parameter _url is a null pointer !");
	ensureHSQLLoading();
	try
	{
	    Connection c=DriverManager.getConnection("jdbc:hsqldb:"+_url.getPath()+";hsqldb.cache_rows=0;shutdown=true", "SA", "");
	    c.setAutoCommit(false);
	    return c;
	}
	catch(Exception e)
	{
	    throw new DatabaseLoadingException("Impossible to create the database into the file "+_url, e);
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
	f=new File(file_base+".lobs");
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
	super(getConnection(_url), "Database from URL : "+_url.getPath());
    }
    
    @Override
    protected void closeConnection() throws SQLException
    {
	Statement s=sql_connection.createStatement();
	s.executeQuery("SHUTDOWN");
	s.close();
	sql_connection.close();
    }
    
    @Override
    protected String getCachedKeyword()
    {
	return "CACHED";
    }
    
    @Override
    boolean doesTableExists(String table_name) throws Exception
    {
	try (ReadQuerry rq=new ReadQuerry(getSqlConnection(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+table_name+"'"))
	{
	    if (rq.result_set.next())
		return true;
	}
	return false;
	
    }
    
    @Override
    ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception
    {
	return new CReadQuerry(this.sql_connection, "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+tableName+"';");
    }
    
    @Override
    void checkConstraints(Table<?> table) throws DatabaseException
    {
	try(ReadQuerry rq=new ReadQuerry(sql_connection, "select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"+table.getName()+"';"))
	{
	    while (rq.result_set.next())
	    {
		String constraint_name=rq.result_set.getString("CONSTRAINT_NAME");
		String constraint_type=rq.result_set.getString("CONSTRAINT_TYPE");
		switch(constraint_type)
		{
		    case "PRIMARY KEY":
		    {
			if (!constraint_name.equals(table.getSqlPrimaryKeyName()))
			    throw new DatabaseVersionException(table, "There a grouped primary key named "+constraint_name+" which should be named "+table.getSqlPrimaryKeyName());
		    }
		    break;
		    case "FOREIGN KEY":
		    {
		    
		    }	
		    break;
		    case "UNIQUE":
		    {
			try(ReadQuerry rq2=new ReadQuerry(sql_connection, "select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"+table.getName()+"' AND CONSTRAINT_NAME='"+constraint_name+"';"))
			{
			    if (rq2.result_set.next())
			    {
				String col=(table.getName()+"."+rq2.result_set.getString("COLUMN_NAME")).toUpperCase();
				boolean found=false;
				for (FieldAccessor fa : table.getFieldAccessors())
				{
				    for (SqlField sf : fa.getDeclaredSqlFields())
				    {
					if (sf.field.equals(col) && fa.isUnique())
					{
					    found=true;
					    break;
					}
				    }
				    if (found)
					break;
				}
				if (!found)
				    throw new DatabaseVersionException(table, "There is a unique sql field "+col+" which does not exists into the OOD database.");
			    }
			}
			
		    }
		    break;
		    case "CHECK":
			break;
		    default :
			throw new DatabaseVersionException(table, "Unknow constraint "+constraint_type);
		}
	    }
	}
	catch(SQLException e)
	{
	    throw new DatabaseException("Impossible to check constraints of the table "+table.getName(), e);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	try(ReadQuerry rq=new ReadQuerry(sql_connection, "select PKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE WHERE FKTABLE_NAME='"+table.getName()+"';"))
	{
	    while (rq.result_set.next())
	    {
		String pointed_table=rq.result_set.getString("PKTABLE_NAME");
		String pointed_col=pointed_table+"."+rq.result_set.getString("PKCOLUMN_NAME");
		String fk=table.getName()+"."+rq.result_set.getString("FKCOLUMN_NAME");
		boolean found=false;
		for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors())
		{
		    for (SqlField sf : fa.getDeclaredSqlFields())
		    {
			if (sf.field.equals(fk) && sf.pointed_field.equals(pointed_col) && sf.pointed_table.equals(pointed_table))
			{
			    found=true;
			    break;
			}
		    }
		    if (found)
			break;
		}
		if (!found)
		    throw new DatabaseVersionException(table, "There is foreign keys defined into the Sql database which have not been found in the OOD database.");
	    }
	}
	catch(SQLException e)
	{
	    throw new DatabaseException("Impossible to check constraints of the table "+table.getName(), e);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	try
	{
	    Pattern col_size_matcher=Pattern.compile("([0-9]+)");
	    for (FieldAccessor fa : table.getFieldAccessors())
	    {
		for (SqlField sf : fa.getDeclaredSqlFields())
		{
		    /*System.out.println("SF : "+sf.short_field);
		    System.out.println("SF : "+table.getName());
		    try(ReadQuerry rq=new ReadQuerry(sql_connection, "SELECT TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, ORDINAL_POSITION, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS"+getSqlComma()))
		    {
			while (rq.result_set.next())
			{
			    System.out.println("\t"+rq.result_set.getString("TABLE_NAME"));
			}
		    }*/
		    try(ReadQuerry rq=new ReadQuerry(sql_connection, "SELECT TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, ORDINAL_POSITION, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+table.getName()+"' AND COLUMN_NAME='"+sf.short_field+"'"+getSqlComma()))
		    {
			if (rq.result_set.next())
			{
			    String type=rq.result_set.getString("TYPE_NAME").toUpperCase();
			    if (!sf.type.toUpperCase().startsWith(type))
				throw new DatabaseVersionException(table, "The type of the field "+sf.field+" should  be "+sf.type+" and not "+type);
			    if (col_size_matcher.matcher(sf.type).matches())
			    {
				int col_size=rq.result_set.getInt("COLUMN_SIZE");
				Pattern pattern2=Pattern.compile("("+col_size+")");
				if (!pattern2.matcher(sf.type).matches())
				    throw new DatabaseVersionException(table, "The column "+sf.field+" has a size equals to "+col_size+" (expected "+sf.type+")");
			    }
			    boolean is_null=rq.result_set.getString("IS_NULLABLE").equals("YES");
			    if (is_null==fa.isNotNull())
				throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is expected to be "+(fa.isNotNull()?"not null":"nullable"));
			    boolean is_autoincrement=rq.result_set.getString("IS_AUTOINCREMENT").equals("YES");
			    if (is_autoincrement!=fa.isAutoPrimaryKey())
				throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is "+(is_autoincrement?"":"not ")+"autoincremented into the Sql database where it is "+(is_autoincrement?"not ":"")+" into the OOD database.");
			    sf.sql_position=rq.result_set.getInt("ORDINAL_POSITION");
			}
			else
			    throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" was not found into the database.");
		    }
		    if (fa.isPrimaryKey())
		    {
			try(ReadQuerry rq=new ReadQuerry(sql_connection, "select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"+table.getName()+"' AND COLUMN_NAME='"+sf.short_field+"' AND CONSTRAINT_NAME='"+table.getSqlPrimaryKeyName()+"';"))
			{	
			    if (!rq.result_set.next())
				throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is not declared as a primary key into the Sql database.");
			}
		    }
		    if (fa.isForeignKey())
		    {
			try(ReadQuerry rq=new ReadQuerry(sql_connection, "select PKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE WHERE FKTABLE_NAME='"+table.getName()+"' AND PKCOLUMN_NAME='"+sf.short_pointed_field+"' AND FKCOLUMN_NAME='"+sf.short_field+"';"))
			{
			    if (!rq.result_set.next())
				throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is a foreign key one of its Sql fields "+sf.field+" is not a foreign key pointing to the table "+sf.pointed_table);
			}
		    }
		    if (fa.isUnique())
		    {
			boolean found=false;
			try(ReadQuerry rq=new ReadQuerry(sql_connection, "select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"+table.getName()+"';"))
			{
			    while (rq.result_set.next())
			    {
				if (rq.result_set.getString("CONSTRAINT_TYPE").equals("UNIQUE"))
				{
				    String constraint_name=rq.result_set.getString("CONSTRAINT_NAME");
				    try(ReadQuerry rq2=new ReadQuerry(sql_connection, "select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"+table.getName()+"' AND CONSTRAINT_NAME='"+constraint_name+"';"))
				    {
					if (rq2.result_set.next())
					{	
					    String col=table.getName()+"."+rq2.result_set.getString("COLUMN_NAME");
					    if (col.equals(sf.field))
					    {
						found=true;
						break;
					    }
					}
				    }
				}
			    }
			}
			if (!found)
			    throw new DatabaseVersionException(table, "The OOD field "+fa.getFieldName()+" is a unique key, but it not declared as unique into the Sql database.");
		    }
		}
	    }
	}
	catch(SQLException e)
	{
	    throw new DatabaseException("Impossible to check constraints of the table "+table.getName(), e);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    static class CReadQuerry extends ColumnsReadQuerry
    {

	public CReadQuerry(Connection _sql_connection, String _querry) throws SQLException
	{
	    super(_sql_connection, _querry);
	    setTableColumnsResultSet(new TCResultSet(this.result_set));
	}
	
    }
    
    
    static class TCResultSet extends TableColumnsResultSet
    {

	TCResultSet(ResultSet _rs)
	{
	    super(_rs);
	}

	@Override
	public String getColumnName() throws SQLException
	{
	    return resultSet.getString("COLUMN_NAME");
	}

	@Override
	public String getTypeName() throws SQLException
	{
	    return resultSet.getString("TYPE_NAME");
	}

	@Override
	public int getColumnSize() throws SQLException
	{
	    return resultSet.getInt("COLUMN_SIZE");
	}

	@Override
	public boolean isNullable() throws SQLException
	{
	    return resultSet.getString("IS_NULLABLE").equals("YES");
	}

	@Override
	public boolean isAutoIncrement() throws SQLException
	{
	    return resultSet.getString("IS_AUTOINCREMENT").equals("YES");
	}
	
    }
    
    @Override
    String getSqlComma()
    {
	return ";";
    }
    @Override
    public int getVarCharLimit()
    {
	return 16777216;
    }
    @Override
    public boolean isVarBinarySupported()
    {
	return true;
    }
    @Override
    public String getByteType()
    {
	return "TINYINT";
    }
    @Override
    public String getIntType()
    {
	return "INTEGER";
    }
    
    @Override
    public String getFloatType()
    {
	return "DOUBLE";
    }
    @Override
    public String getDoubleType()
    {
	return "DOUBLE";
    }
    @Override
    public String getLongType()
    {
	return "BIGINT";
    }
    @Override
    public String getShortType()
    {
	return "SMALLINT";
    }
    @Override
    public String getBigDecimalType()
    {
	return "VARCHAR(16374)";
    }
    @Override
    public String getBigIntegerType()
    {
	return "VARCHAR(16374)";
    }
    

    @Override
    String getSqlNULL()
    {
	return "NULL";
    }
    @Override
    String getSqlNotNULL()
    {
	return "NOT NULL";
    }
    @Override
    public String getSerializableType()
    {
	return "BLOB";
    }
    @Override
    String getSqlQuerryToGetLastGeneratedID()
    {
	return "CALL IDENTITY()";
    }
    @Override
    String getOnUpdateCascadeSqlQuerry()
    {
	return "ON UPDATE CASCADE";
    }
    @Override
    String getOnDeleteCascadeSqlQuerry()
    {
	return "ON DELETE CASCADE";
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
	    Statement st=null;
	    try
	    {
		st=sql_connection.createStatement();
		st.execute("CHECKPOINT"+(_defrag?" DEFRAG":""));
	    }
	    catch(SQLException e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    finally
	    {
		try
		{
		    st.close();
		}
		catch(SQLException e)
		{
		    throw DatabaseException.getDatabaseException(e);
		}
		
	    }
	}
    }

}
