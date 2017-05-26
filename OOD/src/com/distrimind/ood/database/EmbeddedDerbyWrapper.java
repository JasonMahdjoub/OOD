
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
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Savepoint;
import java.util.regex.Pattern;

import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.ReadQuerry;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.FileTools;

/**
 * Sql connection wrapper for Derby DB
 * 
 * @author Jason Mahdjoub
 * @version 1.3
 * @since OOD 1.4
 */
public class EmbeddedDerbyWrapper extends DatabaseWrapper
{
    private static boolean derby_loaded = false;
    private final String dbURL;
    private final File fileDirectory;
    private static void ensureDerbyLoading() throws DatabaseLoadingException
    {
	synchronized (EmbeddedDerbyWrapper.class)
	{
	    if (!derby_loaded)
	    {
		try
		{
		    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
			    .newInstance();
		    derby_loaded = true;
		}
		catch (Exception e)
		{
		    throw new DatabaseLoadingException(
			    "Impossible to load Derby ", e);
		}
	    }
	}
    }

    /**
     * Constructor
     * 
     * @param _directory
     *            the database directory
     * @throws NullPointerException
     *             if parameters are null pointers.
     * @throws IllegalArgumentException
     *             If the given _directory is not a directory.
     * @throws DatabaseException 
     */
    public EmbeddedDerbyWrapper(File _directory) throws IllegalArgumentException, DatabaseException
    {
	super(/*getConnection(_directory), */"Database from file : " + _directory.getAbsolutePath());
	dbURL = getDBUrl(_directory);
	fileDirectory=_directory;
    }

    @Override
    public boolean supportTransactions()
    {
	return false;
	
    }
    
    @Override
    protected String getDropTableIfExistsKeyWord()
    {
	return "";
    }
    
    @Override
    protected String getDropTableCascadeKeyWord()
    {
	return "";
    }

    private static String getDBUrl(File _directory)
    {
	return "jdbc:derby:" + _directory.getAbsolutePath();
    }

    private static Connection getConnection(File _directory) throws DatabaseLoadingException
    {
	if (_directory == null)
	    throw new NullPointerException(
		    "The parameter _file_name is a null pointer !");
	if (_directory.exists() && !_directory.isDirectory())
	    throw new IllegalArgumentException(
		    "The given file name must be directory !");
	System.gc();
	ensureDerbyLoading();
	try
	{
	    Connection c = DriverManager
		    .getConnection(getDBUrl(_directory) + ";create=true");

	    c.setAutoCommit(false);
	    return c;
	}
	catch (Exception e)
	{
	    throw new DatabaseLoadingException(
		    "Impossible to create the database "
			    + _directory.getAbsolutePath()
			    + " into the directory "
			    + _directory.getAbsolutePath(),
		    e);
	}
    }

    @Override
    protected void closeConnection(Connection connection) throws SQLException
    {
	try
	{
	    DriverManager.getConnection(dbURL + ";shutdown=true");
	}
	catch (SQLNonTransientConnectionException e)
	{

	}
	finally
	{
	    connection.close();
	}
    }

    @Override
    protected String getCachedKeyword()
    {
	return "";
    }

    /**
     * Delete all the files associated to the database directory.
     * 
     * @param _directory
     *            the database directory
     */
    public static void deleteDatabaseFiles(File _directory)
    {
	if (_directory.exists() && _directory.isDirectory())
	{
	    FileTools.deleteDirectory(_directory);
	}
    }

    @Override
    protected boolean doesTableExists(String tableName) throws Exception
    {
	Connection sql_connection=getConnectionAssociatedWithCurrentThread();
	DatabaseMetaData dbmd = sql_connection.getMetaData();
	ResultSet rs = dbmd.getTables(null, null, null, null);
	while (rs.next())
	{
	    if (rs.getString("TABLE_NAME").equals(tableName))
		return true;
	}
	return false;
    }

    @Override
    protected ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception
    {
	Connection sql_connection=getConnectionAssociatedWithCurrentThread();
	return new CReadQuerry(sql_connection, sql_connection.getMetaData()
		.getColumns(null, null, tableName, null));
    }

    @Override
    protected void checkConstraints(Table<?> table) throws DatabaseException
    {
	Connection sql_connection=getConnectionAssociatedWithCurrentThread();
	try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getPrimaryKeys(null, null, table.getName())))
	{
	    while (rq.result_set.next())
	    {
		String pkName=rq.result_set.getString("PK_NAME");
		if (pkName!=null && !pkName.equals(table.getSqlPrimaryKeyName()))
		    throw new DatabaseVersionException(table, "There a grouped primary key named "+rq.result_set.getString("PK_NAME")+" which should be named "+table.getSqlPrimaryKeyName());
	    }
	    try(ReadQuerry rq2=new ReadQuerry(sql_connection, sql_connection.getMetaData().getIndexInfo(null, null, table.getName(), false, false)))
	    {
		while(rq2.result_set.next())
		{
		    String colName=rq2.result_set.getString("COLUMN_NAME");
		    if (colName!=null)
		    {
			boolean found=false;
			for (FieldAccessor fa : table.getFieldAccessors())
			{
			    for (SqlField sf : fa.getDeclaredSqlFields())
			    {
				if (sf.short_field.equals(colName))
				{
				    if (fa.isUnique()==rq2.result_set.getBoolean("NON_UNIQUE") && fa.isPrimaryKey()==rq2.result_set.getBoolean("NON_UNIQUE"))
				    {
					throw new DatabaseVersionException(table, "The field "+colName+" has not the same unique constraint into the OOD database.");					
				    }
				    found=true;
				    break;
				}
			    }
			    if (found)
				break;
			}
			if (!found)
			    throw new DatabaseVersionException(table, "There is a unique sql field "+colName+" which does not exists into the OOD database.");
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
	for (Table<?> t : getListTables(table.getClass().getPackage()))
	{
	    try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getExportedKeys(null, null, t.getName())))
	    {
		while (rq.result_set.next())
		{
		    String fk_table_name=rq.result_set.getString("FKTABLE_NAME");
		    if (fk_table_name.equals(table.getName()))
		    {
			String pointed_table=rq.result_set.getString("PKTABLE_NAME");
			String pointed_col=pointed_table+"."+rq.result_set.getString("PKCOLUMN_NAME");
			String fk=rq.result_set.getString("FKCOLUMN_NAME");
			boolean found=false;
			for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors())
			{
		    
			    for (SqlField sf : fa.getDeclaredSqlFields())
			    {
			
				if (sf.short_field.equals(fk) && sf.pointed_field.equals(pointed_col) && sf.pointed_table.equals(pointed_table))
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
	try
	{
	    Pattern col_size_matcher=Pattern.compile("([0-9]+)");
	    for (FieldAccessor fa : table.getFieldAccessors())
	    {
		for (SqlField sf : fa.getDeclaredSqlFields())
		{
		    
		    try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getColumns(null, null, table.getName(), sf.short_field)))
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
			try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getPrimaryKeys(null, null, table.getName())))
			{
			    boolean found=false;
			    while (rq.result_set.next())
			    {
				if (rq.result_set.getString("COLUMN_NAME").equals(sf.short_field) && rq.result_set.getString("PK_NAME").equals(table.getSqlPrimaryKeyName()))
				    found=true;
			    }
			    if (!found)
				throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is not declared as a primary key into the Sql database.");
			}
		    }
		    if (fa.isForeignKey())
		    {
			boolean found=false;
			for (Table<?> t : getListTables(table.getClass().getPackage()))
			{
			    try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getExportedKeys(null, null, t.getName())))
			    {
				while (rq.result_set.next())
				{
				    String fk_table_name=rq.result_set.getString("FKTABLE_NAME");
				    if (fk_table_name.equals(table.getName()))
				    {
					 if (rq.result_set.getString("FKCOLUMN_NAME").equals(sf.short_field))
					     found=true;
				    }
				    if (found)
					break;
				}
			    }
			    if (found)
				break;
			}
			if (!found)
			    throw new DatabaseVersionException(table, "The field "+fa.getFieldName()+" is a foreign key one of its Sql fields "+sf.field+" is not a foreign key pointing to the table "+sf.pointed_table);
			
		    }
		    if (fa.isUnique())
		    {
			boolean found=false;
			try(ReadQuerry rq=new ReadQuerry(sql_connection, sql_connection.getMetaData().getIndexInfo(null, null, table.getName(), false, false)))
			{
			    while (rq.result_set.next())
			    {
				String columnName=rq.result_set.getString("COLUMN_NAME");
				if (columnName.equals(sf.short_field))
				{
				    if (!rq.result_set.getBoolean("NON_UNIQUE"))
					found=true;
				    break;
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

	public CReadQuerry(Connection _sql_connection, ResultSet resultSet)
	{
	    super(_sql_connection, resultSet);
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
    protected String getSqlComma()
    {
	return "";
    }

    @Override
    protected int getVarCharLimit()
    {
	return 32672;
    }
    @Override
    protected boolean isVarBinarySupported()
    {
	return false;
    }
    
    @Override
    protected String getSqlNULL()
    {
	return "";
    }
    @Override
    protected String getSqlNotNULL()
    {
	return "NOT NULL";
    }

    @Override
    protected String getByteType()
    {
	return "INTEGER";
    }

    @Override
    protected String getIntType()
    {
	return "INTEGER";
    }

    @Override
    protected String getSerializableType()
    {
	return "BLOB";
    }

    @Override
    protected String getFloatType()
    {
	return "REAL";
    }
    @Override
    protected String getDoubleType()
    {
	return "DOUBLE";
    }

    @Override
    protected String getLongType()
    {
	return "BIGINT";
    }
    @Override
    protected String getShortType()
    {
	return "SMALLINT";
    }
    @Override
    protected String getBigDecimalType()
    {
	return "VARCHAR(16374)";
    }
    @Override
    protected String getBigIntegerType()
    {
	return "VARCHAR(16374)";
    }
    @Override
    protected String getSqlQuerryToGetLastGeneratedID()
    {
	return "values IDENTITY_VAL_LOCAL()";
    }
    @Override
    protected String getOnUpdateCascadeSqlQuerry()
    {
	return "";
    }
    @Override
    protected String getOnDeleteCascadeSqlQuerry()
    {
	return "ON DELETE CASCADE";
    }

    @Override
    protected Blob getBlob(byte[] _bytes)
    {
	return null;
    }

    /**
     * Backup the database into the given directory. 
     * @param directory the directory where to save the database.  
     * @throws DatabaseException if a problem occurs
     */
    @Override
    public void backup(File directory) throws DatabaseException
    {
	if (directory==null)
	    throw new NullPointerException("file");
	
	if (directory.exists())
	{
	    if (!directory.isDirectory())
		throw new IllegalArgumentException("The given path ("+directory.getAbsolutePath()+") must be a directory !");
	}
	final String f=directory.getAbsolutePath();
	
	final String querry="CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)";
	    this.runTransaction(new Transaction() {
	    
		@Override
		public TransactionIsolation getTransactionIsolation()
		{
		    return TransactionIsolation.TRANSACTION_READ_COMMITTED;
		}
		
		@Override
		public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		{
		    try
		    {
			locker.lockRead();
			Connection sql_connection=getConnectionAssociatedWithCurrentThread();
			PreparedStatement preparedStatement = sql_connection.prepareStatement(querry);
			try
			{
			    preparedStatement.setString(1, f);
			    preparedStatement.execute();
			}
			finally
			{
			    preparedStatement.close();
			}
			return null;
		    }
		    catch(Exception e)
		    {
			throw new DatabaseException("",e);
		    }
		    finally
		    {
			    locker.unlockRead();
		    }
		}
	    
		@Override
		public boolean doesWriteData()
		{
		    return false;
		}
	    });
    }

    @Override
    protected Connection reopenConnectionImpl() throws DatabaseLoadingException
    {
	return getConnection(fileDirectory);
    }

    @Override
    protected void rollback(Connection openedConnection) throws SQLException
    {
	    openedConnection.rollback();
    }

    @Override
    protected void commit(Connection openedConnection) throws SQLException, DatabaseException
    {
	getConnectionAssociatedWithCurrentThread().commit();
    }

    @Override
    protected boolean supportSavePoint(Connection openedConnection) 
    {
	return true;
    }

    @Override
    protected void rollback(Connection openedConnection, String _savePointName, Savepoint savepoint) throws SQLException
    {
	openedConnection.rollback(savepoint);

    }

    @Override
    protected void disableAutoCommit(Connection openedConnection) throws SQLException
    {
	openedConnection.setAutoCommit(false);
    }

    @Override
    protected Savepoint savePoint(Connection _openedConnection, String _savePoint) throws SQLException
    {
	return _openedConnection.setSavepoint(_savePoint);
    }
    
    @Override
    protected boolean isThreadSafe()
    {
	return false;
    }

    @Override
    protected void releasePoint(Connection _openedConnection, String _savePointName, Savepoint savepoint) 
    {
	//_openedConnection.releaseSavepoint(savepoint);
	
    }
    
}
