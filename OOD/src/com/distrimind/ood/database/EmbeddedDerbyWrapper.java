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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
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
 * @version 1.0
 */
public class EmbeddedDerbyWrapper extends DatabaseWrapper
{
    private static boolean derby_loaded = false;

    private final String dbURL;

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
     * @throws DatabaseLoadingException
     *             If a Sql exception exception occurs.
     */
    public EmbeddedDerbyWrapper(File _directory) throws IllegalArgumentException, DatabaseLoadingException
    {
	super(getConnection(_directory), "Database from file : " + _directory.getAbsolutePath());
	dbURL = getDBUrl(_directory);
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
    protected void closeConnection() throws SQLException
    {
	try
	{
	    DriverManager.getConnection(dbURL + ";shutdown=true");
	}
	catch (SQLNonTransientConnectionException e)
	{

	}
	sql_connection.close();
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
    boolean doesTableExists(String tableName) throws Exception
    {
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
    ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception
    {
	return new CReadQuerry(this.sql_connection, sql_connection.getMetaData()
		.getColumns(null, null, tableName, null));
    }

    @Override
    void checkConstraints(Table<?> table) throws DatabaseException
    {
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
    String getSqlComma()
    {
	return "";
    }

    @Override
    public int getVarCharLimit()
    {
	return 32672;
    }
    @Override
    public boolean isVarBinarySupported()
    {
	return false;
    }
    
    @Override
    String getSqlNULL()
    {
	return "";
    }
    @Override
    String getSqlNotNULL()
    {
	return "NOT NULL";
    }

    @Override
    public String getByteType()
    {
	return "INTEGER";
    }

    @Override
    public String getIntType()
    {
	return "INTEGER";
    }

    @Override
    public String getSerializableType()
    {
	return "BLOB";
    }

    @Override
    public String getFloatType()
    {
	return "REAL";
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
    String getSqlQuerryToGetLastGeneratedID()
    {
	return "values IDENTITY_VAL_LOCAL()";
    }
    @Override
    String getOnUpdateCascadeSqlQuerry()
    {
	return "";
    }
    @Override
    String getOnDeleteCascadeSqlQuerry()
    {
	return "ON DELETE CASCADE";
    }
    
}
