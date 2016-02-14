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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.distrimind.ood.database.DatabaseWrapper.TableColumnsResultSet;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.LoadToMemory;
import com.distrimind.ood.database.exceptions.ConcurentTransactionDatabaseException;
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.ood.database.exceptions.RecordNotFoundDatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.ListClasses;
import com.distrimind.util.ReadWriteLock;



/**
 * This abstract class represent a generic Sql Table wrapper, which enables the user to do every Sql operation without any SQL query. 
 * To create a table into the database, the user must inherit this class. Every table of the same database must be grouped into the same package. 
 * When the table class is created, the user must include a public inner class named Record which must inherit the DatabaseRecord class. The type &lsaquo;T&rsaquo; of the table corresponds to this inner class. 
 * This inner class corresponds to a record into the corresponding table, with its declared fields. This fields can be native java types, or there corresponding classes (for example : Integer for the native type int). They can be BigInteger or BigDecimal. They can be DatabaseRecord references, for foreign keys. String class, arrays of bytes are also accepted.  
 * Every field which must be included into the database must have an annotation ({@link com.distrimind.ood.database.annotations.Field}, {@link com.distrimind.ood.database.annotations.PrimaryKey}, {@link com.distrimind.ood.database.annotations.AutoPrimaryKey}, {@link com.distrimind.ood.database.annotations.RandomPrimaryKey}, {@link com.distrimind.ood.database.annotations.NotNull}, {@link com.distrimind.ood.database.annotations.Unique}, {@link com.distrimind.ood.database.annotations.ForeignKey}).
 * If no annotation is given, the corresponding field will not be added into the database.
 * Note that the native types are always NotNull. 
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.AutoPrimaryKey} must be int or short values.
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.RandomPrimaryKey} must be long values.
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.ForeignKey} must be DatabaseRecord instances. 
 * 
 * It is possible also to add the annotation {@link com.distrimind.ood.database.annotations.LoadToMemory} just before the table class declaration. If this annotation is present, the content of the table will loaded into the memory which will speed up queries. But note that every table pointed throw a foreign key in this table must have the same annotation. An exception will be generated, during the class instantiation, if this condition is not respected.
 * The user must be careful to not generate a problem of circularity with the declared foreign keys between every database table. An exception is generated during the table instantiation if this problem occurs.
 * 
 * To get the unique instance of its table, the user must call the static functions {@link DatabaseWrapper#getTableInstance(Class)} or {@link DatabaseWrapper#getTableInstance(String)}. The user must never call the default constructor of the class. This constructor must be protected.
 * Before getting any table instance, the user must associate the package containing the class tables of the same database to a Sql database throw the function {@link DatabaseWrapper#loadDatabase(Package)}.
 * 
 * This class is thread safe
 * 
 * @author Jason Mahdjoub
 * @version 1.1
 * @param <T> the type of the record
 */
public abstract class Table<T extends DatabaseRecord>
{
    private final Class<T> class_record;
    private final Constructor<T> default_constructor_field;
    private final ArrayList<FieldAccessor> auto_random_primary_keys_fields=new ArrayList<FieldAccessor>();
    private final ArrayList<FieldAccessor> auto_primary_keys_fields=new ArrayList<FieldAccessor>();
    private final ArrayList<FieldAccessor> primary_keys_fields_no_auto_no_random=new ArrayList<FieldAccessor>();
    private final ArrayList<FieldAccessor> primary_keys_fields=new ArrayList<FieldAccessor>();
    private final ArrayList<FieldAccessor> unique_fields_no_auto_random_primary_keys=new ArrayList<FieldAccessor>();
    final ArrayList<ForeignKeyFieldAccessor> foreign_keys_fields=new ArrayList<ForeignKeyFieldAccessor>();
    private ArrayList<FieldAccessor> fields;
    private final ArrayList<FieldAccessor> fields_without_primary_and_foreign_keys=new ArrayList<FieldAccessor>();
    private AtomicReference<ArrayList<T>> records_instances=new AtomicReference<ArrayList<T>>(new ArrayList<T>());
    private final boolean is_loaded_in_memory;
    private final String table_name;
    
    
    private static class NeighboringTable
    {
	public final DatabaseWrapper sql_connection;
	public final Class<? extends Table<?>> class_table;
	public final ArrayList<Field> concerned_fields;
	private Table<?> t=null;
	private final Class<? extends DatabaseRecord> class_record;
	
	public NeighboringTable(DatabaseWrapper _sql_connection, Class<? extends DatabaseRecord> _class_record, Class<? extends Table<?>> _class_table, ArrayList<Field> _concerned_fields)
	{
	    sql_connection=_sql_connection;
	    class_table=_class_table;
	    concerned_fields=_concerned_fields;
	    class_record=_class_record;
	}
	public HashMap<String, Object> getHashMapFields(Object _instance)
	{
	    HashMap<String, Object> res=new HashMap<String, Object>();
	    for (Field f : concerned_fields)
	    {
		res.put(f.getName(), _instance);
	    }
	    return res;
	}
	
	public Table<?> getPointedTable() throws DatabaseException
	{
	    if (t==null)
		t=sql_connection.getTableInstance(class_table);
	    return t;
	}
	
	public HashMap<String, Object>[] getHashMapsSqlFields(HashMap<String, Object> _primary_keys) throws DatabaseException
	{
		Table<?> t=getPointedTable();

		
		@SuppressWarnings("unchecked")
		HashMap<String, Object> res[]=new HashMap[concerned_fields.size()];
		int index=0;
		for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields)
		{
		    if (fkfa.isAssignableTo(class_record))
		    {
			res[index]=new HashMap<String, Object>();
			for (SqlField sf : fkfa.getDeclaredSqlFields())
			{
			    boolean found=false;
			    for (String field : _primary_keys.keySet())
			    {
				if (field.equals(sf.pointed_field))
				{
				    found=true;
				    res[index].put(sf.field, _primary_keys.get(field));
				    break;
				}
			    }
			    if (!found)
				throw new DatabaseException("Unexpected exception");
			}
			index++;
		    }
		}
		return res;
	}
    }
    
    final ArrayList<NeighboringTable> list_tables_pointing_to_this_table=new ArrayList<NeighboringTable>();
    
    private final SecureRandom rand;
    
    private AtomicBoolean is_synchronized_with_sql_database=new AtomicBoolean(false);
    
    DatabaseWrapper sql_connection;
    
    @SuppressWarnings("rawtypes")
    private final Constructor<GroupedResults> grouped_results_constructor;
    
    
    
    String getSqlPrimaryKeyName()
    {
	return this.getName()+"__PK";
    }
    
    /**
     * This constructor must never be called. Please use the static functions {@link DatabaseWrapper#getTableInstance(Class)} or {@link DatabaseWrapper#getTableInstance(String)}.
     * @throws DatabaseException is database constraints are not respected or if a problem of database version occured during the Sql loading (typically, when the user have modified the fields of its database). 
     */
    @SuppressWarnings("rawtypes")
    protected Table() throws DatabaseException
    {
	table_name=getName(this.getClass());
	try
	{
	    rand=SecureRandom.getInstance("SHA1PRNG", "SUN");
	    rand.nextBytes(new byte[10]);
	}
	catch(NoSuchAlgorithmException | NoSuchProviderException e)
	{
	    throw new DatabaseException("Impossible to initilize a secured random instance.");
	}

	is_loaded_in_memory=this.getClass().isAnnotationPresent(LoadToMemory.class);
	
	if (!Modifier.isFinal(this.getClass().getModifiers()))
	{
	    throw new DatabaseException("The table class "+this.getClass().getName()+" must be a final class.");
	}
	
	boolean constructor_ok=true;
	Constructor<?> constructors[]=this.getClass().getDeclaredConstructors();
	if (constructors.length!=1)
	    constructor_ok=false;
	else
	{
	    if (!Modifier.isProtected(constructors[0].getModifiers()))
		constructor_ok=false;
	    else
		if (constructors[0].getParameterTypes().length!=0)
		    constructor_ok=false;
	}
	if (!constructor_ok)
	    throw new DatabaseException("The class "+this.getClass().getName()+" must have only one constructor which must be declared as protected without any parameter (default constructor)");
	
	@SuppressWarnings("unchecked")
	Class<T> tmp=(Class<T>)Table.getDatabaseRecord((Class<? extends Table<?>>)this.getClass());
	class_record=tmp;
	
	DefaultConstructorAccessPrivilegedAction<T> capa=new DefaultConstructorAccessPrivilegedAction<T>(class_record);
	
	try
	{
	    default_constructor_field=AccessController.doPrivileged(capa);
	}
	catch (PrivilegedActionException e1)
	{
	    throw new DatabaseException("Impossible to find the default constructor of the class "+class_record.getName(), e1);
	}
	    grouped_results_constructor=AccessController.doPrivileged(new PrivilegedAction<Constructor<GroupedResults>>() {

		@Override
		public Constructor<GroupedResults> run()
		{
		    Constructor<GroupedResults> res;
		    try
		    {
			res = (Constructor<GroupedResults>)GroupedResults.class.getDeclaredConstructor(DatabaseWrapper.class, Collection.class, Class.class, (new String[1]).getClass());
			res.setAccessible(true);
			return res;
		    }
		    catch (NoSuchMethodException | SecurityException e)
		    {
			e.printStackTrace();
			System.exit(-1);
			return null;
		    }
		}
	    });
	
	/*sql_connection=current_sql_connection;
	if (sql_connection==null)
	    throw new DatabaseException("The database associated to the package "+this.getClass().getPackage().getName()+" was not loaded. Impossible to instantiate the class/table "+this.getClass().getName()+". Please use the function associatePackageToSqlJetDatabase before !");
	
	
	@SuppressWarnings("unchecked")
	Class<? extends Table<?>> table_class=(Class<? extends Table<?>>)this.getClass();
	fields=FieldAccessor.getFields(sql_connection, table_class);
	if (fields.size()==0)
	    throw new DatabaseException("No field has been declared in the class "+class_record.getName());
	for (FieldAccessor f : fields)
	{
	    if (f.isPrimaryKey())
		primary_keys_fields.add(f);
	    if (f.isAutoPrimaryKey() || f.isRandomPrimaryKey())
		auto_random_primary_keys_fields.add(f);
	    if (f.isAutoPrimaryKey())
		auto_primary_keys_fields.add(f);
	    if (!f.isAutoPrimaryKey() && !f.isRandomPrimaryKey() && f.isPrimaryKey())
		primary_keys_fields_no_auto_no_random.add(f);
	    if (f.isForeignKey())
	    {
		foreign_keys_fields.add((ForeignKeyFieldAccessor)f);
	    }
	    if (!f.isPrimaryKey() && !f.isForeignKey())
		fields_without_primary_and_foreign_keys.add(f);
	    if (f.isUnique() && !f.isAutoPrimaryKey() && !f.isRandomPrimaryKey())
	    {
		unique_fields_no_auto_random_primary_keys.add(f);
	    }
	}
	for (FieldAccessor f : fields)
	{
	    for (FieldAccessor f2 : fields)
	    {
		if (f!=f2)
		{
		    if (f.getFieldName().equalsIgnoreCase(f2.getFieldName()))
		    {
			throw new DatabaseException("The fields "+f.getFieldName()+" and "+f2.getFieldName()+" have the same name considering that Sql fields is not case sensitive !");
		    }
		}
	    }
	}
	if (auto_primary_keys_fields.size()>1)
	    throw new DatabaseException("It can have only one autoincrement primary key with Annotation {@link oodforsqljet.annotations.AutoPrimaryKey}. The record "+class_record.getName()+" has "+auto_primary_keys_fields.size()+" AutoPrimary keys.");
	if (primary_keys_fields.size()==0)
	    throw new DatabaseException("There is no primary key declared into the Record "+class_record.getName());
	
	
	if (this.getName().equals(ROW_COUNT_TABLES))
	    throw new DatabaseException("This table cannot have the name "+ROW_COUNT_TABLES+" (case ignored)");*/
    }
    
    void initializeStep0(DatabaseWrapper wrapper) throws DatabaseException
    {
	sql_connection=wrapper;
	if (sql_connection==null)
	    throw new DatabaseException("No database was given to instanciate the class/table "+this.getClass().getName()+". Please use the function associatePackageToSqlJetDatabase before !");
	
	
	@SuppressWarnings("unchecked")
	Class<? extends Table<?>> table_class=(Class<? extends Table<?>>)this.getClass();
	fields=FieldAccessor.getFields(sql_connection, table_class);
	if (fields.size()==0)
	    throw new DatabaseException("No field has been declared in the class "+class_record.getName());
	for (FieldAccessor f : fields)
	{
	    if (f.isPrimaryKey())
		primary_keys_fields.add(f);
	    if (f.isAutoPrimaryKey() || f.isRandomPrimaryKey())
		auto_random_primary_keys_fields.add(f);
	    if (f.isAutoPrimaryKey())
		auto_primary_keys_fields.add(f);
	    if (!f.isAutoPrimaryKey() && !f.isRandomPrimaryKey() && f.isPrimaryKey())
		primary_keys_fields_no_auto_no_random.add(f);
	    if (f.isForeignKey())
	    {
		foreign_keys_fields.add((ForeignKeyFieldAccessor)f);
	    }
	    if (!f.isPrimaryKey() && !f.isForeignKey())
		fields_without_primary_and_foreign_keys.add(f);
	    if (f.isUnique() && !f.isAutoPrimaryKey() && !f.isRandomPrimaryKey())
	    {
		unique_fields_no_auto_random_primary_keys.add(f);
	    }
	}
	for (FieldAccessor f : fields)
	{
	    for (FieldAccessor f2 : fields)
	    {
		if (f!=f2)
		{
		    if (f.getFieldName().equalsIgnoreCase(f2.getFieldName()))
		    {
			throw new DatabaseException("The fields "+f.getFieldName()+" and "+f2.getFieldName()+" have the same name considering that Sql fields is not case sensitive !");
		    }
		}
	    }
	}
	if (auto_primary_keys_fields.size()>1)
	    throw new DatabaseException("It can have only one autoincrement primary key with Annotation {@link oodforsqljet.annotations.AutoPrimaryKey}. The record "+class_record.getName()+" has "+auto_primary_keys_fields.size()+" AutoPrimary keys.");
	if (primary_keys_fields.size()==0)
	    throw new DatabaseException("There is no primary key declared into the Record "+class_record.getName());
	
	
	if (this.getName().equals(DatabaseWrapper.ROW_COUNT_TABLES))
	    throw new DatabaseException("This table cannot have the name "+DatabaseWrapper.ROW_COUNT_TABLES+" (case ignored)");
    }
    
    void initializeStep1() throws DatabaseException
    {
	for (ForeignKeyFieldAccessor fa : foreign_keys_fields)
	{
	    fa.initialize();
	}
    }
    
    boolean foreign_keys_to_create=false;
    
    void initializeStep2() throws DatabaseException
    {
	/*
	 * Load table in Sql database
	 */
	
	try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableWriteLock())
	{
	    boolean table_found=((Boolean)sql_connection.runTransaction(new Transaction() {
		    
		public boolean doesWriteData()
		{
		    return false;
		}
		@Override
		public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException
		{
		    try
		    {
			return new Boolean(sql_connection.doesTableExists(Table.this.getName()));
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		}
	    })).booleanValue();

	    
	    if (table_found)
	    {
	    /*
	     * check the database 
	     */
		    sql_connection.runTransaction(new Transaction() {
		        
			public boolean doesWriteData()
			{
			    return false;
			}
			
		        @SuppressWarnings("synthetic-access")
			@Override
		        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		        {
		            try
		            {
		        	Pattern col_size_matcher=Pattern.compile("([0-9]+)");
				//try(ReadQuerry rq=new ReadQuerry(_sql_connection.getSqlConnection(), "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"+Table.this.getName()+"';"))
		        	try(ColumnsReadQuerry rq=sql_connection.getColumnMetaData(Table.this.getName()))
				{
		        	    //while (rq.result_set.next())
				    while (rq.tableColumnsResultSet.next())
				    {
					//String col=Table.this.getName()+"."+rq.result_set.getString("COLUMN_NAME");
					String col=Table.this.getName()+"."+rq.tableColumnsResultSet.getColumnName();
					FieldAccessor founded_fa=null;
					SqlField founded_sf=null;
					for (FieldAccessor fa : fields)
					{
					    for (SqlField sf : fa.getDeclaredSqlFields())
					    {
						if (sf.field.equalsIgnoreCase(col))
						{
						    founded_fa=fa;
						    founded_sf=sf;
						    break;
						}
					    }
					    if (founded_fa!=null)
						break;
					}
				    
					if (founded_fa==null)
					    throw new DatabaseVersionException(Table.this, "The table "+Table.this.getName()+" contains a column named "+col+" which does not correspond to any field of the class "+class_record.getName());
					//String type=rq.result_set.getString("TYPE_NAME").toUpperCase();
					String type=rq.tableColumnsResultSet.getTypeName().toUpperCase();
					if (!founded_sf.type.toUpperCase().startsWith(type))
					    throw new DatabaseVersionException(Table.this, "The type of the column "+col+" should  be "+founded_sf.type+" and not "+type);
					if (col_size_matcher.matcher(founded_sf.type).matches())
					{
					    //int col_size=rq.result_set.getInt("COLUMN_SIZE");
					    int col_size=rq.tableColumnsResultSet.getColumnSize();
					    Pattern pattern2=Pattern.compile("("+col_size+")");
					    if (!pattern2.matcher(founded_sf.type).matches())
						throw new DatabaseVersionException(Table.this, "The column "+col+" has a size equals to "+col_size+" (expected "+founded_sf.type+")");
					}
					//boolean is_null=rq.result_set.getString("IS_NULLABLE").equals("YES");
					boolean is_null=rq.tableColumnsResultSet.isNullable();
					if (is_null==founded_fa.isNotNull())
					    throw new DatabaseVersionException(Table.this, "The column "+col+" is expected to be "+(founded_fa.isNotNull()?"not null":"nullable"));
					boolean is_autoincrement=rq.result_set.getString("IS_AUTOINCREMENT").equals("YES");
					if (is_autoincrement!=founded_fa.isAutoPrimaryKey())
					    throw new DatabaseVersionException(Table.this, "The column "+col+" is "+(is_autoincrement?"":"not ")+"autoincremented into the Sql database where it is "+(is_autoincrement?"not ":"")+" into the OOD database.");
				    }
				}
		        	sql_connection.checkConstraints(Table.this);
				return null;
		            }
		            catch(Exception e)
		            {
		        	throw DatabaseException.getDatabaseException(e);
		            }
		        }
		    });
	    }
	    else
	    {
		{
		    
		    final StringBuffer SqlQuerry=new StringBuffer("CREATE "+sql_connection.getCachedKeyword()+" TABLE "+this.getName()+"(");
		    boolean first=true;
		    for (FieldAccessor f : fields)
		    {
			if (first)
			    first=false;
			else
			    SqlQuerry.append(", ");
			SqlQuerry.append(getSqlFieldDeclaration(f));
		    }
		    if (primary_keys_fields.size()>0)
		    {
			SqlQuerry.append(", CONSTRAINT "+getSqlPrimaryKeyName()+" PRIMARY KEY(");
			first=true;
			for (FieldAccessor fa : primary_keys_fields)
			{
			    for (SqlField sf : fa.getDeclaredSqlFields())
			    {
				if (first)
				    first=false;
				else
				    SqlQuerry.append(", ");
				SqlQuerry.append(sf.short_field);
			    }
			}
			SqlQuerry.append(")");
		    }
		
		    foreign_keys_to_create=true;
		
		
		    for (FieldAccessor f : fields)
		    {
			if (f.isUnique() && !f.isForeignKey())
			{
			    SqlQuerry.append(", UNIQUE("+f.getDeclaredSqlFields()[0].short_field+")");
			}
		    }
		
		    SqlQuerry.append(")"+sql_connection.getSqlComma());
		
		    sql_connection.runTransaction(new Transaction() {
		    
			public boolean doesWriteData()
			{
			    return true;
			}
			@Override
			public Object run(DatabaseWrapper sql_connection) throws DatabaseException
			{
			    Statement st=null;
			    try
			    {
				st=sql_connection.getSqlConnection().createStatement();
				
				st.executeUpdate(SqlQuerry.toString());
				
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
			    return null;
			}
		    });
		    sql_connection.runTransaction(new Transaction() {
			public boolean doesWriteData()
			{
			    return true;
			}
		    
			@Override
			public Object run(DatabaseWrapper sql_connection) throws DatabaseException
			{
			    Statement st=null;
			    try
			    {
				st=sql_connection.getSqlConnection().createStatement();
				st.executeUpdate("INSERT INTO "+DatabaseWrapper.ROW_COUNT_TABLES+" VALUES('"+Table.this.getName()+"', 0)");
				st.close();
				st=sql_connection.getSqlConnection().createStatement();
				st.executeUpdate("CREATE TRIGGER "+Table.this.getName()+"_ROW_COUNT_TRIGGER_INSERT__ AFTER INSERT ON "+Table.this.getName()+"\n" +
						"FOR EACH ROW \n" +
						"UPDATE "+DatabaseWrapper.ROW_COUNT_TABLES+" SET ROW_COUNT=ROW_COUNT+1 WHERE TABLE_NAME='"+Table.this.getName()+"'\n");
				st.close();
				st=sql_connection.getSqlConnection().createStatement();
				st.executeUpdate("CREATE TRIGGER "+Table.this.getName()+"_ROW_COUNT_TRIGGER_DELETE__ AFTER DELETE ON "+Table.this.getName()+"\n" +
						"FOR EACH ROW \n" +
						"UPDATE "+DatabaseWrapper.ROW_COUNT_TABLES+" SET ROW_COUNT=ROW_COUNT-1 WHERE TABLE_NAME='"+Table.this.getName()+"'\n");
				st.close();
				st=null;
			    }
			    catch(SQLException e)
			    {
				throw DatabaseException.getDatabaseException(e);
			    }
			    finally
			    {
				try
				{
				    if (st!=null)
					st.close();
				}
				catch(SQLException e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
			    }
			    return null;
			}
		    });
		    
		
		}
	    }
	}
	ArrayList<Class<?>> list_classes;
	try
	{
	    list_classes = ListClasses.getClasses(this.getClass().getPackage());
	}
	catch (ClassNotFoundException e)
	{
	    throw new DatabaseException("Impossible to access to t)he list of classes contained into the package "+this.getClass().getPackage().getName(), e);
	}
	catch (IOException e)
	{
	    throw new DatabaseException("Impossible to access to the list of classes contained into the package "+this.getClass().getPackage().getName(), e);	
	}
	boolean this_class_found=false;
	for (Class<?> c : list_classes)
	{
	    if (Table.class.isAssignableFrom(c))
	    {
		if (c.equals(this.getClass()))
		    this_class_found=true;

		
		@SuppressWarnings("unchecked")
		Class<? extends Table<?>> ct=(Class<? extends Table<?>>)c;
		Class<? extends DatabaseRecord> cdf=getDatabaseRecord(ct);
		ArrayList<Field> concerned_fields=new ArrayList<Field>();
		for (Field f : cdf.getDeclaredFields())
		{
		    if (f.isAnnotationPresent(ForeignKey.class) && f.getType().equals(class_record))
		    {
			concerned_fields.add(f);
		    }
		}
		if (concerned_fields.size()>0)
		list_tables_pointing_to_this_table.add(new NeighboringTable(sql_connection, class_record, ct, concerned_fields));
	    }
	}
	if (!this_class_found)
	    throw new DatabaseException("Impossible to list and found local classes.");
	
    }
    
    void initializeStep3() throws DatabaseException
    {
	try(ReadWriteLock.WriteLock lock2=sql_connection.locker.getAutoCloseableWriteLock())
	{
	if (foreign_keys_to_create)
	{
	    foreign_keys_to_create=false;
	    
	    if (foreign_keys_fields.size()>0)
	    {
		for (ForeignKeyFieldAccessor f : foreign_keys_fields)
		{
		    final StringBuffer SqlQuerry=new StringBuffer("ALTER TABLE "+Table.this.getName()+" ADD FOREIGN KEY(");
		    boolean first=true;
		    for (SqlField sf : f.getDeclaredSqlFields())
		    {
			if (first)
			    first=false;
			else
			    SqlQuerry.append(", ");
			SqlQuerry.append(sf.short_field);
		    }
		    SqlQuerry.append(") REFERENCES "+f.getPointedTable().getName()+"(");
		    first=true;
		    for (SqlField sf : f.getDeclaredSqlFields())
		    {
			if (first)
			    first=false;
			else
			    SqlQuerry.append(", ");
			SqlQuerry.append(sf.short_pointed_field);
		    }
		    //SqlQuerry.append(") ON UPDATE CASCADE ON DELETE CASCADE");
		    SqlQuerry.append(") "+sql_connection.getOnDeleteCascadeSqlQuerry()+" "+sql_connection.getOnUpdateCascadeSqlQuerry());
		    sql_connection.runTransaction(new Transaction() {
		        
			public boolean doesWriteData()
			{
			    return true;
			}
		        @Override
		        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		        {
		            Statement st=null;
		            try
		            {
		    		st=_sql_connection.getSqlConnection().createStatement();
		    		st.executeUpdate(SqlQuerry.toString());
		    		
		    		return null;
		            }
		            catch(Exception e)
		            {
		        	throw DatabaseException.getDatabaseException(e);
		            }
		            finally
		            {
		        	try
		        	{
		        	    st.close();
		        	}
		        	catch(Exception e)
		        	{
		        	    throw DatabaseException.getDatabaseException(e);
			        }
		            }
		        }
		    });
		    
		    
		}
	    }
	    
	}
	}
    }
    
    @SuppressWarnings({ "unchecked", "unused" })
    private T getRecordFromPointingRecord(final SqlFieldInstance[] _sql_field_instances, final ArrayList<DatabaseRecord> _previous_pointing_records) throws DatabaseException
    {
	if (isLoadedInMemory() && is_synchronized_with_sql_database.get())
	{
	    for (T r : getRecords(false))
	    {
		boolean all_equals=true;
		for (FieldAccessor fa : primary_keys_fields)
		{
		    for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(r))
		    {
			boolean found=false;
			for (SqlFieldInstance sfi2 : _sql_field_instances)
			{
			    if (sfi2.pointed_field.equals(sfi.field))
			    {
				found=true;
				if (!FieldAccessor.equals(sfi.instance, sfi2.instance))
				{
				    all_equals=false;
				}
				break;
				
			    }
			}
			if (!found)
			    throw new DatabaseException("Unexpected exception.");
			if (!all_equals)
			    break;
		    }
		    if (!all_equals)
			break;
		}
		if (all_equals)
		    return r;
	    }
	    return null;
	}
	else
	{
	    for (DatabaseRecord dr : _previous_pointing_records)
	    {
		if (dr.getClass().equals(class_record))
		{
		    boolean all_equals=true;
		    for (FieldAccessor fa : primary_keys_fields)
		    {
			for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(dr))
			{
			    boolean found=false;
			    for (SqlFieldInstance sfi2 : _sql_field_instances)
			    {
				if (sfi2.pointed_field.equals(sfi.field))
				{
				    found=true;
				    if (!FieldAccessor.equals(sfi.instance, sfi2.instance))
				    {
					all_equals=false;
				    }
				    break;
					
				}
			    }
			    if (!found)
				throw new DatabaseException("Unexpected exception.");
			    if (!all_equals)
				break;
			 }
			if (!all_equals)
			    break;
		    }
		    if (all_equals)
			return (T)dr;
		}
	    }
	    
	    for (FieldAccessor fa : primary_keys_fields)
	    {
		for (SqlField sfi : fa.getDeclaredSqlFields())
		{
		    boolean found=false;
		    for (SqlFieldInstance sfi2 : _sql_field_instances)
		    {
			if (sfi2.pointed_field.equals(sfi.field))
			{
			    found=true;
			    break;
				
			}
		    }
		    if (!found)
			throw new DatabaseException("Unexpected exception.");
		 }
	    }
	    
	    
	    final StringBuffer querry=new StringBuffer("SELECT "+getSqlSelectStep1Fields()+" FROM "+this.getName()+" WHERE ");
	    boolean first=true;
	    for (SqlFieldInstance sfi : _sql_field_instances)
	    {
		if (first)
		    first=false;
		else
		    querry.append(" AND ");
		querry.append(sfi.pointed_field);
		querry.append(" = ?");
	    }
	    querry.append(sql_connection.getSqlComma());
	    
	    return (T)sql_connection.runTransaction(new Transaction() {
	        
		public boolean doesWriteData()
		{
		    return false;
		}
	        @SuppressWarnings("synthetic-access")
		@Override
	        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	        {
		    try(PreparedReadQuerry rq=new PreparedReadQuerry(_sql_connection.getSqlConnection(), querry.toString()))
		    {
			int i=1;
			for (SqlFieldInstance sfi : _sql_field_instances)
			{
			    rq.statement.setObject(i++, sfi.instance);
			}
			if (rq.execute() && rq.result_set.next())
			{
			    T res=default_constructor_field.newInstance();
			    for (FieldAccessor fa : fields)
			    {
				fa.setValue(res, rq.result_set, _previous_pointing_records);
			    }
			    return res;
			}
			return null;
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
	        }
	    });
	}
	
    }
    
    private String getSqlSelectStep1Fields()
    {
	StringBuffer sb=new StringBuffer();
	boolean first=true;
	for (FieldAccessor fa : fields)
	{
	    for (SqlField sf : fa.getDeclaredSqlFields())
	    {
		if (first)
		    first=false;
		else
		    sb.append(", ");
		sb.append(sf.field);
	    }
	}
	return sb.toString();
    }
    
    private String getSqlGeneralSelect()
    {
	return "SELECT "+getSqlSelectStep1Fields()+" FROM "+this.getName();
    }
    
    
    /**
     * Returns the number of records contained into this table.
     * @return the number of records contained into this table.
     * @throws DatabaseException if a Sql exception occurs.
     */
    public final int getRecordsNumber() throws DatabaseException
    {
	try (Lock lock=new ReadLock(this))
	{
	    if (isLoadedInMemory())
		return getRecords().size();
	    else
	    {
		return getRowCount();
	    }
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    private final int getRowCount() throws DatabaseException
    {
	Transaction t=new Transaction() {
		
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return false;
		    }

		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			try(ReadQuerry rq=new ReadQuerry(_sql_connection.getSqlConnection(), "SELECT ROW_COUNT FROM "+DatabaseWrapper.ROW_COUNT_TABLES+" WHERE TABLE_NAME='"+Table.this.getName()+"'"))
			{
			    if (rq.result_set.next())
			    {
				return new Integer(rq.result_set.getInt(1));
			    }
			    else throw new DatabaseException("Unexpected exception.");
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
		    }
		};
	return ((Integer)sql_connection.runTransaction(t)).intValue();
    }
    
    
    
    /**
     * 
     * @return the fields corresponding to this table
     */
    public final ArrayList<FieldAccessor> getFieldAccessors()
    {
	return fields;
    }

    /**
     * 
     * @return the fields corresponding to this table
     */
    public final ArrayList<ForeignKeyFieldAccessor> getForeignKeysFieldAccessors()
    {
	return this.foreign_keys_fields;
    }
    
    /**
     * 
     * @return the primary keys corresponding to this table
     */
    public final ArrayList<FieldAccessor> getPrimaryKeysFieldAccessors()
    {
	return primary_keys_fields;
    }
    
    private final String getSqlFieldDeclaration(FieldAccessor field)
    {
	String tmp;
	if (field.isNotNull())
	    tmp=" "+sql_connection.getSqlNotNULL();
	else
	    tmp=" "+sql_connection.getSqlNULL();
	
	if (field.isForeignKey())
	{
	    String res="";

	    
	    boolean first=true;
	    for (SqlField sf : field.getDeclaredSqlFields())
	    {
		if (first)
		    first=false;
		else
		    res+=", ";
		res+=sf.short_field+" "+sf.type+tmp;
	    }
	    
	    return res;
	}
	else
	{
	    String res=field.getDeclaredSqlFields()[0].short_field+" "+field.getDeclaredSqlFields()[0].type;
	    if (field.isAutoPrimaryKey())
		res+=" GENERATED BY DEFAULT AS IDENTITY(START WITH "+field.getStartValue()+")";
	    res+=tmp;
	    return res;
	}
	
    }
    /**
     * Returns the corresponding Table Class to a DatabaseRecord.
     * @param _record_class the DatabaseRecord class
     * @return the corresponding Table Class.
     * @throws DatabaseException if database constaints are not respected.
     * @throws NullPointerException if parameters are null pointers.
     */
    public static final Class<? extends Table<?>> getTableClass(Class<? extends DatabaseRecord> _record_class) throws DatabaseException
    {
	if (_record_class==null)
	    throw new NullPointerException("The parameter _record_class is a null pointer !");
	
	Class<?> declaring_class=_record_class.getDeclaringClass();
	if (declaring_class==null)
	    throw new DatabaseException("The DatabaseRecord class "+_record_class.getName()+" was not declared into a class extending "+Table.class.getName());
	if (!(declaring_class.getName()+"$"+"Record").equals(_record_class.getName()))
	    throw new DatabaseException("The DatabaseRecord class "+_record_class.getName()+" have not the expected name "+declaring_class.getName()+"$"+"Record");
	if (!Table.class.isAssignableFrom(declaring_class))
	    throw new DatabaseException("The class "+declaring_class+" in which is declared the class "+_record_class.getName()+" does not extends the class "+Table.class.getName());
	@SuppressWarnings("unchecked")
	Class<? extends Table<?>> res=(Class<? extends Table<?>>)declaring_class;
	return res;
    }
    
    /**
     * Returns the corresponding DatabaseRecord class to a given Table class.
     * @param _table_class the table class
     * @return the corresponding DatabaseRecord.
     * @throws DatabaseException  if database constaints are not respected.
     * @throws NullPointerException if parameters are null pointers.
     */
    public static final Class<? extends DatabaseRecord> getDatabaseRecord(Class<? extends Table<?>> _table_class) throws DatabaseException
    {
	if (_table_class==null)
	    throw new NullPointerException("The parameter _table_class is a null pointer !");

	Class<? extends DatabaseRecord> res=null;
	for (Class<?> c : _table_class.getDeclaredClasses())
	{
	    if (DatabaseRecord.class.isAssignableFrom(c) && c.getSimpleName().equals("Record"))
	    {
		@SuppressWarnings("unchecked")
		Class<? extends DatabaseRecord> tmp=(Class<? extends DatabaseRecord>)c;
		res=tmp;
		break;
	    }
	}
	if (res==null)
	    throw new DatabaseException("The class "+_table_class.getName()+" which inherits the class "+Table.class.getName()+" does not have any inner class named Field which inherits the class "+DatabaseRecord.class.getName());
	else
	{
	    if (!Modifier.isStatic(res.getModifiers()))
		throw new DatabaseException("The class "+res.getName()+" must be a static member class.");
	    boolean ok=true;
	    Constructor<?> constructors[]=res.getDeclaredConstructors();
	    if (constructors.length!=1)
		ok=false;
	    else
	    {
		Constructor<?> constructor=constructors[0];
		if (!Modifier.isProtected(constructor.getModifiers()))
		    ok=false;
		else
		{
		    if (constructor.getParameterTypes().length!=0)
			ok=false;
		}
	    }
	    if (!ok)
		throw new DatabaseException("The class "+res.getName()+" must have only one constructor which must be protected and without any parameter (default constructor).");
	}
	return res;
    }
    
    /**
     * 
     * @return true if the database records of this table is loaded into the memory. false if database records are only stored into the hard drive.
     */
    public final boolean isLoadedInMemory()
    {
	return is_loaded_in_memory;
    }
    
    /**
     * 
     * @return the simple name of this class table.
     */
    public final String getName()
    {
	return table_name;
    }

    /**
     * Format the a class name by replacing '.' chars by '_' chars. Use also upper case. 
     * @param c a class
     * @return the new class name format
     */
    public static final String getName(Class<?> c)
    {
	return c.getCanonicalName().replace(".", "_").toUpperCase();
    }
    
    
    @Override public String toString()
    {
	return "Database Table "+this.getName(); 
    }
    
    /**
     * Returns the records of this table ordered according the given fields, in an ascendant way or in a descendant way. 
     * Note that if you don't want to load this table into the memory, it is preferable to use the function {@link #getOrderedRecords(Filter, boolean, String...)}.
     * The same thing is valid to get the number of records present in this table. It is preferable to use the function {@link #getRecordsNumber()}. 
     * @param _ascendant this parameter must be true if the records should be sorted from the lower value to the highest value, false else.
     * @param _fields the first given field corresponds to the field by which the table is sorted. If two records are equals, then the second given field is used, etc. It must have at minimum one field. Only comparable fields are authorized. It is possible to sort fields according records pointed by foreign keys. In this case, to sort according the field A of the foreign key FK1, please enter "FK1.A".
     * @return the ordered records
     * @throws ConstraintsNotRespectedDatabaseException if a byte array field, a boolean field, or a foreign key field, is given or if an unknown field is given.
     * @since 1.2
     */
    public final ArrayList<T> getOrderedRecords(boolean _ascendant, String..._fields) throws DatabaseException
    {
	return getOrderedRecords(new Filter<T>() {

	    @Override
	    public boolean nextRecord(T _record) 
	    {
		return true;
	    }
	}, _ascendant, _fields);
    }
    
    /**
     * Returns the records of this table, corresponding to a given filter, and ordered according the given fields, in an ascendant way or in a descendant way. 
     * @param _filter the filter which select records to include
     * @param _ascendant this parameter must be true if the records should be sorted from the lower value to the highest value, false else.
     * @param _fields the first given field corresponds to the field by which the table is sorted. If two records are equals, then the second given field is used, etc. It must have at minimum one field. Only comparable fields are authorized. It is possible to sort fields according records pointed by foreign keys. In this case, to sort according the field A of the foreign key FK1, please enter "FK1.A".
     * @return the ordered filtered records
     * @throws ConstraintsNotRespectedDatabaseException if a byte array field, a boolean field, or a foreign key field, is given or if an unknown field is given.
     * @since 1.2
     */
    public final ArrayList<T> getOrderedRecords(final Filter<T> _filter, boolean _ascendant, String..._fields) throws DatabaseException
    {
	try (Lock l=new ReadLock(this))
	{
	    final SortedArray res=new SortedArray(_ascendant, _fields);
	    if (isLoadedInMemory())
	    {
		for (T r : getRecords(false))
		{
		    if (_filter.nextRecord(r))
			res.addRecord(r);
		}
	    }
	    else
	    {
		getListRecordsFromSqlConnection(new Runnable() {
		    
		    @Override
		    public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
		    {
			if (_filter.nextRecord(_instance))
			{
			    res.addRecord(_instance);
		    	}
			return true;
		    }
		        
		    @Override
		    public void init(int _field_count)
		    {
		    }
		}, getSqlGeneralSelect());
	    }
	    return res.getRecords();
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    /**
     * Returns the given records ordered according the given fields, in an ascendant way or in a descendant way.
     * @param _records the records to sort. 
     * @param _ascendant this parameter must be true if the records should be sorted from the lower value to the highest value, false else.
     * @param _fields the first given field corresponds to the field by which the table is sorted. If two records are equals, then the second given field is used, etc. It must have at minimum one field. Only comparable fields are authorized. It is possible to sort fields according records pointed by foreign keys. In this case, to sort according the field A of the foreign key FK1, please enter "FK1.A".
     * @return the ordered filtered records
     * @throws ConstraintsNotRespectedDatabaseException if a byte array field, a boolean field, or a foreign key field, is given or if an unknown field is given.
     * @since 1.2
     */
    public final ArrayList<T> getOrderedRecords(Collection<T> _records, boolean _ascendant, String..._fields) throws DatabaseException
    {
	final SortedArray res=new SortedArray(_records.size(), _ascendant, _fields);
	for (T r : _records)
	    res.addRecord(r);
	return res.getRecords();
    }
    
    private final class FieldComparator 
    {
	private final ArrayList<FieldAccessor> fields;
	
	public FieldComparator(ArrayList<FieldAccessor> _fields)
	{
	    fields=_fields;
	}
	
	public int compare(T _o1, T _o2) throws DatabaseException
	{
	    DatabaseRecord dr1=_o1, dr2=_o2;
	    for (int i=0;i<fields.size()-1;i++)
	    {
		if (dr1==null && dr2!=null)
		    return -1;
		else if (dr1!=null && dr2==null)
		    return 1;
		else if (dr1==null && dr2==null)
		    return 0;
		ForeignKeyFieldAccessor f=(ForeignKeyFieldAccessor)fields.get(i);
		dr1=(DatabaseRecord)f.getValue(dr1);
		dr2=(DatabaseRecord)f.getValue(dr2);
	    }
	    return fields.get(fields.size()-1).compare(dr1, dr2);
	}
    }
    
    
    private final class Comparator 
    {
	private ArrayList<FieldComparator> accessors=new ArrayList<FieldComparator>();
	private final boolean ascendant;
	
	public Comparator(boolean _ascendant, String ... _fields) throws ConstraintsNotRespectedDatabaseException
	{
	    ascendant=_ascendant;
	    if (_fields.length==0)
		throw new ConstraintsNotRespectedDatabaseException("It must have at mean one field to compare.");
	    for (int i=0;i<_fields.length;i++)
	    {
		accessors.add(getFieldComparator(_fields[i]));
	    }
	}
	private int getResult(int val)
	{
	    if (ascendant)
		return val;
	    else
	    {
		return -val;
	    }
	}
	public int compare(T _o1, T _o2) throws DatabaseException
	{
	    int res=0;
	    for (int i=0;i<accessors.size();i++)
	    {
		FieldComparator fa=accessors.get(i);
		res=fa.compare(_o1, _o2);
		if (res!=0)
		    return getResult(res);
	    }
	    return getResult(res);
	}
	@SuppressWarnings("synthetic-access")
	public FieldComparator getFieldComparator(String field) throws ConstraintsNotRespectedDatabaseException
	{
	    ArrayList<String> strings=splitPoint(field);
	    ArrayList<FieldAccessor> fields=new ArrayList<FieldAccessor>();
	    Table<?> current_table=Table.this;
	    
	    for (int i=0;i<strings.size();i++)
	    {
		if (current_table==null)
		    throw new ConstraintsNotRespectedDatabaseException("The field "+field+" does not exists.");
		String f=strings.get(i);
		FieldAccessor founded_field=null;
		for (FieldAccessor fa : current_table.fields)
		{
		    if (fa.getFieldName().equals(f))
		    {
			founded_field=fa;
			break;
		    }
		}
		if (founded_field==null)
		    throw new ConstraintsNotRespectedDatabaseException("The field "+f+" does not exist into the class/table "+current_table.getClass().getName());
		
		fields.add(founded_field);
		
		if (founded_field.isForeignKey())
		    current_table=((ForeignKeyFieldAccessor)founded_field).getPointedTable();
		else
		    current_table=null;
		
	    }
	    if (!fields.get(fields.size()-1).isComparable())
		throw new ConstraintsNotRespectedDatabaseException("The field "+field+" starting in the class/table "+Table.this.getClass().getName()+" is not a comparable field.");
	    return new FieldComparator(fields);
	}
	private ArrayList<String> splitPoint(String s)
	{
	    ArrayList<String> res=new ArrayList<String>(10);
	    int last_index=0;
	    for (int i=0;i<s.length();i++)
	    {
		if (s.charAt(i)=='.')
		{
		    if (i!=last_index)
		    {
			res.add(s.substring(last_index, i));
		    }
		    last_index=i+1;
		}
	    }
	    if (s.length()!=last_index)
	    {
		res.add(s.substring(last_index));
	    }
	
	    return res;
	 }
	
	
    }
    
    private class SortedArray
    {
	private final Comparator comparator;
	private final ArrayList<T> sorted_list;
	public SortedArray(boolean _ascendant, String ... _fields) throws ConstraintsNotRespectedDatabaseException
	{
	    comparator=new Comparator(_ascendant, _fields);
	    sorted_list=new ArrayList<T>();
	}
	public SortedArray(int initial_capacity, boolean _ascendant, String ... _fields) throws ConstraintsNotRespectedDatabaseException
	{
	    comparator=new Comparator(_ascendant, _fields);
	    sorted_list=new ArrayList<T>(initial_capacity);
	}
	
	public void addRecord(T _record) throws DatabaseException
	{
	    int mini=0;
	    int maxi=sorted_list.size();
	    while (maxi-mini>0)
	    {
		int i=mini+(maxi-mini)/2;
		int comp=comparator.compare(sorted_list.get(i), _record);
		if (comp<0)
		{
		    mini=i+1;
		}
		else if (comp>0)
		{
		    maxi=i;
		}
		else
		{
		    mini=i;
		    maxi=i;
		}
	    }
	    sorted_list.add(mini, _record);
	}
	
	public ArrayList<T> getRecords()
	{
	    return sorted_list;
	}
	
	
    }
    
    
    /**
     * Returns the records of this table. 
     * Note that if you don't want to load this table into the memory, it is preferable to use the function {@link #getRecords(Filter)}.
     * The same thing is valid to get the number of records present in this table. It is preferable to use the function {@link #getRecordsNumber()}. 
     * @return all the records of the table.
     * @throws DatabaseException if a Sql exception occurs.
     */
    public final ArrayList<T> getRecords() throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    
    private final ArrayList<T> getRecords(boolean is_already_in_transaction) throws DatabaseException
    {
	//try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableReadLock())
	{
	    if (isLoadedInMemory())
	    {
		if (!is_synchronized_with_sql_database.get())
		{
		    final ArrayList<T>  res=new ArrayList<T>();
		    getListRecordsFromSqlConnection(new Runnable() {
			    
			@Override
			public boolean setInstance(T _instance, ResultSet _cursor)
			{
			    res.add(_instance);
			    return true;
			}
			    
			@Override
			public void init(int _field_count)
			{
			    res.clear();
			    res.ensureCapacity((int)_field_count);
			}
		    }, getSqlGeneralSelect());
		    records_instances.set(res);
		    is_synchronized_with_sql_database.set(true);
		}
		return records_instances.get();
	    }
	    else
	    {
		final ArrayList<T>  res=new ArrayList<T>();
		getListRecordsFromSqlConnection(new Runnable() {
		    
		    @Override
		    public boolean setInstance(T _instance, ResultSet _cursor)
		    {
			res.add(_instance);
			return true;
		    }
		    
		    @Override
		    public void init(int _field_count)
		    {
			res.clear();
			res.ensureCapacity((int)_field_count);
		    }
		}, getSqlGeneralSelect());
		return res;
	    }
	}
    }
    /**
     * Returns the records of this table corresponding to a given filter. 
     * @param _filter the filter
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final ArrayList<T> getRecords(final Filter<T> _filter) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	//synchronized(sql_connection)
	{
	
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(_filter, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    private final ArrayList<T> getRecords(final Filter<T> _filter, boolean is_already_sql_transaction) throws DatabaseException
    {
	final ArrayList<T> res=new ArrayList<T>();

	    if (isLoadedInMemory())
	    {
		ArrayList<T> records=getRecords(is_already_sql_transaction);
		for (T r : records)
		{
		    if (_filter.nextRecord(r))
			res.add(r);
		}
	    }
	    else
	    {
		getListRecordsFromSqlConnection(new Runnable() {
		    
		    @Override
		    public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException
		    {
			if (_filter.nextRecord(r))
			    res.add(r);
			return true;
		    }
		    
		    @Override
		    public void init(int _field_count)
		    {
		    }
		}, getSqlGeneralSelect());
	    }
	return res;
    }
    
    /**
     * Returns true if there is at mean one record which corresponds to the given filter.
     * @param _filter the filter
     * @return true if there is at mean one record which corresponds to the given filter.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final boolean hasRecords(final Filter<T> _filter) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return hasRecords(_filter, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    private final boolean hasRecords(final Filter<T> _filter, boolean is_sql_transaction) throws DatabaseException
    {
	    if (isLoadedInMemory())
	    {
		ArrayList<T> records=getRecords(is_sql_transaction);
		for (T r : records)
		{
		    if (_filter.nextRecord(r))
			return true;
		}
		return false;
	    }
	    else
	    {
		class RunnableTmp extends Runnable
		{
		    boolean res;
		    @Override
		    public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException
		    {
			if (_filter.nextRecord(r))
			{
			    res=true;
			    return false;
			}
			return true;
		    }
		    
		    @Override
		    public void init(int _field_count)
		    {
			res=false;
		    }
		    
		}
		RunnableTmp runnable=new RunnableTmp();
		getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
		return runnable.res;
	    }
    }
    private abstract class SimpleFieldFilter implements Filter<T>
    {
	protected final Map<String, Object> given_fields;
	protected final ArrayList<FieldAccessor> fields_accessor;
	public SimpleFieldFilter(Map<String, Object> _fields, final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException
	{
	    fields_accessor=_fields_accessor;
		for (String s : _fields.keySet())
		{
		    boolean found=false;
		    for (FieldAccessor fa : fields_accessor)
		    {
			if (fa.getFieldName().equals(s))
			{
			    found=true;
			    break;
			}
		    }
		    if (!found)
			throw new FieldDatabaseException("The given field "+s+" is not contained into the table "+Table.this.getClass().getName());
		}
		given_fields=_fields;
	}
    }
    private abstract class MultipleFieldFilter implements Filter<T>
    {
	protected final Map<String, Object> given_fields[];
	protected final ArrayList<FieldAccessor> fields_accessor;
	@SafeVarargs
	public MultipleFieldFilter(final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object> ..._fields) throws DatabaseException
	{
	    fields_accessor=_fields_accessor;
		Set<String> first=null;
		for (Map<String, Object> hm : _fields)
		{
		    if (first==null)
			first=hm.keySet();
		    if (hm.keySet().size()!=first.size())
			throw new FieldDatabaseException("The given fields are not the same in every HashMap<String, Object>. Every HashMap<String,Object> must have the same keys.");
		    for (String s : hm.keySet())
		    {
			boolean found=false;
			for (String s2 : first)
			{
			    if (s2.equals(s))
			    {
				found=true;
				break;
			    }
			}
			if (!found)
			    throw new FieldDatabaseException("The given fields are not the same in every HashMap<String, Object>. Every HashMap<String,Object> must have the same keys.");
			found=false;
			for (FieldAccessor fa : fields_accessor)
			{
			    if (fa.getFieldName().equals(s))
			    {
				found=true;
				break;
			    }
			}
			if (!found)
			    throw new FieldDatabaseException("The given field "+s+" is not contained into this table.");
		    }
		}
		given_fields=_fields;
	    
	}
    }
    private class SimpleAllFieldsFilter extends SimpleFieldFilter
    {

	public SimpleAllFieldsFilter(Map<String, Object> _fields, final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException
	{
	    super(_fields, _fields_accessor);
	}

	@Override
	public boolean nextRecord(T _instance) throws DatabaseException
	{
		boolean toadd=true;
		for (String s : given_fields.keySet())
		{
		    boolean ok=false;
		    for (FieldAccessor fa : fields_accessor)
		    {
			if (fa.getFieldName().equals(s))
			{
			    if (fa.equals(_instance, given_fields.get(s)))
			    {
				ok=true;
			    }
			    break;
			}
		    }
		    if (!ok)
		    {
			toadd=false;
			break;
		    }
		}
	    
		return toadd;
	}
	
    }
    
    private class MultipleAllFieldsFilter extends MultipleFieldFilter
    {

	public MultipleAllFieldsFilter(final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object>[] _records) throws DatabaseException
	{
	    super(_fields_accessor, _records);
	}

	@Override
	public boolean nextRecord(T _instance) throws DatabaseException
	{
		    boolean toadd=true;
		    for (Map<String, Object> hm : given_fields)
		    {
			toadd=true;
			for (String s : hm.keySet())
			{
			    boolean ok=false;
			    for (FieldAccessor fa : fields_accessor)
			    {
				if (fa.getFieldName().equals(s))
				{
				    if (fa.equals(_instance, hm.get(s)))
				    {
					ok=true;
				    }
				    break;
				}
			    }
			    if (!ok)
			    {
				toadd=false;
				break;
			    }
			}
			if (toadd)
			    break;
		    }

		    return toadd;
	}
	
    }
    
    private class SimpleOneOfFieldsFilter extends SimpleFieldFilter
    {

	public SimpleOneOfFieldsFilter(Map<String, Object> _fields, final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException
	{
	    super(_fields, _fields_accessor);
	}

	@Override
	public boolean nextRecord(T _instance) throws DatabaseException
	{
		    boolean toadd=false;
		    for (String s : given_fields.keySet())
		    {
			for (FieldAccessor fa : fields_accessor)
			{
			    if (fa.getFieldName().equals(s))
			    {
				if (fa.equals(_instance, given_fields.get(s)))
				{
				    toadd=true;
				}
				break;
			    }
			}
			if (toadd)
			{
			    break;
			}
		    }

		    return toadd;
	}
	
    }
    
    private class MultipleOneOfFieldsFilter extends MultipleFieldFilter
    {

	public MultipleOneOfFieldsFilter(final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object>[] _records) throws DatabaseException
	{
	    super(_fields_accessor, _records);
	}

	@Override
	public boolean nextRecord(T _instance) throws DatabaseException
	{
		    boolean toadd=true;
		    for (Map<String, Object> hm : given_fields)
		    {
			toadd=false;
			for (String s : hm.keySet())
			{
			    boolean ok=false;
			    for (FieldAccessor fa : fields_accessor)
			    {
				if (fa.getFieldName().equals(s))
				{
				    if (fa.equals(_instance, hm.get(s)))
				    {
					ok=true;
				    }
				    break;
				}
			    }
			    if (ok)
			    {
				toadd=true;
				break;
			    }
			}
			if (toadd)
			    break;
		    }

		    return toadd;
	}
	
    }
    
    
    
    /**
     * Return grouped results (equivalent to group by with SQL). 
     * @param _records the records to group.
     * @param _fields the fields which enables to group the results. It must have at minimum one field. It is possible to use fields according records pointed by foreign keys. In this case, to use the field A of the foreign key FK1, please enter "FK1.A".
     * @return a GroupedResults instance.
     * @throws DatabaseException when a database exception occurs
     */
    @SuppressWarnings("unchecked")
    public GroupedResults<T> getGroupedResults(Collection<T> _records, String ..._fields) throws DatabaseException
    {
	try
	{
	    return (GroupedResults<T>)grouped_results_constructor.newInstance(sql_connection, _records, class_record, _fields);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    /**
     * Return a GroupedResults instance (equivalent to group by with SQL). The results must be included by calling the functions {@link com.distrimind.ood.database.GroupedResults#addRecord(DatabaseRecord)} and {@link com.distrimind.ood.database.GroupedResults#addRecords(Collection)}. 
     * @param _fields the fields which enables to group the results. It must have at minimum one field. It is possible to use fields according records pointed by foreign keys. In this case, to use the field A of the foreign key FK1, please enter "FK1.A".
     * @return a GroupedResults instance.
     * @throws DatabaseException when a database exception occurs
     */
    public GroupedResults<T> getGroupedResults(String ..._fields) throws DatabaseException
    {
	return getGroupedResults(null, _fields);
    }
    
    
    /**
     * Returns the records which correspond to the given fields. All given fields must correspond exactly to the returned records. 
     * @param _fields the fields that filter the result.
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final ArrayList<T> getRecordsWithAllFields(final Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(new SimpleAllFieldsFilter(_fields, fields), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }

    /**
     * Returns true if there is at least one record which correspond the given fields. All given fields must correspond exactly one the records. 
     * @param _fields the fields that must match to one of the records.
     * @return true if there is at least one record which correspond the given fields. 
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final boolean hasRecordsWithAllFields(final Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return hasRecords(new SimpleAllFieldsFilter(_fields, fields), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Returns the records which correspond to one group of fields of the array of fields. For one considered record, it must have one group of fields (record) that all corresponds exactly. 
     * @param _records the fields that filter the result.
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final ArrayList<T> getRecordsWithAllFields(final Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(new MultipleAllFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Returns true if there is at least one record which correspond to one group of fields of the array of fields. For one considered record, it must have one group of fields (record) that all corresponds exactly.
     * @param _records the array fields that must match to one of the records.
     * @return true if there is at least one record which correspond to one group of fields of the array of fields. 
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final boolean hasRecordsWithAllFields(final Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return hasRecords(new MultipleAllFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    /**
     * Returns the records which correspond to one of the given fields. One of the given fields must correspond exactly to the returned records. 
     * @param _fields the fields that filter the result.
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final ArrayList<T> getRecordsWithOneOfFields(final Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(new SimpleOneOfFieldsFilter(_fields, fields), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }

    /**
     * Returns true if there is at least one record which corresponds to one of the given fields. One of the given fields must corresponds exactly one the records. 
     * @param _fields the fields.
     * @return true if there is at least one record which correspond one of the given fields. 
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final boolean hasRecordsWithOneOfFields(final Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return hasRecordsWithOneOfFields(_fields, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
	
    }
    private final boolean hasRecordsWithOneOfFields(final Map<String, Object> _fields, boolean is_sql_transaction) throws DatabaseException
    {
	return hasRecords(new SimpleOneOfFieldsFilter(_fields, fields), is_sql_transaction);
    }
    
    /**
     * Returns the records which correspond to one of the fields of one group of the array of fields.  
     * @param _records the fields that filter the result.
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final ArrayList<T> getRecordsWithOneOfFields(final Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return getRecords(new MultipleOneOfFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Returns if there is at least one record which correspond to one of the fields of one group of the array of fields.  
     * @param _records the fields that filter the result.
     * @return the corresponding records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final boolean hasRecordsWithOneOfFields(final Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return hasRecords(new MultipleOneOfFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    /**
     * Remove records which correspond to the given fields. All given fields must correspond exactly to the records.
     * The deleted records do not have link with other table's records throw foreign keys.
     * @param _fields the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final long removeRecordsWithAllFields(Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
	    	return removeRecords(new SimpleAllFieldsFilter(_fields, fields), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove records which correspond to one group of fields of the array of fields. For one considered record, it must have one group of fields (record) that all corresponds exactly.
     * The deleted records do not have link with other table's records throw foreign keys. 
     * @param _records the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final long removeRecordsWithAllFields(Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecords(new MultipleAllFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove records which correspond to one of the given fields. One of the given fields must correspond exactly to deleted records.
     * The deleted records do not have link with other table's records throw foreign keys. 
     * @param _fields the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final long removeRecordsWithOneOfFields(Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecords(new SimpleOneOfFieldsFilter(_fields, fields),false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove records which correspond to one of the fields of one group of the array of fields.
     * The deleted records do not have link with other table's records throw foreign keys.  
     * @param _records the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final long removeRecordsWithOneOfFields(Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array!");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecords(new MultipleOneOfFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }

    /**
     * Remove records which correspond to the given fields. All given fields must correspond exactly to the records.
     * Records of other tables which have Foreign keys which points to the deleted records are also deleted.
     * @param _fields the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final long removeRecordsWithAllFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecordsWithCascade(new SimpleAllFieldsFilter(_fields, fields), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove records which correspond to one group of fields of the array of fields. For one considered record, it must have one group of fields (record) that all corresponds exactly.
     * Records of other tables which have Foreign keys which points to the deleted records are also deleted. 
     * @param _records the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final long removeRecordsWithAllFieldsWithCascade(Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecordsWithCascade(new MultipleAllFieldsFilter(fields, _records), false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove records which correspond to one of the given fields. One of the given fields must correspond exactly to deleted records.
     * Records of other tables which have Foreign keys which points to the deleted records are also deleted. 
     * @param _fields the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    public final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecordsWithOneOfFieldsWithCascade(_fields, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    private final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> _fields, boolean _is_already_sql_transaction) throws DatabaseException
    {
	return removeRecordsWithCascade(new SimpleOneOfFieldsFilter(_fields, fields), _is_already_sql_transaction);
    }
    /**
     * Remove records which correspond to one of the fields of one group of the array of fields.
     * Records of other tables which have Foreign keys which points to the deleted records are also deleted.  
     * @param _records the fields that filter the result.
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws FieldDatabaseException if the given fields do not correspond to the table fields.
     */
    @SafeVarargs
    public final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecordsWithOneOfFieldsWithCascade(false, _records);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    @SafeVarargs
    private final long removeRecordsWithOneOfFieldsWithCascade(boolean is_already_in_transaction, Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty array !");
	return removeRecordsWithCascade(new MultipleOneOfFieldsFilter(fields, _records), is_already_in_transaction);
    }
    
    private HashMap<String, Object> getSqlPrimaryKeys(T _record) throws DatabaseException
    {
	HashMap<String, Object> res=new HashMap<String, Object>();
	for (FieldAccessor fa : primary_keys_fields)
	{
	    for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(_record))
	    {
		res.put(sfi.field, sfi.instance);
	    }
	}
	return res;
    }
    /**
     * Remove records which correspond to the given filter. 
     * These records are not pointed by other records through foreign keys of other tables. So they are not proposed to the filter class.  
     * 
     * @param _filter the filter
     * @return the number of deleted records
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final long removeRecords(final Filter<T> _filter) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecords(_filter, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    
    private final int removeRecords(final Filter<T> _filter, boolean is_already_in_transaction) throws DatabaseException
    {
	
	//try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableWriteLock())
	{
	    if (isLoadedInMemory())
	    {
		final ArrayList<T> records_to_remove=new ArrayList<T>();
	    
		for (final T r : getRecords(is_already_in_transaction))
		{
		    boolean toremove=true;
		    for (NeighboringTable nt : list_tables_pointing_to_this_table)
		    {
			if (nt.getPointedTable().hasRecordsWithOneOfFields(nt.getHashMapFields(r), false))
			{
			    toremove=false;
			    break;
			}
		    }
		
		    if (toremove && _filter.nextRecord(r))
		    {
			records_to_remove.add(r);
		    }
		}
		if (records_to_remove.size()>0)
		{
		    Transaction transaction=new Transaction() {

			    @Override
		    	    public boolean doesWriteData()
			    {
			        return true;
			    }
		        
			    @SuppressWarnings("synthetic-access")
			    @Override
			    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
			    {
				StringBuffer sb=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(records_to_remove.size()));
				
				
				int nb=0;
				try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), sb.toString()))
				{
				    int index=1;
				    for (T r : records_to_remove)
				    {
					for (FieldAccessor fa : primary_keys_fields)
					{
					    fa.getValue(r, puq.statement, index);
					    index+=fa.getDeclaredSqlFields().length;
					}
				    }
				    nb=puq.statement.executeUpdate();
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}

				
				if (nb!=records_to_remove.size())
				    throw new DatabaseException("Unexpected exception.");
				return null;
			    }
			};
		    if (records_to_remove.size()>0)
		    sql_connection.runTransaction(transaction);
	    
		    __removeRecords(records_to_remove);
		}
		return records_to_remove.size();
	}
	else
	{
	    
	    class RunnableTmp extends Runnable
	    {
	 
		public int deleted_records_number;
		@Override
		public void init(int _field_count)
		{
		    deleted_records_number=0;
		}

		@SuppressWarnings("synthetic-access")
		@Override
		public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
		{
		    try
		    {
			boolean toremove=true;
			if (list_tables_pointing_to_this_table.size()>0)
			{
			    for (int i=0;i<list_tables_pointing_to_this_table.size();i++)
			    {
				
				NeighboringTable nt=list_tables_pointing_to_this_table.get(i);
				
				if (nt.getPointedTable().hasRecordsWithOneOfSqlForeignKeyWithCascade(nt.getHashMapsSqlFields(getSqlPrimaryKeys(_instance))))
				{
				    toremove=false;
				    break;
				}
			    }
			}
			if (toremove && _filter.nextRecord(_instance))
			{
			    _cursor.deleteRow();
			    ++deleted_records_number;
			}
			return true;
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		    
		}
		    
	    }
	    
	    RunnableTmp runnable=new RunnableTmp();
	    getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect(), true);
	    return runnable.deleted_records_number;
	}
	}
    }
    
    private void checkMemory() throws DatabaseException
    {
	{
	    
	    if (isLoadedInMemory())
	    {
		for (final T r : getRecords())
		{
		    class RunnableTmp extends Runnable
		    {
			public boolean found=false;

			@Override
			public void init(int _field_count)
			{
			    found=false;
			}

			@Override
			public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
			{
			    if (Table.this.equals(r, _instance))
			    {
				found=true;
				return false;
			    }
			    return true;
			}
		    }
		    RunnableTmp runnable=new RunnableTmp();
		    getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
		    if (!runnable.found)
			throw new DatabaseIntegrityException("All records present in the memory were not found into the database.");
		}
		final ArrayList<T> records=getRecords();
		class RunnableTmp extends Runnable
		{

		    @Override
		    public void init(int _field_count)
		    {

		    }

		    @Override
		    public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
		    {
			for (T r : records)
			{
			    if (Table.this.equals(r, _instance))
				return false;
			}
			throw new DatabaseIntegrityException("All records present into the database were not found into the memory.");
		    }
		}
		getListRecordsFromSqlConnection(new RunnableTmp(), getSqlGeneralSelect());
	    }
	}
    }
    
    /**
     * This function check the table integrity.
     * It checks if group of primary keys are unique and unique keys are also unique, if foreign keys are valid, and if not null fields are not null. 
     * In the case where the current table is loaded in memory, it checks if all data is synchronized between the memory and the database.
     * This function is used principally with unit tests. Note that the call of this function will load all the table into memory. 
     * @throws DatabaseException if a problem occurs. Don't throw to any DatabaseException if the table integrity is fine.
     */
    public void checkDataIntegrity() throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try(ReadLock l=new ReadLock(this))
	    {
		checkMemory();
		ArrayList<T> records=getRecords();
		for (T r1 : records)
		{
		    for (T r2 : records)
		    {
			if (r1!=r2)
			{
			    boolean allequals=true;
			    for (FieldAccessor fa : primary_keys_fields)
			    {
				if (!fa.equals(r1, fa.getValue(r2)))
				{
				    allequals=false;
				    break;
				}
			    }
			    if (allequals)
				throw new DatabaseIntegrityException("There is records into the table "+this.getName()+" which have the same primary keys.");
			    for (FieldAccessor fa : unique_fields_no_auto_random_primary_keys)
			    {
				if (fa.equals(r1, fa.getValue(r2)))
				    throw new DatabaseIntegrityException("There is records into the table "+this.getName()+" which have the same unique key into the field "+fa.getFieldName());
			    }
			}
		    }
		}
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
   
    /**
     * Returns true if the primary keys of the two given instances are equals. Returns false else.
     * @param _record1 the first record to compare
     * @param _record2 the second record to compare
     * @return true if the primary keys of the two given instances are equals. Returns false else.
     * @throws DatabaseException when a database exception occurs
     */
    public final boolean equals(T _record1, T _record2) throws DatabaseException
    {
	    if (_record1==_record2)
		return true;
	    if (_record1==null || _record2==null)
		return _record1==_record2;
	    for (FieldAccessor fa : primary_keys_fields)
	    {
		if (!fa.equals(_record1, fa.getValue(_record2)))
		    return false;
	    }
	    return true;
    }

    @Override
    public boolean equals(Object o)
    {
	return o==this;
    }
    
    /**
     * Returns true if all fields of the two given instances are equals. Returns false else.
     * @param _record1 the first record to compare
     * @param _record2 the second record to compare
     * @return true if all fields of the two given instances are equals. Returns false else.
     * @throws DatabaseException when a database exception occurs
     */
    public final boolean equalsAllFields(T _record1, T _record2) throws DatabaseException
    {
	    if (_record1==_record2)
		return true;
	    if (_record1==null || _record2==null)
		return _record1==_record2;
	    for (FieldAccessor fa : fields)
	    {
		if (!fa.equals(_record1, fa.getValue(_record2)))
		{
		    return false;
		}
	    }
	    return true;
	
    }

    /**
     * Returns true if all fields (excepted the primary keys) of the two given instances are equals. Returns false else.
     * @param _record1 the first record to compare
     * @param _record2 the second record to compare
     * @return true if all fields (excepted the primary keys) of the two given instances are equals. Returns false else.
     * @throws DatabaseException when a database exception occurs
     */
    public final boolean equalsAllFieldsWithoutPrimaryKeys(T _record1, T _record2) throws DatabaseException
    {
	    if (_record1==_record2)
		return true;
	    if (_record1==null || _record2==null)
		return _record1==_record2;
	    for (FieldAccessor fa : fields)
	    {
		if (!fa.isPrimaryKey())
		{
		    if (!fa.equals(_record1, fa.getValue(_record2)))
			return false;
		}
	    }
	    return true;
	
    }

    /**
     * Returns true if all fields (excepted the primary and foreign keys) of the two given instances are equals. Returns false else.
     * @param _record1 the first record to compare
     * @param _record2 the second record to compare
     * @return true if all fields (excepted the primary and foreign keys) of the two given instances are equals. Returns false else.
     * @throws DatabaseException when a database exception occurs
     */
    public final boolean equalsAllFieldsWithoutPrimaryAndForeignKeys(T _record1, T _record2) throws DatabaseException
    {
	    if (_record1==_record2)
		return true;
	    if (_record1==null || _record2==null)
		return _record1==_record2;
	    for (FieldAccessor fa : fields_without_primary_and_foreign_keys)
	    {
		if (!fa.equals(_record1, fa.getValue(_record2)))
		    return false;
	    }
	    return true;
	
    }
    
    /**
     * Remove records which correspond to the given filter.
     * Records of other tables which have Foreign keys which points to the deleted records are also deleted.  
     * @param _filter the filter
     * @return the number of deleted records.
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final long removeRecordsWithCascade(final Filter<T> _filter) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return removeRecordsWithCascade(_filter, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    

    @SafeVarargs
    private final boolean hasRecordsWithOneOfSqlForeignKeyWithCascade(final HashMap<String, Object> ..._foreign_keys) throws DatabaseException
    {
	//try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableReadLock())
	{
	    final StringBuffer querry=new StringBuffer("SELECT * FROM "+this.getName()+" WHERE ");
	    boolean group_first=true;
	    boolean parenthesis=_foreign_keys.length>1;
	    
	    for (HashMap<String, Object> hm : _foreign_keys)
	    {
		if (group_first)
		    group_first=false;
		else
		    querry.append(" OR ");
		if (parenthesis)
		    querry.append("(");
		boolean first=true;
		for (String f : hm.keySet())
		{
		    if (first)
			first=false;
		    else
			querry.append(" AND ");
		    querry.append(f+" = ?");
		}
		if (parenthesis)
		    querry.append(")");
	    }
		
	    return ((Boolean)sql_connection.runTransaction(new Transaction() {
	        
		@Override
		public boolean doesWriteData()
		{
		    return false;
		}
		
	        @Override
	        public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
	        {
		    try(PreparedReadQuerry prq = new PreparedReadQuerry(_sql_connection.getSqlConnection(), querry.toString()))
		    {
			int index=1;
			for (HashMap<String, Object> hm : _foreign_keys)
			{
			    for (String f : hm.keySet())
			    {
				prq.statement.setObject(index++, hm.get(f));
			    }
			}
			if (prq.execute() && prq.result_set.next())
			{
			    return new Boolean(true);
			}
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		    
		    return new Boolean(false);
	        }
	    })).booleanValue();
	    
	}
    }
    private final long removeRecordsWithCascade(final Filter<T> _filter, boolean _is_already_sql_transaction) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	
	//try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableWriteLock())
	{
	if (isLoadedInMemory())
	{
	    final ArrayList<T> records_to_remove=new ArrayList<T>();
	    
	    for (T r : getRecords(_is_already_sql_transaction))
	    {
		if (_filter.nextRecord(r))
		{
		    records_to_remove.add(r);
		}
	    }
	    if (records_to_remove.size()>0)
	    {
		Transaction transaction=new Transaction() {
		        
			    @SuppressWarnings("synthetic-access")
			    @Override
			    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
			    {
				StringBuffer sb=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(records_to_remove.size()));

				int nb=0;
				try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), sb.toString()))
				{
				    int index=1;
				    for (T r : records_to_remove)
				    {
					for (FieldAccessor fa : primary_keys_fields)
					{
					    fa.getValue(r, puq.statement, index);
					    index+=fa.getDeclaredSqlFields().length;
					}
				    }
				    nb=puq.statement.executeUpdate();
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
				
				if (nb!=records_to_remove.size())
				{
				    throw new DatabaseException("Unexpected exception.");
				}
				return null;
			    }
		    	    @Override
		    	    public boolean doesWriteData()
			    {
			        return true;
			    }
			    
			};
		
		sql_connection.runTransaction(transaction);
		
		__removeRecords(records_to_remove);
		updateMemoryForRemovingRecordsWithCascade(records_to_remove);
	    }
	    return records_to_remove.size();
	}
	else
	{
	    class RunnableTmp extends Runnable
	    {
		public long deleted_records_number;
		@Override
		public void init(int _field_count)
		{
		    deleted_records_number=0;
		}

		@SuppressWarnings("synthetic-access")
		@Override
		public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
		{
		    try
		    {
			if (_filter.nextRecord(_instance))
			{
			    _cursor.deleteRow();
			    ++deleted_records_number;
			    
			    updateMemoryForRemovingRecordWithCascade(_instance);
			}
			return true;
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		    
		}
		    
	    }
	    RunnableTmp runnable=new RunnableTmp();
	    try
	    {
		getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect(), true);
	    }
	    catch(DatabaseException e)
	    {
		for (NeighboringTable nt : list_tables_pointing_to_this_table)
		{
		    Table<?> t=nt.getPointedTable();
		    if (t.isLoadedInMemory() && t.is_synchronized_with_sql_database.get())
		    {
			t.is_synchronized_with_sql_database.set(false);
			t.records_instances.set(null);
		    }
		}
		throw e;
	    }
	    
	    return runnable.deleted_records_number;
	}
	}
    }
    
    /**
     * Remove the given record from the database. 
     * @param _record the record to delete
     * @throws DatabaseException if a Sql exception occurs or if the given record has already been deleted.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if the given record is pointed by another record through a foreign key into another table of the database.
     */
    public final void removeRecord(final T _record) throws DatabaseException
    {
	if (_record==null)
	    throw new NullPointerException("The parameter _record is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
	
		for (NeighboringTable nt : list_tables_pointing_to_this_table)
		{
		    if (nt.getPointedTable().hasRecordsWithOneOfFields(nt.getHashMapFields(_record), false))
		    {
			throw new ConstraintsNotRespectedDatabaseException("The given record is pointed by another record through a foreign key into the table "+nt.getPointedTable().getName()+". Impossible to remove it into the table "+this.getName());
		    }
		}
	
		sql_connection.runTransaction(new Transaction() {
	    
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			StringBuffer querry=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(1));
		
			try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), querry.toString()))
			{
			    int index=1;
			    for (FieldAccessor fa : primary_keys_fields)
			    {
				fa.getValue(_record, puq.statement, index);
				index+=fa.getDeclaredSqlFields().length;
			    }
			    int nb=puq.statement.executeUpdate();
			    if (nb==0)
				throw new RecordNotFoundDatabaseException("the given record was not into the table "+Table.this.getName()+". It has been probably already removed.");
			    else if (nb>1)
				throw new DatabaseIntegrityException("Unexpected exception");
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
			
			return null;
		    }
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return true;
		    }
		    
		});
	
		if (Table.this.isLoadedInMemory() && is_synchronized_with_sql_database.get())
		    Table.this.__removeRecord(_record);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    
    /**
     * Remove the given record from the database. Remove also the records from other tables whose foreign keys points to the record to delete.
     * @param _record the record to delete
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws RecordNotFoundDatabaseException is the given record was not found into the database. This can occurs if the record has already been deleted.
     */
    public final void removeRecordWithCascade(final T _record) throws DatabaseException
    {
	if (_record==null)
	    throw new NullPointerException("The parameter _record is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
	

		class TransactionTmp implements Transaction
		{
		    
		    
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			StringBuffer querry=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(1));
		
		
			try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), querry.toString()))
			{
			    int index=1;
			    for (FieldAccessor fa : primary_keys_fields)
			    {
				fa.getValue(_record, puq.statement, index);
				index+=fa.getDeclaredSqlFields().length;
			    }
			    int nb=puq.statement.executeUpdate();
			    if (nb==0)
				throw new RecordNotFoundDatabaseException("the given record was not into the table "+Table.this.getName()+". It has been probably already removed.");
			    else if (nb>1)
				throw new DatabaseIntegrityException("Unexpected exception");
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
			
			return null;
		    }
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return true;
		    }
		    
		}

	    
		TransactionTmp transaction=new TransactionTmp();
		sql_connection.runTransaction(transaction);
		if (this.isLoadedInMemory() && is_synchronized_with_sql_database.get())
		    __removeRecord(_record);
		updateMemoryForRemovingRecordWithCascade(_record);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }

    private void updateMemoryForRemovingRecordWithCascade(T _record) throws DatabaseException
    {
	for (NeighboringTable nt : list_tables_pointing_to_this_table)
	{
	    Table<?> t=nt.getPointedTable();
	    if (t.isLoadedInMemory() && t.is_synchronized_with_sql_database.get())
	    {
		ArrayList<DatabaseRecord> removed_records=new ArrayList<DatabaseRecord>();
		Iterator<?> it=t.records_instances.get().iterator();
		for (;it.hasNext();)
		{
		    DatabaseRecord dr=(DatabaseRecord)it.next();
		    for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields)
		    {
			if (fkfa.equals(dr, _record))
			{
			    removed_records.add(dr);
			    it.remove();
			    break;
			}
		    }
		}
		t.updateMemoryForRemovingRecordsWithCascade2(removed_records);
	    }
	}
    }
    private void updateMemoryForRemovingRecordsWithCascade(Collection<T> _records) throws DatabaseException
    {
	for (NeighboringTable nt : list_tables_pointing_to_this_table)
	{
	    Table<?> t=nt.getPointedTable();
	    if (t.isLoadedInMemory() && t.is_synchronized_with_sql_database.get())
	    {
		ArrayList<DatabaseRecord> removed_records=new ArrayList<DatabaseRecord>();
		Iterator<?> it=t.records_instances.get().iterator();
		for (;it.hasNext();)
		{
		    DatabaseRecord dr=(DatabaseRecord)it.next();
		    boolean removed=false;
		    for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields)
		    {
			for (DatabaseRecord r : _records)
			{
			    if (fkfa.equals(dr, r))
			    {
				removed_records.add(dr);
				it.remove();
				removed=true;
				break;
			    }
			}
			if (removed)
			    break;
		    }
		}
		t.updateMemoryForRemovingRecordsWithCascade2(removed_records);
	    }
	}
    }
    private void updateMemoryForRemovingRecordsWithCascade2(Collection<DatabaseRecord> _records) throws DatabaseException
    {
	for (NeighboringTable nt : list_tables_pointing_to_this_table)
	{
	    Table<?> t=nt.getPointedTable();
	    if (t.isLoadedInMemory() && t.is_synchronized_with_sql_database.get())
	    {
		ArrayList<DatabaseRecord> removed_records=new ArrayList<DatabaseRecord>();
		Iterator<?> it=t.records_instances.get().iterator();
		for (;it.hasNext();)
		{
		    DatabaseRecord dr=(DatabaseRecord)it.next();
		    boolean removed=false;
		    for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields)
		    {
			for (DatabaseRecord r : _records)
			{
			    if (fkfa.equals(dr, r))
			    {
				removed_records.add(dr);
				it.remove();
				removed=true;
				break;
			    }
			}
			if (removed)
			    break;
		    }
		}
		t.updateMemoryForRemovingRecordsWithCascade2(removed_records);
	    }
	}
    }
   
    
    
    /**
     * Returns true if the given record is pointed by another record through a foreign key of another table.
     * @param _record the record to test
     * @return true if the given record is pointed by another record through a foreign key of another table.
     * @throws DatabaseException if a Sql exception occurs.
     */
    public final boolean isRecordPointedByForeignKeys(T _record) throws DatabaseException
    {
	try (Lock lock=new ReadLock(this))
	{
	    boolean res=false;
	    for (NeighboringTable nt : list_tables_pointing_to_this_table)
	    {
		Table<?> t = nt.getPointedTable();
		if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(_record)))
		{
		    res=true;
		    break;
		}
	    }
	    return res;
	}
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	
    }
    
    /**
     * From a given collection of records, returns those which are not pointed by other records through foreign keys of other tables into the database.
     * @param _records the collection of records to test
     * @return the records not pointed by other records through foreign keys of other tables into the database.
     * @throws DatabaseException if a Sql exception occurs.
     */
    public final ArrayList<T> getRecordsNotPointedByForeignKeys(Collection<T> _records) throws DatabaseException
    {
	try (Lock lock=new WriteLock(this))
	{
		ArrayList<T> res=new ArrayList<T>(_records.size());
		for (T r : _records)
		{
		    boolean toadd=true;
		    for (NeighboringTable nt : list_tables_pointing_to_this_table)
		    {
			Table<?> t = nt.getPointedTable();
			if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(r), false))
			{
			    toadd=false;
			    break;
			}
		    }
		    if (toadd)
			res.add(r);
		}
		return res;
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
    }
    
    private String getSqlPrimaryKeyCondition(int repeat)
    {
	StringBuffer sb=new StringBuffer();
	boolean parenthesis=repeat>1;
	boolean first_group=true;
	while (repeat-->0)
	{
	    if (first_group)
		first_group=false;
	    else
		sb.append(" OR ");
	    if (parenthesis)
		sb.append("(");
	    boolean first=true;
	    for(FieldAccessor fa : primary_keys_fields)
	    {
		for (SqlField sf : fa.getDeclaredSqlFields())
		{
		    if (first)
			first=false;
		    else 
			sb.append(" AND ");
		    sb.append(sf.field);
		    sb.append(" = ?");
		}
	    }
	    if (parenthesis)
		sb.append(")");
	}
	return sb.toString();
    }
    
    /**
     * Remove a list of records from the database. The records do not be pointed by other records through foreign keys of other tables.
     * @param _records the record to delete
     * @throws DatabaseException if a Sql exception occurs or if one of the given records has already been deleted.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if one of the given records is pointed by another record through a foreign key of another table.
     * @throws RecordNotFoundDatabaseException if one of the given records has not been found into the database. This may occurs if the record has already been deleted into the database.
     */
    public final void removeRecords(final Collection<T> _records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		for (NeighboringTable nt : list_tables_pointing_to_this_table)
		{
		    Table<?> t = nt.getPointedTable();
		    for (T record : _records)
			if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(record), false))
			    throw new ConstraintsNotRespectedDatabaseException("One of the given record is pointed by another record through a foreign key into the table "+t.getName()+". Impossible to remove this record into the table "+getName());
		}
		
		sql_connection.runTransaction(new Transaction() {
		    
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), "DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(_records.size())))
			{
			    int index=1;
			    for (T r : _records)
			    {
				for (FieldAccessor fa : primary_keys_fields)
				{
				    fa.getValue(r, puq.statement, index);
				    index+=fa.getDeclaredSqlFields().length;
				}
			    }
			    int number=puq.statement.executeUpdate();
			    if (number!=_records.size())
				throw new RecordNotFoundDatabaseException("There is "+(_records.size()-number)+" records which have not been found into the table "+Table.this.getName()+". No modification have been done.");
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
			
			return null;
		    }
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return true;
		    }
		    
		});
	
		if (isLoadedInMemory() && is_synchronized_with_sql_database.get())
		{
		    __removeRecords(_records);
		}
	
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    /**
     * Remove a list of records from the database. Remove also all records from other tables whose foreign key point to the deleted record.
     * @param _records the record to delete
     * @throws DatabaseException if a Sql exception occurs.
     * @throws NullPointerException if parameters are null pointers.
     * @throws RecordNotFoundDatabaseException if one of the given records was not found found into the database. This may occur if the concerned record has already been deleted.
     */
    public final void removeRecordsWithCascade(Collection<T> _records) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try(Lock lock=new WriteLock(this))
	    {
		removeRecordsWithCascade(_records, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    private final void removeRecordsWithCascade(final Collection<T> _records, boolean is_already_in_transaction) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	//synchronized(sql_connection)
	{
	    class TransactionTmp implements Transaction
	    {
		public TransactionTmp()
		{
		 }

		@SuppressWarnings("synthetic-access")
		@Override
		public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		{
		    StringBuffer sb=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(_records.size()));
		    
		    try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), sb.toString()))
		    {
			int index=1;
			for (T r : _records)
			{
			    for (FieldAccessor fa : primary_keys_fields)
			    {
				fa.getValue(r, puq.statement, index);
				index+=fa.getDeclaredSqlFields().length;
			    }
			}
			int nb=puq.statement.executeUpdate();
			if (nb!=_records.size())
			    throw new RecordNotFoundDatabaseException("There is "+(_records.size()-nb)+" (about "+_records.size()+") records which have not been found into the database. This may occur if the concerned record has already been deleted. No records have been deleted.");
			
		    }
		    catch(Exception e)
		    {
			throw DatabaseException.getDatabaseException(e);
		    }
		    
		    return null;
		}
	    	@Override
	    	public boolean doesWriteData()
	    	{
	    	    return true;
	    	}
		
		    
	    }
	    TransactionTmp transaction=new TransactionTmp();
	    sql_connection.runTransaction(transaction);
	    if (isLoadedInMemory() && is_synchronized_with_sql_database.get())
	    {
		__removeRecords(_records);
	    }
	    updateMemoryForRemovingRecordsWithCascade(_records);
	}
    }
    
    private abstract class Runnable
    {
	public Runnable()
	{
	    
	}
	public abstract void init(int _field_count);
	public abstract boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException;
    }
    
    private final void getListRecordsFromSqlConnection(final Runnable _runnable, final String querry) throws DatabaseException
    {
	getListRecordsFromSqlConnection(_runnable, querry, false);
    }
    private final void getListRecordsFromSqlConnection(final Runnable _runnable, final String querry, final boolean updatable) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    
		
		class TransactionTmp implements Transaction
		{
		    protected final Constructor<T> default_constructor_field;
		    protected final ArrayList<FieldAccessor> fields_accessor;
		    
		    public TransactionTmp(Constructor<T> _default_constructor_field, ArrayList<FieldAccessor> _fields_accessor)
		    {
			default_constructor_field=_default_constructor_field;
			fields_accessor=_fields_accessor;
			
		    }
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(DatabaseWrapper sql_connection) throws DatabaseException
		    {
			
			try(AbstractReadQuerry rq=(updatable?new UpdatableReadQuerry(sql_connection.getSqlConnection(), querry):new ReadQuerry(sql_connection.getSqlConnection(), querry)))
			{
			    int rowcount=getRowCount();
			    _runnable.init(rowcount);
			    
			    while (rq.result_set.next())
			    {
				T field_instance = default_constructor_field.newInstance();
				
				for (FieldAccessor f : fields_accessor)
				{
				    f.setValue(field_instance, rq.result_set);
				}
				rowcount--;
				if (!_runnable.setInstance(field_instance, rq.result_set))
				{
				    rowcount=0;
				    break;
				}
			    }
			    if (rowcount!=0)
				throw new DatabaseException("Unexpected exception "+rowcount);
			    return null;
			}
			catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
			{
			    throw new DatabaseException("Impossible to instantiate a DatabaseRecord ", e);
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
		    }
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return updatable;
		    }
		    
		    
		}
		
		Transaction transaction=new TransactionTmp(default_constructor_field, fields);
		sql_connection.runTransaction(transaction);
	    }
    }
    private abstract class Runnable2
    {
	public Runnable2()
	{
	    
	}
	public abstract void init(int _field_count);
	public abstract boolean setInstance(ResultSet _cursor) throws DatabaseException;
    }

    private final void getListRecordsFromSqlConnection(final Runnable2 _runnable, final String querry) throws DatabaseException
    {
	getListRecordsFromSqlConnection(_runnable, querry, false);
    }
    private final void getListRecordsFromSqlConnection(final Runnable2 _runnable, final String querry, final boolean updatable) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
		class TransactionTmp implements Transaction
		{
		    public TransactionTmp()
		    {
		    }
		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			try(AbstractReadQuerry rq=(updatable?new UpdatableReadQuerry(_sql_connection.getSqlConnection(), querry):new ReadQuerry(_sql_connection.getSqlConnection(), querry)))
			{
			    @SuppressWarnings("synthetic-access")
			    int rowcount=getRowCount();
			    _runnable.init(rowcount);
			    
			    while (rq.result_set.next())
			    {
				rowcount--;
				if (!_runnable.setInstance(rq.result_set))
				{
				    rowcount=0;
				    break;
				}
			    }
			    if (rowcount!=0)
				throw new DatabaseException("Unexpected exception "+rowcount);
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
		        return updatable;
		    }
		    
		    
		}
		
		Transaction transaction=new TransactionTmp();
		sql_connection.runTransaction(transaction);
	}
    }
    /**
     * 
     * @param _record the record
     * @return true if the given record is contained into the table. false, else.
     * @throws DatabaseException when a database exception occurs
     */
    public final boolean contains(final T _record) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try (Lock lock=new ReadLock(this))
	    {
		return contains(_record, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    @SuppressWarnings("unchecked")
    private final boolean contains(boolean is_already_in_transaction, final DatabaseRecord _record) throws DatabaseException
    {
	return contains((T)_record, false);
    }

    private final boolean contains(final T _record, boolean is_already_in_transaction) throws DatabaseException
    {
	if (isLoadedInMemory())
	{
	    for (T r : getRecords(is_already_in_transaction))
	    {
		if (equals(_record, r))
		    return true;
	    }
	    return false;
	}
	else
	{
		Transaction transaction=new Transaction() {
		        
			    @SuppressWarnings("synthetic-access")
			    @Override
			    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
			    {
				try(PreparedReadQuerry rq=new PreparedReadQuerry(_sql_connection.getSqlConnection(), "SELECT * FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(1)))
				{
				    int index=1;
				    for (FieldAccessor fa : primary_keys_fields)
				    {
					fa.getValue(_record, rq.statement, index);
					index+=fa.getDeclaredSqlFields().length;
				    }
				    if (rq.execute() && rq.result_set.next())
					return new Boolean(true);
				    else
					return new Boolean(false);
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
				
			    }
		    	    @Override
		    	    public boolean doesWriteData()
			    {
			        return false;
			    }
			    
			};
		Boolean found=(Boolean)sql_connection.runTransaction(transaction);
		return found.booleanValue();
	}
    }
    
    /**
     * Add a record into the database with a map of fields corresponding to this record. 
     * The string type in the Map corresponds to the name of the field, and the Object type field corresponds the value of the field.   
     * @param _fields the list of fields to include into the new record
     * @return the created record
     * @throws DatabaseException if a problem occurs during the insertion into the Sql database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if the given primary keys already exists into the table, or if a field which has the unique property exists already into the table.
     * @throws FieldDatabaseException if one of the given fields does not exists into the database, or if fields are lacking.
     * @throws RecordNotFoundDatabaseException if one of the field is a foreign key and point to a record of another table which does not exist. 
     */
    public final T addRecord(final Map<String, Object> _fields) throws DatabaseException
    {
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		return addRecord(_fields, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    private final T addRecord(final Map<String, Object> _fields, boolean already_in_transaction) throws DatabaseException
    {

	{
	    
	    {
		int number=0;
		for (FieldAccessor fa : fields)
		{
		    if (!fa.isAutoPrimaryKey() && !fa.isRandomPrimaryKey())
		    {
			Object obj = _fields.get(fa.getFieldName());
			if (obj==null)
			{
			    if (fa.isNotNull())
				throw new FieldDatabaseException("The field "+fa.getFieldName()+"is not present into the given fields.");
			}
			else
			    number++;
		    }
		}
		if (number>_fields.size())
		    throw new FieldDatabaseException("The number ("+_fields.size()+") of given fields does not correspond to the expected minimum number ("+number+") of fields (Null fields, AutoPrimaryKeys and RandomPrimaryKeys are excluded).");
		try
		{
		    class CheckTmp extends Runnable2
		    {
			public boolean check_necessary=false;
			public final ArrayList<FieldAccessor> random_fields_to_check=new ArrayList<FieldAccessor>();
			public boolean include_auto_pk=false;
			public CheckTmp(ArrayList<FieldAccessor> _auto_random_primary_keys_fields) 
			{
		    
			    for (FieldAccessor fa : _auto_random_primary_keys_fields)
			    {
				if (fa.isRandomPrimaryKey() && _fields.containsKey(fa.getFieldName()))
				{
				    if (_fields.get(fa.getFieldName())==null)
					_fields.remove(fa.getFieldName());
				    else
				    {
					random_fields_to_check.add(fa);
					check_necessary=true;
				    }
				}
				if (fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName()))
				{
				    if (_fields.get(fa.getFieldName())==null)
					_fields.remove(fa.getFieldName());
				    else
					include_auto_pk=true;
				}
			    }
			}
			@Override
			public void init(int _field_count)
			{
			}

			@Override
			public boolean setInstance(ResultSet _result_set) throws DatabaseException
			{
			    for (FieldAccessor fa : random_fields_to_check)
			    {
				if (fa.equals(_fields.get(fa.getFieldName()), _result_set))
				    throw new ConstraintsNotRespectedDatabaseException("the given record have the same unique auto/random primary key field "+fa.getFieldName()+" of one of the records stored into the database. No record have been added.");
			    }
			    return true;
			}
			
		    }
		    final CheckTmp ct=new CheckTmp(auto_random_primary_keys_fields);
		    if (ct.check_necessary)
			getListRecordsFromSqlConnection(ct, getSqlGeneralSelect());
	    
		    final T instance=default_constructor_field.newInstance();
		    if (isLoadedInMemory())
		    {
			for (FieldAccessor fa : fields)
			{
			    Object obj;
			    if (fa.isRandomPrimaryKey() && !ct.random_fields_to_check.contains(fa))
			    {
				if (fa.isAssignableTo(BigInteger.class))
				{
				    ArrayList<T> fields_instances=getRecords(false);
				    BigInteger value;
				    boolean ok=false;
				    do
				    {
					ok=false;
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber());
					for (T f : fields_instances)
					{
					    if (fa.equals(f, value))
					    {
						ok=true;
						break;
					    }
					}
				    } while (ok);
				    fa.setValue(instance, value);
				}
				else if (fa.isAssignableTo(long.class))
				{
				    ArrayList<T> fields_instances=getRecords(false);
				    long value;
				    boolean ok=false;
				    do
				    {
					ok=false;
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber()).longValue();
					for (T f : fields_instances)
					{
					    if (fa.equals(f, new Long(value)))
					    {
						ok=true;
						break;
					    }
					}
				    } while (ok);
				    fa.setValue(instance, new Long(value));
				}
				else if (fa.isAssignableTo(int.class))
				{
				    ArrayList<T> fields_instances=getRecords(false);
				    int value;
				    boolean ok=false;
				    do
				    {
					ok=false;
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber()).intValue();
					for (T f : fields_instances)
					{
					    if (fa.equals(f, new Integer(value)))
					    {
						ok=true;
						break;
					    }
					}
				    } while (ok);
				    fa.setValue(instance, new Integer(value));
				}
			    }
			    else if ((fa.isAutoPrimaryKey() && ct.include_auto_pk) || !fa.isAutoPrimaryKey()) 
			    {
				obj=_fields.get(fa.getFieldName());
				fa.setValue(instance, obj);
			    }
			}
		    }
		    else
		    {
			for (final FieldAccessor fa : fields)
			{
			    if (fa.isRandomPrimaryKey() && !ct.random_fields_to_check.contains(fa))
			    {
				if (fa.isAssignableTo(BigInteger.class))
				{
				    BigInteger value;
				    boolean ok=false;
				    do
				    {
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber());
			    	    	final BigInteger val=value;
			    	    	
			    	    	class RunnableTmp extends Runnable2
			    	    	{
			    	    	    public boolean ok=false;
			    	    	    @Override
			    	    	    public void init(int _field_count)
			    	    	    {
			    	    	    }
			    	    	    
			    	    	    @Override
			    	    	    public boolean setInstance(ResultSet _result_set) throws DatabaseException
			    	    	    {
			    	    		boolean res=!fa.equals(val, _result_set);
			    	    		if (!ok && !res)
			    	    		    ok=true;
			    	    		return res;
			    	    	    }
			    	    	}
			    	    	RunnableTmp runnable=new RunnableTmp();
			    	
			    	    	getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
			    	    	ok=runnable.ok;
				    } while (ok);
				    fa.setValue(instance, value);
				}
				else if (fa.isAssignableTo(long.class))
				{
				    long value;
				    boolean ok=false;
				    do
				    {
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber()).longValue();
			    	    	final Long val=new Long(value);
			    	    	
			    	    	class RunnableTmp extends Runnable2
			    	    	{
			    	    	    public boolean ok=false;
			    	    	    @Override
			    	    	    public void init(int _field_count)
			    	    	    {
			    	    	    }
			    	    	    
			    	    	    @Override
			    	    	    public boolean setInstance(ResultSet _result_set) throws DatabaseException
			    	    	    {
			    	    		boolean res=!fa.equals(val, _result_set);
			    	    		if (!ok && !res)
			    	    		    ok=true;
			    	    		return res;
			    	    	    }
			    	    	}
			    	    	RunnableTmp runnable=new RunnableTmp();
			    	    	
			    	    	getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
			    	    	ok=runnable.ok;
				    } while (ok);
				    fa.setValue(instance, new Long(value));
				}
				else if (fa.isAssignableTo(int.class))
				{
				    int value;
				    boolean ok=false;
				    do
				    {
					value=getRandomPositiveBigIntegerValue(fa.getBitsNumber()).intValue();
			    	    	final Integer val=new Integer(value);
			    	    	
			    	    	class RunnableTmp extends Runnable2
			    	    	{
			    	    	    public boolean ok=false;
			    	    	    @Override
			    	    	    public void init(int _field_count)
			    	    	    {
			    	    	    }
			    	    	    
			    	    	    @Override
			    	    	    public boolean setInstance(ResultSet _result_set) throws DatabaseException
			    	    	    {
			    	    		boolean res=!fa.equals(val, _result_set);
			    	    		if (!ok && !res)
			    	    		    ok=true;
			    	    		return res;
			    	    	    }
			    	    	}
			    	    	RunnableTmp runnable=new RunnableTmp();
			    	    	
			    	    	getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
			    	    	ok=runnable.ok;
				    } while (ok);
				    fa.setValue(instance, new Integer(value));
				}
			    }
			    else if ((fa.isAutoPrimaryKey() && ct.include_auto_pk) || !fa.isAutoPrimaryKey())
			    {
				fa.setValue(instance, _fields.get(fa.getFieldName()));
			    }
			}
		    }
		    class TransactionTmp implements Transaction
		    {
			
			protected final ArrayList<FieldAccessor> auto_primary_keys_fields;
			protected final ArrayList<ForeignKeyFieldAccessor> foreign_keys_fields;
			public TransactionTmp(ArrayList<FieldAccessor> _auto_primary_keys_fields, ArrayList<ForeignKeyFieldAccessor> _foreign_keys_fields)
			{
			    auto_primary_keys_fields=_auto_primary_keys_fields;
			    foreign_keys_fields=_foreign_keys_fields;
			}
			
			@Override
			public boolean doesWriteData()
			{
			    return true;
			}
			
			@SuppressWarnings("synthetic-access")
			@Override
			public Object run(DatabaseWrapper _db) throws DatabaseException
			{
			    try
			    {
				for (ForeignKeyFieldAccessor fa : foreign_keys_fields)
				{
				    Object val=fa.getValue(instance);
				    if (!fa.getPointedTable().contains(true, (DatabaseRecord)val))
					throw new RecordNotFoundDatabaseException("The record, contained as foreign key into the field "+fa.getFieldName()+" into the table "+Table.this.getName()+" does not exists into the table "+fa.getPointedTable().getName());
				}
				
				StringBuffer querry=new StringBuffer("INSERT INTO "+Table.this.getName()+"(");
				boolean first=true;
				for(FieldAccessor fa : fields)
				{
				    if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName())) || !fa.isAutoPrimaryKey())
				    {
					for (SqlField sf : fa.getDeclaredSqlFields())
					{
					    if (first)
						first=false;
					    else
						querry.append(", ");
					    querry.append(sf.short_field);
					}
				    }
				}
				querry.append(") VALUES(");
				first=true;
				for(FieldAccessor fa : fields)
				{
				    if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName())) || !fa.isAutoPrimaryKey())
				    {
					for (int i=0;i<fa.getDeclaredSqlFields().length;i++)
					{
					    if (first)
						first=false;
					    else
						querry.append(", ");
					    querry.append("?");
					}
				    }
				}
				querry.append(")"+sql_connection.getSqlComma());
				
				try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_db.getSqlConnection(), querry.toString()))
				{
				    int index=1;
				    for(FieldAccessor fa : fields)
				    {
					if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName())) || !fa.isAutoPrimaryKey())
					{
					    fa.getValue(instance, puq.statement, index);
					    index+=fa.getDeclaredSqlFields().length;
					}
				    }
				    puq.statement.executeUpdate();
				    /*if (auto_primary_keys_fields.size()>0 && !ct.include_auto_pk)
				    {
					puq.statement.getGeneratedKeys().next();
					autovalue=new Long(puq.statement.getGeneratedKeys().getLong(1));
				    }*/
				}
				catch(SQLIntegrityConstraintViolationException e)
				{
				    throw new ConstraintsNotRespectedDatabaseException("Constraints was not respected when inserting a field into the table "+Table.this.getName()+". It is possible that the group of primary keys was not unique, or that a unique field was already present into the database.", e);
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
				
				if (auto_primary_keys_fields.size()>0 && !ct.include_auto_pk)
				{
				    try(ReadQuerry rq=new ReadQuerry(_db.getSqlConnection(), sql_connection.getSqlQuerryToGetLastGeneratedID()))
				    {
					rq.result_set.next();
					Long autovalue=new Long(rq.result_set.getLong(1));
					FieldAccessor fa=auto_primary_keys_fields.get(0);
					if (fa.isAssignableTo(byte.class))
					    fa.setValue(instance, new Byte((byte)autovalue.longValue()));
					else if (fa.isAssignableTo(short.class))
					    fa.setValue(instance, new Short((short)autovalue.longValue()));
					else if (fa.isAssignableTo(int.class))
					    fa.setValue(instance, new Integer((int)autovalue.longValue()));
					else if (fa.isAssignableTo(long.class))
					    fa.setValue(instance, autovalue);
				    }
				    catch(Exception e)
				    {
					throw DatabaseException.getDatabaseException(e);
				    }
				}

				
				return null;
			    }
			    catch(Exception e)
			    {
				throw DatabaseException.getDatabaseException(e);
			    }
			}
			
		    }
		
		    sql_connection.runTransaction(new TransactionTmp(auto_primary_keys_fields, foreign_keys_fields));
	    
		    if (isLoadedInMemory() && is_synchronized_with_sql_database.get())
			__AddRecord(instance);
		    return instance;

		}
		catch(IllegalArgumentException e)
		{
		    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
		}
		catch(InstantiationException e)
		{
		    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
		}
		catch(IllegalAccessException e)
		{
		    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
		}
		catch(InvocationTargetException e)
		{
		    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
		}
	
	    }
	}
    }
    /**
     * Add a collection of records into the database with a collection of maps of fields corresponding to these records. 
     * The string type in the Map corresponds to the name of the field, and the Object type field corresponds the value of the field.   
     * @param _records the list of fields of every record to include into the database
     * @return the created records
     * @throws DatabaseException if a problem occurs during the insertion into the Sql database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if the given primary keys already exists into the table, or if a field which has the unique property exists already into the table.
     * @throws FieldDatabaseException if one of the given fields does not exists into the database, or if fields are lacking.
     * @throws RecordNotFoundDatabaseException if one of the field is a foreign key and point to a record of another table which does not exist. 
     */
    @SafeVarargs
    public final ArrayList<T> addRecords(final Map<String, Object> ..._records) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try(Lock l=new WriteLock(this))
	    {
		ArrayList<T> res=new ArrayList<T>();
		for (Map<String, Object> m : _records)
		    res.add(addRecord(m, false));
		return res;
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    
    /*@SafeVarargs
    public final ArrayList<T> addRecords(final Map<String, Object> ..._records) throws DatabaseException
    {
	if (_records==null)
	    throw new NullPointerException("The parameter _records is a null pointer !");
	if (_records.length==0)
	    throw new NullPointerException("The parameter _records is an empty tab !");
	synchronized(sql_jet_db)
	{
	try (Lock lock=new WriteLock(this))
	{
	for (Map<String, Object> m : _records)
	{
	    int number=0;
	    for (FieldAccessor fa : fields)
	    {
		if (!fa.isAutoPrimaryKey() && !fa.isRandomPrimaryKey())
		{
		    Object obj = m.get(fa.getFieldName());
		    if (obj==null)
		    {
			if (fa.isNotNull())
			    throw new FieldDatabaseException("The field "+fa.getFieldName()+"is not present into the given fields.");
		    }
		    else
			number++;
		}
	    }
	    if (number>m.size())
		throw new FieldDatabaseException("The number ("+m.size()+") of given fields does into one of the given records not correspond to the expected number ("+number+") of minimum fields (Null fields, AutoPrimaryKeys and RandomPrimaryKeys are excluded).");
	}
	try
	{
	    class CheckTmp extends Runnable2
	    {
		public boolean check_necessary=false;
		private boolean check_pk=false;
		private boolean check_unique=false;
		protected final ArrayList<FieldAccessor> primary_keys_fields_no_auto_no_random;
		protected final ArrayList<FieldAccessor> unique_fields_no_auto_random_primary_keys;
		private ArrayList<HashMap<String, Object>> fields_instances=new ArrayList<HashMap<String, Object>>();
		public ArrayList<ArrayList<FieldAccessor>> auto_random_fields_to_check=new ArrayList<ArrayList<FieldAccessor>>();
		public ArrayList<Boolean> has_auto_field=new ArrayList<Boolean>();
		public CheckTmp(ArrayList<FieldAccessor> _primary_keys_fields_no_auto_no_random, ArrayList<FieldAccessor> _unique_fields_no_auto_random_primary_keys, ArrayList<FieldAccessor> _auto_random_primary_keys_fields) 
		{
		    primary_keys_fields_no_auto_no_random=_primary_keys_fields_no_auto_no_random;
		    unique_fields_no_auto_random_primary_keys=_unique_fields_no_auto_random_primary_keys;
		    
		    if (_auto_random_primary_keys_fields.size()==0 && primary_keys_fields_no_auto_no_random.size()>0)
			check_necessary=check_pk=true;
		    if (unique_fields_no_auto_random_primary_keys.size()>0)
			check_necessary=check_unique=true;
		    for (int i=0;i<_records.length;i++)
		    {
			ArrayList<FieldAccessor> arf=new ArrayList<FieldAccessor>(); 
			for (FieldAccessor fa : _auto_random_primary_keys_fields)
			{
			    Map<String, Object> m=_records[i];
			    if (m.containsKey(fa.getFieldName()))
			    {
				if (m.get(fa.getFieldName())==null)
				{
				    m.remove(fa.getFieldName());
				    has_auto_field.add(new Boolean(false));
				}
				else
				{
				    if (fa.isAutoPrimaryKey())
					has_auto_field.add(new Boolean(true));
				    else
					has_auto_field.add(new Boolean(false));
				    arf.add(fa);
				    check_necessary=check_unique=true;
				}
			    }
			    else
				has_auto_field.add(new Boolean(false));
			}
			random_fields_to_check.add(arf);
		    }
		    if (check_necessary)
		    {
			fields_instances.ensureCapacity(_records.length);
			for (int i=0;i<_records.length;i++)
			{
			    fields_instances.add(new HashMap<String, Object>());
			}
		    }
		    if (check_pk)
		    {
			for (int i=0;i<_records.length;i++)
			{
			    Map<String, Object> m=_records[i];
			    HashMap<String, Object> newm=fields_instances.get(i);
			    for (FieldAccessor fa : primary_keys_fields_no_auto_no_random)
			    {
				if (fa.isForeignKey())
				{
				    DatabaseRecord dr=(DatabaseRecord)m.get(fa.getFieldName());
				    SqlField sf = fa.getDeclaredSqlField();
				    newm.put(sf.field, new Long(dr.getRecordID()));
				}
				else
				{
				    newm.put(fa.getDeclaredSqlField().field, m.get(fa.getFieldName()));
				}
			    }
			}
		    }
		    if (check_unique)
		    {
			for (int i=0;i<_records.length;i++)
			{
			    Map<String, Object> m=_records[i];
			    HashMap<String, Object> newm=fields_instances.get(i);
			    for (FieldAccessor fa : unique_fields_no_auto_random_primary_keys)
			    {
				if (!primary_keys_fields_no_auto_no_random.contains(fa))
				{
				    if (fa.isForeignKey())
				    {
					DatabaseRecord dr=(DatabaseRecord)m.get(fa.getFieldName());
					SqlField sf = fa.getDeclaredSqlField();
					newm.put(sf.field, new Long(dr.getRecordID()));
				    }
				    else
				    {
					newm.put(fa.getDeclaredSqlField().field, m.get(fa.getFieldName()));
				    }
				}
			    }
			    for (FieldAccessor fa : random_fields_to_check.get(i))
			    {
				newm.put(fa.getDeclaredSqlField().field, m.get(fa.getFieldName()));
			    }
			}
		    }
		}
		@Override
		public void init(long _field_count)
		{
		}

		@Override
		public boolean setInstance(Cursor _cursor) throws DatabaseException
		{
			if (check_pk)
			{
			    for (HashMap<String, Object> m : fields_instances)
			    {
				boolean allok=true;
				for (FieldAccessor fa : primary_keys_fields_no_auto_no_random)
				{
				    SqlField sf = fa.getDeclaredSqlField();
				    if (!FieldAccessor.equals(_cursor.getValue(sf.field), m.get(sf.field)))
				    {
					allok=false;
					break;
				    }
				}
				if (allok)
				    throw new ConstraintsNotRespectedDatabaseException("One of the given record have the same primary keys of one of the records stored into the database. No record have been added");
			    }
			}
			if (check_unique)
			{
			    for (int i=0;i<fields_instances.size();i++)
			    {
				HashMap<String, Object> m =fields_instances.get(i);
				for (FieldAccessor fa : unique_fields_no_auto_random_primary_keys)
				{
				    boolean allequals=true;
				    SqlField sf = fa.getDeclaredSqlField();
				    if (!FieldAccessor.equals(_cursor.getValue(sf.field), m.get(sf.field)))
				    {
					allequals=false;
					break;
				    }
				    if (allequals)
					throw new ConstraintsNotRespectedDatabaseException("the given record have the same unique field "+fa.getFieldName()+" of one of the records stored into the database. No record have been added.");
				}
				for (FieldAccessor fa : random_fields_to_check.get(i))
				{
				    boolean allequals=true;
				    SqlField sf = fa.getDeclaredSqlField();
				    if (!FieldAccessor.equals(_cursor.getValue(sf.field), m.get(sf.field)))
				    {
					allequals=false;
					break;
				    }
				    if (allequals)
					throw new ConstraintsNotRespectedDatabaseException("the given record have the same unique auto/random primary key field "+fa.getFieldName()+" of one of the records stored into the database. No record have been added.");
				}
			    }
			}
			return true;
		}
		
	    }
	    final CheckTmp ct=new CheckTmp(primary_keys_fields_no_auto_no_random, unique_fields_no_auto_random_primary_keys, auto_random_primary_keys_fields);
	    if (ct.check_necessary)
		getListRecordsFromSqlJet(ct);
	    
	    
	    final ArrayList<T> instances=new ArrayList<T>(_records.length);
	    for (int i=0;i<_records.length;i++)
		instances.add(default_constructor_field.newInstance());
	    ArrayList<ArrayList<Object>> random_values=new ArrayList<ArrayList<Object>>();
	    
	    for (int i=0;i<fields.size();i++)
	    {
		FieldAccessor fa = fields.get(i);
		if (fa.isRandomPrimaryKey())
		{
		    ArrayList<Object> values=new ArrayList<Object>(_records.length);
		    random_values.add(values);
		    for (int j=0;j<_records.length;j++)
		    {
			Map<String, Object> map = _records[j];
			if (map.containsKey(fa.getFieldName()))
			{
			    Object val=map.get(fa.getFieldName());
			    for (Object val2 : values)
			    {
				if (FieldAccessor.equals(val, val2))
				    throw new ConstraintsNotRespectedDatabaseException("There at minimum two of the given records whose fields "+fa.getFieldName()+" have the same random primary keys. Random primary keys must be unique values.");
			    }
			    values.add(val);
			}
		    }
		}
		else
		    random_values.add(new ArrayList<Object>());
	    }
	    final ArrayList<Long> auto_primarykeys_values=new ArrayList<Long>(_records.length);
	    if (isLoadedInMemory())
	    {
		
		for (int i=0;i<instances.size();i++)
		{
		    T instance=instances.get(i);
		    Map<String, Object> map=_records[i];
		    
		    for (int j=0;j<fields.size();j++)
		    {
			FieldAccessor fa = fields.get(j);
			if (fa.isRandomPrimaryKey())
			{
			    ArrayList<Object> values=random_values.get(j);
			    if (ct.auto_random_fields_to_check.get(i).contains(fa))
			    {
				fa.setValue(instance, map.get(fa.getFieldName()));
			    }
			    else
			    {
				if (fa.isAssignableTo(BigInteger.class))
				{
				    ArrayList<T> fields_instances=getRecords(false);
				    BigInteger value;
				    boolean ok=false;
				    do
				    {
					ok=false;
					value=getRandomPositive128BitsValue();
					for (T f : fields_instances)
					{
					    if (fa.equals(f, value))
					    {
						ok=true;
						break;
					    }
					}
					if (!ok)
					{
					    for (Object val : values)
					    {
						if (FieldAccessor.equals(val, value))
						{
						    ok=true;
						}
					    }
					}
				    } while (ok);
				    values.add(value);
				    fa.setValue(instance, value);
				}
				else
				{
				    ArrayList<T> fields_instances=getRecords(false);
				    long value;
				    boolean ok=false;
				    do
				    {
					ok=false;
					value=getRandomPositiveLongValue();
					for (T f : fields_instances)
					{
					    if (fa.equals(f, new Long(value)))
					    {
						ok=true;
						break;
					    }
					}
					if (!ok)
					{
					    for (Object val : values)
					    {
						if (FieldAccessor.equals(val, new Long(value)))
						{
						    ok=true;
						}
					    }
					}
				    } while (ok);
				    values.add(new Long(value));
				    fa.setValue(instance, new Long(value));
				}
			    }
			}
			else if ((fa.isAutoPrimaryKey() && ct.has_auto_field.get(i).booleanValue()) || !fa.isAutoPrimaryKey())
			{
			    Object val=map.get(fa.getFieldName());
			    if (fa.isAutoPrimaryKey())
			    {
				for (Long val2 : auto_primarykeys_values)
				{
				    if (FieldAccessor.equals(val, val2))
					throw new ConstraintsNotRespectedDatabaseException("There at minimum two of the given records whose fields "+fa.getFieldName()+" have the same auto primary keys. Auto primary keys must be unique values.");
				}
				auto_primarykeys_values.add((Long)fa.getValue(instance));
			    }
			    fa.setValue(instance, val);
			    
			}
		    }
		}
	    }
	    else
	    {
		for (int i=0;i<instances.size();i++)
		{
		    T instance=instances.get(i);
		    Map<String, Object> map=_records[i];
		    
		    for (int j=0;j<fields.size();j++)
		    {
			final FieldAccessor fa = fields.get(j);
			if (fa.isRandomPrimaryKey())
			{
			    ArrayList<Object> values=random_values.get(j);
			    if (ct.auto_random_fields_to_check.get(i).contains(fa))
			    {
				fa.setValue(instance, map.get(fa.getFieldName()));
			    }
			    else
			    {
				if (fa.isAssignableTo(BigInteger.class))
				{
				    BigInteger value;
				    boolean ok=false;
				    do
				    {
					do
					{
					    ok=false;
					    value=getRandomPositive128BitsValue();
					    for (Object val : values)
					    {
						if (FieldAccessor.equals(val, value))
						{
						    ok=true;
						}
					    }
					} while(ok);
			    	    	final BigInteger val=value;
			    	    
					class RunnableTmp extends Runnable2
			    	    	{
			    	    	    public boolean ok=false;
			    	    	    @Override
			    	    	    public void init(long _field_count)
			    	    	    {
			    	    	    }

			    	    	    @Override
			    	    	    public boolean setInstance(Cursor _cursor) throws DatabaseException
			    	    	    {
			    	    		    boolean res=!FieldAccessor.equals(_cursor.getValue(fa.getDeclaredSqlField().field), val);
			    	    		    if (!ok && !res)
			    	    			ok=true;
			    	    		    return res;
			    	    	    }
			    	    	}
			    	    	
			    	    	RunnableTmp runnable=new RunnableTmp();
			    	    	getListRecordsFromSqlJet(runnable);
			    	    	ok=runnable.ok;
				    } while (ok);
				    values.add(value);
				    fa.setValue(instance, value);
				}
				else
				{
				    long value;
				    boolean ok=false;
				    do
				    {
					do
					{
					    ok=false;
					    value=getRandomPositiveLongValue();
					    for (Object val : values)
					    {
						if (FieldAccessor.equals(val, new Long(value)))
						{
						    ok=true;
						}
					    }
					} while(ok);

			    	    	final Long val=new Long(value);
			    	    	
					class RunnableTmp extends Runnable2
			    	    	{
			    	    	    public boolean ok=false;
			    	    	    @Override
			    	    	    public void init(long _field_count)
			    	    	    {
			    	    	    }
			    	    	    
			    	    	    @Override
			    	    	    public boolean setInstance(Cursor _cursor) throws DatabaseException
			    	    	    {
			    	    		    boolean res=!FieldAccessor.equals(_cursor.getValue(fa.getDeclaredSqlField().field), val);
			    	    		    if (!ok && !res)
			    	    			ok=true;
			    	    		    return res;
			    	    	    }
			    	    	}
			    	    	RunnableTmp runnable=new RunnableTmp();
			    	    	getListRecordsFromSqlJet(runnable);
			    	    	ok=runnable.ok;
				    } while (ok);
				    values.add(new Long(value));
				    fa.setValue(instance, new Long(value));
				}
			    }
			}
			else if ((fa.isAutoPrimaryKey() && ct.has_auto_field.get(i).booleanValue()) || !fa.isAutoPrimaryKey())
			{
			    Object val=map.get(fa.getFieldName());
			    if (fa.isAutoPrimaryKey())
			    {
				for (Long val2 : auto_primarykeys_values)
				{
				    if (FieldAccessor.equals(val, val2))
					throw new ConstraintsNotRespectedDatabaseException("There at minimum two of the given records whose fields "+fa.getFieldName()+" have the same auto primary keys. Auto primary keys must be unique values.");
				}
				auto_primarykeys_values.add((Long)fa.getValue(instance));
			    }
			    fa.setValue(instance, val);
			}
		    }
		}
	    }
		class TransactionTmp implements Transaction
		{
		    protected final ISqlJetTable sql_jet_table;
		    protected final ArrayList<FieldAccessor> auto_primary_keys_fields;
		    protected final ArrayList<ForeignKeyFieldAccessor> foreign_keys_fields;
		    public TransactionTmp(ISqlJetTable _sql_jet_table, ArrayList<FieldAccessor> _auto_primary_keys_fields, ArrayList<ForeignKeyFieldAccessor> _foreign_keys_fields)
		    {
			sql_jet_table=_sql_jet_table;
			auto_primary_keys_fields=_auto_primary_keys_fields;
			foreign_keys_fields=_foreign_keys_fields;
		    }
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(HSQLDBWrapper _db) throws DatabaseException
		    {
			try
			{
			    for (ForeignKeyFieldAccessor fa : foreign_keys_fields)
			    {
				for (T instance : instances)
				{
				    Object val=fa.getValue(instance);
				    if (!fa.getPointedTable().contains(true, (DatabaseRecord)val))
					throw new RecordNotFoundDatabaseException("The record, contained as foreign key into the field "+fa.getFieldName()+" into the table "+Table.this.getName()+" does not exists into the table "+fa.getPointedTable().getName());
				}
			    }
			    if (auto_primarykeys_values.size()>0)
			    {
				for (int i=0;i<instances.size();i++)
				{
				    T instance=instances.get(i);
				    if (ct.has_auto_field.get(i).booleanValue())
				    {
					long rowid=sql_jet_table.insertByFieldNames(getSqlFieldsInstances(instance, true));
					setRecordID(instance, rowid);
				    }
				}
				for (int i=0;i<instances.size();i++)
				{
				    T instance=instances.get(i);
				    if (!ct.has_auto_field.get(i).booleanValue())
				    {
					long rowid=sql_jet_table.insertByFieldNames(getSqlFieldsInstances(instance, false));
					setRecordID(instance, rowid);
					if (auto_primary_keys_fields.size()>0)
					{
					    try(Cursor isjc=new Cursor(sql_jet_table, Table.this.getClass().getPackage()))
					    {
						isjc.goTo(rowid);
					    
						for (FieldAccessor fa : auto_primary_keys_fields)
						{
						    fa.setValue(instance, isjc.getValue(fa.getDeclaredSqlField().field));
						}
					    }
					}
				    }
				}
			    }
			    else
			    {
				for (T instance : instances)
				{
				    long rowid=sql_jet_table.insertByFieldNames(getSqlFieldsInstances(instance, false));
				    setRecordID(instance, rowid);
				    
				    if (auto_primary_keys_fields.size()>0)
				    {
					try(Cursor isjc=new Cursor(sql_jet_table, Table.this.getClass().getPackage()))
					{
					    isjc.goTo(rowid);
				    
					    for (FieldAccessor fa : auto_primary_keys_fields)
					    {
						fa.setValue(instance, isjc.getValue(fa.getDeclaredSqlField().field));
					    }
					}
				    }
				}
			    }
			    return null;
			}
			catch(SqlJetException e)
			{
			    throw new DatabaseException(e);
			}
		    }
		    
		}
		
		sql_jet_db.sql_connection.runTransaction(new TransactionTmp(sql_jet_table, auto_primary_keys_fields, foreign_keys_fields));
	    
	    if (isLoadedInMemory() && is_synchronized_with_sql_database.get())
		__AddRecords(instances);
	    return instances;

	}
	catch(IllegalArgumentException e)
	{
	    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
	}
	catch(InstantiationException e)
	{
	    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
	}
	catch(IllegalAccessException e)
	{
	    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
	}
	catch(InvocationTargetException e)
	{
	    throw new DatabaseException("Impossible to add a new field on the table/class "+this.getName()+".", e);
	}
	}
	catch(Exception e)
	{
	    DatabaseException de=getOriginalDatabaseException(e);
	    if (de!=null)
		throw de;
	    throw new DatabaseException("", e);
	}
	}
    }*/
    
    
    /**
     * Alter a record into the database with a map of fields corresponding to this record. 
     * The string type in the Map corresponds to the name of the field, and the Object type field corresponds the value of the field.   
     * If primary keys are altered, every foreign key pointing to this record will be transparently altered. However, if records pointing to this altered record remain in memory, they will no be altered if the current table has not the annotation {#link oodforsqljet.annotations.LoadToMemory}. The only solution in this case is to reload the concerned records through the functions starting with "getRecord". 
     * @param _record the record to alter 
     * @param _fields the list of fields to include into the new record
     * @throws DatabaseException if a problem occurs during the insertion into the Sql database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if the given primary keys already exists into the table, if a field which has the unique property exists alreay into the table, or if primary keys changing induce a problem of constraints through foreign keys into other tables, which are also primary keys.
     * @throws FieldDatabaseException if one of the given fields does not exists into the database, or if one of the given fields are auto or random primary keys.
     * @throws RecordNotFoundDatabaseException if the given record is not included into the database, or if one of the field is a foreign key and point to a record of another table which does not exist. 
     */

    public final void alterRecord(final T _record, final Map<String, Object> _fields) throws DatabaseException
    {
	if (_record==null)
	    throw new NullPointerException("The parameter _record is a null pointer !");
	if (_fields==null)
	    throw new NullPointerException("The parameter _fields is a null pointer !");
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		for (String s : _fields.keySet())
		{
		    boolean found=false;
		    for (FieldAccessor fa : fields)
		    {
			if (fa.getFieldName().equals(s))
			{
			    if (fa.isForeignKey())
			    {
				ForeignKeyFieldAccessor fkfa=(ForeignKeyFieldAccessor)fa;
				if (!fkfa.getPointedTable().contains(false,(DatabaseRecord)_fields.get(fa.getFieldName())))
				{
				    throw new FieldDatabaseException("The field "+fa.getFieldName()+" given in parameters point to a DatabaseRecord which is not contained into the database.");
				}
			    }
			    found=true;
			    break;
			}
		    }
		    if (!found)
			throw new FieldDatabaseException("The given field "+s+" is not contained into the table "+this.getName());
		}
	
		class CheckTmp extends Runnable2
		{
		    private final HashMap<String, Object> keys;
		    public boolean check_necessary=false;
		    public final ArrayList<Boolean> check_random=new ArrayList<Boolean>();  
		    private final ArrayList<FieldAccessor> random_primary_keys_fields;
		    public CheckTmp(ArrayList<FieldAccessor> _auto_random_primary_keys_fields) throws DatabaseException
		    {
			random_primary_keys_fields=_auto_random_primary_keys_fields;
			keys=new HashMap<String, Object>();
			for (int i=0;i<random_primary_keys_fields.size();i++)
			{
			    FieldAccessor fa = random_primary_keys_fields.get(i);
			    if (_fields.containsKey(fa.getFieldName()))
			    {
				Object field=_fields.get(fa.getFieldName());
				if (!fa.equals(_record, field))
				{
				    keys.put(fa.getFieldName(), field);
				    check_necessary=true;
				    check_random.add(new Boolean(true));
				}
				else
				    check_random.add(new Boolean(false));
			    }
			    else
				check_random.add(new Boolean(false));
			}
		    
		    }
		    @Override
		    public void init(int _field_count)
		    {
		    }

		    @Override
		    public boolean setInstance(ResultSet _result_set) throws DatabaseException
		    {
			for (int i=0;i<random_primary_keys_fields.size();i++)
			{
			    if (check_random.get(i).booleanValue())
			    {
				FieldAccessor fa = random_primary_keys_fields.get(i);
				if (fa.equals(keys.get(fa.getFieldName()), _result_set))
				    throw new ConstraintsNotRespectedDatabaseException("the given record have the same auto/random primary key field "+fa.getFieldName()+" of one of the records stored into the database. No record have been added.");
			    }
			}
			return true;
		    }
		}
		CheckTmp ct=new CheckTmp(auto_random_primary_keys_fields);
		if (ct.check_necessary)
		    getListRecordsFromSqlConnection(ct, getSqlGeneralSelect());
	    

		class TransactionTmp implements Transaction
		{
		    protected final ArrayList<FieldAccessor> fields_accessor;
		
		    public TransactionTmp(ArrayList<FieldAccessor> _fields_accessor)  
		    {
			fields_accessor=_fields_accessor;
		    }
	    	    @Override
	    	    public boolean doesWriteData()
		    {
		        return true;
		    }
		
		
		    @SuppressWarnings("synthetic-access")
		    @Override
		    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
		    {
			try
			{
			    StringBuffer querry=new StringBuffer("UPDATE "+Table.this.getName()+" SET ");
			    T instance=default_constructor_field.newInstance();
			    boolean first=true;
			    for (FieldAccessor fa : fields_accessor)
			    {
				if (_fields.containsKey(fa.getFieldName()))
				{
				    fa.setValue(instance, _fields.get(fa.getFieldName()));
				
				    for (SqlField sf : fa.getDeclaredSqlFields())
				    {
					if (first)
					    first=false;
					else
					    querry.append(", ");
					querry.append(sf.short_field+" = ?");
				    }
				}
			    }
			    querry.append(" WHERE ");
			    first=true;
			    for (FieldAccessor fa : primary_keys_fields)
			    {
				for (SqlField sf : fa.getDeclaredSqlFields())
				{
				    if (first)
					first=false;
				    else
					querry.append(" AND ");
				    querry.append(sf.field+" = ?");
				}
			    }
			
			    try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), querry.toString()))
			    {
				int index=1;
				for (FieldAccessor fa : fields_accessor)
				{
				    if (_fields.containsKey(fa.getFieldName()))
				    {
					fa.getValue(instance, puq.statement, index);
					index+=fa.getDeclaredSqlFields().length;
				    }
				}
				for (FieldAccessor fa : primary_keys_fields)
				{
				    fa.getValue(_record, puq.statement, index);
				    index+=fa.getDeclaredSqlFields().length;
				}
				int nb=puq.statement.executeUpdate();
				if (nb>1)
				    throw new DatabaseIntegrityException("More than one record have been found with the given primary keys. No record have been altered.");
				if (nb==0)
				    throw new RecordNotFoundDatabaseException("The given record was not found");
				for (FieldAccessor fa : fields_accessor)
				{
				    if (_fields.containsKey(fa.getFieldName()))
				    {
					fa.setValue(_record, _fields.get(fa.getFieldName()));
				    }
				}
			    
			    }
			    catch(SQLIntegrityConstraintViolationException e)
			    {
				throw new ConstraintsNotRespectedDatabaseException("Constraints was not respected. It possible that the given primary keys or the given unique keys does not respect constraints of unicity.", e);
			    }
			    return null;
			}
			catch(Exception e)
			{
			    throw DatabaseException.getDatabaseException(e);
			}
			
		    }


		}
		
		sql_connection.runTransaction(new TransactionTmp(fields));
		
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    
	}
    }
    /**
     * Alter records into the database through a given inherited {@link com.distrimind.ood.database.AlterRecordFilter} class.
     * 
     * The function parse all records present into this table. 
     * For each of them, it calls the function {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}.
     * 
     * If the function {@link com.distrimind.ood.database.AlterRecordFilter#alter(Map)} is called into the inherited function {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}, 
     * then all fields present in the given map will be updated into the record, after the end of the {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)} function call. 
     * If the given map is a null reference, the correspondent record will not be altered.
     * Note that modification of primary keys and unique keys are not permitted with this function. To do that, please use the function {@link #alterRecord(DatabaseRecord, Map)}.
     * 
     * If the function {@link com.distrimind.ood.database.AlterRecordFilter#remove()} is called, then the current record will be deleted after the end of the {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)} function call, only if no record point to this record.
     * 
     * If the function {@link com.distrimind.ood.database.AlterRecordFilter#removeWithCascade()} is called, then the current record will be deleted after the end of the {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)} function call. 
     * Records pointing to this record will also be deleted. 
     *    
     * @param _filter the filter enabling to alter the desired records. 
     * @throws DatabaseException if a problem occurs during the insertion into the Sql database.
     * @throws NullPointerException if parameters are null pointers.
     * @throws ConstraintsNotRespectedDatabaseException if a primary key or a unique key is altered, or if one of the given fields is a null pointer whereas this field must be not null.
     * @throws FieldDatabaseException if one of the given fields to change does not exists into the database, or if one of the given fields to change is a primary key or a unique field.
     * @throws RecordNotFoundDatabaseException if one of the given field to alter is a foreign key which points to a non-existing record.
     * @see AlterRecordFilter
     */
    public final void alterRecords(final AlterRecordFilter<T> _filter) throws DatabaseException
    {
	if (_filter==null)
	    throw new NullPointerException("The parameter _filter is a null pointer !");
	
	
	//synchronized(sql_connection)
	{
	    try (Lock lock=new WriteLock(this))
	    {
		if (isLoadedInMemory())
		{
		    final ArrayList<T> records_to_delete=new ArrayList<>();
		    final ArrayList<T> records_to_delete_with_cascade=new ArrayList<>();
		    for (final T r : getRecords(false))
		    {
			_filter.reset();
			_filter.nextRecord(r);
			
			if (_filter.hasToBeRemoved())
			{
			    boolean canberemoved=true;
			    for (NeighboringTable nt : list_tables_pointing_to_this_table)
			    {
				if (nt.getPointedTable().hasRecordsWithOneOfFields(nt.getHashMapFields(r), false))
				{
				    canberemoved=false;
				    break;
				}
			    }
			    if (canberemoved)
				records_to_delete.add(r);
			}
			else if (_filter.hasToBeRemovedWithCascade())
			{
			    records_to_delete_with_cascade.add(r);
			}
			else
			{
			    final Map<String, Object> map=_filter.getModifications();
			    if (map!=null && map.size()>0)
			    {
				for (String s : map.keySet())
				{
				    FieldAccessor founded_field=null;
				    for (FieldAccessor fa : fields)
				    {
					if (fa.getFieldName().equals(s))
					{
					    founded_field=fa;
					    break;
					}
				    }
				    if (founded_field==null)
					throw new FieldDatabaseException("The given field "+s+" does not exists into the record "+class_record.getName()+". ");
				    if (founded_field.isPrimaryKey())
					throw new FieldDatabaseException("Attempting to alter the primary key field "+founded_field.getFieldName()+" into the table "+getName()+". This operation is not permitted into this function."+". ");
				    if (founded_field.isUnique())
					throw new FieldDatabaseException("Attempting to alter the unique field "+founded_field.getFieldName()+" into the table "+getName()+". This operation is not permitted into this function."+". ");
				    if (founded_field.isForeignKey())
				    {
					Object val=founded_field.getValue(r);
					if (!((ForeignKeyFieldAccessor)founded_field).getPointedTable().contains(true, (DatabaseRecord)val))
					    throw new RecordNotFoundDatabaseException("The record, contained as foreign key into the given field "+founded_field.getFieldName()+" into the table "+Table.this.getName()+" does not exists into the table "+((ForeignKeyFieldAccessor)founded_field).getPointedTable().getName()+". ");
				    }
				}
				sql_connection.runTransaction(new Transaction() {
			        
				    @SuppressWarnings("synthetic-access")
				    @Override
				    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
				    {
					try
					{
					    StringBuffer querry=new StringBuffer("UPDATE "+Table.this.getName()+" SET ");
					    T instance=default_constructor_field.newInstance();
					    boolean first=true;
					    for (FieldAccessor fa : fields)
					    {
						if (map.containsKey(fa.getFieldName()))
						{
						    fa.setValue(instance, map.get(fa.getFieldName()));
						
						    for (SqlField sf : fa.getDeclaredSqlFields())
						    {
							if (first)
							    first=false;
							else
							    querry.append(", ");
							querry.append(sf.short_field+" = ?");
						    }
						}
					    }
					    querry.append(" WHERE ");
					    first=true;
					    for (FieldAccessor fa : primary_keys_fields)
					    {
						for (SqlField sf : fa.getDeclaredSqlFields())
						{
						    if (first)
							first=false;
						    else
							querry.append(" AND ");
						    querry.append(sf.field+" = ?");
						}
					    }
					
					    try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), querry.toString()))
					    {
						int index=1;
						for (FieldAccessor fa : fields)
						{
						    if (map.containsKey(fa.getFieldName()))
						    {
							fa.getValue(instance, puq.statement, index);
							index+=fa.getDeclaredSqlFields().length;
						    }
						}
						for (FieldAccessor fa : primary_keys_fields)
						{
						    fa.getValue(r, puq.statement, index);
						    index+=fa.getDeclaredSqlFields().length;
						}
						int nb=puq.statement.executeUpdate();
						if (nb!=1)
						    throw new DatabaseIntegrityException("More than one record have been found with the given primary keys. No record have been altered.");
					    
						for (FieldAccessor fa : fields)
						{
						    if (map.containsKey(fa.getFieldName()))
						    {
							fa.setValue(r, map.get(fa.getFieldName()));
						    }
						}
					    
					    }
					    catch(SQLIntegrityConstraintViolationException e)
					    {
						throw new ConstraintsNotRespectedDatabaseException("Constraints was not respected. It possible that the given primary keys or the given unique keys does not respect constraints of unicity.", e);
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
			    }
			}
		    }
		    
		    if (records_to_delete.size()>0)
		    {
			    Transaction transaction=new Transaction() {
			        
				    @SuppressWarnings("synthetic-access")
				    @Override
				    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
				    {
					StringBuffer sb=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(records_to_delete.size()));
					
					int nb=0;
					try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), sb.toString()))
					{
					    int index=1;
					    for (T r : records_to_delete)
					    {
						for (FieldAccessor fa : primary_keys_fields)
						{
						    fa.getValue(r, puq.statement, index);
						    index+=fa.getDeclaredSqlFields().length;
						}
					    }
					    nb=puq.statement.executeUpdate();
					}
					catch(Exception e)
					{
					    throw DatabaseException.getDatabaseException(e);
					}

					
					if (nb!=records_to_delete.size())
					    throw new DatabaseException("Unexpected exception.");
					return null;
				    }
			    	    @Override
			    	    public boolean doesWriteData()
				    {
				        return true;
				    }
				    
				};
			    
			    sql_connection.runTransaction(transaction);
		    
			    __removeRecords(records_to_delete);
		    }
		    if (records_to_delete_with_cascade.size()>0)
		    {
			Transaction transaction=new Transaction() {
			        
			    @SuppressWarnings("synthetic-access")
			    @Override
			    public Object run(DatabaseWrapper _sql_connection) throws DatabaseException
			    {
				StringBuffer sb=new StringBuffer("DELETE FROM "+Table.this.getName()+" WHERE "+getSqlPrimaryKeyCondition(records_to_delete_with_cascade.size()));

				int nb=0;
				try(PreparedUpdateQuerry puq=new PreparedUpdateQuerry(_sql_connection.getSqlConnection(), sb.toString()))
				{
				    int index=1;
				    for (T r : records_to_delete_with_cascade)
				    {
					for (FieldAccessor fa : primary_keys_fields)
					{
					    fa.getValue(r, puq.statement, index);
					    index+=fa.getDeclaredSqlFields().length;
					}
				    }
				    nb=puq.statement.executeUpdate();
				}
				catch(Exception e)
				{
				    throw DatabaseException.getDatabaseException(e);
				}
				
				if (nb!=records_to_delete_with_cascade.size())
				{
				    throw new DatabaseException("Unexpected exception.");
				}
				return null;
			    }
		    	    @Override
		    	    public boolean doesWriteData()
			    {
			        return true;
			    }
			    
			};
		
			sql_connection.runTransaction(transaction);
		
			__removeRecords(records_to_delete_with_cascade);
			updateMemoryForRemovingRecordsWithCascade(records_to_delete_with_cascade);
		    }
		    
		}
		else
		{
		    class RunnableTmp extends Runnable
		    {
			protected final ArrayList<FieldAccessor> fields_accessor;

			public RunnableTmp(ArrayList<FieldAccessor> _fields_accessor) 
			{
			    fields_accessor=_fields_accessor;
			}
			
			
			@Override
			public void init(int _field_count)
			{
			    
			}

			@SuppressWarnings("synthetic-access")
			@Override
			public boolean setInstance(T _instance, ResultSet _result_set) throws DatabaseException
			{
			    try
			    {
				_filter.reset();
				_filter.nextRecord(_instance);
				if (_filter.hasToBeRemoved())
				{
				    boolean canberemoved=true;
				    if (list_tables_pointing_to_this_table.size()>0)
				    {
					for (int i=0;i<list_tables_pointing_to_this_table.size();i++)
					{
						
					    NeighboringTable nt=list_tables_pointing_to_this_table.get(i);
						
					    if (nt.getPointedTable().hasRecordsWithOneOfSqlForeignKeyWithCascade(nt.getHashMapsSqlFields(getSqlPrimaryKeys(_instance))))
					    {
						canberemoved=false;
						break;
					    }
					}
				    }
				    if (canberemoved)
				    {
					_result_set.deleteRow();
				    }
				}
				else if(_filter.hasToBeRemovedWithCascade())
				{
				    _result_set.deleteRow();
				    updateMemoryForRemovingRecordWithCascade(_instance);
				}
				else
				{
				    Map<String, Object> map=_filter.getModifications();
				    if (map!=null && map.size()>0)
				    {
					for (String s : map.keySet())
					{
					    FieldAccessor founded_field=null;
					    for (FieldAccessor fa : fields_accessor)
					    {
						if (fa.getFieldName().equals(s))
						{
						    founded_field=fa;
						    break;
						}
					    }
					    if (founded_field==null)
						throw new FieldDatabaseException("The given field "+s+" does not exists into the record "+class_record.getName());
					    if (founded_field.isPrimaryKey())
						throw new FieldDatabaseException("Attempting to alter the primary key field "+founded_field.getFieldName()+" into the table "+getName()+". This operation is not permitted into this function.");
					    if (founded_field.isUnique())
						throw new FieldDatabaseException("Attempting to alter the unique field "+founded_field.getFieldName()+" into the table "+getName()+". This operation is not permitted into this function.");
					    if (founded_field.isForeignKey())
					    {
						Object val=founded_field.getValue(_instance);
						if (!((ForeignKeyFieldAccessor)founded_field).getPointedTable().contains(true, (DatabaseRecord)val))
						    throw new RecordNotFoundDatabaseException("The record, contained as foreign key into the given field "+founded_field.getFieldName()+" into the table "+Table.this.getName()+" does not exists into the table "+((ForeignKeyFieldAccessor)founded_field).getPointedTable().getName());
					    }
					}
					for (FieldAccessor fa : fields)
					{
					    if (map.containsKey(fa.getFieldName()))
					    {
						fa.updateValue(_instance, map.get(fa.getFieldName()), _result_set);
					    }
					}
					_result_set.updateRow();
				    }
				}
				return true;
			    }
			    catch(Exception e)
			    {
				throw DatabaseException.getDatabaseException(e);
			    }
			}
		    }
		    RunnableTmp runnable=new RunnableTmp(fields);
		    getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect(), true);
		}
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	    
	}
    }
    
    protected final BigInteger getRandomPositiveBigIntegerValue(int bits)
    {
	return new BigInteger(bits, rand);
    }

    /**
     * Return the record stored into the database, which corresponds to the given primary keys.
     * The string type in the Map corresponds to the name of the field, and the Object type field corresponds the value of the field.
     * Just include the primary keys into the fields.
     * @param keys the primary keys values
     * @return the corresponding record. Return null if no record have been founded.
     * @throws DatabaseException if a Sql problem have occured.
     * @throws FieldDatabaseException if all primary keys have not been given, or if fields which are not primary keys were given.
     * @throws NullPointerException if parameters are null pointers.
     */
    public final T getRecord(final Map<String, Object> keys) throws DatabaseException
    {
	//synchronized(sql_connection)
	{
	    try(Lock lock=new ReadLock(this))
	    {
		return getRecord(keys, false);
	    }
	    catch(Exception e)
	    {
		throw DatabaseException.getDatabaseException(e);
	    }
	}
    }
    private final T getRecord(final Map<String, Object> keys, boolean is_already_in_transaction) throws DatabaseException
    {
	if (!is_already_in_transaction)
	{
	    if (keys==null)
		throw new NullPointerException("The parameter keys is a null pointer !");
	
	    if (keys.size()!=primary_keys_fields.size())
		throw new FieldDatabaseException("The number of given primary keys ("+keys.size()+") is not equal to the expected number of primary keys ("+primary_keys_fields.size()+").");
	   
	    for (FieldAccessor f : primary_keys_fields)
	    {
		Object obj=keys.get(f.getFieldName());
		if (obj==null)
		    throw new FieldDatabaseException("The key "+f.getFieldName()+"is not present into the given keys.");
	    }
	}
	try
	{
	    if (isLoadedInMemory())
	    {
		ArrayList<T> field_instances=getRecords(false);
		for (T field_instance : field_instances)
		{
		    boolean ok=true;
		    for (FieldAccessor f : primary_keys_fields)
		    {
			Object obj=keys.get(f.getFieldName());
			if (!f.equals(field_instance, obj))
			{
			    ok=false;
			    break;
			}
		    }
		    if (ok)
			return field_instance;
		}
		return null;
	    }
	    else
	    {
		//synchronized(sql_connection)
		{
		    class RunnableTmp extends Runnable
		    {
			public T instance=null;
			protected ArrayList<FieldAccessor> primary_keys_fields;
			
			public RunnableTmp(ArrayList<FieldAccessor> _primary_keys_fields)
			{
			    primary_keys_fields=_primary_keys_fields;
			}
			@Override
			public void init(int _field_count)
			{
			}

			@Override
			public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException
			{
				boolean ok=true;
				for (FieldAccessor f : primary_keys_fields)
				{
				    Object obj=keys.get(f.getFieldName());
				    if (!f.equals(_instance, obj))
				    {
					ok=false;
					break;
				    }
				}
				if (ok)
				{
				    instance=_instance;
				    return false;
				}
				return true;
			}
		    }
		    RunnableTmp runnable=new RunnableTmp(primary_keys_fields);
		    getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect());
		    return runnable.instance;
		}
	    }
	}
	catch(IllegalArgumentException e)
	{
	    throw new DatabaseException("Impossible to access to the database fields.", e);
	}
    }
    
    
    private final void __AddRecord(T _record)
    {
	@SuppressWarnings("unchecked")
	ArrayList<T> res=(ArrayList<T>)records_instances.get().clone();
	res.add(_record);
	records_instances.set(res);
    }
    /*private final void __AddRecords(Collection<T> _records)
    {
	@SuppressWarnings("unchecked")
	ArrayList<T> res=(ArrayList<T>)records_instances.get().clone();
	res.addAll(_records);
	records_instances.set(res);
    }*/
    
    private final void __removeRecord(T _record) throws DatabaseException
    {
	@SuppressWarnings("unchecked")
	ArrayList<T> res=(ArrayList<T>)records_instances.get().clone();
	boolean removed=false;
	    for (Iterator<T> it=res.iterator();it.hasNext();)
	    {
		T f2=it.next();
		if (_record==f2/*equals(_record, f2)*/)
		{
		    it.remove();
		    removed=true;
		    break;
		}
	    }
	if (!removed)
	    throw new DatabaseIntegrityException("Unexpected exception");
	records_instances.set(res);
    }
    private final void __removeRecords(Collection<T> _records) throws DatabaseException
    {
	@SuppressWarnings("unchecked")
	ArrayList<T> res=(ArrayList<T>)records_instances.get().clone();
	int number=_records.size();
	for (T f: _records)
	{
	    for (Iterator<T> it=res.iterator();it.hasNext();)
	    {
		T f2=it.next();
		if (f==f2/*equals(f, f2)*/)
		{
		    it.remove();
		    --number;
		    break;
		}
	    }
	}
	if (number!=0)
	    throw new DatabaseIntegrityException("Unexpected exception");
	records_instances.set(res);
    }
    /**
     * Returns the Sql database which is associated to this table.
     * @return the associated Sql database
     */
    public final DatabaseWrapper getDatabaseWrapper()
    {
	return sql_connection;
    }
    static final class DefaultConstructorAccessPrivilegedAction<TC> implements PrivilegedExceptionAction<Constructor<TC>>
    {
	private final Class<TC> m_cls;
	
	public DefaultConstructorAccessPrivilegedAction(Class<TC> _cls)
	{
	    m_cls=_cls;
	}
	
	public Constructor<TC> run () throws Exception
	{
	    Constructor<TC> c=m_cls.getDeclaredConstructor();
	    c.setAccessible(true);
	    return c;
	}
    }
    
    //Lock current_lock=null;
    final HashMap<Thread, Lock> current_locks=new HashMap<>();
    
    private static abstract class Lock implements AutoCloseable
    {
	protected Table<?> current_table;
	protected Lock previous_lock;
	protected Lock(/*Table<?> _current_table*/)
	{
	    /*current_table=_current_table;
	    previous_lock=current_table.current_lock;*/
	}
	
	protected void initialize(Table<?> _current_table)
	{
	    current_table=_current_table;
	    //previous_lock=current_table.current_lock;
	    previous_lock=current_table.current_locks.get(Thread.currentThread());
	}
	
	protected abstract boolean isValid();
	
	protected abstract void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException;

	protected static boolean indirectlyPointTo(Table<?> _table, Table<?> _pointed_table)
	{
	    for (ForeignKeyFieldAccessor fa : _table.foreign_keys_fields)
	    {
		if (fa.getPointedTable()==_pointed_table)
		    return true;
	    }
	    for (ForeignKeyFieldAccessor fa : _table.foreign_keys_fields)
	    {
		return indirectlyPointTo(fa.getPointedTable(), _pointed_table);
	    }
	    return false;
	}
	
	protected void cancel(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException
	{
	    ArrayList<Table<?>> list=new ArrayList<Table<?>>(20);
	    _comes_from_tables.remove(this.current_table);
	    list.add(this.current_table);
	    for (Table<?> t : _comes_from_tables)
	    {
		Lock l=t.current_locks.get(Thread.currentThread());
		if (l!=null)
		    l.close(list);
	    }
	    if (previous_lock==null)
		current_table.current_locks.remove(Thread.currentThread());
	    else
		current_table.current_locks.put(Thread.currentThread(), previous_lock);
	    
	}
    }
    
    private static class WriteLock extends Lock
    {
	public WriteLock(Table<?> _current_table) throws DatabaseException
	{
	    this(_current_table, new ArrayList<Table<?>>(20), _current_table);
	}

	protected WriteLock(Table<?> _current_table, ArrayList<Table<?>> _comes_from_tables, Table<?> _from_comes_original_table) throws DatabaseException
	{
	    super();
	    synchronized(_current_table.sql_connection.locker)
	    {
		
		try
		{
		    _current_table.sql_connection.locker.lockWrite();
		    initialize(_current_table);
		    _comes_from_tables.add(current_table);
	    
		    if (!isValid())
			throw new ConcurentTransactionDatabaseException("Attempting to write, through several nested queries, on the table "+current_table.getName()+".");
		    for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
		    {
			Table<?> t=nt.getPointedTable();
			if (!_comes_from_tables.contains(t))
			{
			    new WriteLock(t, _comes_from_tables, _from_comes_original_table);
			}
		    }
		    for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields)
		    {
			Table<?> t=fa.getPointedTable();
			if(_comes_from_tables.size()==1 || (!_comes_from_tables.contains(t) && !Lock.indirectlyPointTo(t, _from_comes_original_table)))
			{
			    new ReadLock(t, _comes_from_tables);
			}
		    }
		    
		    current_table.current_locks.put(Thread.currentThread(), this);
		    
		}
		catch(DatabaseException e)
		{
		    try
		    {
			this.cancel(_comes_from_tables);
		    }
		    catch(DatabaseException e2)
		    {
			e2.printStackTrace();
			throw new IllegalAccessError("");
		    }
		    current_table.sql_connection.locker.unlockWrite();
		    throw e;
		}
	    }
	}
	
	@Override
	protected boolean isValid()
	{
	    if (current_table.current_locks.size()==0)
		return true;
	    Lock l=current_table.current_locks.get(Thread.currentThread());
	    return l==null;
	}
	
	@Override
	public void close() throws Exception
	{
	    close(new ArrayList<Table<?>>(20));
	}

	@Override
	protected void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException
	{
	    synchronized(current_table.sql_connection.locker)
	    {
		try
		{
		    //current_table.current_lock=null;
		    if (previous_lock==null)
			current_table.current_locks.remove(Thread.currentThread());
		    else
			current_table.current_locks.put(Thread.currentThread(), previous_lock);
		    //current_table.current_lock=previous_lock;
		    _comes_from_tables.add(current_table);
		    for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
		    {
			Table<?> t=nt.getPointedTable();
			if (!_comes_from_tables.contains(t))
			{
			    t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
			}
		    }
		    for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields)
		    {
			Table<?> t=fa.getPointedTable();
			if (!_comes_from_tables.contains(t))
			    t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
		    }
		}
		finally
		{
		    current_table.sql_connection.locker.unlockWrite();
		}
	    }
	}
    }
    private static class ReadLock extends Lock
    {
	public ReadLock(Table<?> _current_table) throws DatabaseException
	{
	    this(_current_table, new ArrayList<Table<?>>(20));
	}

	protected ReadLock(Table<?> _current_table, ArrayList<Table<?>> _comes_from_tables) throws DatabaseException
	{
	    super();
	    synchronized(_current_table.sql_connection.locker)
	    {
		
		try
		{
		    _current_table.sql_connection.locker.lockRead();
		    initialize(_current_table);
		    if (!isValid())
			throw new ConcurentTransactionDatabaseException("Attempting to read and write, through several nested queries, on the table "+current_table.getName()+".");
		    _comes_from_tables.add(current_table);
		    /*for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
	    		{	
				Table<?> t=nt.getPointedTable();
				if (!_comes_from_tables.contains(t))
		    		new ReadLock(t, _comes_from_tables);
	    		}*/
		    for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields)
		    {
			Table<?> t=fa.getPointedTable();
			if (!_comes_from_tables.contains(t))
			    new ReadLock(t, _comes_from_tables);
		    }
		    
		    current_table.current_locks.put(Thread.currentThread(), this);
		    
		}
		catch(DatabaseException e)
		{
		    try
		    {
			this.cancel(_comes_from_tables);
		    }	
		    catch(DatabaseException e2)
		    {
			e2.printStackTrace();
			throw new IllegalAccessError("");
		    }
		    current_table.sql_connection.locker.unlockRead();
		    throw e;
		}
	    }
	}
	
	@Override
	protected boolean isValid()
	{
	    if (current_table.current_locks.size()==0)
		return true;
	    Lock cur=current_table.current_locks.get(Thread.currentThread());
	    return cur==null || cur instanceof ReadLock;
	}
	
	
	@Override
	public void close() throws Exception
	{
	    close(new ArrayList<Table<?>>(20));
	}
	

	@Override
	protected void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException 
	{
	    synchronized(current_table.sql_connection.locker)
	    {
		try
		{
		    if (previous_lock==null)
			current_table.current_locks.remove(Thread.currentThread());
		    else
			current_table.current_locks.put(Thread.currentThread(), previous_lock);
		    //current_table.current_lock=previous_lock;
		    _comes_from_tables.add(current_table);
		    /*for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
	    		{
				Table<?> t=nt.getPointedTable();
				if (!_comes_from_tables.contains(t))
		    		t.current_lock.close(_comes_from_tables);
	    		}*/
		    for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields)
		    {
			Table<?> t=fa.getPointedTable();
			if(!_comes_from_tables.contains(t))
			    t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
		    }
		}
		finally
		{
		    current_table.sql_connection.locker.unlockRead();
		}
	    }
	}
    }
    
    
    static abstract class Querry implements AutoCloseable
    {
	protected final Connection sql_connection;
	public Querry(Connection _sql_connection)
	{
	    sql_connection=_sql_connection;
	}
	
    }
    static abstract class AbstractReadQuerry extends Querry
    {
	public Statement statement;
	public ResultSet result_set;
	protected AbstractReadQuerry(Connection _sql_connection, String querry, int _result_set_type, int _result_set_concurency) throws SQLException
	{
	    super(_sql_connection);
	    statement=sql_connection.createStatement(_result_set_type, _result_set_concurency);
	    result_set=statement.executeQuery(querry);
	}
	protected AbstractReadQuerry(Connection _sql_connection, ResultSet resultSet)
	{
	    super(_sql_connection);
	    statement=null;
	    result_set=resultSet;
	}
	@Override
	public void close() throws Exception
	{
	    result_set.close();
	    result_set=null;
	    if (statement!=null)
	    {
		statement.close();
		statement=null;
	    }
	}
    }
    
    static class ReadQuerry extends AbstractReadQuerry
    {
	public ReadQuerry(Connection _sql_connection, String querry) throws SQLException
	{
	    super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}
	public ReadQuerry(Connection _sql_connection, ResultSet resultSet)
	{
	    super(_sql_connection, resultSet);
	}
    }
    static abstract class ColumnsReadQuerry extends AbstractReadQuerry
    {
	TableColumnsResultSet tableColumnsResultSet;
	
	public ColumnsReadQuerry(Connection _sql_connection, String querry) throws SQLException
	{
	    super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}
	public ColumnsReadQuerry(Connection _sql_connection, ResultSet resultSet)
	{
	    super(_sql_connection, resultSet);
	}
	public void setTableColumnsResultSet(TableColumnsResultSet tableColumnsResultSet)
	{
	    this.tableColumnsResultSet=tableColumnsResultSet;
	}
    }
    static class UpdatableReadQuerry extends AbstractReadQuerry
    {
	public UpdatableReadQuerry(Connection _sql_connection, String querry) throws SQLException
	{
	    super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	}
    }
    
    static class PreparedReadQuerry extends Querry
    {
	public PreparedStatement statement;
	public ResultSet result_set=null;
	public PreparedReadQuerry(Connection _sql_connection, String querry) throws SQLException
	{
	    super(_sql_connection);
	    statement=sql_connection.prepareStatement(querry);
	}
	
	public boolean execute() throws SQLException
	{
	    boolean res=statement.execute();
	    result_set=statement.getResultSet();
	    return res;
	}
	
	@Override
	public void close() throws Exception
	{
	    if (result_set!=null)
		result_set.close();
	    result_set=null;
	    statement.close();
	    statement=null;
	}
    }
    
    static class PreparedUpdateQuerry extends Querry
    {
	public PreparedStatement statement;
	public PreparedUpdateQuerry(Connection _sql_connection, String querry) throws SQLException
	{
	    super(_sql_connection);
	    statement=sql_connection.prepareStatement(querry);
	}
	@Override
	public void close() throws Exception
	{
	    statement.close();
	    statement=null;
	}
	
    }
    
}
