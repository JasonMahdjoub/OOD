
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.ood.database.fieldaccessors.AbstractDencetralizedIDFieldAccessor;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.ood.interpreter.Interpreter;
import com.distrimind.ood.interpreter.RuleInstance;
import com.distrimind.ood.interpreter.RuleInstance.TableJunction;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.ReadWriteLock;

/**
 * This abstract class represent a generic Sql Table wrapper, which enables the
 * user to do every Sql operation without any SQL query. To create a table into
 * the database, the user must inherit this class. Every table of the same
 * database must be grouped into the same package. When the table class is
 * created, the user must include a public inner class named Record which must
 * inherit the DatabaseRecord class. The type &lsaquo;T&rsaquo; of the table
 * corresponds to this inner class. This inner class corresponds to a record
 * into the corresponding table, with its declared fields. This fields can be
 * native java types, or there corresponding classes (for example : Integer for
 * the native type int). They can be BigInteger or BigDecimal. They can be
 * DatabaseRecord references, for foreign keys. String class, arrays of bytes
 * are also accepted. Every field which must be included into the database must
 * have an annotation ({@link com.distrimind.ood.database.annotations.Field},
 * {@link com.distrimind.ood.database.annotations.PrimaryKey},
 * {@link com.distrimind.ood.database.annotations.AutoPrimaryKey},
 * {@link com.distrimind.ood.database.annotations.RandomPrimaryKey},
 * {@link com.distrimind.ood.database.annotations.NotNull},
 * {@link com.distrimind.ood.database.annotations.Unique},
 * {@link com.distrimind.ood.database.annotations.ForeignKey}). If no annotation
 * is given, the corresponding field will not be added into the database. Note
 * that the native types are always NotNull. Fields which have the annotation
 * {@link com.distrimind.ood.database.annotations.AutoPrimaryKey} must be
 * integer or short values. Fields which have the annotation
 * {@link com.distrimind.ood.database.annotations.RandomPrimaryKey} must be long
 * values. Fields which have the annotation
 * {@link com.distrimind.ood.database.annotations.ForeignKey} must be
 * DatabaseRecord instances.
 * 
 * It is possible also to add the annotation
 * {@link com.distrimind.ood.database.annotations.LoadToMemory} just before the
 * table class declaration. If this annotation is present, the content of the
 * table will loaded into the memory which will speed up queries. But note that
 * every table pointed throw a foreign key in this table must have the same
 * annotation. An exception will be generated, during the class instantiation,
 * if this condition is not respected. The user must be careful to not generate
 * a problem of circularity with the declared foreign keys between every
 * database table. An exception is generated during the table instantiation if
 * this problem occurs.
 * 
 * To get the unique instance of its table, the user must call the static
 * functions {@link DatabaseWrapper#getTableInstance(Class)} or
 * {@link DatabaseWrapper#getTableInstance(String)}. The user must never call
 * the default constructor of the class. This constructor must be protected.
 * Before getting any table instance, the user must associate the package
 * containing the class tables of the same database to a Sql database throw the
 * function {@link DatabaseWrapper#loadDatabase(Package)}.
 * 
 * This class is thread safe
 * 
 * @author Jason Mahdjoub
 * @version 2.1
 * @since OOD 1.0
 * @param <T>
 *            the type of the record
 */
public abstract class Table<T extends DatabaseRecord> {
	final Class<T> class_record;
	final Constructor<T> default_constructor_field;
	final ArrayList<FieldAccessor> auto_random_primary_keys_fields = new ArrayList<FieldAccessor>();
	final ArrayList<FieldAccessor> auto_primary_keys_fields = new ArrayList<FieldAccessor>();
	private final ArrayList<FieldAccessor> primary_keys_fields_no_auto_no_random = new ArrayList<FieldAccessor>();
	final ArrayList<FieldAccessor> primary_keys_fields = new ArrayList<FieldAccessor>();
	final ArrayList<FieldAccessor> unique_fields_no_auto_random_primary_keys = new ArrayList<FieldAccessor>();
	final ArrayList<ForeignKeyFieldAccessor> foreign_keys_fields = new ArrayList<ForeignKeyFieldAccessor>();
	ArrayList<FieldAccessor> fields;
	private final ArrayList<FieldAccessor> fields_without_primary_and_foreign_keys = new ArrayList<FieldAccessor>();
	private AtomicReference<ArrayList<T>> records_instances = new AtomicReference<ArrayList<T>>(new ArrayList<T>());
	private final boolean is_loaded_in_memory;
	private final String table_name;
	private boolean supportSynchronizationWithOtherPeers = false;
	private DatabaseConfiguration tables = null;
	private boolean containsLoopBetweenTables = false;
	public static final int maxTableNameSizeBytes = 8192;
	public static final int maxPrimaryKeysSizeBytes = 65536;

	public Constructor<T> getDefaultRecordConstructor() {
		return default_constructor_field;
	}

	private static class NeighboringTable {
		public final DatabaseWrapper sql_connection;
		public final Class<? extends Table<?>> class_table;
		public final ArrayList<Field> concerned_fields;
		private Table<?> t = null;
		private final Class<? extends DatabaseRecord> class_record;

		public NeighboringTable(DatabaseWrapper _sql_connection, Class<? extends DatabaseRecord> _class_record,
				Class<? extends Table<?>> _class_table, ArrayList<Field> _concerned_fields) {
			sql_connection = _sql_connection;
			class_table = _class_table;
			concerned_fields = _concerned_fields;
			class_record = _class_record;
		}

		public HashMap<String, Object> getHashMapFields(Object _instance) {
			HashMap<String, Object> res = new HashMap<String, Object>();
			for (Field f : concerned_fields) {
				res.put(f.getName(), _instance);
			}
			return res;
		}

		public Table<?> getPoitingTable() throws DatabaseException {
			if (t == null)
				t = sql_connection.getTableInstance(class_table);
			return t;
		}

		public HashMap<String, Object>[] getHashMapsSqlFields(HashMap<String, Object> _primary_keys)
				throws DatabaseException {
			Table<?> t = getPoitingTable();

			@SuppressWarnings("unchecked")
			HashMap<String, Object> res[] = new HashMap[concerned_fields.size()];
			int index = 0;
			for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields) {
				if (fkfa.isAssignableTo(class_record)) {
					res[index] = new HashMap<String, Object>();
					for (SqlField sf : fkfa.getDeclaredSqlFields()) {
						boolean found = false;
						for (String field : _primary_keys.keySet()) {
							if (field.equals(sf.pointed_field)) {
								found = true;
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

		public Class<? extends Table<?>> getPointingTableClass() {
			return class_table;
		}

	}

	final ArrayList<NeighboringTable> list_tables_pointing_to_this_table = new ArrayList<NeighboringTable>();
	boolean isPointedByTableLoadedIntoMemory = false;
	final SecureRandom rand;

	private volatile boolean is_synchronized_with_sql_database = false;
	private volatile long last_refresh = System.currentTimeMillis();
	private final long refreshInterval;

	public List<Class<? extends Table<?>>> getTablesClassesPointingToThisTable() {
		ArrayList<Class<? extends Table<?>>> res = new ArrayList<>(list_tables_pointing_to_this_table.size());
		for (NeighboringTable n : list_tables_pointing_to_this_table) {
			res.add(n.getPointingTableClass());
		}
		return res;
	}

	boolean isSynchronizedWithSqlDatabase() {
		return (refreshInterval > 0 && last_refresh + refreshInterval > System.currentTimeMillis())
				|| is_synchronized_with_sql_database;
	}

	void memoryRefreshed() {
		is_synchronized_with_sql_database = true;
		last_refresh = System.currentTimeMillis();
	}

	void memoryToRefresh() {
		is_synchronized_with_sql_database = false;
	}

	volatile DatabaseWrapper sql_connection;

	@SuppressWarnings("rawtypes")
	private final Constructor<GroupedResults> grouped_results_constructor;

	boolean isPointedDirectlyOrIndirectlyByTablesLoadedIntoMemory() {
		return isPointedByTableLoadedIntoMemory;
	}

	boolean hasToBeLocked() {
		return !sql_connection.isThreadSafe()
				|| (isPointedDirectlyOrIndirectlyByTablesLoadedIntoMemory() || is_loaded_in_memory);
	}

	void lockIfNecessary(boolean writeLock) {
		if (hasToBeLocked()) {
			if (writeLock)
				sql_connection.locker.lockWrite();
			else
				sql_connection.locker.lockRead();
		}
	}

	void unlockIfNecessary(boolean writeLock) {
		if (hasToBeLocked()) {
			if (writeLock)
				sql_connection.locker.unlockWrite();
			else
				sql_connection.locker.unlockRead();
		}
	}

	String getSqlPrimaryKeyName() {
		return this.getName() + "__PK";
	}

	/**
	 * This constructor must never be called. Please use the static functions
	 * {@link DatabaseWrapper#getTableInstance(Class)} or
	 * {@link DatabaseWrapper#getTableInstance(String)}.
	 * 
	 * @throws DatabaseException
	 *             is database constraints are not respected or if a problem of
	 *             database version occured during the Sql loading (typically, when
	 *             the user have modified the fields of its database).
	 */
	@SuppressWarnings("rawtypes")
	protected Table() throws DatabaseException {
		table_name = getName(this.getClass());
		try {
			rand = SecureRandom.getInstance("SHA1PRNG", "SUN");
			rand.nextBytes(new byte[10]);
		} catch (NoSuchAlgorithmException | NoSuchProviderException e) {
			throw new DatabaseException("Impossible to initilize a secured random instance.");
		}

		is_loaded_in_memory = this.getClass().isAnnotationPresent(LoadToMemory.class);
		if (is_loaded_in_memory)
			refreshInterval = this.getClass().getAnnotation(LoadToMemory.class).refreshInterval();
		else
			refreshInterval = 0;

		if (!Modifier.isFinal(this.getClass().getModifiers())) {
			throw new DatabaseException("The table class " + this.getClass().getName() + " must be a final class.");
		}

		boolean constructor_ok = true;
		Constructor<?> constructors[] = this.getClass().getDeclaredConstructors();
		if (constructors.length != 1)
			constructor_ok = false;
		else {
			if (!Modifier.isProtected(constructors[0].getModifiers()))
				constructor_ok = false;
			else if (constructors[0].getParameterTypes().length != 0)
				constructor_ok = false;
		}
		if (!constructor_ok)
			throw new DatabaseException("The class " + this.getClass().getName()
					+ " must have only one constructor which must be declared as protected without any parameter (default constructor)");

		@SuppressWarnings("unchecked")
		Class<T> tmp = (Class<T>) Table.getDatabaseRecord((Class<? extends Table<?>>) this.getClass());
		class_record = tmp;

		DefaultConstructorAccessPrivilegedAction<T> capa = new DefaultConstructorAccessPrivilegedAction<T>(
				class_record);

		try {
			default_constructor_field = AccessController.doPrivileged(capa);
		} catch (PrivilegedActionException e1) {
			throw new DatabaseException(
					"Impossible to find the default constructor of the class " + class_record.getName(), e1);
		}
		grouped_results_constructor = AccessController
				.doPrivileged(new PrivilegedAction<Constructor<GroupedResults>>() {

					@Override
					public Constructor<GroupedResults> run() {
						Constructor<GroupedResults> res;
						try {
							res = (Constructor<GroupedResults>) GroupedResults.class.getDeclaredConstructor(
									DatabaseWrapper.class, Collection.class, Class.class, (new String[1]).getClass());
							res.setAccessible(true);
							return res;
						} catch (NoSuchMethodException | SecurityException e) {
							e.printStackTrace();
							System.exit(-1);
							return null;
						}
					}
				});

	}

	void initializeStep0(DatabaseWrapper wrapper) throws DatabaseException {
		sql_connection = wrapper;
		if (sql_connection == null)
			throw new DatabaseException(
					"No database was given to instanciate the class/table " + this.getClass().getName()
							+ ". Please use the function associatePackageToSqlJetDatabase before !");

		@SuppressWarnings("unchecked")
		Class<? extends Table<?>> table_class = (Class<? extends Table<?>>) this.getClass();
		fields = FieldAccessor.getFields(sql_connection, table_class);
		if (fields.size() == 0)
			throw new DatabaseException("No field has been declared in the class " + class_record.getName());
		for (FieldAccessor f : fields) {
			if (f.isPrimaryKey())
				primary_keys_fields.add(f);
			if (f.isAutoPrimaryKey() || f.isRandomPrimaryKey())
				auto_random_primary_keys_fields.add(f);
			if (f.isAutoPrimaryKey())
				auto_primary_keys_fields.add(f);
			if (!f.isAutoPrimaryKey() && !f.isRandomPrimaryKey() && f.isPrimaryKey())
				primary_keys_fields_no_auto_no_random.add(f);
			if (f.isForeignKey()) {
				foreign_keys_fields.add((ForeignKeyFieldAccessor) f);
			}
			if (!f.isPrimaryKey() && !f.isForeignKey())
				fields_without_primary_and_foreign_keys.add(f);
			if (f.isUnique() && !f.isAutoPrimaryKey() && !f.isRandomPrimaryKey()) {
				unique_fields_no_auto_random_primary_keys.add(f);
			}
		}
		for (FieldAccessor f : fields) {
			for (FieldAccessor f2 : fields) {
				if (f != f2) {
					if (f.getFieldName().equalsIgnoreCase(f2.getFieldName())) {
						throw new DatabaseException("The fields " + f.getFieldName() + " and " + f2.getFieldName()
								+ " have the same name considering that Sql fields is not case sensitive !");
					}
				}
			}
		}
		if (auto_primary_keys_fields.size() > 1)
			throw new DatabaseException(
					"It can have only one autoincrement primary key with Annotation {@link oodforsqljet.annotations.AutoPrimaryKey}. The record "
							+ class_record.getName() + " has " + auto_primary_keys_fields.size()
							+ " AutoPrimary keys.");
		if (primary_keys_fields.size() == 0)
			throw new DatabaseException("There is no primary key declared into the Record " + class_record.getName());

		if (this.getName().equals(DatabaseWrapper.ROW_COUNT_TABLES))
			throw new DatabaseException(
					"This table cannot have the name " + DatabaseWrapper.ROW_COUNT_TABLES + " (case ignored)");
	}

	void removeTableFromDatabaseStep1() throws DatabaseException {
		try {
			Statement st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection().createStatement();
			st.executeUpdate("DROP TRIGGER " + Table.this.getName() + "_ROW_COUNT_TRIGGER_DELETE__"
					+ sql_connection.getSqlComma());
			st.close();
			st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection().createStatement();
			st.executeUpdate("DROP TRIGGER " + Table.this.getName() + "_ROW_COUNT_TRIGGER_INSERT__"
					+ sql_connection.getSqlComma());
			st.close();
			st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection().createStatement();
			st.executeUpdate("DELETE FROM " + DatabaseWrapper.ROW_COUNT_TABLES + " WHERE TABLE_NAME='"
					+ Table.this.getName() + "'" + sql_connection.getSqlComma());
			st.close();
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	void removeTableFromDatabaseStep2() throws DatabaseException {
		if (sql_connection == null)
			return;
		for (NeighboringTable t : this.list_tables_pointing_to_this_table) {
			if (t.getPoitingTable().sql_connection != null) {
				t.getPoitingTable().removeTableFromDatabaseStep2();
			}
		}
		Statement st = null;
		try {
			final StringBuffer SqlQuerry = new StringBuffer("DROP TABLE " + this.getName() + " "
					+ sql_connection.getDropTableIfExistsKeyWord() + " " + sql_connection.getDropTableCascadeKeyWord());

			st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection().createStatement();
			st.executeUpdate(SqlQuerry.toString());
			sql_connection = null;
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		} finally {
			try {
				st.close();
			} catch (SQLException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

	}

	void initializeStep1(DatabaseConfiguration tables) throws DatabaseException {
		if (tables == null)
			throw new NullPointerException("tables");

		for (ForeignKeyFieldAccessor fa : foreign_keys_fields) {
			fa.initialize();
		}
		this.tables = tables;
	}

	public DatabaseConfiguration getDatabaseConfiguration() {
		return tables;
	}

	boolean foreign_keys_to_create = false;

	boolean initializeStep2(final boolean createDatabaseIfNecessaryAndCheckIt) throws DatabaseException {
		containsLoopBetweenTables = containsLoop(new HashSet<Class<? extends Table<?>>>());
		/*
		 * Load table in Sql database
		 */
		boolean table_found;
		try (ReadWriteLock.Lock lock = sql_connection.locker.getAutoCloseableWriteLock()) {
			table_found = ((Boolean) sql_connection.runTransaction(new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException {
					try {
						return new Boolean(sql_connection.doesTableExists(Table.this.getName()));
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			})).booleanValue();

			if (table_found) {
				/*
				 * check the database
				 */
				if (createDatabaseIfNecessaryAndCheckIt)
					sql_connection.runTransaction(new Transaction() {

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_SERIALIZABLE;
						}

						@Override
						public boolean doesWriteData() {
							return false;
						}

						@Override
						public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
							try {
								Pattern col_size_matcher = Pattern.compile("([0-9]+)");
								// try(ReadQuerry rq=new ReadQuerry(_sql_connection.getSqlConnection(), "SELECT
								// COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, IS_AUTOINCREMENT FROM
								// INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE
								// TABLE_NAME='"+Table.this.getName()+"';"))
								try (ColumnsReadQuerry rq = sql_connection.getColumnMetaData(Table.this.getName())) {
									// while (rq.result_set.next())
									while (rq.tableColumnsResultSet.next()) {
										// String col=Table.this.getName()+"."+rq.result_set.getString("COLUMN_NAME");
										String col = Table.this.getName() + "."
												+ rq.tableColumnsResultSet.getColumnName();
										FieldAccessor founded_fa = null;
										SqlField founded_sf = null;
										for (FieldAccessor fa : fields) {
											for (SqlField sf : fa.getDeclaredSqlFields()) {
												if (sf.field.equalsIgnoreCase(col)) {
													founded_fa = fa;
													founded_sf = sf;
													break;
												}
											}
											if (founded_fa != null)
												break;
										}

										if (founded_fa == null)
											throw new DatabaseVersionException(Table.this,
													"The table " + Table.this.getName() + " contains a column named "
															+ col
															+ " which does not correspond to any field of the class "
															+ class_record.getName());
										// String type=rq.result_set.getString("TYPE_NAME").toUpperCase();
										String type = rq.tableColumnsResultSet.getTypeName().toUpperCase();
										if (!founded_sf.type.toUpperCase().startsWith(type))
											throw new DatabaseVersionException(Table.this, "The type of the column "
													+ col + " should  be " + founded_sf.type + " and not " + type);
										if (col_size_matcher.matcher(founded_sf.type).matches()) {
											// int col_size=rq.result_set.getInt("COLUMN_SIZE");
											int col_size = rq.tableColumnsResultSet.getColumnSize();
											Pattern pattern2 = Pattern.compile("(" + col_size + ")");
											if (!pattern2.matcher(founded_sf.type).matches())
												throw new DatabaseVersionException(Table.this,
														"The column " + col + " has a size equals to " + col_size
																+ " (expected " + founded_sf.type + ")");
										}
										// boolean is_null=rq.result_set.getString("IS_NULLABLE").equals("YES");
										boolean is_null = rq.tableColumnsResultSet.isNullable();
										if (is_null == founded_sf.not_null)
											throw new DatabaseVersionException(Table.this,
													"The column " + col + " is expected to be "
															+ (founded_sf.not_null ? "not null" : "nullable"));
										boolean is_autoincrement = rq.result_set.getString("IS_AUTOINCREMENT")
												.equals("YES");
										if (is_autoincrement != founded_fa.isAutoPrimaryKey())
											throw new DatabaseVersionException(Table.this,
													"The column " + col + " is " + (is_autoincrement ? "" : "not ")
															+ "autoincremented into the Sql database where it is "
															+ (is_autoincrement ? "not " : "")
															+ " into the OOD database.");
									}
								}
								sql_connection.checkConstraints(Table.this);
								return null;
							} catch (Exception e) {
								throw DatabaseException.getDatabaseException(e);
							}
						}
					});
			} else {
				if (createDatabaseIfNecessaryAndCheckIt) {

					final StringBuffer SqlQuerry = new StringBuffer(
							"CREATE " + sql_connection.getCachedKeyword() + " TABLE " + this.getName() + "(");

					boolean first = true;
					for (FieldAccessor f : fields) {
						if (first)
							first = false;
						else
							SqlQuerry.append(", ");
						SqlQuerry.append(getSqlFieldDeclaration(f));
					}
					if (primary_keys_fields.size() > 0) {
						SqlQuerry.append(", CONSTRAINT " + getSqlPrimaryKeyName() + " PRIMARY KEY(");
						first = true;
						for (FieldAccessor fa : primary_keys_fields) {
							for (SqlField sf : fa.getDeclaredSqlFields()) {
								if (first)
									first = false;
								else
									SqlQuerry.append(", ");
								SqlQuerry.append(sf.short_field);
							}
						}
						SqlQuerry.append(")");
					}

					foreign_keys_to_create = true;

					for (FieldAccessor f : fields) {
						if (f.isUnique() && !f.isForeignKey()) {
							first = true;
							SqlQuerry.append(", UNIQUE(");
							for (SqlField sf : f.getDeclaredSqlFields()) {
								if (first)
									first = false;
								else
									SqlQuerry.append(", ");
								SqlQuerry.append(sf.short_field);
							}
							SqlQuerry.append(")");
						}
					}

					SqlQuerry.append(")" + sql_connection.getSqlComma());

					sql_connection.runTransaction(new Transaction() {

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_SERIALIZABLE;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

						@Override
						public Object run(DatabaseWrapper sql_connection) throws DatabaseException {
							Statement st = null;
							try {
								st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();

								st.executeUpdate(SqlQuerry.toString());

							} catch (SQLException e) {
								throw DatabaseException.getDatabaseException(e);
							} finally {
								try {
									st.close();
								} catch (SQLException e) {
									throw DatabaseException.getDatabaseException(e);
								}
							}
							return null;
						}
					});
					sql_connection.runTransaction(new Transaction() {
						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_SERIALIZABLE;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

						@Override
						public Object run(DatabaseWrapper sql_connection) throws DatabaseException {
							Statement st = null;
							try {
								st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("INSERT INTO " + DatabaseWrapper.ROW_COUNT_TABLES + " VALUES('"
										+ Table.this.getName() + "', 0)" + sql_connection.getSqlComma());
								st.close();
								st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("CREATE TRIGGER " + Table.this.getName()
										+ "_ROW_COUNT_TRIGGER_INSERT__ AFTER INSERT ON " + Table.this.getName() + "\n"
										+ "FOR EACH ROW \n" + "UPDATE " + DatabaseWrapper.ROW_COUNT_TABLES
										+ " SET ROW_COUNT=ROW_COUNT+1 WHERE TABLE_NAME='" + Table.this.getName() + "'\n"
										+ sql_connection.getSqlComma());
								st.close();
								st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("CREATE TRIGGER " + Table.this.getName()
										+ "_ROW_COUNT_TRIGGER_DELETE__ AFTER DELETE ON " + Table.this.getName() + "\n"
										+ "FOR EACH ROW \n" + "UPDATE " + DatabaseWrapper.ROW_COUNT_TABLES
										+ " SET ROW_COUNT=ROW_COUNT-1 WHERE TABLE_NAME='" + Table.this.getName() + "'\n"
										+ sql_connection.getSqlComma());
								st.close();
								st = null;
							} catch (SQLException e) {
								throw DatabaseException.getDatabaseException(e);
							} finally {
								try {
									if (st != null)
										st.close();
								} catch (SQLException e) {
									throw DatabaseException.getDatabaseException(e);
								}
							}
							return null;
						}
					});
					for (FieldAccessor fa : fields) {
						if (fa.hasToCreateIndex()) {
							final StringBuffer indexCreationQuerry = new StringBuffer("CREATE INDEX ");
							indexCreationQuerry.append(fa.getIndexName());
							indexCreationQuerry.append(" ON ");
							indexCreationQuerry.append(getName() + " (");
							boolean first2 = true;
							for (SqlField sf : fa.getDeclaredSqlFields()) {
								if (first2)
									first2 = false;
								else
									indexCreationQuerry.append(", ");
								indexCreationQuerry.append(sf.short_field + (fa.isDescendentIndex() ? " DESC" : ""));
							}
							indexCreationQuerry.append(")");
							sql_connection.runTransaction(new Transaction() {

								@Override
								public TransactionIsolation getTransactionIsolation() {
									return TransactionIsolation.TRANSACTION_SERIALIZABLE;
								}

								@Override
								public boolean doesWriteData() {
									return true;
								}

								@Override
								public Object run(DatabaseWrapper sql_connection) throws DatabaseException {
									Statement st = null;
									try {
										st = sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
												.createStatement();

										st.executeUpdate(indexCreationQuerry.toString());

									} catch (SQLException e) {
										throw DatabaseException.getDatabaseException(e);
									} finally {
										try {
											st.close();
										} catch (SQLException e) {
											throw DatabaseException.getDatabaseException(e);
										}
									}
									return null;
								}
							});
						}
					}

				} else
					throw new DatabaseException("Table " + this.getName() + " doest not exists !");
			}
		}
		boolean this_class_found = false;
		for (Class<? extends Table<?>> c : tables.getTableClasses()) {
			if (c.equals(this.getClass()))
				this_class_found = true;

			Class<? extends DatabaseRecord> cdf = getDatabaseRecord(c);
			ArrayList<Field> concerned_fields = new ArrayList<Field>();
			for (Field f : cdf.getDeclaredFields()) {
				if (f.isAnnotationPresent(ForeignKey.class) && f.getType().equals(class_record)) {
					concerned_fields.add(f);
				}
			}
			if (concerned_fields.size() > 0) {
				list_tables_pointing_to_this_table
						.add(new NeighboringTable(sql_connection, class_record, c, concerned_fields));
			}

		}

		if (!this_class_found)
			throw new DatabaseException("Impossible to list and found local classes.");
		return table_found;

	}

	boolean isPointedByTableLoadedIntoMemoryInCascade(List<NeighboringTable> list_tables_pointing_to_this_table,
			List<Class<?>> tableAlreadyParsed) throws DatabaseException {
		if (tableAlreadyParsed.contains(this.getClass()))
			return false;
		tableAlreadyParsed.add(this.getClass());
		for (NeighboringTable nt : list_tables_pointing_to_this_table) {
			if (nt.class_table.isAnnotationPresent(LoadToMemory.class))
				return true;
			Table<?> t = sql_connection.getTableInstance(nt.class_table);
			if (t.isPointedByTableLoadedIntoMemoryInCascade(t.list_tables_pointing_to_this_table, tableAlreadyParsed))
				return true;
		}
		return false;
	}

	void initializeStep3() throws DatabaseException {
		try (ReadWriteLock.WriteLock lock2 = sql_connection.locker.getAutoCloseableWriteLock()) {

			isPointedByTableLoadedIntoMemory = isPointedByTableLoadedIntoMemoryInCascade(
					list_tables_pointing_to_this_table, new ArrayList<Class<?>>());

			if (foreign_keys_to_create) {
				foreign_keys_to_create = false;

				if (foreign_keys_fields.size() > 0) {
					for (ForeignKeyFieldAccessor f : foreign_keys_fields) {
						final StringBuffer SqlQuerry = new StringBuffer(
								"ALTER TABLE " + Table.this.getName() + " ADD FOREIGN KEY(");
						boolean first = true;
						for (SqlField sf : f.getDeclaredSqlFields()) {
							if (first)
								first = false;
							else
								SqlQuerry.append(", ");
							SqlQuerry.append(sf.short_field);
						}
						SqlQuerry.append(") REFERENCES " + f.getPointedTable().getName() + "(");
						first = true;
						for (SqlField sf : f.getDeclaredSqlFields()) {
							if (first)
								first = false;
							else
								SqlQuerry.append(", ");
							SqlQuerry.append(sf.short_pointed_field);
						}
						// SqlQuerry.append(") ON UPDATE CASCADE ON DELETE CASCADE");
						SqlQuerry.append(") " + sql_connection.getOnDeleteCascadeSqlQuerry() + " "
								+ sql_connection.getOnUpdateCascadeSqlQuerry());
						sql_connection.runTransaction(new Transaction() {

							@Override
							public TransactionIsolation getTransactionIsolation() {
								return TransactionIsolation.TRANSACTION_SERIALIZABLE;
							}

							@Override
							public boolean doesWriteData() {
								return true;
							}

							@Override
							public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
								Statement st = null;
								try {
									st = _sql_connection.getConnectionAssociatedWithCurrentThread().getConnection()
											.createStatement();
									st.executeUpdate(SqlQuerry.toString());

									return null;
								} catch (Exception e) {
									throw DatabaseException.getDatabaseException(e);
								} finally {
									try {
										st.close();
									} catch (Exception e) {
										throw DatabaseException.getDatabaseException(e);
									}
								}
							}
						});

					}
				}

			}
			supportSynchronizationWithOtherPeers = isGloballyDecentralizable(new HashSet<Table<?>>());
		}
	}

	public boolean supportSynchronizationWithOtherPeers() {
		return supportSynchronizationWithOtherPeers;
	}

	private boolean isGloballyDecentralizable(HashSet<Table<?>> checkedTables) throws DatabaseException {
		if (!isLocallyDecentralizable())
			return false;
		checkedTables.add(this);
		for (NeighboringTable nt : list_tables_pointing_to_this_table) {
			Table<?> t = nt.getPoitingTable();
			if (!checkedTables.contains(t)) {
				if (!t.isGloballyDecentralizable(checkedTables))
					return false;
			}
		}
		for (ForeignKeyFieldAccessor fa : foreign_keys_fields) {
			Table<?> t = fa.getPointedTable();
			if (!checkedTables.contains(t) && !t.isGloballyDecentralizable(checkedTables))
				return false;
		}
		return true;
	}

	private boolean isLocallyDecentralizable() {
		return hasDecentralizedPrimaryKey() && !hasNonDecentralizedIDUniqueKey();
	}

	private boolean hasDecentralizedPrimaryKey() {
		for (FieldAccessor fa : primary_keys_fields) {
			if (fa instanceof AbstractDencetralizedIDFieldAccessor)
				return true;
		}
		return false;
	}

	private boolean hasNonDecentralizedIDUniqueKey() {
		for (FieldAccessor fa : fields) {
			if (!(fa instanceof AbstractDencetralizedIDFieldAccessor) && fa.isUnique())
				return true;
		}
		return false;
	}

	@SuppressWarnings({ "unchecked", "unused" })
	private T getRecordFromPointingRecord(final SqlFieldInstance[] _sql_field_instances,
			final ArrayList<DatabaseRecord> _previous_pointing_records) throws DatabaseException {
		if (isLoadedInMemory() && isSynchronizedWithSqlDatabase()) {
			for (T r : getRecords(-1, -1, false)) {
				boolean all_equals = true;
				for (FieldAccessor fa : primary_keys_fields) {
					for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(r)) {
						boolean found = false;
						for (SqlFieldInstance sfi2 : _sql_field_instances) {
							if (sfi2.pointed_field.equals(sfi.field)) {
								found = true;
								if (!FieldAccessor.equalsBetween(sfi.instance, sfi2.instance)) {
									all_equals = false;
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
		} else {
			for (DatabaseRecord dr : _previous_pointing_records) {
				if (dr.getClass().equals(class_record)) {
					boolean all_equals = true;
					for (FieldAccessor fa : primary_keys_fields) {
						for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(dr)) {
							boolean found = false;
							for (SqlFieldInstance sfi2 : _sql_field_instances) {
								if (sfi2.pointed_field.equals(sfi.field)) {
									found = true;
									if (!FieldAccessor.equalsBetween(sfi.instance, sfi2.instance)) {
										all_equals = false;
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
						return (T) dr;
				}
			}

			for (FieldAccessor fa : primary_keys_fields) {
				for (SqlField sfi : fa.getDeclaredSqlFields()) {
					boolean found = false;
					for (SqlFieldInstance sfi2 : _sql_field_instances) {
						if (sfi2.pointed_field.equals(sfi.field)) {
							found = true;
							break;

						}
					}
					if (!found)
						throw new DatabaseException("Unexpected exception.");
				}
			}

			final StringBuffer querry = new StringBuffer(
					"SELECT " + getSqlSelectStep1Fields(true, null) + " FROM " + getFromPart(true, null) + " WHERE ");
			boolean first = true;
			for (SqlFieldInstance sfi : _sql_field_instances) {
				if (first)
					first = false;
				else
					querry.append(" AND ");
				querry.append(sfi.pointed_field);
				querry.append(" = ?");
			}
			querry.append(sql_connection.getSqlComma());

			final SqlQuerry sqlquerry = new SqlQuerry(querry.toString()) {

				@Override
				void finishPrepareStatement(PreparedStatement st) throws SQLException {
					int i = 1;
					for (SqlFieldInstance sfi : _sql_field_instances) {
						st.setObject(i++, sfi.instance);
					}
				}

			};

			return (T) sql_connection.runTransaction(new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try (ReadQuerry rq = new ReadQuerry(
							_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(), sqlquerry)) {
						if (rq.result_set.next()) {
							T res = getNewRecordInstance();
							for (FieldAccessor fa : fields) {
								fa.setValue(res, rq.result_set, _previous_pointing_records);
							}
							return res;
						}
						return null;
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			});
		}

	}

	T getNewRecordInstance(Constructor<T> constructor)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		T res = constructor.newInstance();
		res.__createdIntoDatabase = true;
		return res;
	}

	T getNewRecordInstance()
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return getNewRecordInstance(default_constructor_field);
	}

	private String getSqlSelectStep1Fields(boolean includeAllJunctions, Set<TableJunction> tablesJunction) {
		if (isLoadedInMemory()) {
			includeAllJunctions = false;
			tablesJunction = null;
		}

		StringBuffer sb = new StringBuffer();
		getSqlSelectStep1Fields(includeAllJunctions, tablesJunction, sb);
		return sb.toString();
	}

	private void getSqlSelectStep1Fields(boolean includeAllJunctions, Set<TableJunction> tablesJunction,
			StringBuffer sb) {
		if (containsLoopBetweenTables) {
			includeAllJunctions = false;
			if (tablesJunction != null)
				tablesJunction = null;
		}

		for (FieldAccessor fa : fields) {
			for (SqlField sf : fa.getDeclaredSqlFields()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(sf.field);
			}
			if (fa instanceof ForeignKeyFieldAccessor) {
				Table<?> t = ((ForeignKeyFieldAccessor) fa).getPointedTable();
				if (includeAllJunctions || containsPointedTable(tablesJunction, t))
					t.getSqlSelectStep1Fields(includeAllJunctions, tablesJunction, sb);
			}
		}

	}

	SqlQuerry getSqlGeneralSelect(boolean loadJunctions) {
		return new SqlQuerry(
				"SELECT " + getSqlSelectStep1Fields(loadJunctions, null) + " FROM " + getFromPart(loadJunctions, null));
	}

	SqlQuerry getSqlGeneralSelect(boolean loadJunctions, boolean ascendant, String orderByFields[]) {
		if (orderByFields != null && orderByFields.length > 0)
			loadJunctions = true;
		return new SqlQuerry("SELECT " + getSqlSelectStep1Fields(loadJunctions, null) + " FROM "
				+ getFromPart(loadJunctions, null) + getOrderByPart(ascendant, orderByFields));
	}

	SqlQuerry getSqlGeneralSelect(boolean loadJunctions, String condition, final Map<Integer, Object> parameters) {
		return getSqlGeneralSelect(loadJunctions, condition, parameters, true, new String[] {});
	}

	SqlQuerry getSqlGeneralCount(String condition, final Map<Integer, Object> parameters,
			Set<TableJunction> tablesJunction) {
		if (condition == null || condition.trim().equals(""))
			return new SqlQuerry("SELECT COUNT(*) FROM " + getFromPart(false, tablesJunction));
		else
			return new SqlQuerry("SELECT COUNT(*) FROM " + getFromPart(false, tablesJunction) + " WHERE " + condition) {
				@Override
				void finishPrepareStatement(PreparedStatement st) throws SQLException {
					if (parameters != null) {
						int index = 1;
						Object p = parameters.get(new Integer(index++));
						while (p != null) {
							st.setObject(index, p);
							p = parameters.get(new Integer(index++));
						}

					}
				}
			};

	}

	private String getFromPart(boolean includeAllJunctions, Set<TableJunction> tablesJunction) {
		if (isLoadedInMemory()) {
			includeAllJunctions = false;
			tablesJunction = null;
		}
		StringBuffer sb = new StringBuffer(this.getName());
		if (!includeAllJunctions && (tablesJunction == null || tablesJunction.size() == 0))
			return sb.toString();

		for (ForeignKeyFieldAccessor fa : getForeignKeysFieldAccessors()) {
			sb.append(getFromPart(fa, includeAllJunctions, tablesJunction));
		}
		return sb.toString();
	}

	private boolean containsPointedTable(Set<TableJunction> tablesJunction, Table<?> table) {
		if (tablesJunction == null)
			return false;
		for (TableJunction tj : tablesJunction) {
			if (tj.getTablePointed().equals(table))
				return true;
		}
		return false;

	}

	@SuppressWarnings("unchecked")
	private boolean containsLoop(Set<Class<? extends Table<?>>> tablesParsed) {

		if (!tablesParsed.add((Class<? extends Table<?>>) this.getClass()))
			return true;
		for (ForeignKeyFieldAccessor fa2 : getForeignKeysFieldAccessors()) {
			Table<?> t = fa2.getPointedTable();
			if (t.containsLoop(tablesParsed))
				return true;
		}
		return false;

	}

	private StringBuffer getFromPart(ForeignKeyFieldAccessor fa, boolean includeAllJunctions,
			Set<TableJunction> tablesJunction) {
		if (containsLoopBetweenTables) {
			includeAllJunctions = false;
			if (tablesJunction != null)
				tablesJunction = null;
		}

		StringBuffer sb = new StringBuffer();
		if (!includeAllJunctions && !containsPointedTable(tablesJunction, fa.getPointedTable()))
			return sb;
		sb.append(" LEFT OUTER JOIN ");
		sb.append(fa.getPointedTable().getName());
		sb.append(" ON ");
		boolean firstOn = true;
		for (SqlField sf : fa.getDeclaredSqlFields()) {
			if (firstOn)
				firstOn = false;
			else
				sb.append(" AND ");
			sb.append(sf.field);
			sb.append("=");
			sb.append(sf.pointed_field);
		}
		Table<?> t = fa.getPointedTable();
		for (ForeignKeyFieldAccessor fa2 : t.getForeignKeysFieldAccessors()) {
			sb.append(t.getFromPart(fa2, includeAllJunctions, tablesJunction));
		}
		return sb;
	}

	String getOrderByPart(boolean _ascendant, String... _fields) {
		if (_fields == null || _fields.length == 0)
			return "";
		StringBuffer orderBySqlFields = new StringBuffer();
		for (String s : _fields) {
			if (orderBySqlFields.length() > 0)
				orderBySqlFields.append(", ");
			orderBySqlFields.append(getFieldToComparare(s));
			orderBySqlFields.append(_ascendant ? " ASC" : " DESC");
		}

		if (orderBySqlFields.length() > 0) {
			orderBySqlFields.insert(0, " ORDER BY ");

		}
		return orderBySqlFields.toString();
	}

	SqlQuerry getSqlGeneralSelect(boolean loadJunctions, final String condition, final Map<Integer, Object> parameters,
			boolean _ascendant, String... _fields) {
		if (_fields.length > 0)
			loadJunctions = true;
		if (condition == null || condition.trim().equals(""))
			return new SqlQuerry("SELECT " + getSqlSelectStep1Fields(loadJunctions, null) + " FROM "
					+ getFromPart(loadJunctions, null) + " " + getOrderByPart(_ascendant, _fields));
		else
			return new SqlQuerry("SELECT " + getSqlSelectStep1Fields(loadJunctions, null) + " FROM "
					+ getFromPart(loadJunctions, null) + " WHERE " + condition + getOrderByPart(_ascendant, _fields)) {
				@Override
				void finishPrepareStatement(PreparedStatement st) throws SQLException {
					if (parameters != null) {
						int index = 1;

						Object p = parameters.get(new Integer(index));
						while (p != null) {

							FieldAccessor.setValue(getDatabaseWrapper(), st, index, p);
							// st.setObject(index, p);
							p = parameters.get(new Integer(++index));
						}

					}
				}
			};
	}

	private class SqlGeneralSelectQuerryWithFieldMatch extends SqlQuerry {
		private final Map<String, Object> fields;

		SqlGeneralSelectQuerryWithFieldMatch(boolean loadJunctions, Map<String, Object> fields, String AndOr,
				boolean ascendant, String[] orderByFields) {
			super(getSqlGeneralSelectWithFieldMatch(loadJunctions, fields, AndOr, ascendant, orderByFields));
			this.fields = fields;
		}

		@Override
		public void finishPrepareStatement(PreparedStatement st) throws DatabaseException {
			int index = 1;
			for (String key : fields.keySet()) {
				for (FieldAccessor fa : getFieldAccessors()) {
					if (fa.getFieldName().equals(key)) {
						fa.getValue(st, index, fields.get(key));
						index += fa.getDeclaredSqlFields().length;
						break;
					}
				}
			}
		}
	}

	private class SqlGeneralSelectQuerryWithMultipleFieldMatch extends SqlQuerry {
		private final Map<String, Object> records[];

		SqlGeneralSelectQuerryWithMultipleFieldMatch(boolean loadJunctions, Map<String, Object> records[], String AndOr,
				boolean asendant, String[] orderByFields) {
			super(getSqlGeneralSelectWithMultipleFieldMatch(loadJunctions, records, AndOr, asendant, orderByFields));
			this.records = records;
		}

		@Override
		public void finishPrepareStatement(PreparedStatement st) throws DatabaseException {
			int index = 1;
			for (Map<String, Object> fields : records) {
				for (String key : fields.keySet()) {
					for (FieldAccessor fa : getFieldAccessors()) {
						if (fa.getFieldName().equals(key)) {
							fa.getValue(st, index++, fields.get(key));
							break;
						}
					}
				}
			}
		}
	}

	String getSqlGeneralSelectWithFieldMatch(boolean loadJunctions, Map<String, Object> fields, String AndOr,
			boolean ascendant, String[] orderByFields) {
		StringBuffer sb = new StringBuffer(getSqlGeneralSelect(loadJunctions).getQuerry());
		boolean first = true;
		sb.append(" WHERE");
		for (String key : fields.keySet()) {
			for (FieldAccessor fa : getFieldAccessors()) {
				if (fa.getFieldName().equals(key)) {
					if (first) {
						first = false;
					} else
						sb.append(" " + AndOr + " ");
					sb.append("(");
					boolean firstMultiField = true;
					for (SqlField sf : fa.getDeclaredSqlFields()) {
						sb.append(" ");
						if (firstMultiField)
							firstMultiField = false;
						else
							sb.append(" AND ");
						sb.append(sf.field + "=?");
					}
					sb.append(")");
					break;
				}
			}
		}
		sb.append(getOrderByPart(ascendant, orderByFields));
		sb.append(sql_connection.getSqlComma());
		return sb.toString();
	}

	String getSqlGeneralSelectWithMultipleFieldMatch(boolean loadJunctions, Map<String, Object> records[], String AndOr,
			boolean asendant, String[] orderByFields) {
		StringBuffer sb = new StringBuffer(getSqlGeneralSelect(loadJunctions).getQuerry());

		boolean firstOR = true;
		sb.append(" WHERE");
		for (Map<String, Object> fields : records) {
			if (firstOR)
				firstOR = false;
			else
				sb.append(" OR");
			sb.append(" (");
			boolean first = true;
			for (String key : fields.keySet()) {
				for (FieldAccessor fa : getFieldAccessors()) {
					if (fa.getFieldName().equals(key)) {
						for (SqlField sf : fa.getDeclaredSqlFields()) {
							sb.append(" ");
							if (first)
								first = false;
							else
								sb.append(AndOr + " ");
							sb.append(sf.field + "=?");
						}
						break;
					}
				}
			}
			sb.append(")");
		}
		sb.append(getOrderByPart(asendant, orderByFields));
		sb.append(sql_connection.getSqlComma());
		return sb.toString();
	}

	/**
	 * Returns the number of records contained into this table.
	 * 
	 * @return the number of records contained into this table.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber() throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			if (isLoadedInMemory())
				return getRecords().size();
			else {
				return getRowCount();
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to the given parameters
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of records corresponding to the given parameters
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber(Filter<T> _filter, String whereCondition, Map<String, Object> parameters)
			throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(_filter, whereCondition, parameters, false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to the given parameters
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of records corresponding to the given parameters
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber(Filter<T> _filter, String whereCondition, Object... parameters)
			throws DatabaseException {
		return getRecordsNumber(_filter, whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Returns the number of records corresponding to the given parameters
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of records corresponding to the given parameters
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber(String whereCondition, Object... parameters) throws DatabaseException {
		return getRecordsNumber(whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Returns the number of records corresponding to the given parameters
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of records corresponding to the given parameters
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber(String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(whereCondition, parameters, false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to the given parameters
	 * 
	 * @param _filter
	 *            the filter
	 * @return the number of records corresponding to the given parameters
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumber(Filter<T> _filter) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(_filter, false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to all given fields
	 * 
	 * @return the number of records corresponding to all given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	@SafeVarargs
	public final long getRecordsNumberWithAllFields(final Map<String, Object>... _records) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(new MultipleAllFieldsFilter(true, null, fields, _records), false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to all given fields
	 * 
	 * @return the number of records corresponding to all given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	@SafeVarargs
	public final long getRecordsNumberWithAllFields(final Object[]... _records) throws DatabaseException {
		return getRecordsNumberWithAllFields(convertToMap((Object[]) _records));
	}

	/**
	 * Returns the number of records corresponding to all given fields
	 * 
	 * @return the number of records corresponding to all given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumberWithAllFields(final Map<String, Object> _records) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(new SimpleAllFieldsFilter(true, null, _records, fields), false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding to all given fields
	 * 
	 * @return the number of records corresponding to all given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumberWithAllFields(final Object... _records) throws DatabaseException {
		return getRecordsNumberWithAllFields(convertToMap(_records));
	}

	/**
	 * Returns the number of records corresponding one of the given fields
	 * 
	 * @return the number of records corresponding one of the given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	@SafeVarargs
	public final long getRecordsNumberWithOneOfFields(final Map<String, Object>... _records) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(new MultipleOneOfFieldsFilter(true, null, fields, _records), false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding one of the given fields
	 * 
	 * @return the number of records corresponding one of the given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	@SafeVarargs
	public final long getRecordsNumberWithOneOfFields(final Object[]... _records) throws DatabaseException {
		return getRecordsNumberWithOneOfFields(convertToMap((Object[]) _records));
	}

	/**
	 * Returns the number of records corresponding one of the given fields
	 * 
	 * @return the number of records corresponding one of the given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumberWithOneOfFields(final Map<String, Object> _records) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			return getRowCount(new SimpleOneOfFieldsFilter(true, null, _records, fields), false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Returns the number of records corresponding one of the given fields
	 * 
	 * @return the number of records corresponding one of the given fields
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final long getRecordsNumberWithOneOfFields(final Object... _records) throws DatabaseException {
		return getRecordsNumberWithOneOfFields(convertToMap(_records));
	}

	private final long getRowCount() throws DatabaseException {
		Transaction t = new Transaction() {

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_COMMITTED;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try (ReadQuerry rq = new ReadQuerry(
						_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
						new SqlQuerry("SELECT ROW_COUNT FROM " + DatabaseWrapper.ROW_COUNT_TABLES
								+ " WHERE TABLE_NAME='" + Table.this.getName() + "'"))) {
					if (rq.result_set.next()) {
						return new Long(rq.result_set.getLong(1));
					} else
						throw new DatabaseException("Unexpected exception.");
				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}
		};
		return ((Long) sql_connection.runTransaction(t)).longValue();
	}

	private final long getRowCount(final Filter<T> _filter, String where, Map<String, Object> parameters,
			boolean is_already_sql_transaction) throws DatabaseException {
		final RuleInstance rule = Interpreter.getRuleInstance(where);
		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_already_sql_transaction);
			long rowcount = 0;
			for (T r : records) {
				if (rule.isConcernedBy(this, parameters, r)) {
					if (_filter.nextRecord(r))
						++rowcount;
					if (_filter.isTableParsingStoped())
						break;
				}
			}
			return rowcount;
		} else {
			HashMap<Integer, Object> sqlParameters = new HashMap<>();
			String sqlQuery = rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
					.toString();
			final AtomicLong rowcount = new AtomicLong(0);
			getListRecordsFromSqlConnection(new Runnable() {

				@Override
				public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException {
					if (_filter.nextRecord(r))
						rowcount.incrementAndGet();
					return !_filter.isTableParsingStoped();
				}

				@Override
				public void init(int _field_count) {
				}
			}, getSqlGeneralSelect(true, sqlQuery, sqlParameters), TransactionIsolation.TRANSACTION_READ_COMMITTED, -1,
					-1, false);
			return rowcount.get();
		}
	}

	private final long getRowCount(String where, Map<String, Object> parameters, boolean is_already_sql_transaction)
			throws DatabaseException {
		final RuleInstance rule = Interpreter.getRuleInstance(where);
		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_already_sql_transaction);
			long rowcount = 0;
			for (T r : records) {
				if (rule.isConcernedBy(this, parameters, r)) {
					++rowcount;
				}
			}
			return rowcount;
		} else {
			final HashMap<Integer, Object> sqlParameters = new HashMap<>();
			final Set<TableJunction> tablesJunction = new HashSet<>();
			final String sqlQuery = rule.translateToSqlQuery(this, parameters, sqlParameters, tablesJunction)
					.toString();
			Transaction t = new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try (ReadQuerry rq = new ReadQuerry(
							_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
							getSqlGeneralCount(sqlQuery, sqlParameters, tablesJunction))) {
						if (rq.result_set.next()) {
							return new Long(rq.result_set.getLong(1));
						} else
							throw new DatabaseException("Unexpected exception.");
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			};
			return ((Long) sql_connection.runTransaction(t)).longValue();
		}
	}

	private final long getRowCount(final Filter<T> _filter, boolean is_already_sql_transaction)
			throws DatabaseException {

		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_already_sql_transaction);
			long count = 0;
			for (T r : records) {
				if (_filter.nextRecord(r))
					++count;

				if (_filter.isTableParsingStoped())
					break;
			}
			return count;
		} else {
			final boolean persoFilter = (_filter instanceof Table.PersonnalFilter);
			final AtomicLong pos = new AtomicLong(0);
			getListRecordsFromSqlConnection(new Runnable() {

				@Override
				public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException {
					if (persoFilter || _filter.nextRecord(r))
						pos.incrementAndGet();
					return !_filter.isTableParsingStoped();
				}

				@Override
				public void init(int _field_count) {
				}
			}, persoFilter ? ((PersonnalFilter) _filter).getSQLQuerry(true) : getSqlGeneralSelect(true),
					TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
			return pos.get();
		}
	}

	/**
	 * 
	 * @return the fields corresponding to this table
	 */
	public final ArrayList<FieldAccessor> getFieldAccessors() {
		return fields;
	}

	/**
	 * @param fieldName
	 *            the field name
	 * @return the field corresponding to this table and the given field name
	 * @throws DatabaseException
	 * 
	 */
	public final FieldAccessor getFieldAccessor(String fieldName) {
		return getFieldAccessor(fieldName, new HashSet<RuleInstance.TableJunction>());
	}

	public final FieldAccessor getFieldAccessor(String fieldName, Set<RuleInstance.TableJunction> tablesJunction) {
		int index = 0;
		while (index < fieldName.length()) {
			if (fieldName.charAt(index) == '.')
				break;
			++index;
		}
		String prefix = fieldName.substring(0, index);

		for (FieldAccessor f : fields) {
			if (f.getFieldName().equals(prefix)) {
				if (index == fieldName.length())
					return f;
				else if (f instanceof ForeignKeyFieldAccessor) {

					ForeignKeyFieldAccessor fkfa = (ForeignKeyFieldAccessor) f;
					tablesJunction.add(new RuleInstance.TableJunction(this, fkfa.getPointedTable(), fkfa));
					return fkfa.getPointedTable().getFieldAccessor(fieldName.substring(index + 1), tablesJunction);
				}
			}
		}
		return null;
	}

	/**
	 * 
	 * @return the fields corresponding to this table
	 */
	public final ArrayList<ForeignKeyFieldAccessor> getForeignKeysFieldAccessors() {
		return this.foreign_keys_fields;
	}

	/**
	 * 
	 * @return the primary keys corresponding to this table
	 */
	public final ArrayList<FieldAccessor> getPrimaryKeysFieldAccessors() {
		return primary_keys_fields;
	}

	private final String getSqlFieldDeclaration(FieldAccessor field) {
		final String sqlNull = " " + sql_connection.getSqlNULL();
		final String sqlNotNull = " " + sql_connection.getSqlNotNULL();

		if (field.isForeignKey()) {
			String res = "";

			boolean first = true;
			for (SqlField sf : field.getDeclaredSqlFields()) {
				if (first)
					first = false;
				else
					res += ", ";
				res += sf.short_field + " " + sf.type + (sf.not_null ? sqlNotNull : sqlNull);
			}

			return res;
		} else {
			String res = "";
			boolean first = true;

			for (SqlField sf : field.getDeclaredSqlFields()) {
				if (first)
					first = false;
				else
					res += ", ";

				res += sf.short_field + " " + sf.type;
				if (field.isAutoPrimaryKey())
					res += " GENERATED BY DEFAULT AS IDENTITY(START WITH " + field.getStartValue() + ")";
				res += (sf.not_null ? sqlNotNull : sqlNull);
			}
			return res;
		}

	}

	/**
	 * Returns the corresponding Table Class to a DatabaseRecord.
	 * 
	 * @param _record_class
	 *            the DatabaseRecord class
	 * @return the corresponding Table Class.
	 * @throws DatabaseException
	 *             if database constaints are not respected.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public static final Class<? extends Table<?>> getTableClass(Class<? extends DatabaseRecord> _record_class)
			throws DatabaseException {
		if (_record_class == null)
			throw new NullPointerException("The parameter _record_class is a null pointer !");

		Class<?> declaring_class = _record_class.getDeclaringClass();
		if (declaring_class == null)
			throw new DatabaseException("The DatabaseRecord class " + _record_class.getName()
					+ " was not declared into a class extending " + Table.class.getName());
		if (!(declaring_class.getName() + "$" + "Record").equals(_record_class.getName()))
			throw new DatabaseException("The DatabaseRecord class " + _record_class.getName()
					+ " have not the expected name " + declaring_class.getName() + "$" + "Record");
		if (!Table.class.isAssignableFrom(declaring_class))
			throw new DatabaseException("The class " + declaring_class + " in which is declared the class "
					+ _record_class.getName() + " does not extends the class " + Table.class.getName());
		@SuppressWarnings("unchecked")
		Class<? extends Table<?>> res = (Class<? extends Table<?>>) declaring_class;
		return res;
	}

	/**
	 * Returns the corresponding DatabaseRecord class to a given Table class.
	 * 
	 * @param _table_class
	 *            the table class
	 * @return the corresponding DatabaseRecord.
	 * @throws DatabaseException
	 *             if database constaints are not respected.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public static final Class<? extends DatabaseRecord> getDatabaseRecord(Class<? extends Table<?>> _table_class)
			throws DatabaseException {
		if (_table_class == null)
			throw new NullPointerException("The parameter _table_class is a null pointer !");

		Class<? extends DatabaseRecord> res = null;
		for (Class<?> c : _table_class.getDeclaredClasses()) {
			if (DatabaseRecord.class.isAssignableFrom(c) && c.getSimpleName().equals("Record")) {
				@SuppressWarnings("unchecked")
				Class<? extends DatabaseRecord> tmp = (Class<? extends DatabaseRecord>) c;
				res = tmp;
				break;
			}
		}
		if (res == null)
			throw new DatabaseException("The class " + _table_class.getName() + " which inherits the class "
					+ Table.class.getName() + " does not have any inner class named Field which inherits the class "
					+ DatabaseRecord.class.getName());
		else {
			if (!Modifier.isStatic(res.getModifiers()))
				throw new DatabaseException("The class " + res.getName() + " must be a static member class.");
			boolean ok = true;
			try {
				res.getDeclaredConstructor();
			} catch (NoSuchMethodException e) {
				ok = false;
			}
			if (!ok)
				throw new DatabaseException(
						"The class " + res.getName() + " must have a default constructor without any parameter.");
		}
		return res;
	}

	/**
	 * 
	 * @return true if the database records of this table is loaded into the memory.
	 *         false if database records are only stored into the hard drive.
	 */
	public final boolean isLoadedInMemory() {
		return is_loaded_in_memory;
	}

	/**
	 * 
	 * @return the simple name of this class table.
	 */
	public final String getName() {
		return table_name;
	}

	/**
	 * Format the a class name by replacing '.' chars by '_' chars. Use also upper
	 * case.
	 * 
	 * @param c
	 *            a class
	 * @return the new class name format
	 */
	public static final String getName(Class<?> c) {
		return c.getCanonicalName().replace(".", "_").toUpperCase();
	}

	@Override
	public String toString() {
		return "Database Table " + this.getName();
	}

	/**
	 * Returns the records of this table ordered according the given fields, in an
	 * ascendant way or in a descendant way. Note that if you don't want to load
	 * this table into the memory, it is preferable to use the function
	 * {@link #getOrderedRecords(Filter, boolean, String...)}. The same thing is
	 * valid to get the number of records present in this table. It is preferable to
	 * use the function {@link #getRecordsNumber()}.
	 * 
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 1.2
	 */
	public final ArrayList<T> getOrderedRecords(boolean _ascendant, String... _fields) throws DatabaseException {
		return getOrderedRecords(null, new HashMap<String, Object>(), _ascendant, _fields);
	}

	/**
	 * Returns the records of this table, corresponding to a given filter, and
	 * ordered according the given fields, in an ascendant way or in a descendant
	 * way.
	 * 
	 * @param _filter
	 *            the filter which select records to include
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 1.2
	 */
	public final ArrayList<T> getOrderedRecords(final Filter<T> _filter, boolean _ascendant, String... _fields)
			throws DatabaseException {
		return getOrderedRecords(_filter, null, new HashMap<String, Object>(), _ascendant, _fields);
	}

	/**
	 * Returns the records of this table, corresponding to a given filter, and
	 * ordered according the given fields, in an ascendant way or in a descendant
	 * way.
	 * 
	 * @param _filter
	 *            the filter which select records to include
	 * @param whereCondition
	 *            the sql equivalent where condition
	 * @param parameters
	 *            the sql parameters used for the where condition
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 2.0.0
	 */
	public final ArrayList<T> getOrderedRecords(final Filter<T> _filter, String whereCondition, Object[] parameters,
			boolean _ascendant, String... _fields) throws DatabaseException {
		return getOrderedRecords(_filter, whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters), _ascendant, _fields);
	}

	/**
	 * Returns the records of this table, corresponding to a given filter, and
	 * ordered according the given fields, in an ascendant way or in a descendant
	 * way.
	 * 
	 * @param _filter
	 *            the filter which select records to include
	 * @param whereCondition
	 *            the sql equivalent where condition
	 * @param parameters
	 *            the sql parameters used for the where condition
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 2.0.0
	 */
	public final ArrayList<T> getOrderedRecords(final Filter<T> _filter, String whereCondition,
			Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getPaginedOrderedRecords(-1, -1, _filter, whereCondition, parameters, _ascendant, _fields);
	}

	/**
	 * Returns the records of this table, corresponding to a given filter, and
	 * ordered according the given fields, in an ascendant way or in a descendant
	 * way.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param _filter
	 *            the filter which select records to include
	 * @param whereCondition
	 *            the sql equivalent where condition
	 * @param parameters
	 *            the sql parameters used for the where condition
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 2.0.0
	 */
	public final ArrayList<T> getPaginedOrderedRecords(int rowpos, int rowlength, final Filter<T> _filter,
			String whereCondition, Map<String, Object> parameters, boolean _ascendant, String... _fields)
			throws DatabaseException {
		try (Lock l = new ReadLock(this)) {
			final RuleInstance rule = whereCondition == null ? null : Interpreter.getRuleInstance(whereCondition);
			if (isLoadedInMemory()) {
				final SortedArray res = new SortedArray(rowpos, rowlength, _ascendant, _fields);
				for (T r : getRecords(-1, -1, false)) {
					if ((rule == null || rule.isConcernedBy(this, parameters, r)) && _filter.nextRecord(r))
						res.addRecord(r);
					if (_filter.isTableParsingStoped())
						break;
				}
				return res.getRecords();
			} else {
				HashMap<Integer, Object> sqlParameters = new HashMap<>();
				String sqlQuery = rule == null ? null
						: rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
								.toString();
				final ArrayList<T> res = new ArrayList<>();
				getListRecordsFromSqlConnection(new Runnable() {

					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
						if (_filter.nextRecord(_instance)) {
							res.add(_instance);
						}
						return !_filter.isTableParsingStoped();
					}

					@Override
					public void init(int _field_count) {
					}
				}, rule == null ? getSqlGeneralSelect(true, _ascendant, _fields)
						: getSqlGeneralSelect(true, sqlQuery, sqlParameters, _ascendant, _fields),
						TransactionIsolation.TRANSACTION_READ_COMMITTED, rowpos, rowlength);
				return res;
			}

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	public String getFieldToComparare(String field) {
		String strings[] = field.split("\\.");

		Table<?> current_table = Table.this;

		FieldAccessor founded_field = null;
		for (int i = 0; i < strings.length; i++) {
			if (current_table == null)
				throw new IllegalArgumentException("The field " + field + " does not exists.");
			String f = strings[i];

			for (FieldAccessor fa : current_table.fields) {
				if (fa.getFieldName().equals(f)) {
					founded_field = fa;
					break;
				}
			}
			if (founded_field == null)
				throw new IllegalArgumentException("The field " + f + " does not exist into the class/table "
						+ current_table.getClass().getName());

			if (founded_field.isForeignKey())
				current_table = ((ForeignKeyFieldAccessor) founded_field).getPointedTable();
			else {
				current_table = null;
			}
		}

		if (!founded_field.isComparable() || founded_field.getDeclaredSqlFields().length > 1)
			throw new IllegalArgumentException("The field " + field + " starting in the class/table "
					+ Table.this.getClass().getName() + " is not a comparable field.");
		return founded_field.getDeclaredSqlFields()[0].field;
	}

	/**
	 * Returns the given records ordered according the given fields, in an ascendant
	 * way or in a descendant way.
	 * 
	 * @param _records
	 *            the records to sort.
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 1.2
	 */
	public final ArrayList<T> getOrderedRecords(Collection<T> _records, boolean _ascendant, String... _fields)
			throws DatabaseException {
		final SortedArray res = new SortedArray(-1, -1, _records.size(), _ascendant, _fields);
		for (T r : _records)
			res.addRecord(r);
		return res.getRecords();
	}

	/**
	 * Returns the records of this table, corresponding to a query, and ordered
	 * according the given fields, in an ascendant way or in a descendant way.
	 * 
	 * @param whereCondition
	 *            the sql equivalent where condition
	 * @param parameters
	 *            the sql parameters used for the where condition
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 2.0.0
	 */
	public final ArrayList<T> getOrderedRecords(String whereCondition, Map<String, Object> parameters,
			boolean _ascendant, String... _fields) throws DatabaseException {
		return getPaginedOrderedRecords(-1, -1, whereCondition, parameters, _ascendant, _fields);
	}

	/**
	 * Returns the records of this table, corresponding to a query, and ordered
	 * according the given fields, in an ascendant way or in a descendant way.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param whereCondition
	 *            the sql equivalent where condition
	 * @param parameters
	 *            the sql parameters used for the where condition
	 * @param _ascendant
	 *            this parameter must be true if the records should be sorted from
	 *            the lower value to the highest value, false else.
	 * @param _fields
	 *            the first given field corresponds to the field by which the table
	 *            is sorted. If two records are equals, then the second given field
	 *            is used, etc. It must have at minimum one field. Only comparable
	 *            fields are authorized. It is possible to sort fields according
	 *            records pointed by foreign keys. In this case, to sort according
	 *            the field A of the foreign key FK1, please enter "FK1.A".
	 * @return the ordered filtered records
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a byte array field, a boolean field, or a foreign key field,
	 *             is given or if an unknown field is given.
	 * @since 2.0.0
	 */
	public final ArrayList<T> getPaginedOrderedRecords(int rowpos, int rowlength, String whereCondition,
			Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		try (Lock l = new ReadLock(this)) {
			final RuleInstance rule = whereCondition == null ? null : Interpreter.getRuleInstance(whereCondition);

			if (isLoadedInMemory()) {
				final SortedArray res = new SortedArray(rowpos, rowlength, _ascendant, _fields);
				for (T r : getRecords(-1, -1, false)) {
					if ((rule == null || rule.isConcernedBy(this, parameters, r)))
						res.addRecord(r);
				}
				return res.getRecords();
			} else {
				HashMap<Integer, Object> sqlParameters = new HashMap<>();
				String sqlQuery = rule == null ? null
						: rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
								.toString();
				final ArrayList<T> res = new ArrayList<>();
				getListRecordsFromSqlConnection(new Runnable() {

					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) {
						res.add(_instance);
						return true;
					}

					@Override
					public void init(int _field_count) {
					}
				}, getSqlGeneralSelect(true, sqlQuery, sqlParameters, _ascendant, _fields),
						TransactionIsolation.TRANSACTION_READ_COMMITTED, rowpos, rowlength);
				return res;
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private final class FieldComparator {
		private final ArrayList<FieldAccessor> fields;

		public FieldComparator(ArrayList<FieldAccessor> _fields) {
			fields = _fields;
		}

		public int compare(T _o1, T _o2) throws DatabaseException {
			DatabaseRecord dr1 = _o1, dr2 = _o2;
			for (int i = 0; i < fields.size() - 1; i++) {
				if (dr1 == null && dr2 != null)
					return -1;
				else if (dr1 != null && dr2 == null)
					return 1;
				else if (dr1 == null && dr2 == null)
					return 0;
				ForeignKeyFieldAccessor f = (ForeignKeyFieldAccessor) fields.get(i);
				dr1 = (DatabaseRecord) f.getValue(dr1);
				dr2 = (DatabaseRecord) f.getValue(dr2);
			}
			return fields.get(fields.size() - 1).compare(dr1, dr2);
		}
	}

	private final class Comparator {
		private ArrayList<FieldComparator> accessors = new ArrayList<FieldComparator>();
		private final boolean ascendant;

		public Comparator(boolean _ascendant, String... _fields) throws ConstraintsNotRespectedDatabaseException {
			ascendant = _ascendant;
			if (_fields.length == 0)
				throw new ConstraintsNotRespectedDatabaseException("It must have at mean one field to compare.");
			for (int i = 0; i < _fields.length; i++) {
				accessors.add(getFieldComparator(_fields[i]));
			}
		}

		private int getResult(int val) {
			if (ascendant)
				return val;
			else {
				return -val;
			}
		}

		public int compare(T _o1, T _o2) throws DatabaseException {
			int res = 0;
			for (int i = 0; i < accessors.size(); i++) {
				FieldComparator fa = accessors.get(i);
				res = fa.compare(_o1, _o2);
				if (res != 0)
					return getResult(res);
			}
			return getResult(res);
		}

		public FieldComparator getFieldComparator(String field) throws ConstraintsNotRespectedDatabaseException {
			String strings[] = field.split("\\.");
			ArrayList<FieldAccessor> fields = new ArrayList<FieldAccessor>();
			Table<?> current_table = Table.this;

			for (int i = 0; i < strings.length; i++) {
				if (current_table == null)
					throw new ConstraintsNotRespectedDatabaseException("The field " + field + " does not exists.");
				String f = strings[i];
				FieldAccessor founded_field = null;
				for (FieldAccessor fa : current_table.fields) {
					if (fa.getFieldName().equals(f)) {
						founded_field = fa;
						break;
					}
				}
				if (founded_field == null)
					throw new ConstraintsNotRespectedDatabaseException("The field " + f
							+ " does not exist into the class/table " + current_table.getClass().getName());

				fields.add(founded_field);

				if (founded_field.isForeignKey())
					current_table = ((ForeignKeyFieldAccessor) founded_field).getPointedTable();
				else
					current_table = null;

			}
			if (!fields.get(fields.size() - 1).isComparable())
				throw new ConstraintsNotRespectedDatabaseException(
						"The field " + field + " starting in the class/table " + Table.this.getClass().getName()
								+ " is not a comparable field.");
			return new FieldComparator(fields);
		}

	}

	private class SortedArray {
		private final Comparator comparator;
		private final ArrayList<T> sorted_list;
		private final int rowpos, rowlength;

		public SortedArray(int rowpos, int rowlength, boolean _ascendant, String... _fields)
				throws ConstraintsNotRespectedDatabaseException {
			comparator = new Comparator(_ascendant, _fields);
			sorted_list = new ArrayList<T>();
			this.rowpos = rowpos;
			this.rowlength = rowlength;
		}

		public SortedArray(int rowpos, int rowlength, int initial_capacity, boolean _ascendant, String... _fields)
				throws ConstraintsNotRespectedDatabaseException {
			comparator = new Comparator(_ascendant, _fields);
			sorted_list = new ArrayList<T>(initial_capacity);
			this.rowpos = rowpos;
			this.rowlength = rowlength;
		}

		public void addRecord(T _record) throws DatabaseException {
			int mini = 0;
			int maxi = sorted_list.size();
			while (maxi - mini > 0) {
				int i = mini + (maxi - mini) / 2;
				int comp = comparator.compare(sorted_list.get(i), _record);
				if (comp < 0) {
					mini = i + 1;
				} else if (comp > 0) {
					maxi = i;
				} else {
					mini = i;
					maxi = i;
				}
			}
			sorted_list.add(mini, _record);
		}

		public ArrayList<T> getRecords() {
			if (rowpos > 0 && rowlength > 0) {
				int size = Math.max(0, Math.min(sorted_list.size() - rowpos, rowlength));
				ArrayList<T> res = new ArrayList<>(size);
				for (int i = 0; i < size; i++)
					res.add(sorted_list.get(rowpos + i));
				return res;
			} else
				return sorted_list;
		}

	}

	/**
	 * Returns the records of this table. Note that if you don't want to load this
	 * table into the memory, it is preferable to use the function
	 * {@link #getRecords(Filter)}. The same thing is valid to get the number of
	 * records present in this table. It is preferable to use the function
	 * {@link #getRecordsNumber()}.
	 * 
	 * @return all the records of the table.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final ArrayList<T> getRecords() throws DatabaseException {
		return getPaginedRecords(-1, -1);
	}

	/**
	 * Returns the records of this table. Note that if you don't want to load this
	 * table into the memory, it is preferable to use the function
	 * {@link #getRecords(Filter)}. The same thing is valid to get the number of
	 * records present in this table. It is preferable to use the function
	 * {@link #getRecordsNumber()}.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @return all the records of the table.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength) throws DatabaseException {
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	final ArrayList<T> getRecords(int rowpos, int rowlength, boolean is_already_in_transaction)
			throws DatabaseException {
		// try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableReadLock())
		{
			if (isLoadedInMemory()) {
				if (!isSynchronizedWithSqlDatabase()) {
					final ArrayList<T> res = new ArrayList<T>();
					getListRecordsFromSqlConnection(new Runnable() {

						@Override
						public boolean setInstance(T _instance, ResultSet _cursor) {
							res.add(_instance);
							return true;
						}

						@Override
						public void init(int _field_count) {
							res.clear();
							res.ensureCapacity((int) _field_count);
						}
					}, getSqlGeneralSelect(true), TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
					records_instances.set(res);
					memoryRefreshed();
				}
				if (rowpos > 0 && rowlength > 0) {
					ArrayList<T> records = records_instances.get();
					int size = Math.max(Math.min(records.size() - rowpos - 1, rowlength), 0);
					ArrayList<T> res = new ArrayList<>(size);
					for (int i = 0; i < size; i++) {
						res.add(records.get(i + rowpos - 1));
					}
					return res;
				} else {
					return records_instances.get();
				}
			} else {
				final ArrayList<T> res = new ArrayList<T>();
				getListRecordsFromSqlConnection(new Runnable() {

					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) {
						res.add(_instance);
						return true;
					}

					@Override
					public void init(int _field_count) {
						res.clear();
						res.ensureCapacity((int) _field_count);
					}
				}, getSqlGeneralSelect(true), TransactionIsolation.TRANSACTION_READ_COMMITTED, rowpos, rowlength);
				return res;
			}
		}
	}

	protected class MemoryTableIterator implements TableIterator<T> {
		private Iterator<T> iterator;

		MemoryTableIterator(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			return iterator.next();
		}

		@Override
		public void close() {

		}

		@Override
		public void remove() {
			throw new IllegalAccessError();

		}
	}

	protected class DirectTableIterator implements TableIterator<T> {
		final AbstractReadQuerry readQuerry;
		protected final Constructor<T> default_constructor_field;
		protected final ArrayList<FieldAccessor> fields_accessor;
		private boolean checkNext = true;
		private boolean curNext = false;
		final Lock lock;
		private boolean closed = false;

		DirectTableIterator(Constructor<T> _default_constructor_field, ArrayList<FieldAccessor> _fields_accessor)
				throws DatabaseException {
			try {
				lock = new ReadLock(Table.this);
				default_constructor_field = _default_constructor_field;
				fields_accessor = _fields_accessor;
				readQuerry = new ReadQuerry(sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
						getSqlGeneralSelect(true));
			} catch (SQLException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		@Override
		public boolean hasNext() {
			if (closed)
				return false;
			try {
				if (checkNext) {
					curNext = readQuerry.result_set.next();
					if (!curNext)
						close();
					checkNext = false;
				}
				return curNext;
			} catch (Exception e) {
				e.printStackTrace();
				throw new NoSuchElementException("Unexpected exception");
			}
		}

		@Override
		public void finalize() {
			try {
				close();
			} catch (Exception e) {

			}
		}

		@Override
		public T next() {
			if (closed)
				throw new NoSuchElementException("Iterator closed");
			if (!curNext)
				throw new NoSuchElementException();
			try {
				T field_instance = getNewRecordInstance(default_constructor_field);

				for (FieldAccessor f : fields_accessor) {
					f.setValue(field_instance, readQuerry.result_set);
				}
				checkNext = true;
				return field_instance;
			} catch (Exception e) {
				e.printStackTrace();
				throw new NoSuchElementException("Unexpected exception");
			}
		}

		@Override
		public void close() throws DatabaseException {
			try {
				if (!closed) {
					readQuerry.close();
					lock.close();
					closed = true;
				}
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}

		@Override
		public void remove() {
			throw new IllegalAccessError();
		}

	}

	/**
	 * Returns an iterator parsing records of this table. This iterator is
	 * {@link AutoCloseable}. Do not forget to close the iterator.
	 * 
	 * @return an iterator representing all the records of the table.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	/*
	 * public final TableIterator<T> getIterator() throws DatabaseException {
	 * //try(ReadWriteLock.Lock
	 * lock=sql_connection.locker.getAutoCloseableReadLock()) { if
	 * (isLoadedInMemory()) { try (Lock lock=new ReadLock(this)) { if
	 * (!isSynchronizedWithSqlDatabase()) { final ArrayList<T> res=new
	 * ArrayList<T>(); getListRecordsFromSqlConnection(new Runnable() {
	 * 
	 * @Override public boolean setInstance(T _instance, ResultSet _cursor) {
	 * res.add(_instance); return true; }
	 * 
	 * @Override public void init(int _field_count) { res.clear();
	 * res.ensureCapacity((int)_field_count); } }, getSqlGeneralSelect(),
	 * TransactionIsolation.TRANSACTION_REPEATABLE_READ);
	 * records_instances.set(res); memoryRefreshed(); } return new
	 * MemoryTableIterator(records_instances.get().iterator());
	 * 
	 * } catch(Exception e) { throw DatabaseException.getDatabaseException(e); }
	 * 
	 * } else { return new DirectTableIterator(default_constructor_field, fields); }
	 * }
	 * 
	 * }
	 */
	/**
	 * Returns the records of this table corresponding to a given filter.
	 * 
	 * @param _filter
	 *            the filter
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getRecords(final Filter<T> _filter) throws DatabaseException {
		return getPaginedRecords(-1, -1, _filter);
	}

	/**
	 * Returns the records of this table corresponding to a given filter.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param _filter
	 *            the filter
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength, final Filter<T> _filter)
			throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{

			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength, _filter, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns the records of this table corresponding to a given filter and a given
	 * SQL condition.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getRecords(final Filter<T> _filter, String whereCondition, Object... parameters)
			throws DatabaseException {
		return getRecords(_filter, whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Returns the records of this table corresponding to a given filter and a given
	 * SQL condition.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getRecords(final Filter<T> _filter, String whereCondition, Map<String, Object> paramters)
			throws DatabaseException {
		return getPaginedRecords(-1, -1, _filter, whereCondition, paramters);
	}

	/**
	 * Returns the records of this table corresponding to a given filter and a given
	 * SQL condition.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength, final Filter<T> _filter,
			String whereCondition, Object... parameters) throws DatabaseException {
		return getPaginedRecords(rowpos, rowlength, _filter, whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Returns the records of this table corresponding to a given filter and a given
	 * SQL condition.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength, final Filter<T> _filter,
			String whereCondition, Map<String, Object> paramters) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{

			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength, _filter, whereCondition, paramters, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns the records of this table corresponding to a given SQL condition.
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getRecords(String whereCondition, Object... parameters) throws DatabaseException {
		return getPaginedRecords(-1, -1, whereCondition,
				whereCondition == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Returns the records of this table corresponding to a given SQL condition.
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getRecords(String whereCondition, Map<String, Object> paramters)
			throws DatabaseException {
		return getPaginedRecords(-1, -1, whereCondition, paramters);
	}

	/**
	 * Returns the records of this table corresponding to a given SQL condition.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength, String whereCondition, Object... parameters)
			throws DatabaseException {
		return getPaginedRecords(rowpos, rowlength, whereCondition, convertToMap(parameters));
	}

	/**
	 * Returns the records of this table corresponding to a given SQL condition.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final ArrayList<T> getPaginedRecords(int rowpos, int rowlength, String whereCondition,
			Map<String, Object> paramters) throws DatabaseException {
		// synchronized(sql_connection)
		{

			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength, new Filter<T>() {

					@Override
					public boolean nextRecord(T _record) {
						return true;
					}
				}, whereCondition, paramters, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final ArrayList<T> getRecords(final int rowpos, final int rowlength, final Filter<T> _filter, String where,
			Map<String, Object> parameters, boolean is_already_sql_transaction) throws DatabaseException {
		final ArrayList<T> res = new ArrayList<T>();
		final RuleInstance rule = Interpreter.getRuleInstance(where);
		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_already_sql_transaction);
			int pos = 0;
			for (T r : records) {
				if (rule.isConcernedBy(this, parameters, r)
						&& ((rowpos <= 0 || rowlength <= 0) || (++pos >= rowpos && (rowpos - pos) < rowlength))) {
					if (_filter.nextRecord(r))
						res.add(r);
					if (_filter.isTableParsingStoped()
							|| (rowpos > 0 && rowlength > 0 && (rowpos - pos) >= rowlength - 1))
						break;
				}
			}
		} else {
			HashMap<Integer, Object> sqlParameters = new HashMap<>();
			String sqlQuery = rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
					.toString();
			final AtomicInteger pos = new AtomicInteger(0);
			getListRecordsFromSqlConnection(new Runnable() {

				@Override
				public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException {
					if (_filter.nextRecord(r) && ((rowpos <= 0 || rowlength <= 0)
							|| (pos.incrementAndGet() >= rowpos && (rowpos - pos.get()) < rowlength)))
						res.add(r);
					return !_filter.isTableParsingStoped()
							|| !(rowpos > 0 && rowlength > 0 && (rowpos - pos.get()) >= rowlength - 1);
				}

				@Override
				public void init(int _field_count) {
				}
			}, getSqlGeneralSelect(true, sqlQuery, sqlParameters), TransactionIsolation.TRANSACTION_READ_COMMITTED, -1,
					-1, false);
		}
		return res;
	}

	private final ArrayList<T> getRecords(final int rowpos, final int rowlength, final Filter<T> _filter,
			boolean is_already_sql_transaction) throws DatabaseException {
		final ArrayList<T> res = new ArrayList<T>();

		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_already_sql_transaction);
			int pos = 0;
			for (T r : records) {
				if (_filter.nextRecord(r)
						&& ((rowpos < 1 || rowlength < 1) || ((++pos) >= rowpos && (rowpos - pos) < rowlength)))
					res.add(r);

				if (_filter.isTableParsingStoped()
						|| (rowpos > 0 && rowlength > 0 && (rowpos - pos) >= (rowlength - 1)))
					break;
			}
		} else {
			final boolean persoFilter = (_filter instanceof Table.PersonnalFilter);
			final AtomicInteger pos = new AtomicInteger(0);
			getListRecordsFromSqlConnection(new Runnable() {

				@Override
				public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException {
					if ((persoFilter || _filter.nextRecord(r)) && ((rowpos < 1 || rowlength < 1)
							|| (pos.incrementAndGet() >= rowpos && (rowpos - pos.get()) < rowlength)))
						res.add(r);
					return !_filter.isTableParsingStoped()
							|| !(rowpos > 0 && rowlength > 0 && (rowpos - pos.get()) >= (rowlength - 1));
				}

				@Override
				public void init(int _field_count) {
				}
			}, persoFilter ? ((PersonnalFilter) _filter).getSQLQuerry(true) : getSqlGeneralSelect(true),
					TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
		}
		return res;
	}

	/**
	 * Returns true if there is at mean one record which corresponds to the given
	 * filter.
	 * 
	 * @param _filter
	 *            the filter
	 * @return true if there is at mean one record which corresponds to the given
	 *         filter.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final boolean hasRecords(final Filter<T> _filter) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return hasRecords(_filter, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final boolean hasRecords(final Filter<T> _filter, boolean is_sql_transaction) throws DatabaseException {
		if (isLoadedInMemory()) {
			ArrayList<T> records = getRecords(-1, -1, is_sql_transaction);
			for (T r : records) {
				if (_filter.nextRecord(r))
					return true;
				if (_filter.isTableParsingStoped())
					break;
			}
			return false;
		} else {
			final boolean persoFilter = (_filter instanceof Table.PersonnalFilter);
			class RunnableTmp extends Runnable {
				boolean res;

				@Override
				public boolean setInstance(T r, ResultSet _cursor) throws DatabaseException {
					if (persoFilter || _filter.nextRecord(r)) {
						res = true;
						return false;
					}
					return !_filter.isTableParsingStoped();
				}

				@Override
				public void init(int _field_count) {
					res = false;
				}

			}
			RunnableTmp runnable = new RunnableTmp();
			getListRecordsFromSqlConnection(runnable,
					persoFilter ? ((PersonnalFilter) _filter).getSQLQuerry(true) : getSqlGeneralSelect(true),
					TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
			return runnable.res;
		}
	}

	abstract class PersonnalFilter extends Filter<T> {
		protected final boolean ascendant;
		protected final String[] orderByFields;

		PersonnalFilter(boolean ascendant, String[] orderByFields) {
			this.ascendant = ascendant;
			this.orderByFields = orderByFields;
		}

		abstract SqlQuerry getSQLQuerry(boolean loadJunctions);

	}

	private abstract class SimpleFieldFilter extends PersonnalFilter {
		protected final Map<String, Object> given_fields;
		protected final ArrayList<FieldAccessor> fields_accessor;

		public SimpleFieldFilter(boolean ascendant, String[] orderByFields, Map<String, Object> _fields,
				final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException {
			super(ascendant, orderByFields);
			fields_accessor = _fields_accessor;
			for (String s : _fields.keySet()) {
				boolean found = false;
				for (FieldAccessor fa : fields_accessor) {
					if (fa.getFieldName().equals(s)) {
						found = true;
						break;
					}
				}
				if (!found)
					throw new FieldDatabaseException("The given field " + s + " is not contained into the table "
							+ Table.this.getClass().getName());
			}
			given_fields = _fields;
		}

	}

	private abstract class MultipleFieldFilter extends PersonnalFilter {
		protected final Map<String, Object> given_fields[];
		protected final ArrayList<FieldAccessor> fields_accessor;

		@SafeVarargs
		public MultipleFieldFilter(boolean ascendant, String[] orderByFields,
				final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object>... _fields)
				throws DatabaseException {
			super(ascendant, orderByFields);
			fields_accessor = _fields_accessor;
			Set<String> first = null;
			for (Map<String, Object> hm : _fields) {
				if (first == null)
					first = hm.keySet();
				if (hm.keySet().size() != first.size())
					throw new FieldDatabaseException(
							"The given fields are not the same in every HashMap<String, Object>. Every HashMap<String,Object> must have the same keys.");
				for (String s : hm.keySet()) {
					boolean found = false;
					for (String s2 : first) {
						if (s2.equals(s)) {
							found = true;
							break;
						}
					}
					if (!found)
						throw new FieldDatabaseException(
								"The given fields are not the same in every HashMap<String, Object>. Every HashMap<String,Object> must have the same keys.");
					found = false;
					for (FieldAccessor fa : fields_accessor) {
						if (fa.getFieldName().equals(s)) {
							found = true;
							break;
						}
					}
					if (!found)
						throw new FieldDatabaseException("The given field " + s + " is not contained into this table.");
				}
			}
			given_fields = _fields;

		}

	}

	private class SimpleAllFieldsFilter extends SimpleFieldFilter {

		public SimpleAllFieldsFilter(boolean ascendant, String[] orderByFields, Map<String, Object> _fields,
				final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException {
			super(ascendant, orderByFields, _fields, _fields_accessor);
		}

		@Override
		public boolean nextRecord(T _instance) throws DatabaseException {
			boolean toadd = true;
			for (String s : given_fields.keySet()) {
				boolean ok = false;
				for (FieldAccessor fa : fields_accessor) {
					if (fa.getFieldName().equals(s)) {
						if (fa.equals(_instance, given_fields.get(s))) {
							ok = true;
						}
						break;
					}
				}
				if (!ok) {
					toadd = false;
					break;
				}
			}

			return toadd;
		}

		@Override
		public SqlQuerry getSQLQuerry(boolean loadJunctions) {
			return new SqlGeneralSelectQuerryWithFieldMatch(loadJunctions, this.given_fields, "AND", ascendant,
					orderByFields);
		}

	}

	private class MultipleAllFieldsFilter extends MultipleFieldFilter {

		public MultipleAllFieldsFilter(boolean ascendant, String[] orderByFields,
				final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object>[] _records)
				throws DatabaseException {
			super(ascendant, orderByFields, _fields_accessor, _records);
		}

		@Override
		public boolean nextRecord(T _instance) throws DatabaseException {
			boolean toadd = true;
			for (Map<String, Object> hm : given_fields) {
				toadd = true;
				for (String s : hm.keySet()) {
					boolean ok = false;
					for (FieldAccessor fa : fields_accessor) {
						if (fa.getFieldName().equals(s)) {
							if (fa.equals(_instance, hm.get(s))) {
								ok = true;
							}
							break;
						}
					}
					if (!ok) {
						toadd = false;
						break;
					}
				}
				if (toadd)
					break;
			}

			return toadd;
		}

		@Override
		public SqlQuerry getSQLQuerry(boolean loadJunctions) {
			return new SqlGeneralSelectQuerryWithMultipleFieldMatch(loadJunctions, this.given_fields, "AND", ascendant,
					orderByFields);
		}

	}

	private class SimpleOneOfFieldsFilter extends SimpleFieldFilter {

		public SimpleOneOfFieldsFilter(boolean ascendant, String[] orderByFields, Map<String, Object> _fields,
				final ArrayList<FieldAccessor> _fields_accessor) throws DatabaseException {
			super(ascendant, orderByFields, _fields, _fields_accessor);
		}

		@Override
		public boolean nextRecord(T _instance) throws DatabaseException {
			boolean toadd = false;
			for (String s : given_fields.keySet()) {
				for (FieldAccessor fa : fields_accessor) {
					if (fa.getFieldName().equals(s)) {
						if (fa.equals(_instance, given_fields.get(s))) {
							toadd = true;
						}
						break;
					}
				}
				if (toadd) {
					break;
				}
			}

			return toadd;
		}

		@Override
		public SqlQuerry getSQLQuerry(boolean loadJunctions) {
			return new SqlGeneralSelectQuerryWithFieldMatch(loadJunctions, this.given_fields, "OR", ascendant,
					orderByFields);
		}

	}

	private class MultipleOneOfFieldsFilter extends MultipleFieldFilter {

		public MultipleOneOfFieldsFilter(boolean ascendant, String[] orderByFields,
				final ArrayList<FieldAccessor> _fields_accessor, Map<String, Object>[] _records)
				throws DatabaseException {
			super(ascendant, orderByFields, _fields_accessor, _records);
		}

		@Override
		public boolean nextRecord(T _instance) throws DatabaseException {
			boolean toadd = true;
			for (Map<String, Object> hm : given_fields) {
				toadd = false;
				for (String s : hm.keySet()) {
					boolean ok = false;
					for (FieldAccessor fa : fields_accessor) {
						if (fa.getFieldName().equals(s)) {
							if (fa.equals(_instance, hm.get(s))) {
								ok = true;
							}
							break;
						}
					}
					if (ok) {
						toadd = true;
						break;
					}
				}
				if (toadd)
					break;
			}

			return toadd;
		}

		@Override
		public SqlQuerry getSQLQuerry(boolean loadJunctions) {
			return new SqlGeneralSelectQuerryWithMultipleFieldMatch(loadJunctions, this.given_fields, "OR", ascendant,
					orderByFields);
		}

	}

	/**
	 * Return grouped results (equivalent to group by with SQL).
	 * 
	 * @param _records
	 *            the records to group.
	 * @param _fields
	 *            the fields which enables to group the results. It must have at
	 *            minimum one field. It is possible to use fields according records
	 *            pointed by foreign keys. In this case, to use the field A of the
	 *            foreign key FK1, please enter "FK1.A".
	 * @return a GroupedResults instance.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	@SuppressWarnings("unchecked")
	public final GroupedResults<T> getGroupedResults(Collection<T> _records, String... _fields)
			throws DatabaseException {
		try {
			return (GroupedResults<T>) grouped_results_constructor.newInstance(sql_connection, _records, class_record,
					_fields);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/**
	 * Return a GroupedResults instance (equivalent to group by with SQL). The
	 * results must be included by calling the functions
	 * {@link com.distrimind.ood.database.GroupedResults#addRecord(DatabaseRecord)}
	 * and
	 * {@link com.distrimind.ood.database.GroupedResults#addRecords(Collection)}.
	 * 
	 * @param _fields
	 *            the fields which enables to group the results. It must have at
	 *            minimum one field. It is possible to use fields according records
	 *            pointed by foreign keys. In this case, to use the field A of the
	 *            foreign key FK1, please enter "FK1.A".
	 * @return a GroupedResults instance.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final GroupedResults<T> getGroupedResults(String... _fields) throws DatabaseException {
		return getGroupedResults(null, _fields);
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return getRecordsWithAllFields(transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields,
			Object... _fields) throws DatabaseException {
		return getRecordsWithAllFieldsOrdered(ascendant, orderByFields, transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithAllFields(final Map<String, Object> _fields) throws DatabaseException {
		return getRecordsWithAllFieldsOrdered(true, null, _fields);
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields,
			final Map<String, Object> _fields) throws DatabaseException {
		return getPaginedRecordsWithAllFieldsOrdered(-1, -1, ascendant, orderByFields, _fields);
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getPaginedRecordsWithAllFieldsOrdered(int rowcount, int rowlength, boolean ascendant,
			String[] orderByFields, Object... _fields) throws DatabaseException {
		return getPaginedRecordsWithAllFieldsOrdered(rowcount, rowlength, ascendant, orderByFields,
				transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to the given fields. All given fields
	 * must correspond exactly to the returned records.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getPaginedRecordsWithAllFieldsOrdered(int rowcount, int rowlength, boolean ascendant,
			String[] orderByFields, final Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowcount, rowlength,
						new SimpleAllFieldsFilter(ascendant, orderByFields, _fields, fields), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns true if there is at least one record which correspond the given
	 * fields. All given fields must correspond exactly one the records.
	 * 
	 * @param _fields
	 *            the fields that must match to one of the records. Must be formated
	 *            as follow : {"field1", value1,"field2", value2, etc.}
	 * @return true if there is at least one record which correspond the given
	 *         fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final boolean hasRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return hasRecordsWithAllFields(transformToMapField(_fields));
	}

	/**
	 * Returns true if there is at least one record which correspond the given
	 * fields. All given fields must correspond exactly one the records.
	 * 
	 * @param _fields
	 *            the fields that must match to one of the records.
	 * @return true if there is at least one record which correspond the given
	 *         fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final boolean hasRecordsWithAllFields(final Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return hasRecords(new SimpleAllFieldsFilter(true, null, _fields, fields), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return getRecordsWithAllFields(transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields,
			Object[]... _records) throws DatabaseException {
		return getRecordsWithAllFieldsOrdered(ascendant, orderByFields, transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithAllFields(final Map<String, Object>... _records) throws DatabaseException {
		return getRecordsWithAllFieldsOrdered(true, null, _records);
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields,
			final Map<String, Object>... _records) throws DatabaseException {
		return getPaginedRecordsWithAllFieldsOrdered(-1, -1, ascendant, orderByFields, _records);
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getPaginedRecordsWithAllFieldsOrdered(int rowcount, int rowlength, boolean ascendant,
			String[] orderByFields, Object[]... _records) throws DatabaseException {
		return getPaginedRecordsWithAllFieldsOrdered(rowcount, rowlength, ascendant, orderByFields,
				transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getPaginedRecordsWithAllFieldsOrdered(int rowcount, int rowlength, boolean ascendant,
			String[] orderByFields, final Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowcount, rowlength,
						new MultipleAllFieldsFilter(ascendant, orderByFields, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns true if there is at least one record which correspond to one group of
	 * fields of the array of fields. For one considered record, it must have one
	 * group of fields (record) that all corresponds exactly.
	 * 
	 * @param _records
	 *            the array fields that must match to one of the records. Must be
	 *            formated as follow : {"field1", value1,"field2", value2, etc.}
	 * @return true if there is at least one record which correspond to one group of
	 *         fields of the array of fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final boolean hasRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return hasRecordsWithAllFields(transformToMapField(_records));
	}

	/**
	 * Returns true if there is at least one record which correspond to one group of
	 * fields of the array of fields. For one considered record, it must have one
	 * group of fields (record) that all corresponds exactly.
	 * 
	 * @param _records
	 *            the array fields that must match to one of the records.
	 * @return true if there is at least one record which correspond to one group of
	 *         fields of the array of fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final boolean hasRecordsWithAllFields(final Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return hasRecords(new MultipleAllFieldsFilter(true, null, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithOneOfFields(Object... _fields) throws DatabaseException {
		return getRecordsWithOneOfFields(transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields,
			Object... _fields) throws DatabaseException {
		return getRecordsWithOneOfFieldsOrdered(ascendant, orderByFields, transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithOneOfFields(final Map<String, Object> _fields) throws DatabaseException {
		return getRecordsWithOneOfFieldsOrdered(true, null, _fields);
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields,
			final Map<String, Object> _fields) throws DatabaseException {
		return getPaginedRecordsWithOneOfFieldsOrdered(-1, -1, ascendant, orderByFields, _fields);
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(int rowpos, int rowlength, boolean ascendant,
			String[] orderByFields, Object... _fields) throws DatabaseException {
		return getPaginedRecordsWithOneOfFieldsOrdered(rowpos, rowlength, ascendant, orderByFields,
				transformToMapField(_fields));
	}

	/**
	 * Returns the records which correspond to one of the given fields. One of the
	 * given fields must correspond exactly to the returned records.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final ArrayList<T> getPaginedRecordsWithOneOfFieldsOrdered(int rowpos, int rowlength, boolean ascendant,
			String[] orderByFields, final Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength,
						new SimpleOneOfFieldsFilter(ascendant, orderByFields, _fields, fields), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns true if there is at least one record which corresponds to one of the
	 * given fields. One of the given fields must corresponds exactly one the
	 * records.
	 * 
	 * @param _fields
	 *            the fields. Must be formated as follow : {"field1",
	 *            value1,"field2", value2, etc.}
	 * @return true if there is at least one record which correspond one of the
	 *         given fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final boolean hasRecordsWithOneOfFields(final Object... _fields) throws DatabaseException {
		return hasRecordsWithOneOfFields(transformToMapField(_fields));
	}

	/**
	 * Returns true if there is at least one record which corresponds to one of the
	 * given fields. One of the given fields must corresponds exactly one the
	 * records.
	 * 
	 * @param _fields
	 *            the fields.
	 * @return true if there is at least one record which correspond one of the
	 *         given fields.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final boolean hasRecordsWithOneOfFields(final Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return hasRecordsWithOneOfFields(_fields, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

	}

	final boolean hasRecordsWithOneOfFields(final Map<String, Object> _fields, boolean is_sql_transaction)
			throws DatabaseException {
		return hasRecords(new SimpleOneOfFieldsFilter(true, null, _fields, fields), is_sql_transaction);
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithOneOfFields(final Object[]... _records) throws DatabaseException {
		return getRecordsWithOneOfFields(transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(final Object[]... _records) throws DatabaseException {
		return getRecordsWithOneOfFieldsOrdered(true, null, transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithOneOfFields(final Map<String, Object>... _records)
			throws DatabaseException {
		return getRecordsWithOneOfFieldsOrdered(true, null, _records);
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields,
			final Map<String, Object>... _records) throws DatabaseException {
		return getPaginedRecordsWithOneOfFieldsOrdered(-1, -1, ascendant, orderByFields, _records);
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getRecordsWithOneOfFieldsOrdered(int rowpos, int rowlength, final Object[]... _records)
			throws DatabaseException {
		return getPaginedRecordsWithOneOfFieldsOrdered(rowpos, rowlength, true, null, transformToMapField(_records));
	}

	/**
	 * Returns the records which correspond to one of the fields of one group of the
	 * array of fields.
	 * 
	 * @param rowpos
	 *            row position (first starts with 1)
	 * @param rowlength
	 *            page length (size of the returned result)
	 * @param ascendant
	 *            order by ascendant (true) or descendant (false)
	 * @param orderByFields
	 *            order by the given fields
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final ArrayList<T> getPaginedRecordsWithOneOfFieldsOrdered(int rowpos, int rowlength, boolean ascendant,
			String[] orderByFields, final Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecords(rowpos, rowlength,
						new MultipleOneOfFieldsFilter(ascendant, orderByFields, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns if there is at least one record which correspond to one of the fields
	 * of one group of the array of fields.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final boolean hasRecordsWithOneOfFields(final Object[]... _records) throws DatabaseException {
		return hasRecordsWithOneOfFields(transformToMapField(_records));
	}

	/**
	 * Returns if there is at least one record which correspond to one of the fields
	 * of one group of the array of fields.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the corresponding records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final boolean hasRecordsWithOneOfFields(final Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return hasRecords(new MultipleOneOfFieldsFilter(true, null, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to the given fields. All given fields must
	 * correspond exactly to the records. The deleted records do not have link with
	 * other table's records throw foreign keys.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return removeRecordsWithAllFields(transformToMapField(_fields));
	}

	/**
	 * Remove records which correspond to the given fields. All given fields must
	 * correspond exactly to the records. The deleted records do not have link with
	 * other table's records throw foreign keys.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithAllFields(Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(new SimpleAllFieldsFilter(true, null, _fields, fields), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly. The deleted records do not have link with other
	 * table's records throw foreign keys.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return removeRecordsWithAllFields(transformToMapField(_records));
	}

	/**
	 * Remove records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly. The deleted records do not have link with other
	 * table's records throw foreign keys.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithAllFields(Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(new MultipleAllFieldsFilter(true, null, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to one of the given fields. One of the given
	 * fields must correspond exactly to deleted records. The deleted records do not
	 * have link with other table's records throw foreign keys.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithOneOfFields(Object... _fields) throws DatabaseException {
		return removeRecordsWithOneOfFields(transformToMapField(_fields));
	}

	/**
	 * Remove records which correspond to one of the given fields. One of the given
	 * fields must correspond exactly to deleted records. The deleted records do not
	 * have link with other table's records throw foreign keys.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithOneOfFields(Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(new SimpleOneOfFieldsFilter(true, null, _fields, fields), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to one of the fields of one group of the
	 * array of fields. The deleted records do not have link with other table's
	 * records throw foreign keys.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithOneOfFields(Object[]... _records) throws DatabaseException {
		return removeRecordsWithOneOfFields(transformToMapField(_records));
	}

	/**
	 * Remove records which correspond to one of the fields of one group of the
	 * array of fields. The deleted records do not have link with other table's
	 * records throw foreign keys.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithOneOfFields(Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array!");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(new MultipleOneOfFieldsFilter(true, null, fields, _records), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to the given fields. All given fields must
	 * correspond exactly to the records. Records of other tables which have Foreign
	 * keys which points to the deleted records are also deleted.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithAllFieldsWithCascade(Object... _fields) throws DatabaseException {
		return removeRecordsWithAllFieldsWithCascade(transformToMapField(_fields));
	}

	/**
	 * Remove records which correspond to the given fields. All given fields must
	 * correspond exactly to the records. Records of other tables which have Foreign
	 * keys which points to the deleted records are also deleted.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithAllFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecordsWithCascade(new SimpleAllFieldsFilter(true, null, _fields, fields), null,
						new HashMap<String, Object>(), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly. Records of other tables which have Foreign keys
	 * which points to the deleted records are also deleted.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithAllFieldsWithCascade(Object[]... _records) throws DatabaseException {
		return removeRecordsWithAllFieldsWithCascade(transformToMapField(_records));
	}

	/**
	 * Remove records which correspond to one group of fields of the array of
	 * fields. For one considered record, it must have one group of fields (record)
	 * that all corresponds exactly. Records of other tables which have Foreign keys
	 * which points to the deleted records are also deleted.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithAllFieldsWithCascade(Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecordsWithCascade(new MultipleAllFieldsFilter(true, null, fields, _records), null,
						new HashMap<String, Object>(), false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to one of the given fields. One of the given
	 * fields must correspond exactly to deleted records. Records of other tables
	 * which have Foreign keys which points to the deleted records are also deleted.
	 * 
	 * @param _fields
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithOneOfFieldsWithCascade(Object... _fields) throws DatabaseException {
		return removeRecordsWithOneOfFieldsWithCascade(transformToMapField(_fields));
	}

	/**
	 * Remove records which correspond to one of the given fields. One of the given
	 * fields must correspond exactly to deleted records. Records of other tables
	 * which have Foreign keys which points to the deleted records are also deleted.
	 * 
	 * @param _fields
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	public final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecordsWithOneOfFieldsWithCascade(_fields, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> _fields,
			boolean _is_already_sql_transaction) throws DatabaseException {
		return removeRecordsWithCascade(new SimpleOneOfFieldsFilter(true, null, _fields, fields), null,
				new HashMap<String, Object>(), _is_already_sql_transaction);
	}

	/**
	 * Remove records which correspond to one of the fields of one group of the
	 * array of fields. Records of other tables which have Foreign keys which points
	 * to the deleted records are also deleted.
	 * 
	 * @param _records
	 *            the fields that filter the result. Must be formated as follow :
	 *            {"field1", value1,"field2", value2, etc.}
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithOneOfFieldsWithCascade(Object[]... _records) throws DatabaseException {
		return removeRecordsWithOneOfFieldsWithCascade(transformToMapField(_records));
	}

	/**
	 * Remove records which correspond to one of the fields of one group of the
	 * array of fields. Records of other tables which have Foreign keys which points
	 * to the deleted records are also deleted.
	 * 
	 * @param _records
	 *            the fields that filter the result.
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws FieldDatabaseException
	 *             if the given fields do not correspond to the table fields.
	 */
	@SafeVarargs
	public final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object>... _records)
			throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecordsWithOneOfFieldsWithCascade(false, _records);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	@SafeVarargs
	private final long removeRecordsWithOneOfFieldsWithCascade(boolean is_already_in_transaction,
			Map<String, Object>... _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.length == 0)
			throw new NullPointerException("The parameter _records is an empty array !");
		return removeRecordsWithCascade(new MultipleOneOfFieldsFilter(true, null, fields, _records), null,
				new HashMap<String, Object>(), is_already_in_transaction);
	}

	private HashMap<String, Object> getSqlPrimaryKeys(T _record) throws DatabaseException {
		HashMap<String, Object> res = new HashMap<String, Object>();
		for (FieldAccessor fa : primary_keys_fields) {
			for (SqlFieldInstance sfi : fa.getSqlFieldsInstances(_record)) {
				res.put(sfi.field, sfi.instance);
			}
		}
		return res;
	}

	/**
	 * Remove records which correspond to the given filter. These records are not
	 * pointed by other records through foreign keys of other tables. So they are
	 * not proposed to the filter class.
	 * 
	 * @param _filter
	 *            the filter
	 * @return the number of deleted records
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecords(final Filter<T> _filter) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(_filter, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to a WHERE condition. These records are not
	 * pointed by other records through foreign keys of other tables. So they are
	 * not proposed to the filter class.
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecords(String whereCommand, Object... parameters) throws DatabaseException {
		return removeRecords(new Filter<T>() {

			@Override
			public boolean nextRecord(T _record) {
				return true;
			}
		}, whereCommand, whereCommand == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Remove records which correspond to the given filter and WHERE condition.
	 * These records are not pointed by other records through foreign keys of other
	 * tables. So they are not proposed to the filter class.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecords(final Filter<T> _filter, String whereCommand, Object... parameters)
			throws DatabaseException {
		return removeRecords(_filter, whereCommand,
				whereCommand == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Remove records which correspond to the given filter and WHERE condition.
	 * These records are not pointed by other records through foreign keys of other
	 * tables. So they are not proposed to the filter class.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecords(final Filter<T> _filter, String whereCommand, Map<String, Object> parameters)
			throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(_filter, whereCommand, parameters, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to the given WHERE condition. These records
	 * are not pointed by other records through foreign keys of other tables. So
	 * they are not proposed to the filter class.
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecords(String whereCommand, Map<String, Object> parameters) throws DatabaseException {
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecords(new Filter<T>() {

					@Override
					public boolean nextRecord(T _record) {
						return true;
					}

				}, whereCommand, parameters, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final int removeRecords(final Filter<T> _filter, String where, final Map<String, Object> parameters,
			boolean is_already_in_transaction) throws DatabaseException {

		// try(ReadWriteLock.Lock
		// lock=sql_connection.locker.getAutoCloseableWriteLock())
		{
			final RuleInstance rule = Interpreter.getRuleInstance(where);
			if (isLoadedInMemory()) {
				final ArrayList<T> records_to_remove = new ArrayList<T>();

				for (final T r : getRecords(-1, -1, is_already_in_transaction)) {
					boolean toremove = true;
					for (NeighboringTable nt : list_tables_pointing_to_this_table) {
						if (nt.getPoitingTable().hasRecordsWithOneOfFields(nt.getHashMapFields(r), false)) {
							toremove = false;
							break;
						}
					}

					if (toremove && rule.isConcernedBy(this, parameters, r) && _filter.nextRecord(r)) {
						records_to_remove.add(r);
					}
					if (_filter.isTableParsingStoped())
						break;
				}
				if (records_to_remove.size() > 0) {
					Transaction transaction = new Transaction() {

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

						@SuppressWarnings("synthetic-access")
						@Override
						public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
							StringBuffer sb = new StringBuffer(
									"DELETE " + getName() + " FROM " + getFromPart(true, null) + " WHERE "
											+ getSqlPrimaryKeyCondition(records_to_remove.size()));

							int nb = 0;
							try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
									_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
									sb.toString())) {
								int index = 1;
								for (T r : records_to_remove) {
									for (FieldAccessor fa : primary_keys_fields) {
										fa.getValue(r, puq.statement, index);
										index += fa.getDeclaredSqlFields().length;
									}
									r.__createdIntoDatabase = false;
								}
								nb = puq.statement.executeUpdate();
							} catch (Exception e) {
								throw DatabaseException.getDatabaseException(e);
							}

							if (nb != records_to_remove.size())
								throw new DatabaseException("Unexpected exception.");
							return null;
						}
					};
					if (records_to_remove.size() > 0)
						sql_connection.runTransaction(transaction);

					__removeRecords(records_to_remove);
					for (T r : records_to_remove)
						getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
								new TableEvent<>(-1, DatabaseEventType.REMOVE, r, null, null));
				}
				return records_to_remove.size();
			} else {

				class RunnableTmp extends Runnable {
					private final RuleInstance rule;

					RunnableTmp(RuleInstance rule) {
						this.rule = rule;
					}

					public int deleted_records_number;

					@Override
					public void init(int _field_count) {
						deleted_records_number = 0;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
						try {
							boolean toremove = rule == null || rule.isConcernedBy(Table.this, parameters, _instance);
							if (toremove && list_tables_pointing_to_this_table.size() > 0) {
								for (int i = 0; i < list_tables_pointing_to_this_table.size(); i++) {

									NeighboringTable nt = list_tables_pointing_to_this_table.get(i);

									if (nt.getPoitingTable().hasRecordsWithOneOfSqlForeignKeyWithCascade(
											nt.getHashMapsSqlFields(getSqlPrimaryKeys(_instance)))) {
										toremove = false;
										break;
									}
								}
							}
							if (toremove && _filter.nextRecord(_instance)) {
								_cursor.deleteRow();
								++deleted_records_number;
								_instance.__createdIntoDatabase = false;
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE, _instance, null, null));
							}
							return !_filter.isTableParsingStoped();
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}

					}

				}
				HashMap<Integer, Object> sqlParameters = new HashMap<>();
				String sqlQuery = null;
				if (rule != null && rule.isIndependantFromOtherTables(this)) {
					sqlQuery = rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
							.toString();
				}

				RunnableTmp runnable = new RunnableTmp(sqlQuery == null ? rule : null);
				getListRecordsFromSqlConnection(runnable,
						sqlQuery == null ? getSqlGeneralSelect(false)
								: getSqlGeneralSelect(false, sqlQuery, sqlParameters),
						TransactionIsolation.TRANSACTION_REPEATABLE_READ, -1, -1, true);
				return runnable.deleted_records_number;
			}
		}
	}

	private final int removeRecords(final Filter<T> _filter, boolean is_already_in_transaction)
			throws DatabaseException {

		// try(ReadWriteLock.Lock
		// lock=sql_connection.locker.getAutoCloseableWriteLock())
		{
			if (isLoadedInMemory()) {
				final ArrayList<T> records_to_remove = new ArrayList<T>();

				for (final T r : getRecords(-1, -1, is_already_in_transaction)) {
					boolean toremove = true;
					for (NeighboringTable nt : list_tables_pointing_to_this_table) {
						if (nt.getPoitingTable().hasRecordsWithOneOfFields(nt.getHashMapFields(r), false)) {
							toremove = false;
							break;
						}
					}
					if (toremove && _filter.nextRecord(r)) {
						records_to_remove.add(r);
					}
					if (_filter.isTableParsingStoped())
						break;
				}
				if (records_to_remove.size() > 0) {
					Transaction transaction = new Transaction() {

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

						@SuppressWarnings("synthetic-access")
						@Override
						public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
							StringBuffer sb = new StringBuffer("DELETE FROM " + Table.this.getName() + " WHERE "
									+ getSqlPrimaryKeyCondition(records_to_remove.size()));

							int nb = 0;
							try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
									_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
									sb.toString())) {
								int index = 1;
								for (T r : records_to_remove) {
									for (FieldAccessor fa : primary_keys_fields) {
										fa.getValue(r, puq.statement, index);
										index += fa.getDeclaredSqlFields().length;
									}
									r.__createdIntoDatabase = false;
								}
								nb = puq.statement.executeUpdate();
							} catch (Exception e) {
								throw DatabaseException.getDatabaseException(e);
							}

							if (nb != records_to_remove.size())
								throw new DatabaseException("Unexpected exception.");
							for (T r : records_to_remove)
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE, r, null, null));

							return null;
						}
					};
					if (records_to_remove.size() > 0)
						sql_connection.runTransaction(transaction);

					__removeRecords(records_to_remove);
				}
				return records_to_remove.size();
			} else {

				class RunnableTmp extends Runnable {

					public int deleted_records_number;

					@Override
					public void init(int _field_count) {
						deleted_records_number = 0;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
						try {
							boolean toremove = true;
							if (list_tables_pointing_to_this_table.size() > 0) {
								for (int i = 0; i < list_tables_pointing_to_this_table.size(); i++) {

									NeighboringTable nt = list_tables_pointing_to_this_table.get(i);

									if (nt.getPoitingTable().hasRecordsWithOneOfSqlForeignKeyWithCascade(
											nt.getHashMapsSqlFields(getSqlPrimaryKeys(_instance)))) {
										toremove = false;
										break;
									}
								}
							}
							if (toremove && _filter.nextRecord(_instance)) {

								_cursor.deleteRow();
								++deleted_records_number;
								_instance.__createdIntoDatabase = false;
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE, _instance, null, null));
							}
							return !_filter.isTableParsingStoped();
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}

					}

				}

				RunnableTmp runnable = new RunnableTmp();
				getListRecordsFromSqlConnection(runnable,
						(_filter instanceof Table.PersonnalFilter) ? ((PersonnalFilter) _filter).getSQLQuerry(false)
								: getSqlGeneralSelect(false),
						TransactionIsolation.TRANSACTION_REPEATABLE_READ, -1, -1, true);
				return runnable.deleted_records_number;
			}
		}
	}

	void checkMemory() throws DatabaseException {
		{

			if (isLoadedInMemory()) {
				for (final T r : getRecords(-1, -1, false)) {
					class RunnableTmp extends Runnable {
						public boolean found = false;

						@Override
						public void init(int _field_count) {
							found = false;
						}

						@Override
						public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
							if (Table.this.equals(r, _instance)) {
								found = true;
								return false;
							}
							return true;
						}
					}
					RunnableTmp runnable = new RunnableTmp();
					getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect(true),
							TransactionIsolation.TRANSACTION_SERIALIZABLE, -1, -1);
					if (!runnable.found)
						throw new DatabaseIntegrityException(
								"All records present in the memory were not found into the database.");
				}
				final ArrayList<T> records = getRecords(-1, -1, false);
				class RunnableTmp extends Runnable {

					@Override
					public void init(int _field_count) {

					}

					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
						for (T r : records) {
							if (Table.this.equals(r, _instance))
								return false;
						}
						throw new DatabaseIntegrityException(
								"All records present into the database were not found into the memory.");
					}
				}
				getListRecordsFromSqlConnection(new RunnableTmp(), getSqlGeneralSelect(true),
						TransactionIsolation.TRANSACTION_SERIALIZABLE, -1, -1);
			}
		}
	}

	/**
	 * This function check the table integrity. It checks if group of primary keys
	 * are unique and unique keys are also unique, if foreign keys are valid, and if
	 * not null fields are not null. In the case where the current table is loaded
	 * in memory, it checks if all data is synchronized between the memory and the
	 * database. This function is used principally with unit tests. Note that the
	 * call of this function will load all the table into memory.
	 * 
	 * @throws DatabaseException
	 *             if a problem occurs. Don't throw to any DatabaseException if the
	 *             table integrity is fine.
	 */
	public final void checkDataIntegrity() throws DatabaseException {

		{
			try (ReadLock l = new ReadLock(this)) {
				sql_connection.runTransaction(new Transaction() {

					@Override
					public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
						checkMemory();
						ArrayList<T> records = getRecords(-1, -1, false);
						for (T r1 : records) {
							for (T r2 : records) {
								if (r1 != r2) {
									boolean allequals = true;
									for (FieldAccessor fa : primary_keys_fields) {
										if (!fa.equals(r1, fa.getValue(r2))) {
											allequals = false;
											break;
										}
									}
									if (allequals)
										throw new DatabaseIntegrityException("There is records into the table "
												+ getName() + " which have the same primary keys.");
									for (FieldAccessor fa : unique_fields_no_auto_random_primary_keys) {
										if (fa.equals(r1, fa.getValue(r2)))
											throw new DatabaseIntegrityException("There is records into the table "
													+ getName() + " which have the same unique key into the field "
													+ fa.getFieldName());
									}
								}
							}
						}
						return null;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_SERIALIZABLE;
					}

					@Override
					public boolean doesWriteData() {
						return false;
					}
				});
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Returns true if the primary keys of the two given instances are equals.
	 * Returns false else.
	 * 
	 * @param _record1
	 *            the first record to compare
	 * @param _record2
	 *            the second record to compare
	 * @return true if the primary keys of the two given instances are equals.
	 *         Returns false else.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final boolean equals(T _record1, T _record2) throws DatabaseException {
		if (_record1 == _record2)
			return true;
		if (_record1 == null || _record2 == null)
			return _record1 == _record2;
		for (FieldAccessor fa : primary_keys_fields) {
			if (!fa.equals(_record1, fa.getValue(_record2)))
				return false;
		}
		return true;
	}

	@Override
	public boolean equals(Object o) {
		return o == this;
	}

	/**
	 * Returns true if all fields of the two given instances are equals. Returns
	 * false else.
	 * 
	 * @param _record1
	 *            the first record to compare
	 * @param _record2
	 *            the second record to compare
	 * @return true if all fields of the two given instances are equals. Returns
	 *         false else.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final boolean equalsAllFields(T _record1, T _record2) throws DatabaseException {
		if (_record1 == _record2)
			return true;
		if (_record1 == null || _record2 == null)
			return _record1 == _record2;
		for (FieldAccessor fa : fields) {
			if (!fa.equals(_record1, fa.getValue(_record2))) {
				return false;
			}
		}
		return true;

	}

	/**
	 * Returns true if all fields (excepted the primary keys) of the two given
	 * instances are equals. Returns false else.
	 * 
	 * @param _record1
	 *            the first record to compare
	 * @param _record2
	 *            the second record to compare
	 * @return true if all fields (excepted the primary keys) of the two given
	 *         instances are equals. Returns false else.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final boolean equalsAllFieldsWithoutPrimaryKeys(T _record1, T _record2) throws DatabaseException {
		if (_record1 == _record2)
			return true;
		if (_record1 == null || _record2 == null)
			return _record1 == _record2;
		for (FieldAccessor fa : fields) {
			if (!fa.isPrimaryKey()) {
				if (!fa.equals(_record1, fa.getValue(_record2)))
					return false;
			}
		}
		return true;

	}

	/**
	 * Returns true if all fields (excepted the primary and foreign keys) of the two
	 * given instances are equals. Returns false else.
	 * 
	 * @param _record1
	 *            the first record to compare
	 * @param _record2
	 *            the second record to compare
	 * @return true if all fields (excepted the primary and foreign keys) of the two
	 *         given instances are equals. Returns false else.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final boolean equalsAllFieldsWithoutPrimaryAndForeignKeys(T _record1, T _record2) throws DatabaseException {
		if (_record1 == _record2)
			return true;
		if (_record1 == null || _record2 == null)
			return _record1 == _record2;
		for (FieldAccessor fa : fields_without_primary_and_foreign_keys) {
			if (!fa.equals(_record1, fa.getValue(_record2)))
				return false;
		}
		return true;

	}

	/**
	 * Remove records which correspond to the given filter. Records of other tables
	 * which have Foreign keys which points to the deleted records are also deleted.
	 * 
	 * @param _filter
	 *            the filter
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecordsWithCascade(final Filter<T> _filter) throws DatabaseException {
		return removeRecordsWithCascade(_filter, null, new HashMap<String, Object>());
	}

	/**
	 * Remove records which correspond to the given filter and a given sql where
	 * close Records of other tables which have Foreign keys which points to the
	 * deleted records are also deleted.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecordsWithCascade(final Filter<T> _filter, String whereCommand, Object... parameters)
			throws DatabaseException {
		return removeRecordsWithCascade(_filter, whereCommand,
				whereCommand == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	/**
	 * Remove records which correspond to the given filter and a given sql where
	 * close Records of other tables which have Foreign keys which points to the
	 * deleted records are also deleted.
	 * 
	 * @param _filter
	 *            the filter
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecordsWithCascade(final Filter<T> _filter, String whereCommand,
			Map<String, Object> parameters) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return removeRecordsWithCascade(_filter, whereCommand, parameters, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove records which correspond to the sql where close Records of other
	 * tables which have Foreign keys which points to the deleted records are also
	 * deleted.
	 * 
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @return the number of deleted records.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final long removeRecordsWithCascade(String whereCommand, Object... parameters) throws DatabaseException {
		return removeRecordsWithCascade(new Filter<T>() {

			@Override
			public boolean nextRecord(T _record) {
				return true;
			}
		}, whereCommand, whereCommand == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	@SafeVarargs
	private final boolean hasRecordsWithOneOfSqlForeignKeyWithCascade(final HashMap<String, Object>... _foreign_keys)
			throws DatabaseException {
		// try(ReadWriteLock.Lock lock=sql_connection.locker.getAutoCloseableReadLock())
		{
			final StringBuffer querry = new StringBuffer("SELECT * FROM " + this.getName() + " WHERE ");
			boolean group_first = true;
			boolean parenthesis = _foreign_keys.length > 1;

			for (HashMap<String, Object> hm : _foreign_keys) {
				if (group_first)
					group_first = false;
				else
					querry.append(" OR ");
				if (parenthesis)
					querry.append("(");
				boolean first = true;
				for (String f : hm.keySet()) {
					if (first)
						first = false;
					else
						querry.append(" AND ");
					querry.append(f + " = ?");
				}
				if (parenthesis)
					querry.append(")");
			}

			final SqlQuerry sqlquerry = new SqlQuerry(querry.toString()) {
				@Override
				void finishPrepareStatement(PreparedStatement st) throws SQLException {
					int index = 1;
					for (HashMap<String, Object> hm : _foreign_keys) {
						for (String f : hm.keySet()) {
							st.setObject(index++, hm.get(f));
						}
					}
				}
			};

			return ((Boolean) sql_connection.runTransaction(new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try (ReadQuerry prq = new ReadQuerry(
							_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(), sqlquerry)) {
						if (prq.result_set.next()) {
							return new Boolean(true);
						}
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}

					return new Boolean(false);
				}
			})).booleanValue();

		}
	}

	private final long removeRecordsWithCascade(final Filter<T> _filter, String where,
			final Map<String, Object> parameters, boolean _is_already_sql_transaction) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");

		// try(ReadWriteLock.Lock
		// lock=sql_connection.locker.getAutoCloseableWriteLock())
		{
			final RuleInstance rule = where == null ? null : Interpreter.getRuleInstance(where);
			if (isLoadedInMemory()) {
				final ArrayList<T> records_to_remove = new ArrayList<T>();

				for (T r : getRecords(-1, -1, _is_already_sql_transaction)) {
					if ((rule == null || rule.isConcernedBy(this, parameters, r)) && _filter.nextRecord(r)) {
						records_to_remove.add(r);
					}
					if (_filter.isTableParsingStoped())
						break;
				}
				if (records_to_remove.size() > 0) {
					Transaction transaction = new Transaction() {
						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_SERIALIZABLE;
						}

						@SuppressWarnings("synthetic-access")
						@Override
						public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
							StringBuffer sb = new StringBuffer("DELETE FROM " + Table.this.getName() + " WHERE "
									+ getSqlPrimaryKeyCondition(records_to_remove.size()));

							int nb = 0;
							try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
									_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
									sb.toString())) {
								int index = 1;
								for (T r : records_to_remove) {
									for (FieldAccessor fa : primary_keys_fields) {
										fa.getValue(r, puq.statement, index);
										index += fa.getDeclaredSqlFields().length;
									}
									r.__createdIntoDatabase = false;
								}
								nb = puq.statement.executeUpdate();
							} catch (Exception e) {
								throw DatabaseException.getDatabaseException(e);
							}

							if (nb != records_to_remove.size()) {
								throw new DatabaseException("Unexpected exception.");
							}

							for (T r : records_to_remove)
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, r, null, null));

							return null;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

					};

					sql_connection.runTransaction(transaction);

					__removeRecords(records_to_remove);
					updateMemoryForRemovingRecordsWithCascade(records_to_remove);
				}
				return records_to_remove.size();
			} else {
				class RunnableTmp extends Runnable {
					private final RuleInstance rule;

					RunnableTmp(RuleInstance rule) {
						this.rule = rule;
					}

					public long deleted_records_number;

					@Override
					public void init(int _field_count) {
						deleted_records_number = 0;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
						try {
							if ((rule == null || rule.isConcernedBy(Table.this, parameters, _instance))
									&& _filter.nextRecord(_instance)) {
								_cursor.deleteRow();
								++deleted_records_number;
								_instance.__createdIntoDatabase = false;
								updateMemoryForRemovingRecordWithCascade(_instance);
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, _instance, null,
												null));
							}
							return !_filter.isTableParsingStoped();
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}

					}

				}

				HashMap<Integer, Object> sqlParameters = new HashMap<>();
				String sqlQuery = null;
				if (rule != null && rule.isIndependantFromOtherTables(this)) {
					sqlQuery = rule.translateToSqlQuery(this, parameters, sqlParameters, new HashSet<TableJunction>())
							.toString();
				}

				RunnableTmp runnable = new RunnableTmp(sqlQuery == null ? rule : null);
				try {
					getListRecordsFromSqlConnection(runnable,
							(_filter instanceof Table.PersonnalFilter) ? ((PersonnalFilter) _filter).getSQLQuerry(false)
									: (sqlQuery == null ? getSqlGeneralSelect(false)
											: getSqlGeneralSelect(false, sqlQuery, sqlParameters)),
							TransactionIsolation.TRANSACTION_SERIALIZABLE, -1, -1, true);
				} catch (DatabaseException e) {
					for (NeighboringTable nt : list_tables_pointing_to_this_table) {
						Table<?> t = nt.getPoitingTable();
						if (t.isLoadedInMemory() && t.isSynchronizedWithSqlDatabase()) {
							t.memoryToRefresh();
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
	 * 
	 * @param _record
	 *            the record to delete
	 * @throws DatabaseException
	 *             if a Sql exception occurs or if the given record has already been
	 *             deleted.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given record is pointed by another record through a
	 *             foreign key into another table of the database.
	 * @throws RecordNotFoundDatabaseException
	 *             if the given record was not found
	 */
	public final void removeRecord(final T _record) throws DatabaseException {
		removeUntypedRecord((DatabaseRecord) _record, true, null);
	}

	final void removeUntypedRecord(final DatabaseRecord record, final boolean synchronizeIfNecessary,
			final Set<AbstractDecentralizedID> hostsDestinations) throws DatabaseException {
		if (record == null)
			throw new NullPointerException("The parameter _record is a null pointer !");
		@SuppressWarnings("unchecked")
		final T _record = (T) record;
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {

				for (NeighboringTable nt : list_tables_pointing_to_this_table) {
					if (nt.getPoitingTable().hasRecordsWithOneOfFields(nt.getHashMapFields(_record), false)) {
						throw new ConstraintsNotRespectedDatabaseException(
								"The given record is pointed by another record through a foreign key into the table "
										+ nt.getPoitingTable().getName() + ". Impossible to remove it into the table "
										+ this.getName());
					}
				}

				sql_connection.runTransaction(new Transaction() {
					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
						StringBuffer querry = new StringBuffer(
								"DELETE FROM " + Table.this.getName() + " WHERE " + getSqlPrimaryKeyCondition(1));

						try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
								_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
								querry.toString())) {
							int index = 1;
							for (FieldAccessor fa : primary_keys_fields) {
								fa.getValue(_record, puq.statement, index);
								index += fa.getDeclaredSqlFields().length;
							}
							int nb = puq.statement.executeUpdate();
							if (nb == 0)
								throw new RecordNotFoundDatabaseException("the given record was not into the table "
										+ Table.this.getName() + ". It has been probably already removed.");
							else if (nb > 1)
								throw new DatabaseIntegrityException("Unexpected exception");
							if (synchronizeIfNecessary)
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE, _record, null,
												hostsDestinations));
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}

						return null;
					}

					@Override
					public boolean doesWriteData() {
						return true;
					}

				});

				if (Table.this.isLoadedInMemory() && isSynchronizedWithSqlDatabase())
					Table.this.__removeRecord(_record);
				_record.__createdIntoDatabase = false;

			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove the given record from the database. Remove also the records from other
	 * tables whose foreign keys points to the record to delete.
	 * 
	 * @param _record
	 *            the record to delete
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws RecordNotFoundDatabaseException
	 *             is the given record was not found into the database. This can
	 *             occurs if the record has already been deleted.
	 */
	public final void removeRecordWithCascade(final T _record) throws DatabaseException {
		removeUntypedRecordWithCascade(_record, true, null);
	}

	final void removeUntypedRecordWithCascade(final DatabaseRecord record, final boolean synchronizeIfNecessary,
			final Set<AbstractDecentralizedID> hostsDestinations) throws DatabaseException {
		if (record == null)
			throw new NullPointerException("The parameter _record is a null pointer !");
		@SuppressWarnings("unchecked")
		final T _record = (T) record;
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {

				class TransactionTmp implements Transaction {
					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_SERIALIZABLE;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
						StringBuffer querry = new StringBuffer(
								"DELETE FROM " + Table.this.getName() + " WHERE " + getSqlPrimaryKeyCondition(1));

						try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
								_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
								querry.toString())) {
							int index = 1;
							for (FieldAccessor fa : primary_keys_fields) {
								fa.getValue(_record, puq.statement, index);
								index += fa.getDeclaredSqlFields().length;
							}
							int nb = puq.statement.executeUpdate();
							if (nb == 0)
								throw new RecordNotFoundDatabaseException("the given record was not into the table "
										+ Table.this.getName() + ". It has been probably already removed.");
							else if (nb > 1)
								throw new DatabaseIntegrityException("Unexpected exception");
							if (synchronizeIfNecessary)
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, _record, null,
												hostsDestinations));
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}

						return null;
					}

					@Override
					public boolean doesWriteData() {
						return true;
					}

				}

				TransactionTmp transaction = new TransactionTmp();
				sql_connection.runTransaction(transaction);
				if (this.isLoadedInMemory() && isSynchronizedWithSqlDatabase())
					__removeRecord(_record);
				_record.__createdIntoDatabase = false;
				updateMemoryForRemovingRecordWithCascade(_record);

			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private void updateMemoryForRemovingRecordWithCascade(T _record) throws DatabaseException {
		for (NeighboringTable nt : list_tables_pointing_to_this_table) {
			Table<?> t = nt.getPoitingTable();
			if (t.isLoadedInMemory() && t.isSynchronizedWithSqlDatabase()) {
				ArrayList<DatabaseRecord> removed_records = new ArrayList<DatabaseRecord>();
				Iterator<?> it = t.records_instances.get().iterator();
				for (; it.hasNext();) {
					DatabaseRecord dr = (DatabaseRecord) it.next();
					for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields) {
						if (fkfa.equals(dr, _record)) {
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

	void updateMemoryForRemovingRecordsWithCascade(Collection<T> _records) throws DatabaseException {
		for (NeighboringTable nt : list_tables_pointing_to_this_table) {
			Table<?> t = nt.getPoitingTable();
			if (t.isLoadedInMemory() && t.isSynchronizedWithSqlDatabase()) {
				ArrayList<DatabaseRecord> removed_records = new ArrayList<DatabaseRecord>();
				Iterator<?> it = t.records_instances.get().iterator();
				for (; it.hasNext();) {
					DatabaseRecord dr = (DatabaseRecord) it.next();
					boolean removed = false;
					for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields) {
						for (DatabaseRecord r : _records) {
							if (fkfa.equals(dr, r)) {
								removed_records.add(dr);
								it.remove();
								removed = true;
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

	private void updateMemoryForRemovingRecordsWithCascade2(Collection<DatabaseRecord> _records)
			throws DatabaseException {
		for (NeighboringTable nt : list_tables_pointing_to_this_table) {
			Table<?> t = nt.getPoitingTable();
			if (t.isLoadedInMemory() && t.isSynchronizedWithSqlDatabase()) {
				ArrayList<DatabaseRecord> removed_records = new ArrayList<DatabaseRecord>();
				Iterator<?> it = t.records_instances.get().iterator();
				for (; it.hasNext();) {
					DatabaseRecord dr = (DatabaseRecord) it.next();
					boolean removed = false;
					for (ForeignKeyFieldAccessor fkfa : t.foreign_keys_fields) {
						for (DatabaseRecord r : _records) {
							if (fkfa.equals(dr, r)) {
								removed_records.add(dr);
								it.remove();
								removed = true;
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
	 * Returns true if the given record is pointed by another record through a
	 * foreign key of another table.
	 * 
	 * @param _record
	 *            the record to test
	 * @return true if the given record is pointed by another record through a
	 *         foreign key of another table.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final boolean isRecordPointedByForeignKeys(T _record) throws DatabaseException {
		try (Lock lock = new ReadLock(this)) {
			boolean res = false;
			for (NeighboringTable nt : list_tables_pointing_to_this_table) {
				Table<?> t = nt.getPoitingTable();
				if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(_record))) {
					res = true;
					break;
				}
			}
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	/**
	 * From a given collection of records, returns those which are not pointed by
	 * other records through foreign keys of other tables into the database.
	 * 
	 * @param _records
	 *            the collection of records to test
	 * @return the records not pointed by other records through foreign keys of
	 *         other tables into the database.
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 */
	public final ArrayList<T> getRecordsNotPointedByForeignKeys(Collection<T> _records) throws DatabaseException {
		try (Lock lock = new WriteLock(this)) {
			ArrayList<T> res = new ArrayList<T>(_records.size());
			for (T r : _records) {
				boolean toadd = true;
				for (NeighboringTable nt : list_tables_pointing_to_this_table) {
					Table<?> t = nt.getPoitingTable();
					if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(r), false)) {
						toadd = false;
						break;
					}
				}
				if (toadd)
					res.add(r);
			}
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	private String getSqlPrimaryKeyCondition(int repeat) {
		StringBuffer sb = new StringBuffer();
		boolean parenthesis = repeat > 1;
		boolean first_group = true;
		while (repeat-- > 0) {
			if (first_group)
				first_group = false;
			else
				sb.append(" OR ");
			if (parenthesis)
				sb.append("(");
			boolean first = true;
			for (FieldAccessor fa : primary_keys_fields) {
				for (SqlField sf : fa.getDeclaredSqlFields()) {
					if (first)
						first = false;
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
	 * Remove a list of records from the database. The records do not be pointed by
	 * other records through foreign keys of other tables.
	 * 
	 * @param _records
	 *            the record to delete
	 * @throws DatabaseException
	 *             if a Sql exception occurs or if one of the given records has
	 *             already been deleted.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if one of the given records is pointed by another record through
	 *             a foreign key of another table.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the given records has not been found into the database.
	 *             This may occurs if the record has already been deleted into the
	 *             database.
	 */
	public final void removeRecords(final Collection<T> _records) throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.isEmpty())
			return;
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				for (NeighboringTable nt : list_tables_pointing_to_this_table) {
					Table<?> t = nt.getPoitingTable();
					for (T record : _records)
						if (t.hasRecordsWithOneOfFields(nt.getHashMapFields(record), false))
							throw new ConstraintsNotRespectedDatabaseException(
									"One of the given record is pointed by another record through a foreign key into the table "
											+ t.getName() + ". Impossible to remove this record into the table "
											+ getName());
				}

				sql_connection.runTransaction(new Transaction() {

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_SERIALIZABLE;
					}

					@SuppressWarnings("synthetic-access")
					@Override
					public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
						try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
								_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
								"DELETE FROM " + Table.this.getName() + " WHERE "
										+ getSqlPrimaryKeyCondition(_records.size()))) {
							int index = 1;
							for (T r : _records) {
								for (FieldAccessor fa : primary_keys_fields) {
									fa.getValue(r, puq.statement, index);
									index += fa.getDeclaredSqlFields().length;
								}
								r.__createdIntoDatabase = false;
							}
							int number = puq.statement.executeUpdate();
							if (number != _records.size())
								throw new RecordNotFoundDatabaseException("There is " + (_records.size() - number)
										+ " records which have not been found into the table " + Table.this.getName()
										+ ". No modification have been done.");
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}
						for (T r : _records)
							getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
									new TableEvent<>(-1, DatabaseEventType.REMOVE, r, null, null));

						return null;
					}

					@Override
					public boolean doesWriteData() {
						return true;
					}

				});

				if (isLoadedInMemory() && isSynchronizedWithSqlDatabase()) {
					__removeRecords(_records);
				}

			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Remove a list of records from the database. Remove also all records from
	 * other tables whose foreign key point to the deleted record.
	 * 
	 * @param _records
	 *            the record to delete
	 * @throws DatabaseException
	 *             if a Sql exception occurs.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the given records was not found found into the
	 *             database. This may occur if the concerned record has already been
	 *             deleted.
	 */
	public final void removeRecordsWithCascade(Collection<T> _records) throws DatabaseException {
		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				removeRecordsWithCascade(_records, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final void removeRecordsWithCascade(final Collection<T> _records, boolean is_already_in_transaction)
			throws DatabaseException {
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		if (_records.isEmpty())
			return;
		// synchronized(sql_connection)
		{
			class TransactionTmp implements Transaction {
				public TransactionTmp() {
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_SERIALIZABLE;
				}

				@SuppressWarnings("synthetic-access")
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					StringBuffer sb = new StringBuffer("DELETE FROM " + Table.this.getName() + " WHERE "
							+ getSqlPrimaryKeyCondition(_records.size()));

					try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
							_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
							sb.toString())) {
						int index = 1;
						for (T r : _records) {
							for (FieldAccessor fa : primary_keys_fields) {
								fa.getValue(r, puq.statement, index);
								index += fa.getDeclaredSqlFields().length;
							}
							r.__createdIntoDatabase = false;
						}
						int nb = puq.statement.executeUpdate();
						if (nb != _records.size())
							throw new RecordNotFoundDatabaseException("There is " + (_records.size() - nb) + " (about "
									+ _records.size()
									+ ") records which have not been found into the database. This may occur if the concerned record has already been deleted. No records have been deleted.");
						for (T r : _records)
							getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
									new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, r, null, null));

					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}

					return null;
				}

				@Override
				public boolean doesWriteData() {
					return true;
				}

			}
			TransactionTmp transaction = new TransactionTmp();
			sql_connection.runTransaction(transaction);
			if (isLoadedInMemory() && isSynchronizedWithSqlDatabase()) {
				__removeRecords(_records);
			}
			updateMemoryForRemovingRecordsWithCascade(_records);
		}
	}

	abstract class Runnable {
		public Runnable() {

		}

		public abstract void init(int _max_field_count);

		public void init() {
			init(20);
		}

		public abstract boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException;
	}

	static class SqlQuerry {
		String querry;

		SqlQuerry(String querry) {
			this.querry = querry;
		}

		String getQuerry() {
			return this.querry;
		}

		@SuppressWarnings("unused")
		void finishPrepareStatement(PreparedStatement st) throws SQLException, DatabaseException {

		}

	}
	/*
	 * private final void getListRecordsFromSqlConnection(final Runnable _runnable,
	 * final SqlQuerry querry, final TransactionIsolation transactionIsolation)
	 * throws DatabaseException { getListRecordsFromSqlConnection(_runnable, querry,
	 * transactionIsolation, -1, -1); } final void
	 * getListRecordsFromSqlConnection(final Runnable _runnable, final SqlQuerry
	 * querry, final TransactionIsolation transactionIsolation, final boolean
	 * updatable) throws DatabaseException {
	 * getListRecordsFromSqlConnection(_runnable, querry, transactionIsolation, -1,
	 * -1, updatable); }
	 */

	private final void getListRecordsFromSqlConnection(final Runnable _runnable, final SqlQuerry querry,
			final TransactionIsolation transactionIsolation, int startPosition, int length) throws DatabaseException {
		getListRecordsFromSqlConnection(_runnable, querry, transactionIsolation, startPosition, length, false);
	}

	final void getListRecordsFromSqlConnection(final Runnable _runnable, final SqlQuerry querry,
			final TransactionIsolation transactionIsolation, final int startPosition, final int length,
			final boolean updatable) throws DatabaseException {
		// synchronized(sql_connection)
		{

			class TransactionTmp implements Transaction {
				protected final Constructor<T> default_constructor_field;
				protected final ArrayList<FieldAccessor> fields_accessor;

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return transactionIsolation;
				}

				public TransactionTmp(Constructor<T> _default_constructor_field,
						ArrayList<FieldAccessor> _fields_accessor) {
					default_constructor_field = _default_constructor_field;
					fields_accessor = _fields_accessor;

				}

				@Override
				public Object run(DatabaseWrapper sql_connection) throws DatabaseException {
					try (AbstractReadQuerry rq = (updatable
							? new UpdatableReadQuerry(
									sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(), querry)
							: new ReadQuerry(sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
									querry))) {
						// int rowcount=getRowCount();
						int rowcount = 0;
						_runnable.init();
						if (startPosition > 0)
							rq.result_set.absolute(startPosition - 1);
						while (rq.result_set.next()) {
							T field_instance = getNewRecordInstance(default_constructor_field);

							for (FieldAccessor f : fields_accessor) {
								f.setValue(field_instance, rq.result_set);
							}
							// rowcount--;
							if (!_runnable.setInstance(field_instance, rq.result_set)) {
								// rowcount=0;
								break;
							}
							++rowcount;
							if (startPosition > 0 && length > 0 && rowcount >= length)
								break;
						}
						/*
						 * if (rowcount!=0) throw new
						 * DatabaseException("Unexpected exception "+rowcount);
						 */
						return null;
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException e) {
						throw new DatabaseException("Impossible to instantiate a DatabaseRecord ", e);
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}

				@Override
				public boolean doesWriteData() {
					return updatable;
				}

			}

			Transaction transaction = new TransactionTmp(default_constructor_field, fields);
			sql_connection.runTransaction(transaction);
		}
	}

	static abstract class Runnable2 {
		public Runnable2() {

		}

		public abstract void init(int _field_count);

		public void init() {
			init(20);
		}

		public abstract boolean setInstance(ResultSet _cursor) throws DatabaseException;
	}
	/*
	 * final void getListRecordsFromSqlConnection(final Runnable2 _runnable, final
	 * SqlQuerry querry, TransactionIsolation transactionIsolation) throws
	 * DatabaseException { getListRecordsFromSqlConnection(_runnable, querry,
	 * transactionIsolation,-1, -1); } final void
	 * getListRecordsFromSqlConnection(final Runnable2 _runnable, final SqlQuerry
	 * querry, final TransactionIsolation transactionIsolation, final boolean
	 * updatable) throws DatabaseException {
	 * getListRecordsFromSqlConnection(_runnable, querry, transactionIsolation,-1,
	 * -1, updatable); }
	 */

	final void getListRecordsFromSqlConnection(final Runnable2 _runnable, final SqlQuerry querry,
			TransactionIsolation transactionIsolation, int rowoffset, int rowlength) throws DatabaseException {
		getListRecordsFromSqlConnection(_runnable, querry, transactionIsolation, rowoffset, rowlength, false);
	}

	final void getListRecordsFromSqlConnection(final Runnable2 _runnable, final SqlQuerry querry,
			final TransactionIsolation transactionIsolation, final int rowoffset, final int rowlength,
			final boolean updatable) throws DatabaseException {
		// synchronized(sql_connection)
		{
			class TransactionTmp implements Transaction {
				public TransactionTmp() {
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return transactionIsolation;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try (AbstractReadQuerry rq = (updatable
							? new UpdatableReadQuerry(
									_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(), querry)
							: new ReadQuerry(_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
									querry))) {
						// int rowcount=getRowCount();
						_runnable.init();
						int rowcount = 0;
						if (rowoffset > 0)
							rq.result_set.absolute(rowoffset - 1);
						while (rq.result_set.next()) {
							// rowcount--;
							if (!_runnable.setInstance(rq.result_set)) {
								// rowcount=0;
								break;
							}
							++rowcount;
							if (rowoffset > 0 && rowlength > 0 && rowcount >= rowlength)
								break;
						}
						/*
						 * if (rowcount!=0) throw new
						 * DatabaseException("Unexpected exception "+rowcount);
						 */
						return null;
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}

				@Override
				public boolean doesWriteData() {
					return updatable;
				}

			}

			Transaction transaction = new TransactionTmp();
			sql_connection.runTransaction(transaction);
		}
	}

	/**
	 * 
	 * @param _record
	 *            the record
	 * @return true if the given record is contained into the table. false, else.
	 * @throws DatabaseException
	 *             when a database exception occurs
	 */
	public final boolean contains(final T _record) throws DatabaseException {
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return contains(_record, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	final boolean contains(boolean is_already_in_transaction, final DatabaseRecord _record) throws DatabaseException {
		return contains((T) _record, false);
	}

	private final boolean contains(final T _record, boolean is_already_in_transaction) throws DatabaseException {
		if (_record == null)
			return false;

		if (isLoadedInMemory()) {
			for (T r : getRecords(-1, -1, is_already_in_transaction)) {
				if (equals(_record, r))
					return true;
			}
			return false;
		} else {

			final SqlQuerry sqlquerry = new SqlQuerry(
					"SELECT * FROM " + Table.this.getName() + " WHERE " + getSqlPrimaryKeyCondition(1)) {
				@Override
				void finishPrepareStatement(PreparedStatement st) throws DatabaseException {
					int index = 1;
					for (FieldAccessor fa : getPrimaryKeysFieldAccessors()) {
						fa.getValue(_record, st, index);
						index += fa.getDeclaredSqlFields().length;
					}
				}
			};

			Transaction transaction = new Transaction() {

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try (ReadQuerry rq = new ReadQuerry(
							_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(), sqlquerry)) {
						if (rq.result_set.next())
							return new Boolean(true);
						else
							return new Boolean(false);
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}

				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}

			};
			Boolean found = (Boolean) sql_connection.runTransaction(transaction);
			return found.booleanValue();
		}
	}

	private Map<String, Object> transformToMapField(Object... _fields) throws DatabaseException {
		HashMap<String, Object> res = new HashMap<>();
		if (_fields == null)
			throw new NullPointerException("_fields");
		if (_fields.length == 0 || _fields.length % 2 != 0)
			throw new DatabaseException("Bad field tab format ! fields tab length = " + _fields.length);
		for (int i = 0; i < _fields.length; i += 2) {
			if (_fields[i] == null || !(_fields[i] instanceof String))
				throw new DatabaseException("Bad field tab format !");
			res.put((String) _fields[i], _fields[i + 1]);
		}
		return res;
	}

	private Map<String, Object>[] transformToMapField(Object[]... _records) throws DatabaseException {

		@SuppressWarnings("unchecked")
		Map<String, Object>[] tab = new Map[_records.length];
		for (int i = 0; i < _records.length; i++)
			tab[i] = transformToMapField(_records[i]);
		return tab;
	}

	/**
	 * Add a record into the database with a map of fields corresponding to this
	 * record. The string type in the Map corresponds to the name of the field, and
	 * the Object type field corresponds the value of the field.
	 * 
	 * @param _fields
	 *            the list of fields to include into the new record. Must be
	 *            formated as follow : {"field1", value1,"field2", value2, etc.}
	 * @return the created record
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, or if a
	 *             field which has the unique property exists already into the
	 *             table.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if fields are lacking.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the field is a foreign key and point to a record of
	 *             another table which does not exist.
	 */
	public final T addRecord(Object... _fields) throws DatabaseException {
		return addRecord(transformToMapField(_fields));
	}

	/**
	 * Add a record into the database with a map of fields corresponding to this
	 * record. The string type in the Map corresponds to the name of the field, and
	 * the Object type field corresponds the value of the field.
	 * 
	 * @param _fields
	 *            the list of fields to include into the new record
	 * @return the created record
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, or if a
	 *             field which has the unique property exists already into the
	 *             table.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if fields are lacking.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the field is a foreign key and point to a record of
	 *             another table which does not exist.
	 */
	public final T addRecord(final Map<String, Object> _fields) throws DatabaseException {
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");

		// synchronized(sql_connection)
		{
			try (Lock lock = new WriteLock(this)) {
				return addRecord(_fields, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Add a record into the database.
	 * 
	 * @param record
	 *            the record to add
	 * @return the added record with its new auto generated identifiers
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, or if a
	 *             field which has the unique property exists already into the
	 *             table.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if fields are lacking.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the field is a foreign key and point to a record of
	 *             another table which does not exist.
	 */
	@SuppressWarnings("unchecked")
	public final T addRecord(T record) throws DatabaseException {
		return (T) addUntypedRecord(record, false, true, null);
	}

	final DatabaseRecord addUntypedRecord(DatabaseRecord record, boolean includeAutoGeneratedKeys,
			boolean synchronizeIfNecessary, Set<AbstractDecentralizedID> hostsDestination) throws DatabaseException {
		if (record == null)
			throw new NullPointerException("The parameter record is a null pointer !");
		if (record.__createdIntoDatabase)
			throw new IllegalArgumentException("The given record has already been added !");

		try (Lock lock = new WriteLock(this)) {
			Map<String, Object> map = getMap(record, true, includeAutoGeneratedKeys);
			T res = addRecord(map, false, record, synchronizeIfNecessary, hostsDestination);
			res.__createdIntoDatabase = true;
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/*
	 * final T addDatabaseRecord(DatabaseRecord record) throws DatabaseException {
	 * if (record==null) throw new
	 * NullPointerException("The parameter record is a null pointer !"); if
	 * (record.__createdIntoDatabase) throw new
	 * IllegalArgumentException("The given record has already been removed !");
	 * 
	 * try (Lock lock=new WriteLock(this)) { Map<String, Object> map=getMap(record,
	 * true, false); T res=addRecord(map, false, record);
	 * res.__createdIntoDatabase=true; return res; } catch(Exception e) { throw
	 * DatabaseException.getDatabaseException(e); }
	 * 
	 * }
	 */
	Map<String, Object> getMap(DatabaseRecord record, boolean includePrimaryKeys, boolean includeAutoGeneratedKeys)
			throws DatabaseException {
		Map<String, Object> map = new HashMap<>();
		for (FieldAccessor fa : getFieldAccessors()) {

			boolean include = true;
			if (fa.isPrimaryKey()) {
				if (!includePrimaryKeys)
					include = false;
				else if ((fa.isAutoPrimaryKey() || fa.isRandomPrimaryKey()) && !includeAutoGeneratedKeys)
					include = false;
			}
			if (include) {
				map.put(fa.getFieldName(), fa.getValue(record));
			}
		}
		return map;
	}

	private final T addRecord(final Map<String, Object> _fields, boolean already_in_transaction)
			throws DatabaseException {
		return addRecord(_fields, already_in_transaction, null, true, null);
	}

	@SuppressWarnings("unchecked")
	private final T addRecord(final Map<String, Object> _fields, final boolean already_in_transaction,
			final DatabaseRecord originalRecord, final boolean synchronizeIfNecessary,
			final Set<AbstractDecentralizedID> hostsDestinations) throws DatabaseException {

		return (T) sql_connection.runTransaction(new Transaction() {

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				int number = 0;
				for (FieldAccessor fa : fields) {
					if (!fa.isAutoPrimaryKey() && !fa.isRandomPrimaryKey()) {
						Object obj = _fields.get(fa.getFieldName());
						if (obj == null) {
							if (fa.isNotNull())
								throw new FieldDatabaseException("The field " + fa.getFieldName() + " can't be null.");
						} else
							number++;
					}
				}
				if (number > _fields.size())
					throw new FieldDatabaseException("The number (" + _fields.size()
							+ ") of given fields does not correspond to the expected minimum number (" + number
							+ ") of fields (Null fields, AutoPrimaryKeys and RandomPrimaryKeys are excluded).");
				try {
					class CheckTmp extends Runnable2 {
						public boolean check_necessary = false;
						public final ArrayList<FieldAccessor> random_fields_to_check = new ArrayList<FieldAccessor>();
						public boolean include_auto_pk = false;

						public CheckTmp(ArrayList<FieldAccessor> _auto_random_primary_keys_fields) {

							for (FieldAccessor fa : _auto_random_primary_keys_fields) {
								if (fa.isRandomPrimaryKey() && _fields.containsKey(fa.getFieldName())) {
									if (_fields.get(fa.getFieldName()) == null)
										_fields.remove(fa.getFieldName());
									else {
										random_fields_to_check.add(fa);
										check_necessary = true;
									}
								}
								if (fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName())) {
									if (_fields.get(fa.getFieldName()) == null)
										_fields.remove(fa.getFieldName());
									else
										include_auto_pk = true;
								}
							}
						}

						@Override
						public void init(int _field_count) {
						}

						@Override
						public boolean setInstance(ResultSet _result_set) throws DatabaseException {
							for (FieldAccessor fa : random_fields_to_check) {
								if (fa.equals(_fields.get(fa.getFieldName()), _result_set))
									throw new ConstraintsNotRespectedDatabaseException(
											"the given record have the same unique auto/random primary key field "
													+ fa.getFieldName()
													+ " of one of the records stored into the database. No record have been added.");
							}
							return true;
						}

					}
					final CheckTmp ct = new CheckTmp(auto_random_primary_keys_fields);
					if (ct.check_necessary)
						getListRecordsFromSqlConnection(ct, getSqlGeneralSelect(true),
								TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);

					final T instance = originalRecord == null ? getNewRecordInstance() : (T) originalRecord;
					if (isLoadedInMemory()) {
						for (FieldAccessor fa : fields) {
							Object obj;
							if (fa.isRandomPrimaryKey() && !ct.random_fields_to_check.contains(fa)) {
								ArrayList<T> fields_instances = getRecords(-1, -1, false);
								Object value;
								boolean ok = false;
								value = fa.autoGenerateValue(rand);
								if (fa.needToCheckUniquenessOfAutoGeneratedValues()) {
									do {
										ok = false;
										for (T f : fields_instances) {
											if (fa.equals(f, value)) {
												ok = true;
												break;
											}
										}
										if (ok)
											value = fa.autoGenerateValue(rand);
									} while (ok);
								}
								fa.setValue(instance, value);
							} else if ((fa.isAutoPrimaryKey() && ct.include_auto_pk) || !fa.isAutoPrimaryKey()) {
								obj = _fields.get(fa.getFieldName());
								fa.setValue(instance, obj);
							}
						}
					} else {
						for (final FieldAccessor fa : fields) {
							if (fa.isRandomPrimaryKey() && !ct.random_fields_to_check.contains(fa)) {
								Object value = fa.autoGenerateValue(rand);
								boolean ok = false;
								if (fa.needToCheckUniquenessOfAutoGeneratedValues()) {
									do {
										final Object val = value;

										class RunnableTmp extends Runnable2 {
											public boolean ok = false;

											@Override
											public void init(int _field_count) {
											}

											@Override
											public boolean setInstance(ResultSet _result_set) throws DatabaseException {
												boolean res = !fa.equals(val, _result_set);
												if (!ok && !res)
													ok = true;
												return res;
											}
										}
										RunnableTmp runnable = new RunnableTmp();

										getListRecordsFromSqlConnection(runnable, getSqlGeneralSelect(true),
												TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
										ok = runnable.ok;
										if (ok)
											value = fa.autoGenerateValue(rand);
									} while (ok);
								}
								fa.setValue(instance, value);
							} else if ((fa.isAutoPrimaryKey() && ct.include_auto_pk) || !fa.isAutoPrimaryKey()) {
								fa.setValue(instance, _fields.get(fa.getFieldName()));
							}
						}
					}
					class TransactionTmp implements Transaction {

						protected final ArrayList<FieldAccessor> auto_primary_keys_fields;
						protected final ArrayList<ForeignKeyFieldAccessor> foreign_keys_fields;

						public TransactionTmp(ArrayList<FieldAccessor> _auto_primary_keys_fields,
								ArrayList<ForeignKeyFieldAccessor> _foreign_keys_fields) {
							auto_primary_keys_fields = _auto_primary_keys_fields;
							foreign_keys_fields = _foreign_keys_fields;
						}

						@Override
						public boolean doesWriteData() {
							return true;
						}

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_SERIALIZABLE;
						}

						@Override
						public Object run(DatabaseWrapper _db) throws DatabaseException {
							try {
								for (ForeignKeyFieldAccessor fa : foreign_keys_fields) {
									Object val = fa.getValue(instance);
									if (val != null && !fa.getPointedTable().contains(true, (DatabaseRecord) val))
										throw new RecordNotFoundDatabaseException(
												"The record, contained as foreign key into the field "
														+ fa.getFieldName() + " into the table " + Table.this.getName()
														+ " does not exists into the table "
														+ fa.getPointedTable().getName());
								}

								StringBuffer querry = new StringBuffer("INSERT INTO " + Table.this.getName() + "(");
								boolean first = true;
								for (FieldAccessor fa : fields) {
									if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName()))
											|| !fa.isAutoPrimaryKey()) {
										for (SqlField sf : fa.getDeclaredSqlFields()) {
											if (first)
												first = false;
											else
												querry.append(", ");
											querry.append(sf.short_field);
										}
									}
								}
								querry.append(") VALUES(");
								first = true;
								for (FieldAccessor fa : fields) {
									if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName()))
											|| !fa.isAutoPrimaryKey()) {
										for (int i = 0; i < fa.getDeclaredSqlFields().length; i++) {
											if (first)
												first = false;
											else
												querry.append(", ");
											querry.append("?");
										}
									}
								}
								querry.append(")" + sql_connection.getSqlComma());

								try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
										_db.getConnectionAssociatedWithCurrentThread().getConnection(),
										querry.toString())) {
									int index = 1;
									for (FieldAccessor fa : fields) {
										if ((fa.isAutoPrimaryKey() && _fields.containsKey(fa.getFieldName()))
												|| !fa.isAutoPrimaryKey()) {
											fa.getValue(instance, puq.statement, index);
											index += fa.getDeclaredSqlFields().length;
										}
									}
									puq.statement.executeUpdate();
									/*
									 * if (auto_primary_keys_fields.size()>0 && !ct.include_auto_pk) {
									 * puq.statement.getGeneratedKeys().next(); autovalue=new
									 * Long(puq.statement.getGeneratedKeys().getLong(1)); }
									 */
								} catch (SQLIntegrityConstraintViolationException e) {
									throw new ConstraintsNotRespectedDatabaseException(
											"Constraints was not respected when inserting a field into the table "
													+ Table.this.getName()
													+ ". It is possible that the group of primary keys was not unique, or that a unique field was already present into the database.",
											e);
								} catch (Exception e) {
									throw DatabaseException.getDatabaseException(e);
								}

								if (auto_primary_keys_fields.size() > 0 && !ct.include_auto_pk) {
									try (ReadQuerry rq = new ReadQuerry(
											_db.getConnectionAssociatedWithCurrentThread().getConnection(),
											new SqlQuerry(sql_connection.getSqlQuerryToGetLastGeneratedID()))) {
										rq.result_set.next();
										Long autovalue = new Long(rq.result_set.getLong(1));
										FieldAccessor fa = auto_primary_keys_fields.get(0);
										if (fa.isAssignableTo(byte.class))
											fa.setValue(instance, new Byte((byte) autovalue.longValue()));
										else if (fa.isAssignableTo(short.class))
											fa.setValue(instance, new Short((short) autovalue.longValue()));
										else if (fa.isAssignableTo(int.class))
											fa.setValue(instance, new Integer((int) autovalue.longValue()));
										else if (fa.isAssignableTo(long.class))
											fa.setValue(instance, autovalue);
									} catch (Exception e) {
										throw DatabaseException.getDatabaseException(e);
									}
								}

								return null;
							} catch (Exception e) {
								throw DatabaseException.getDatabaseException(e);
							}
						}

					}

					sql_connection.runTransaction(new TransactionTmp(auto_primary_keys_fields, foreign_keys_fields));

					if (isLoadedInMemory() && isSynchronizedWithSqlDatabase())
						__AddRecord(instance);
					if (synchronizeIfNecessary)
						getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
								new TableEvent<>(-1, DatabaseEventType.ADD, null, instance, hostsDestinations));
					return instance;

				} catch (IllegalArgumentException e) {
					throw new DatabaseException("Impossible to add a new field on the table/class " + getName() + ".",
							e);
				} catch (InstantiationException e) {
					throw new DatabaseException("Impossible to add a new field on the table/class " + getName() + ".",
							e);
				} catch (IllegalAccessException e) {
					throw new DatabaseException("Impossible to add a new field on the table/class " + getName() + ".",
							e);
				} catch (InvocationTargetException e) {
					throw new DatabaseException("Impossible to add a new field on the table/class " + getName() + ".",
							e);
				}
			}

		});
	}

	/**
	 * Add a collection of records into the database with a collection of maps of
	 * fields corresponding to these records. The string type in the Map corresponds
	 * to the name of the field, and the Object type field corresponds the value of
	 * the field.
	 * 
	 * @param _records
	 *            the list of fields of every record to include into the database.
	 *            Must be formated as follow : {"field1", value1,"field2", value2,
	 *            etc.}
	 * @return the created records
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, or if a
	 *             field which has the unique property exists already into the
	 *             table.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if fields are lacking.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the field is a foreign key and point to a record of
	 *             another table which does not exist.
	 */
	@SafeVarargs
	public final ArrayList<T> addRecords(Object[]... _records) throws DatabaseException {
		return addRecords(transformToMapField(_records));
	}

	/**
	 * Add a collection of records into the database with a collection of maps of
	 * fields corresponding to these records. The string type in the Map corresponds
	 * to the name of the field, and the Object type field corresponds the value of
	 * the field.
	 * 
	 * @param _records
	 *            the list of fields of every record to include into the database
	 * @return the created records
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, or if a
	 *             field which has the unique property exists already into the
	 *             table.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if fields are lacking.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the field is a foreign key and point to a record of
	 *             another table which does not exist.
	 */
	@SafeVarargs
	public final ArrayList<T> addRecords(final Map<String, Object>... _records) throws DatabaseException {
		// synchronized(sql_connection)
		{
			try (Lock l = new WriteLock(this)) {
				ArrayList<T> res = new ArrayList<T>();
				for (Map<String, Object> m : _records)
					res.add(addRecord(m, false));
				return res;
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	/**
	 * Alter a record into the database. The string type in the Map corresponds to
	 * the name of the field, and the Object type field corresponds the value of the
	 * field. Internal record field must altered before calling this function. It is
	 * not possible to alter primary keys with this function. Please call instead
	 * {@link #updateRecord(DatabaseRecord, Map)} or
	 * {@link #updateRecord(DatabaseRecord, Object[])}
	 * 
	 * @param _record
	 *            the record to alter
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, if a
	 *             field which has the unique property exists alreay into the table,
	 *             or if primary keys changing induce a problem of constraints
	 *             through foreign keys into other tables, which are also primary
	 *             keys.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if one of the given fields are auto or random primary keys.
	 * @throws RecordNotFoundDatabaseException
	 *             if the given record is not included into the database, or if one
	 *             of the field is a foreign key and point to a record of another
	 *             table which does not exist.
	 */
	public final void updateRecord(final T _record) throws DatabaseException {
		updateUntypedRecord(_record, true, null);
	}

	final void updateUntypedRecord(final DatabaseRecord record, boolean synchronizeIfNecessary,
			Set<AbstractDecentralizedID> resentTo) throws DatabaseException {
		@SuppressWarnings("unchecked")
		T _record = (T) record;
		Map<String, Object> map = getMap(_record, false, false);
		updateRecord(_record, map, synchronizeIfNecessary, resentTo);
	}

	/**
	 * Alter a record into the database with a map of fields corresponding to this
	 * record. The string type in the Map corresponds to the name of the field, and
	 * the Object type field corresponds the value of the field. If primary keys are
	 * altered, every foreign key pointing to this record will be transparently
	 * altered. However, if records pointing to this altered record remain in
	 * memory, they will no be altered if the current table has not the annotation
	 * {#link oodforsqljet.annotations.LoadToMemory}. The only solution in this case
	 * is to reload the concerned records through the functions starting with
	 * "getRecord".
	 * 
	 * @param _record
	 *            the record to alter
	 * @param _fields
	 *            the list of fields to include into the new record. Must be
	 *            formated as follow : {"field1", value1,"field2", value2, etc.}
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, if a
	 *             field which has the unique property exists alreay into the table,
	 *             or if primary keys changing induce a problem of constraints
	 *             through foreign keys into other tables, which are also primary
	 *             keys.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if one of the given fields are auto or random primary keys.
	 * @throws RecordNotFoundDatabaseException
	 *             if the given record is not included into the database, or if one
	 *             of the field is a foreign key and point to a record of another
	 *             table which does not exist.
	 */
	public final void updateRecord(final T _record, Object... _fields) throws DatabaseException {
		updateRecord(_record, transformToMapField(_fields));
	}

	T copyRecord(T _record) throws InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, DatabaseException {
		T oldRecord = default_constructor_field.newInstance();
		for (FieldAccessor fa : fields) {
			fa.setValue(oldRecord, fa.getValue(_record));
		}
		return oldRecord;
	}

	/**
	 * Alter a record into the database with a map of fields corresponding to this
	 * record. The string type in the Map corresponds to the name of the field, and
	 * the Object type field corresponds the value of the field. If primary keys are
	 * altered, every foreign key pointing to this record will be transparently
	 * altered. However, if records pointing to this altered record remain in
	 * memory, they will no be altered if the current table has not the annotation
	 * {#link oodforsqljet.annotations.LoadToMemory}. The only solution in this case
	 * is to reload the concerned records through the functions starting with
	 * "getRecord".
	 * 
	 * @param _record
	 *            the record to alter
	 * @param _fields
	 *            the list of fields to include into the new record
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if the given primary keys already exists into the table, if a
	 *             field which has the unique property exists alreay into the table,
	 *             or if primary keys changing induce a problem of constraints
	 *             through foreign keys into other tables, which are also primary
	 *             keys.
	 * @throws FieldDatabaseException
	 *             if one of the given fields does not exists into the database, or
	 *             if one of the given fields are auto or random primary keys.
	 * @throws RecordNotFoundDatabaseException
	 *             if the given record is not included into the database, or if one
	 *             of the field is a foreign key and point to a record of another
	 *             table which does not exist.
	 */
	public final void updateRecord(final T _record, final Map<String, Object> _fields) throws DatabaseException {
		updateRecord(_record, _fields, true, null);
	}

	final void updateRecord(final T _record, final Map<String, Object> _fields, final boolean synchronizeIfNecessary,
			final Set<AbstractDecentralizedID> resentTo) throws DatabaseException {
		if (_record == null)
			throw new NullPointerException("The parameter _record is a null pointer !");
		if (_fields == null)
			throw new NullPointerException("The parameter _fields is a null pointer !");
		try (Lock lock = new WriteLock(Table.this)) {

			sql_connection.runTransaction(new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_SERIALIZABLE;
				}

				@Override
				public boolean doesWriteData() {
					return true;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						T oldRecord = copyRecord(_record);
						boolean pkChanged = false;
						for (String s : _fields.keySet()) {
							boolean found = false;
							for (FieldAccessor fa : fields) {
								if (fa.getFieldName().equals(s)) {
									if (fa.isPrimaryKey() && fa.equals(_record, _fields.get(s)))
										pkChanged = true;
									if (fa.isForeignKey()) {
										ForeignKeyFieldAccessor fkfa = (ForeignKeyFieldAccessor) fa;
										DatabaseRecord dr = (DatabaseRecord) _fields.get(fa.getFieldName());
										if (dr != null && !fkfa.getPointedTable().contains(false, dr)) {
											throw new RecordNotFoundDatabaseException("The field " + fa.getFieldName()
													+ " given in parameters point to a DatabaseRecord which is not contained into the database.");
										}
									}
									found = true;
									break;
								}
							}
							if (!found)
								throw new FieldDatabaseException(
										"The given field " + s + " is not contained into the table " + getName());
						}

						class CheckTmp extends Runnable2 {
							private final HashMap<String, Object> keys;
							public boolean check_necessary = false;
							public final ArrayList<Boolean> check_random = new ArrayList<Boolean>();
							private final ArrayList<FieldAccessor> random_primary_keys_fields;

							public CheckTmp(ArrayList<FieldAccessor> _auto_random_primary_keys_fields)
									throws DatabaseException {
								random_primary_keys_fields = _auto_random_primary_keys_fields;
								keys = new HashMap<String, Object>();
								for (int i = 0; i < random_primary_keys_fields.size(); i++) {
									FieldAccessor fa = random_primary_keys_fields.get(i);
									if (_fields.containsKey(fa.getFieldName())) {
										Object field = _fields.get(fa.getFieldName());
										if (!fa.equals(_record, field)) {
											keys.put(fa.getFieldName(), field);
											check_necessary = true;
											check_random.add(new Boolean(true));
										} else
											check_random.add(new Boolean(false));
									} else
										check_random.add(new Boolean(false));
								}

							}

							@Override
							public void init(int _field_count) {
							}

							@Override
							public boolean setInstance(ResultSet _result_set) throws DatabaseException {
								for (int i = 0; i < random_primary_keys_fields.size(); i++) {
									if (check_random.get(i).booleanValue()) {
										FieldAccessor fa = random_primary_keys_fields.get(i);
										if (fa.equals(keys.get(fa.getFieldName()), _result_set))
											throw new ConstraintsNotRespectedDatabaseException(
													"the given record have the same auto/random primary key field "
															+ fa.getFieldName()
															+ " of one of the records stored into the database. No record have been added.");
									}
								}
								return true;
							}
						}
						CheckTmp ct = new CheckTmp(auto_random_primary_keys_fields);
						if (ct.check_necessary)
							getListRecordsFromSqlConnection(ct, getSqlGeneralSelect(true),
									TransactionIsolation.TRANSACTION_REPEATABLE_READ, -1, -1);

						class TransactionTmp implements Transaction {
							protected final ArrayList<FieldAccessor> fields_accessor;

							public TransactionTmp(ArrayList<FieldAccessor> _fields_accessor) {
								fields_accessor = _fields_accessor;
							}

							@Override
							public boolean doesWriteData() {
								return true;
							}

							@Override
							public TransactionIsolation getTransactionIsolation() {
								return TransactionIsolation.TRANSACTION_READ_COMMITTED;
							}

							@Override
							public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
								try {
									StringBuffer querry = new StringBuffer("UPDATE " + Table.this.getName() + " SET ");
									T instance = getNewRecordInstance();
									boolean first = true;
									for (FieldAccessor fa : fields_accessor) {
										if (_fields.containsKey(fa.getFieldName())) {
											fa.setValue(instance, _fields.get(fa.getFieldName()));

											for (SqlField sf : fa.getDeclaredSqlFields()) {
												if (first)
													first = false;
												else
													querry.append(", ");
												querry.append(sf.field + " = ?");
											}
										}
									}
									querry.append(" WHERE ");
									first = true;
									for (FieldAccessor fa : primary_keys_fields) {
										for (SqlField sf : fa.getDeclaredSqlFields()) {
											if (first)
												first = false;
											else
												querry.append(" AND ");
											querry.append(sf.field + " = ?");
										}
									}

									try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
											_sql_connection.getConnectionAssociatedWithCurrentThread().getConnection(),
											querry.toString())) {
										int index = 1;
										for (FieldAccessor fa : fields_accessor) {
											if (_fields.containsKey(fa.getFieldName())) {
												fa.getValue(instance, puq.statement, index);
												index += fa.getDeclaredSqlFields().length;
											}
										}
										for (FieldAccessor fa : primary_keys_fields) {
											fa.getValue(_record, puq.statement, index);
											index += fa.getDeclaredSqlFields().length;
										}
										int nb = puq.statement.executeUpdate();
										if (nb > 1)
											throw new DatabaseIntegrityException(
													"More than one record have been found with the given primary keys. No record have been altered.");
										if (nb == 0)
											throw new RecordNotFoundDatabaseException("The given record was not found");
										for (FieldAccessor fa : fields_accessor) {
											if (_fields.containsKey(fa.getFieldName())) {
												fa.setValue(_record, _fields.get(fa.getFieldName()));
											}
										}

									} catch (SQLIntegrityConstraintViolationException e) {
										throw new ConstraintsNotRespectedDatabaseException(
												"Constraints was not respected. It possible that the given primary keys or the given unique keys does not respect constraints of unicity.",
												e);
									}
									return null;
								} catch (Exception e) {
									throw DatabaseException.getDatabaseException(e);
								}

							}

						}

						sql_connection.runTransaction(new TransactionTmp(fields));
						if (synchronizeIfNecessary) {
							if (pkChanged) {
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.REMOVE, oldRecord, null, resentTo));
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.ADD, null, _record, resentTo));
							} else
								getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
										new TableEvent<>(-1, DatabaseEventType.UPDATE, oldRecord, _record, resentTo));
						}

					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
					return null;
				}

			});
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	/**
	 * Alter records into the database through a given inherited
	 * {@link com.distrimind.ood.database.AlterRecordFilter} class.
	 * 
	 * The function parse all records present into this table. For each of them, it
	 * calls the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#update(Map)} is called
	 * into the inherited function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)},
	 * then all fields present in the given map will be updated into the record,
	 * after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. If the given map is a null reference, the correspondent record
	 * will not be altered. Note that modification of primary keys and unique keys
	 * are not permitted with this function. To do that, please use the function
	 * {@link #updateRecord(DatabaseRecord, Map)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#remove()} is called,
	 * then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call, only if no record point to this record.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#removeWithCascade()} is
	 * called, then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. Records pointing to this record will also be deleted.
	 * 
	 * @param _filter
	 *            the filter enabling to alter the desired records.
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a primary key or a unique key is altered, or if one of the
	 *             given fields is a null pointer whereas this field must be not
	 *             null.
	 * @throws FieldDatabaseException
	 *             if one of the given fields to change does not exists into the
	 *             database, or if one of the given fields to change is a primary
	 *             key or a unique field.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the given field to alter is a foreign key which points
	 *             to a non-existing record.
	 * @see AlterRecordFilter
	 */
	public final void updateRecords(final AlterRecordFilter<T> _filter) throws DatabaseException {
		updateRecords(_filter, null, new HashMap<>());
	}

	/**
	 * Alter records into the database through a given inherited
	 * {@link com.distrimind.ood.database.AlterRecordFilter} class.
	 * 
	 * The function parse all records present into this table, that verify the given
	 * WHERE condition. For each of them, it calls the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#update(Map)} is called
	 * into the inherited function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)},
	 * then all fields present in the given map will be updated into the record,
	 * after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. If the given map is a null reference, the correspondent record
	 * will not be altered. Note that modification of primary keys and unique keys
	 * are not permitted with this function. To do that, please use the function
	 * {@link #updateRecord(DatabaseRecord, Map)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#remove()} is called,
	 * then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call, only if no record point to this record.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#removeWithCascade()} is
	 * called, then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. Records pointing to this record will also be deleted.
	 * 
	 * @param _filter
	 *            the filter enabling to alter the desired records.
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a primary key or a unique key is altered, or if one of the
	 *             given fields is a null pointer whereas this field must be not
	 *             null.
	 * @throws FieldDatabaseException
	 *             if one of the given fields to change does not exists into the
	 *             database, or if one of the given fields to change is a primary
	 *             key or a unique field.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the given field to alter is a foreign key which points
	 *             to a non-existing record.
	 * @see AlterRecordFilter
	 */
	public final void updateRecords(final AlterRecordFilter<T> _filter, String whereCommand, Object... parameters)
			throws DatabaseException {
		updateRecords(_filter, whereCommand,
				whereCommand == null ? new HashMap<String, Object>() : convertToMap(parameters));
	}

	private Map<String, Object> convertToMap(Object... parameters) {
		Map<String, Object> res = new HashMap<>();
		if (parameters == null)
			return res;
		if (parameters.length % 2 != 0)
			throw new IllegalArgumentException("parameters must be a pair of name+value !");
		for (int i = 0; i < parameters.length; i += 2) {
			if (!(parameters[i] instanceof String))
				throw new IllegalArgumentException("The first pair parameter must be string !");
			res.put((String) parameters[i], parameters[i + 1]);
		}
		return res;
	}

	/**
	 * Alter records into the database through a given inherited
	 * {@link com.distrimind.ood.database.AlterRecordFilter} class.
	 * 
	 * The function parse all records present into this table, that verify the given
	 * WHERE condition. For each of them, it calls the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#update(Map)} is called
	 * into the inherited function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)},
	 * then all fields present in the given map will be updated into the record,
	 * after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. If the given map is a null reference, the correspondent record
	 * will not be altered. Note that modification of primary keys and unique keys
	 * are not permitted with this function. To do that, please use the function
	 * {@link #updateRecord(DatabaseRecord, Map)}.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#remove()} is called,
	 * then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call, only if no record point to this record.
	 * 
	 * If the function
	 * {@link com.distrimind.ood.database.AlterRecordFilter#removeWithCascade()} is
	 * called, then the current record will be deleted after the end of the
	 * {@link com.distrimind.ood.database.AlterRecordFilter#nextRecord(DatabaseRecord)}
	 * function call. Records pointing to this record will also be deleted.
	 * 
	 * @param _filter
	 *            the filter enabling to alter the desired records.
	 * @param whereCondition
	 *            the SQL WHERE condition that filter the results
	 * @param parameters
	 *            the used parameters with the WHERE condition
	 * @throws DatabaseException
	 *             if a problem occurs during the insertion into the Sql database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws ConstraintsNotRespectedDatabaseException
	 *             if a primary key or a unique key is altered, or if one of the
	 *             given fields is a null pointer whereas this field must be not
	 *             null.
	 * @throws FieldDatabaseException
	 *             if one of the given fields to change does not exists into the
	 *             database, or if one of the given fields to change is a primary
	 *             key or a unique field.
	 * @throws RecordNotFoundDatabaseException
	 *             if one of the given field to alter is a foreign key which points
	 *             to a non-existing record.
	 * @see AlterRecordFilter
	 */
	public final void updateRecords(final AlterRecordFilter<T> _filter, String whereCommand,
			final Map<String, Object> parameters) throws DatabaseException {
		if (_filter == null)
			throw new NullPointerException("The parameter _filter is a null pointer !");

		try (Lock lock = new WriteLock(Table.this)) {
			final RuleInstance rule = (whereCommand != null && !whereCommand.trim().equals(""))
					? Interpreter.getRuleInstance(whereCommand)
					: null;
			sql_connection.runTransaction(new Transaction() {

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_SERIALIZABLE;
				}

				@Override
				public boolean doesWriteData() {
					return true;
				}

				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						if (isLoadedInMemory()) {
							final ArrayList<T> records_to_delete = new ArrayList<>();
							final ArrayList<T> records_to_delete_with_cascade = new ArrayList<>();
							for (final T r : getRecords(-1, -1, false)) {
								_filter.reset();
								final T oldRecord = copyRecord(r);
								if ((rule != null && rule.isConcernedBy(Table.this, parameters, r)) || rule == null) {
									_filter.nextRecord(r);

									if (_filter.hasToBeRemoved()) {
										boolean canberemoved = true;
										for (NeighboringTable nt : list_tables_pointing_to_this_table) {
											if (nt.getPoitingTable().hasRecordsWithOneOfFields(nt.getHashMapFields(r),
													false)) {
												canberemoved = false;
												break;
											}
										}
										if (canberemoved)
											records_to_delete.add(r);
									} else if (_filter.hasToBeRemovedWithCascade()) {
										records_to_delete_with_cascade.add(r);
									} else {
										Map<String, Object> m = _filter.getModifications();
										if (m == null && _filter.isModificatiedFromRecordInstance())
											m = getMap(r, false, false);
										final Map<String, Object> map = m;
										boolean pkChangedTmp = false;
										if (map != null && map.size() > 0) {
											for (String s : map.keySet()) {
												FieldAccessor founded_field = null;
												for (FieldAccessor fa : fields) {
													if (fa.getFieldName().equals(s)) {
														if (fa.isPrimaryKey() && fa.equals(r, map.get(s)))
															pkChangedTmp = true;

														founded_field = fa;
														break;
													}
												}
												if (founded_field == null)
													throw new FieldDatabaseException(
															"The given field " + s + " does not exists into the record "
																	+ class_record.getName() + ". ");
												if (founded_field.isPrimaryKey())
													throw new FieldDatabaseException(
															"Attempting to alter the primary key field "
																	+ founded_field.getFieldName() + " into the table "
																	+ getName()
																	+ ". This operation is not permitted into this function."
																	+ ". ");
												if (founded_field.isUnique())
													throw new FieldDatabaseException(
															"Attempting to alter the unique field "
																	+ founded_field.getFieldName() + " into the table "
																	+ getName()
																	+ ". This operation is not permitted into this function."
																	+ ". ");
												if (founded_field.isForeignKey()) {
													Object val = founded_field.getValue(r);
													if (!((ForeignKeyFieldAccessor) founded_field).getPointedTable()
															.contains(true, (DatabaseRecord) val))
														throw new RecordNotFoundDatabaseException(
																"The record, contained as foreign key into the given field "
																		+ founded_field.getFieldName()
																		+ " into the table " + Table.this.getName()
																		+ " does not exists into the table "
																		+ ((ForeignKeyFieldAccessor) founded_field)
																				.getPointedTable().getName()
																		+ ". ");
												}
											}
											final boolean pkChanged = pkChangedTmp;
											sql_connection.runTransaction(new Transaction() {

												@Override
												public TransactionIsolation getTransactionIsolation() {
													return TransactionIsolation.TRANSACTION_READ_COMMITTED;
												}

												@Override
												public Object run(DatabaseWrapper _sql_connection)
														throws DatabaseException {
													try {
														StringBuffer querry = new StringBuffer(
																"UPDATE " + Table.this.getName() + " SET ");
														T instance = getNewRecordInstance();
														boolean first = true;
														for (FieldAccessor fa : fields) {
															if (map.containsKey(fa.getFieldName())) {
																fa.setValue(instance, map.get(fa.getFieldName()));

																for (SqlField sf : fa.getDeclaredSqlFields()) {
																	if (first)
																		first = false;
																	else
																		querry.append(", ");
																	querry.append(sf.short_field + " = ?");
																}
															}
														}
														querry.append(" WHERE ");
														first = true;
														for (FieldAccessor fa : primary_keys_fields) {
															for (SqlField sf : fa.getDeclaredSqlFields()) {
																if (first)
																	first = false;
																else
																	querry.append(" AND ");
																querry.append(sf.field + " = ?");
															}
														}

														try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(
																_sql_connection
																		.getConnectionAssociatedWithCurrentThread()
																		.getConnection(),
																querry.toString())) {
															int index = 1;
															for (FieldAccessor fa : fields) {
																if (map.containsKey(fa.getFieldName())) {
																	fa.getValue(instance, puq.statement, index);
																	index += fa.getDeclaredSqlFields().length;
																}
															}
															for (FieldAccessor fa : primary_keys_fields) {
																fa.getValue(r, puq.statement, index);
																index += fa.getDeclaredSqlFields().length;
															}
															int nb = puq.statement.executeUpdate();
															if (nb != 1)
																throw new DatabaseIntegrityException(
																		"More than one record have been found with the given primary keys. No record have been altered.");

															for (FieldAccessor fa : fields) {
																if (map.containsKey(fa.getFieldName())) {
																	fa.setValue(r, map.get(fa.getFieldName()));
																}
															}

														} catch (SQLIntegrityConstraintViolationException e) {
															throw new ConstraintsNotRespectedDatabaseException(
																	"Constraints was not respected. It possible that the given primary keys or the given unique keys does not respect constraints of unicity.",
																	e);
														}
														if (pkChanged) {
															getDatabaseWrapper()
																	.getConnectionAssociatedWithCurrentThread()
																	.addEvent(Table.this,
																			new TableEvent<>(-1,
																					DatabaseEventType.REMOVE, oldRecord,
																					null, null));
															getDatabaseWrapper()
																	.getConnectionAssociatedWithCurrentThread()
																	.addEvent(Table.this, new TableEvent<>(-1,
																			DatabaseEventType.ADD, null, r, null));
														} else
															getDatabaseWrapper()
																	.getConnectionAssociatedWithCurrentThread()
																	.addEvent(Table.this,
																			new TableEvent<>(-1,
																					DatabaseEventType.UPDATE, oldRecord,
																					r, null));
														return null;
													} catch (Exception e) {
														throw DatabaseException.getDatabaseException(e);
													}
												}

												@Override
												public boolean doesWriteData() {
													return true;
												}

											});
										}
									}
								}
							}

							if (records_to_delete.size() > 0) {
								Transaction transaction = new Transaction() {

									@Override
									public TransactionIsolation getTransactionIsolation() {
										return TransactionIsolation.TRANSACTION_READ_COMMITTED;
									}

									@SuppressWarnings("synthetic-access")
									@Override
									public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
										StringBuffer sb = new StringBuffer("DELETE FROM " + Table.this.getName()
												+ " WHERE " + getSqlPrimaryKeyCondition(records_to_delete.size()));

										int nb = 0;
										try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(_sql_connection
												.getConnectionAssociatedWithCurrentThread().getConnection(),
												sb.toString())) {
											int index = 1;
											for (T r : records_to_delete) {
												for (FieldAccessor fa : primary_keys_fields) {
													fa.getValue(r, puq.statement, index);
													index += fa.getDeclaredSqlFields().length;
												}
											}
											nb = puq.statement.executeUpdate();
										} catch (Exception e) {
											throw DatabaseException.getDatabaseException(e);
										}

										if (nb != records_to_delete.size())
											throw new DatabaseException("Unexpected exception.");
										return null;
									}

									@Override
									public boolean doesWriteData() {
										return true;
									}

								};

								sql_connection.runTransaction(transaction);

								__removeRecords(records_to_delete);

								for (T r : records_to_delete)
									getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
											new TableEvent<>(-1, DatabaseEventType.REMOVE, r, null, null));
							}
							if (records_to_delete_with_cascade.size() > 0) {
								Transaction transaction = new Transaction() {

									@Override
									public TransactionIsolation getTransactionIsolation() {
										return TransactionIsolation.TRANSACTION_READ_COMMITTED;
									}

									@SuppressWarnings("synthetic-access")
									@Override
									public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
										StringBuffer sb = new StringBuffer("DELETE FROM " + Table.this.getName()
												+ " WHERE "
												+ getSqlPrimaryKeyCondition(records_to_delete_with_cascade.size()));

										int nb = 0;
										try (PreparedUpdateQuerry puq = new PreparedUpdateQuerry(_sql_connection
												.getConnectionAssociatedWithCurrentThread().getConnection(),
												sb.toString())) {
											int index = 1;
											for (T r : records_to_delete_with_cascade) {
												for (FieldAccessor fa : primary_keys_fields) {
													fa.getValue(r, puq.statement, index);
													index += fa.getDeclaredSqlFields().length;
												}
											}
											nb = puq.statement.executeUpdate();
										} catch (Exception e) {
											throw DatabaseException.getDatabaseException(e);
										}

										if (nb != records_to_delete_with_cascade.size()) {
											throw new DatabaseException("Unexpected exception.");
										}
										return null;
									}

									@Override
									public boolean doesWriteData() {
										return true;
									}

								};

								sql_connection.runTransaction(transaction);

								__removeRecords(records_to_delete_with_cascade);
								updateMemoryForRemovingRecordsWithCascade(records_to_delete_with_cascade);
								for (T r : records_to_delete_with_cascade)
									getDatabaseWrapper().getConnectionAssociatedWithCurrentThread().addEvent(Table.this,
											new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, r, null, null));

							}

						} else {
							class RunnableTmp extends Runnable {
								protected final ArrayList<FieldAccessor> fields_accessor;
								private final RuleInstance rule;

								public RunnableTmp(ArrayList<FieldAccessor> _fields_accessor, RuleInstance rule) {
									fields_accessor = _fields_accessor;
									this.rule = rule;
								}

								@Override
								public void init(int _field_count) {

								}

								@SuppressWarnings("synthetic-access")
								@Override
								public boolean setInstance(T _instance, ResultSet _result_set)
										throws DatabaseException {
									try {
										if ((rule == null || rule.isConcernedBy(Table.this, parameters, _instance))) {
											_filter.reset();
											final T oldRecord = copyRecord(_instance);
											_filter.nextRecord(_instance);
											if (_filter.hasToBeRemoved()) {
												boolean canberemoved = true;
												if (list_tables_pointing_to_this_table.size() > 0) {
													for (int i = 0; i < list_tables_pointing_to_this_table
															.size(); i++) {

														NeighboringTable nt = list_tables_pointing_to_this_table.get(i);

														if (nt.getPoitingTable()
																.hasRecordsWithOneOfSqlForeignKeyWithCascade(
																		nt.getHashMapsSqlFields(
																				getSqlPrimaryKeys(_instance)))) {
															canberemoved = false;
															break;
														}
													}
												}
												if (canberemoved) {
													_result_set.deleteRow();
													getDatabaseWrapper().getConnectionAssociatedWithCurrentThread()
															.addEvent(Table.this, new TableEvent<>(-1,
																	DatabaseEventType.REMOVE, oldRecord, null, null));
												}
											} else if (_filter.hasToBeRemovedWithCascade()) {
												_result_set.deleteRow();
												updateMemoryForRemovingRecordWithCascade(_instance);
												getDatabaseWrapper().getConnectionAssociatedWithCurrentThread()
														.addEvent(Table.this,
																new TableEvent<>(-1,
																		DatabaseEventType.REMOVE_WITH_CASCADE,
																		oldRecord, null, null));
											} else {
												Map<String, Object> m = _filter.getModifications();
												if (m == null && _filter.isModificatiedFromRecordInstance())
													m = getMap(_instance, false, false);
												final Map<String, Object> map = m;

												if (map != null && map.size() > 0) {
													for (String s : map.keySet()) {
														FieldAccessor founded_field = null;
														for (FieldAccessor fa : fields_accessor) {
															if (fa.getFieldName().equals(s)) {
																founded_field = fa;
																break;
															}
														}
														if (founded_field == null)
															throw new FieldDatabaseException("The given field " + s
																	+ " does not exists into the record "
																	+ class_record.getName());
														if (founded_field.isPrimaryKey())
															throw new FieldDatabaseException(
																	"Attempting to alter the primary key field "
																			+ founded_field.getFieldName()
																			+ " into the table " + getName()
																			+ ". This operation is not permitted into this function.");
														if (founded_field.isUnique())
															throw new FieldDatabaseException(
																	"Attempting to alter the unique field "
																			+ founded_field.getFieldName()
																			+ " into the table " + getName()
																			+ ". This operation is not permitted into this function.");
														if (founded_field.isForeignKey()) {
															Object val = founded_field.getValue(_instance);
															if (!((ForeignKeyFieldAccessor) founded_field)
																	.getPointedTable()
																	.contains(true, (DatabaseRecord) val))
																throw new RecordNotFoundDatabaseException(
																		"The record, contained as foreign key into the given field "
																				+ founded_field.getFieldName()
																				+ " into the table "
																				+ Table.this.getName()
																				+ " does not exists into the table "
																				+ ((ForeignKeyFieldAccessor) founded_field)
																						.getPointedTable().getName());
														}
													}
													for (FieldAccessor fa : fields) {
														if (map.containsKey(fa.getFieldName())) {
															fa.updateValue(_instance, map.get(fa.getFieldName()),
																	_result_set);
														}
													}
													_result_set.updateRow();
													getDatabaseWrapper().getConnectionAssociatedWithCurrentThread()
															.addEvent(Table.this,
																	new TableEvent<>(-1, DatabaseEventType.UPDATE,
																			oldRecord, _instance, null));
												}
											}
										}
										return true;
									} catch (Exception e) {
										throw DatabaseException.getDatabaseException(e);
									}
								}
							}
							HashMap<Integer, Object> sqlParameters = new HashMap<>();
							String sqlQuery = null;
							if (rule != null && rule.isIndependantFromOtherTables(Table.this)) {
								sqlQuery = rule.translateToSqlQuery(Table.this, parameters, sqlParameters,
										new HashSet<TableJunction>()).toString();
							}

							RunnableTmp runnable = new RunnableTmp(fields, sqlQuery == null ? rule : null);
							getListRecordsFromSqlConnection(runnable,
									sqlQuery == null ? getSqlGeneralSelect(false)
											: getSqlGeneralSelect(false, sqlQuery, sqlParameters),
									TransactionIsolation.TRANSACTION_SERIALIZABLE, -1, -1, true);

						}
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
					return null;
				}

			});
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	/*
	 * protected final BigInteger getRandomPositiveBigIntegerValue(int bits) {
	 * return new BigInteger(bits, rand); }
	 */

	/**
	 * Return the record stored into the database, which corresponds to the given
	 * primary keys. The string type in the Map corresponds to the name of the
	 * field, and the Object type field corresponds the value of the field. Just
	 * include the primary keys into the fields.
	 * 
	 * @param keys
	 *            the primary keys values. Must be formated as follow : {"field1",
	 *            value1,"field2", value2, etc.}
	 * @return the corresponding record. Return null if no record have been founded.
	 * @throws DatabaseException
	 *             if a Sql problem have occured.
	 * @throws FieldDatabaseException
	 *             if all primary keys have not been given, or if fields which are
	 *             not primary keys were given.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final T getRecord(Object... keys) throws DatabaseException {
		return getRecord(transformToMapField(keys));
	}

	/**
	 * Return the record stored into the database, which corresponds to the given
	 * primary keys. The string type in the Map corresponds to the name of the
	 * field, and the Object type field corresponds the value of the field. Just
	 * include the primary keys into the fields.
	 * 
	 * @param keys
	 *            the primary keys values
	 * @return the corresponding record. Return null if no record have been founded.
	 * @throws DatabaseException
	 *             if a Sql problem have occured.
	 * @throws FieldDatabaseException
	 *             if all primary keys have not been given, or if fields which are
	 *             not primary keys were given.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final T getRecord(final Map<String, Object> keys) throws DatabaseException {
		// synchronized(sql_connection)
		{
			try (Lock lock = new ReadLock(this)) {
				return getRecord(keys, false);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}

	private final T getRecord(final Map<String, Object> keys, boolean is_already_in_transaction)
			throws DatabaseException {
		if (!is_already_in_transaction) {
			if (keys == null)
				throw new NullPointerException("The parameter keys is a null pointer !");

			if (keys.size() != primary_keys_fields.size())
				throw new FieldDatabaseException("The number of given primary keys (" + keys.size()
						+ ") is not equal to the expected number of primary keys (" + primary_keys_fields.size()
						+ ").");

			for (FieldAccessor f : primary_keys_fields) {
				Object obj = keys.get(f.getFieldName());
				if (obj == null)
					throw new FieldDatabaseException(
							"The key " + f.getFieldName() + " is not present into the given keys.");
			}
		}
		try {
			if (isLoadedInMemory()) {
				ArrayList<T> field_instances = getRecords(-1, -1, false);
				for (T field_instance : field_instances) {
					boolean ok = true;
					for (FieldAccessor f : primary_keys_fields) {
						Object obj = keys.get(f.getFieldName());
						if (!f.equals(field_instance, obj)) {
							ok = false;
							break;
						}
					}
					if (ok)
						return field_instance;
				}
				return null;
			} else {
				// synchronized(sql_connection)
				{
					class RunnableTmp extends Runnable {
						public T instance = null;
						protected ArrayList<FieldAccessor> primary_keys_fields;

						public RunnableTmp(ArrayList<FieldAccessor> _primary_keys_fields) {
							primary_keys_fields = _primary_keys_fields;
						}

						@Override
						public void init(int _field_count) {
						}

						@Override
						public boolean setInstance(T _instance, ResultSet _cursor) throws DatabaseException {
							boolean ok = true;
							for (FieldAccessor f : primary_keys_fields) {
								Object obj = keys.get(f.getFieldName());
								if (!f.equals(_instance, obj)) {
									ok = false;
									break;
								}
							}
							if (ok) {
								instance = _instance;
								return false;
							}
							return true;
						}
					}
					RunnableTmp runnable = new RunnableTmp(primary_keys_fields);
					getListRecordsFromSqlConnection(runnable,
							new SqlGeneralSelectQuerryWithFieldMatch(true, keys, "AND", true, null),
							TransactionIsolation.TRANSACTION_READ_COMMITTED, -1, -1);
					return runnable.instance;
				}
			}
		} catch (IllegalArgumentException e) {
			throw new DatabaseException("Impossible to access to the database fields.", e);
		}
	}

	final void __AddRecord(T _record) {
		@SuppressWarnings("unchecked")
		ArrayList<T> res = (ArrayList<T>) records_instances.get().clone();
		res.add(_record);
		records_instances.set(res);
	}
	/*
	 * private final void __AddRecords(Collection<T> _records) {
	 * 
	 * @SuppressWarnings("unchecked") ArrayList<T>
	 * res=(ArrayList<T>)records_instances.get().clone(); res.addAll(_records);
	 * records_instances.set(res); }
	 */

	private final void __removeRecord(T _record) throws DatabaseException {
		@SuppressWarnings("unchecked")
		ArrayList<T> res = (ArrayList<T>) records_instances.get().clone();
		boolean removed = false;
		for (Iterator<T> it = res.iterator(); it.hasNext();) {
			T f2 = it.next();
			if (_record == f2/* equals(_record, f2) */) {
				it.remove();
				removed = true;
				break;
			}
		}
		if (!removed)
			throw new DatabaseIntegrityException("Unexpected exception");
		records_instances.set(res);
	}

	final void __removeRecords(Collection<T> _records) throws DatabaseException {
		@SuppressWarnings("unchecked")
		ArrayList<T> res = (ArrayList<T>) records_instances.get().clone();
		int number = _records.size();
		for (T f : _records) {
			for (Iterator<T> it = res.iterator(); it.hasNext();) {
				T f2 = it.next();
				if (f == f2/* equals(f, f2) */) {
					it.remove();
					--number;
					break;
				}
			}
		}
		if (number != 0)
			throw new DatabaseIntegrityException("Unexpected exception");
		records_instances.set(res);
	}

	/**
	 * Returns the Sql database which is associated to this table.
	 * 
	 * @return the associated Sql database
	 */
	public final DatabaseWrapper getDatabaseWrapper() {
		return sql_connection;
	}

	static final class DefaultConstructorAccessPrivilegedAction<TC>
			implements PrivilegedExceptionAction<Constructor<TC>> {
		private final Class<TC> m_cls;

		public DefaultConstructorAccessPrivilegedAction(Class<TC> _cls) {
			m_cls = _cls;
		}

		public Constructor<TC> run() throws Exception {
			Constructor<TC> c = m_cls.getDeclaredConstructor();
			c.setAccessible(true);
			return c;
		}
	}

	// Lock current_lock=null;
	final HashMap<Thread, Lock> current_locks = new HashMap<>();

	private static abstract class Lock implements AutoCloseable {
		protected Table<?> current_table;
		protected Lock previous_lock;

		protected Lock(/* Table<?> _current_table */) {
			/*
			 * current_table=_current_table; previous_lock=current_table.current_lock;
			 */
		}

		protected void initialize(Table<?> _current_table) {
			current_table = _current_table;
			// previous_lock=current_table.current_lock;
			previous_lock = current_table.current_locks.get(Thread.currentThread());
		}

		protected abstract boolean isValid();

		protected abstract void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException;

		protected static boolean indirectlyPointTo(Table<?> _table, Table<?> _pointed_table) {
			return indirectlyPointTo(_table, _pointed_table, new ArrayList<Table<?>>());
		}

		private static boolean indirectlyPointTo(Table<?> _table, Table<?> _pointed_table,
				ArrayList<Table<?>> _tablesAlreadyParsed) {
			_tablesAlreadyParsed.add(_table);
			for (ForeignKeyFieldAccessor fa : _table.foreign_keys_fields) {
				if (fa.getPointedTable() == _pointed_table)
					return true;
			}
			for (ForeignKeyFieldAccessor fa : _table.foreign_keys_fields) {
				if (_tablesAlreadyParsed.contains(fa.getPointedTable()))
					continue;

				return indirectlyPointTo(fa.getPointedTable(), _pointed_table, _tablesAlreadyParsed);
			}
			return false;
		}

		protected void cancel(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException {
			ArrayList<Table<?>> list = new ArrayList<Table<?>>(20);
			_comes_from_tables.remove(this.current_table);
			list.add(this.current_table);
			for (Table<?> t : _comes_from_tables) {
				Lock l = t.current_locks.get(Thread.currentThread());
				if (l != null)
					l.close(list);
			}
			if (previous_lock == null)
				current_table.current_locks.remove(Thread.currentThread());
			else
				current_table.current_locks.put(Thread.currentThread(), previous_lock);

		}
	}

	static class WriteLock extends Lock {
		public WriteLock(Table<?> _current_table) throws DatabaseException {
			this(_current_table, new ArrayList<Table<?>>(20), _current_table);
		}

		protected WriteLock(Table<?> _current_table, ArrayList<Table<?>> _comes_from_tables,
				Table<?> _from_comes_original_table) throws DatabaseException {
			super();
			synchronized (_current_table.sql_connection.locker) {

				try {
					_current_table.lockIfNecessary(true);
					initialize(_current_table);
					_comes_from_tables.add(current_table);

					if (!isValid())
						throw new ConcurentTransactionDatabaseException(
								"Attempting to write, through several nested queries, on the table "
										+ current_table.getName() + ".");
					for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table) {
						Table<?> t = nt.getPoitingTable();
						if (!_comes_from_tables.contains(t)) {
							new WriteLock(t, _comes_from_tables, _from_comes_original_table);
						}
					}
					for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields) {
						Table<?> t = fa.getPointedTable();
						if (_comes_from_tables.size() == 1 || (!_comes_from_tables.contains(t)
								&& !Lock.indirectlyPointTo(t, _from_comes_original_table))) {
							new ReadLock(t, _comes_from_tables);
						}
					}

					current_table.current_locks.put(Thread.currentThread(), this);

				} catch (DatabaseException e) {
					try {
						this.cancel(_comes_from_tables);
					} catch (DatabaseException e2) {
						e2.printStackTrace();
						throw new IllegalAccessError("");
					}
					current_table.unlockIfNecessary(true);
					throw e;
				}
			}
		}

		@Override
		protected boolean isValid() {
			if (current_table.current_locks.size() == 0)
				return true;
			Lock l = current_table.current_locks.get(Thread.currentThread());
			return l == null;
		}

		@Override
		public void close() throws Exception {
			close(new ArrayList<Table<?>>(20));
		}

		@Override
		protected void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException {
			if (current_table==null)
				return;
			synchronized (current_table.sql_connection.locker) {
				try {
					// current_table.current_lock=null;
					if (previous_lock == null)
						current_table.current_locks.remove(Thread.currentThread());
					else
						current_table.current_locks.put(Thread.currentThread(), previous_lock);
					// current_table.current_lock=previous_lock;
					_comes_from_tables.add(current_table);
					for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table) {
						Table<?> t = nt.getPoitingTable();
						if (!_comes_from_tables.contains(t)) {
							t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
						}
					}
					for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields) {
						Table<?> t = fa.getPointedTable();
						if (!_comes_from_tables.contains(t))
							t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
					}
				} finally {
					current_table.unlockIfNecessary(true);
				}
			}
		}
	}

	private static class ReadLock extends Lock {
		public ReadLock(Table<?> _current_table) throws DatabaseException {
			this(_current_table, new ArrayList<Table<?>>(20));
		}

		protected ReadLock(Table<?> _current_table, ArrayList<Table<?>> _comes_from_tables) throws DatabaseException {
			super();
			synchronized (_current_table.sql_connection.locker) {

				try {
					_current_table.lockIfNecessary(false);

					initialize(_current_table);
					if (!isValid())
						throw new ConcurentTransactionDatabaseException(
								"Attempting to read and write, through several nested queries, on the table "
										+ current_table.getName() + ".");
					_comes_from_tables.add(current_table);
					/*
					 * for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
					 * { Table<?> t=nt.getPointedTable(); if (!_comes_from_tables.contains(t)) new
					 * ReadLock(t, _comes_from_tables); }
					 */
					for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields) {
						Table<?> t = fa.getPointedTable();
						if (!_comes_from_tables.contains(t))
							new ReadLock(t, _comes_from_tables);
					}

					current_table.current_locks.put(Thread.currentThread(), this);

				} catch (DatabaseException e) {
					try {
						this.cancel(_comes_from_tables);
					} catch (DatabaseException e2) {
						e2.printStackTrace();
						throw new IllegalAccessError("");
					}
					current_table.unlockIfNecessary(false);
					throw e;
				}
			}
		}

		@Override
		protected boolean isValid() {
			if (current_table.current_locks.size() == 0)
				return true;
			Lock cur = current_table.current_locks.get(Thread.currentThread());
			return cur == null || cur instanceof ReadLock;
		}

		@Override
		public void close() throws Exception {
			close(new ArrayList<Table<?>>(20));
		}

		@Override
		protected void close(ArrayList<Table<?>> _comes_from_tables) throws DatabaseException {
			if (current_table==null)
				return;
			synchronized (current_table.sql_connection.locker) {
				try {
					if (previous_lock == null)
						current_table.current_locks.remove(Thread.currentThread());
					else
						current_table.current_locks.put(Thread.currentThread(), previous_lock);
					// current_table.current_lock=previous_lock;
					_comes_from_tables.add(current_table);
					/*
					 * for (NeighboringTable nt : current_table.list_tables_pointing_to_this_table)
					 * { Table<?> t=nt.getPointedTable(); if (!_comes_from_tables.contains(t))
					 * t.current_lock.close(_comes_from_tables); }
					 */
					for (ForeignKeyFieldAccessor fa : current_table.foreign_keys_fields) {
						Table<?> t = fa.getPointedTable();
						if (!_comes_from_tables.contains(t))
							t.current_locks.get(Thread.currentThread()).close(_comes_from_tables);
					}
				} finally {
					current_table.unlockIfNecessary(false);
				}
			}
		}
	}

	static abstract class Querry implements AutoCloseable {
		protected final Connection sql_connection;

		public Querry(Connection _sql_connection) {
			sql_connection = _sql_connection;
		}

	}

	static abstract class AbstractReadQuerry extends Querry {
		public PreparedStatement statement;
		public ResultSet result_set;

		protected AbstractReadQuerry(Connection _sql_connection, SqlQuerry querry, int _result_set_type,
				int _result_set_concurency) throws SQLException, DatabaseException {
			super(_sql_connection);

			statement = sql_connection.prepareStatement(querry.getQuerry(), _result_set_type, _result_set_concurency);
			querry.finishPrepareStatement(statement);
			result_set = statement.executeQuery();
		}

		protected AbstractReadQuerry(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection);
			statement = null;
			result_set = resultSet;
		}

		@Override
		public void close() throws Exception {
			result_set.close();
			result_set = null;
			if (statement != null) {
				statement.close();
				statement = null;
			}
		}
	}

	static class ReadQuerry extends AbstractReadQuerry {
		public ReadQuerry(Connection _sql_connection, SqlQuerry querry) throws SQLException, DatabaseException {
			super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}

		public ReadQuerry(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection, resultSet);
		}
	}

	static abstract class ColumnsReadQuerry extends AbstractReadQuerry {
		TableColumnsResultSet tableColumnsResultSet;

		public ColumnsReadQuerry(Connection _sql_connection, SqlQuerry querry) throws SQLException, DatabaseException {
			super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}

		public ColumnsReadQuerry(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection, resultSet);
		}

		public void setTableColumnsResultSet(TableColumnsResultSet tableColumnsResultSet) {
			this.tableColumnsResultSet = tableColumnsResultSet;
		}
	}

	static class UpdatableReadQuerry extends AbstractReadQuerry {
		public UpdatableReadQuerry(Connection _sql_connection, SqlQuerry querry)
				throws SQLException, DatabaseException {
			super(_sql_connection, querry, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		}
	}

	/*
	 * static class PreparedReadQuerry extends Querry { public PreparedStatement
	 * statement; public ResultSet result_set=null; public
	 * PreparedReadQuerry(Connection _sql_connection, String querry) throws
	 * SQLException { super(_sql_connection);
	 * statement=sql_connection.prepareStatement(querry); }
	 * 
	 * public boolean execute() throws SQLException { boolean
	 * res=statement.execute(); result_set=statement.getResultSet(); return res; }
	 * 
	 * @Override public void close() throws Exception { if (result_set!=null)
	 * result_set.close(); result_set=null; statement.close(); statement=null; } }
	 * static class PreparedUpdatableReadQuerry extends Querry { public
	 * PreparedStatement statement; public ResultSet result_set=null; public
	 * PreparedUpdatableReadQuerry(Connection _sql_connection, String querry) throws
	 * SQLException { super(_sql_connection);
	 * statement=sql_connection.prepareStatement(querry); }
	 * 
	 * public boolean execute() throws SQLException { boolean
	 * res=statement.execute(); result_set=statement.getResultSet(); return res; }
	 * 
	 * @Override public void close() throws Exception { if (result_set!=null)
	 * result_set.close(); result_set=null; statement.close(); statement=null; } }
	 */

	static class PreparedUpdateQuerry extends Querry {
		public PreparedStatement statement;

		public PreparedUpdateQuerry(Connection _sql_connection, String querry) throws SQLException {
			super(_sql_connection);

			statement = sql_connection.prepareStatement(querry, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_UPDATABLE);
		}

		@Override
		public void close() throws Exception {
			statement.close();
			statement = null;
		}
	}

	void serialize(DatabaseRecord record, DataOutputStream oos, boolean includePK, boolean includeFK)
			throws DatabaseException {

		try {
			for (FieldAccessor fa : fields) {
				if ((includePK || !fa.isPrimaryKey()) && (includeFK || !fa.isForeignKey()))
					fa.serialize(oos, record);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	T unserialize(DataInputStream ois, boolean includePK, boolean includeFK) throws DatabaseException {
		try {
			T res = default_constructor_field.newInstance();

			for (FieldAccessor fa : fields) {
				if ((includePK || !fa.isPrimaryKey()) && (includeFK || !fa.isForeignKey()))
					fa.unserialize(ois, res);
			}
			return res;
		} catch (DatabaseException e) {
			if (e.getCause() instanceof EOFException)
				throw new SerializationDatabaseException("Unexpected EOF", e);
			throw e;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	void unserializePrimaryKeys(DatabaseRecord record, byte tab[]) throws DatabaseException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(tab)) {
			try (DataInputStream ois = new DataInputStream(bais)) {
				for (FieldAccessor fa : primary_keys_fields) {
					fa.unserialize(ois, record);
				}
			}
		} catch (DatabaseException e) {
			if (e.getCause() instanceof EOFException)
				throw new SerializationDatabaseException("Unexpected EOF", e);
			throw e;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	void unserializePrimaryKeys(HashMap<String, Object> map, byte tab[]) throws DatabaseException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(tab)) {
			try (DataInputStream ois = new DataInputStream(bais)) {
				for (FieldAccessor fa : primary_keys_fields) {
					fa.unserialize(ois, map);
				}
			}
		} catch (DatabaseException e) {
			if (e.getCause() instanceof EOFException)
				throw new SerializationDatabaseException("Unexpected EOF", e);
			throw e;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	void unserializeFields(DatabaseRecord record, byte tab[], boolean includePK, boolean includeFK,
			boolean includeNonKey) throws DatabaseException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(tab)) {
			try (DataInputStream ois = new DataInputStream(bais)) {
				for (FieldAccessor fa : fields) {
					if (fa.isPrimaryKey()) {
						if (includePK)
							fa.unserialize(ois, record);
					} else if (fa.isForeignKey()) {
						if (includeFK)
							fa.unserialize(ois, record);
					} else if (includeNonKey)
						fa.unserialize(ois, record);
				}
			}
		} catch (DatabaseException e) {
			if (e.getCause() instanceof EOFException)
				throw new SerializationDatabaseException("Unexpected EOF", e);
			throw e;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	byte[] serializePrimaryKeys(Map<String, Object> mapKeys) throws DatabaseException {

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			T record = default_constructor_field.newInstance();
			try (DataOutputStream oos = new DataOutputStream(baos)) {
				for (FieldAccessor fa : primary_keys_fields) {
					Object o = mapKeys.get(fa.getFieldName());
					if (o == null)
						throw new IllegalAccessError();
					fa.setValue(record, o);
					fa.serialize(oos, record);
				}
			}
			return baos.toByteArray();
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	byte[] serializePrimaryKeys(T record) throws DatabaseException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream oos = new DataOutputStream(baos)) {
				for (FieldAccessor fa : primary_keys_fields) {
					fa.serialize(oos, record);
				}
			}
			return baos.toByteArray();
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	byte[] serializeFields(T record, boolean includePK, boolean includeForeignKeyField, boolean includeNonKeyField)
			throws DatabaseException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream oos = new DataOutputStream(baos)) {
				for (FieldAccessor fa : fields) {
					if (fa.isPrimaryKey()) {
						if (includePK)
							fa.serialize(oos, record);
					} else if (fa.isForeignKey()) {
						if (includeForeignKeyField)
							fa.serialize(oos, record);
					} else if (includeNonKeyField) {
						fa.serialize(oos, record);
					}
				}
			}
			return baos.toByteArray();
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

}
