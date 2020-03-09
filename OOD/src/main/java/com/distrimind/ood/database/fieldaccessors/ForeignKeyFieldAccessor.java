
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

package com.distrimind.ood.database.fieldaccessors;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.3
 * @since OOD 1.0
 */
public class ForeignKeyFieldAccessor extends FieldAccessor {
	protected SqlField[] sql_fields = null;
	protected ArrayList<FieldAccessor> linked_primary_keys = null;
	protected String linked_table_name = null;
	protected Table<? extends DatabaseRecord> pointed_table = null;
	private int tableVersion;
	// private final Class<?>[] compatible_classes;

	private static Method get_record_method;
	private static Method get_new_record_instance_method;
	static {
		try {
			get_record_method = Table.class.getDeclaredMethod("getRecordFromPointingRecord",
					SqlFieldInstance[].class, ArrayList.class);
			get_record_method.setAccessible(true);
			get_new_record_instance_method = Table.class.getDeclaredMethod("getNewRecordInstance", Constructor.class, boolean.class);
			get_new_record_instance_method.setAccessible(true);
		} catch (SecurityException e) {
			System.err.println(
					"Impossible to access to the function getRecordFromPointingRecord of the class Table. This is an inner bug of MadKitGroupExtension. Please contact the developers. Impossible to continue. See the next error :");
			e.printStackTrace();
			System.exit(-1);
		} catch (NoSuchMethodException e) {
			System.err.println(
					"Impossible to found to the function getRecordFromPointingRecord of the class Table. This is an inner bug of MadKitGroupExtension. Please contact the developers. Impossible to continue. See the next error :");
			e.printStackTrace();
			System.exit(-1);
		}

	}

	protected ForeignKeyFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection,
			Field _field, String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, getCompatibleClasses(_field), table, severalPrimaryKeysPresentIntoTable);
		this.tableVersion=table.getDatabaseVersion();
		if (!DatabaseRecord.class.isAssignableFrom(_field.getType()))
			throw new DatabaseException("The field " + _field.getName() + " of the class "
					+ _field.getDeclaringClass().getName() + " is not a DatabaseRecord.");
		if (!field.getType().getPackage().equals(field.getDeclaringClass().getPackage()))
			throw new DatabaseException("The package of the pointed DatabaseRecord class " + field.getType().getName()
					+ ", is not the same then the package of the containing class "
					+ field.getDeclaringClass().getName() + " (Foregin key=" + field.getName() + ").");

	}

	public void initialize() throws DatabaseException {
		if (sql_fields == null) {
			@SuppressWarnings("unchecked")
			Class<? extends DatabaseRecord> c = (Class<? extends DatabaseRecord>) field.getType();
			try {
				Method f=AccessController.doPrivileged(new PrivilegedAction<Method>() {
					@Override
					public Method run() {
						Method f;
						try {
							f = DatabaseWrapper.class.getDeclaredMethod("getTableInstance", Class.class, int.class);
						} catch (NoSuchMethodException e) {
							e.printStackTrace();
							return null;
						}
						f.setAccessible(true);
						return f;
					}
				});
				pointed_table = (Table<?>)f.invoke(sql_connection, Table.getTableClass(c), tableVersion);
			} catch (IllegalAccessException | InvocationTargetException | NullPointerException e) {
				throw DatabaseException.getDatabaseException(e);
			}
			linked_primary_keys = pointed_table.getPrimaryKeysFieldAccessors();
			linked_table_name = pointed_table.getSqlTableName();

			ArrayList<SqlField> sql_fields = new ArrayList<>();
			for (FieldAccessor fa : linked_primary_keys) {
				if (fa.isForeignKey()) {
					((ForeignKeyFieldAccessor) fa).initialize();
				}
				for (SqlField sf : fa.getDeclaredSqlFields()) {
					sql_fields.add(new SqlField(table_name + "." + this.getSqlFieldName() + "__" + pointed_table.getSqlTableName()
							+ "_" + sf.short_field_without_quote, sf.type, pointed_table.getSqlTableName(), sf.field, isNotNull()));

				}
			}
			this.sql_fields = new SqlField[sql_fields.size()];
			for (int i = 0; i < sql_fields.size(); i++)
				this.sql_fields[i] = sql_fields.get(i);
		}
	}

	@Override
	public void changeInternalTableName(String oldInternalTableName, String internalTableName, int newTableVersion) throws DatabaseException {
		super.changeInternalTableName(oldInternalTableName, internalTableName, newTableVersion);
		this.linked_table_name=this.linked_table_name.replace(oldInternalTableName, internalTableName);
		this.tableVersion=newTableVersion;
		this.sql_fields=null;
	}

	public Table<? extends DatabaseRecord> getPointedTable() {
		return pointed_table;
	}

	@Override
	public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			if (_field_instance == null) {
				if (isNotNull())
					throw new FieldDatabaseException("The given _field_instance, used to store the field "
							+ field.getName() + " (type=" + field.getType().getName() + ", declaring_class="
							+ field.getDeclaringClass().getName() + ") into the DatabaseRecord class "
							+ field.getDeclaringClass().getName()
							+ ", is null and should not be (property NotNull present).");
			} else if (!(_field_instance.getClass().equals(field.getType())))
				throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName() + ", should be a "
						+ field.getType().getName() + " and not a " + _field_instance.getClass().getName());
			if (_field_instance == _class_instance)
				throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName()
						+ ", is the same reference than the correspondant table (autoreference).");
			field.set(_class_instance, _field_instance);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		if (_field_instance != null && !(_field_instance.getClass().equals(field.getType())))
			return false;
		try {

			if (_field_instance == null && isNotNull())
				return false;
			Object val1 = field.get(_class_instance);
			Object val2;
			val2 = _field_instance;
			if (val1 == val2)
				return true;
			if (val1 == null || val2 == null)
				return false;
			for (FieldAccessor fa : linked_primary_keys) {
				if (!fa.equals(val1, fa.getValue(val2))) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}

	}

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		if (_field_instance != null && !(_field_instance.getClass().equals(field.getType())))
			return false;

		for (FieldAccessor fa : linked_primary_keys) {
			if (!fa.equals(_field_instance, _result_set, new SqlFieldTranslation(fa, _sft)))
				return false;
		}
		return true;
	}

	private static Class<?>[] getCompatibleClasses(Field field) {
		Class<?>[] compatible_classes = new Class<?>[1];
		compatible_classes[0] = field.getType();

		return compatible_classes;
	}

	@Override
	public Object getValue(Object _class_instance) throws DatabaseException {
		try {
			return field.get(_class_instance);
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	public SqlField[] getDeclaredSqlFields() {
		return sql_fields;
	}

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(Object _instance) throws DatabaseException {
		Object val = this.getValue(_instance);
		SqlFieldInstance[] res = new SqlFieldInstance[sql_fields.length];
		if (val == null) {
			for (int i = 0; i < sql_fields.length; i++)
				res[i] = new SqlFieldInstance(sql_fields[i], null);
		} else {
			int i = 0;
			for (FieldAccessor fa : linked_primary_keys) {
				SqlFieldInstance[] linked_sql_field_instances = fa.getSqlFieldsInstances(val);
				for (SqlFieldInstance sfi : linked_sql_field_instances) {
					res[i++] = new SqlFieldInstance(
							table_name + "." + this.getSqlFieldName() + "__" + pointed_table.getSqlTableName() + "_"
									+ sfi.short_field_without_quote,
							sfi.type, linked_table_name, sfi.field, sfi.not_null, sfi.instance);
				}
			}
		}
		return res;

	}

	@Override
	public boolean isAlwaysNotNull() {
		return false;
	}

	@Override
	public boolean isComparable() {
		return false;
	}

	@Override
	public int compare(Object _r1, Object _r2) throws DatabaseException {
		throw new DatabaseException("Unexpected exception");
	}

	private int jonctionEnabled(ResultSet _result_set) {
		try {
			for (FieldAccessor fa : getPointedTable().getFieldAccessors()) {
				if (!fa.isPrimaryKey()) {
					if (fa.isForeignKey()) {
						int res = ((ForeignKeyFieldAccessor) fa).jonctionEnabled(_result_set);
						if (res != 2)
							return res;
					}
					fa.getColmunIndex(_result_set, fa.getDeclaredSqlFields()[0].field_without_quote);
					return 1;
				}
			}
			return 2;
		} catch (SQLException e) {
			return 0;
		}
	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {

			Table<?> t = getPointedTable();

			if (t.isLoadedInMemory() || jonctionEnabled(_result_set) == 0) {
				ArrayList<DatabaseRecord> list = _pointing_records == null ? new ArrayList<DatabaseRecord>()
						: _pointing_records;
				list.add((DatabaseRecord) _class_instance);
				SqlField[] sfs = getDeclaredSqlFields();

				SqlFieldInstance[] sfis = new SqlFieldInstance[sfs.length];
				for (int i = 0; i < sfs.length; i++) {
					sfis[i] = new SqlFieldInstance(sfs[i], _result_set.getObject(sfs[i].short_field_without_quote));
				}
				field.set(_class_instance, get_record_method.invoke(getPointedTable(), sfis, list));
			} else {
				DatabaseRecord dr = (DatabaseRecord) get_new_record_instance_method.invoke(t,
						t.getDefaultRecordConstructor(), true);

				for (FieldAccessor fa : t.getFieldAccessors()) {
					try {
						fa.setValue(dr, _result_set);
					} catch (DatabaseIntegrityException e) {
						if (fa.isPrimaryKey()) {
							dr = null;
							break;
						} else
							throw e;
					}
				}
				field.set(_class_instance, dr);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	public void getValue(Object _class_instance, PreparedStatement _prepared_statement, int _field_start)
			throws DatabaseException {
		try {
			getValue(_prepared_statement, _field_start, field.get(_class_instance));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void getValue(PreparedStatement _prepared_statement, int _field_start, Object o) throws DatabaseException {
		try {
			DatabaseRecord dr = (DatabaseRecord) o;

			for (FieldAccessor fa : linked_primary_keys) {
				if (dr == null) {
					for (SqlField ignored : this.sql_fields) {
						_prepared_statement.setObject(_field_start++, null);
					}
				} else {
					fa.getValue(dr, _prepared_statement, _field_start);
					_field_start += fa.getDeclaredSqlFields().length;
				}

			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		setValue(_class_instance, _field_instance);
		try {
			DatabaseRecord dr = (DatabaseRecord) field.get(_class_instance);
			for (FieldAccessor fa : linked_primary_keys) {

				if (dr == null) {
					for (SqlField sf : sql_fields) {
						_result_set.updateObject(sf.short_field_without_quote, null);
					}
				} else {
					fa.updateResultSetValue(dr, _result_set, new SqlFieldTranslation(this));
				}

			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			DatabaseRecord dr = (DatabaseRecord) field.get(_class_instance);
			for (FieldAccessor fa : linked_primary_keys) {

				if (dr == null) {
					for (SqlField sf : sql_fields) {
						_result_set.updateObject(_sft.translateField(sf), null);
					}
				} else {
					fa.updateResultSetValue(dr, _result_set, new SqlFieldTranslation(this, _sft));
				}

			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean canBePrimaryOrUniqueKey() {
		return true;
	}

	@Override
	public void serialize(RandomOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			DatabaseRecord dr = (DatabaseRecord) getValue(_class_instance);
			if (dr != null) {
				_oos.writeBoolean(true);
				for (FieldAccessor fa : pointed_table.getPrimaryKeysFieldAccessors()) {
					fa.serialize(_oos, dr);
				}
			} else
				_oos.writeBoolean(false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void deserialize(RandomInputStream _ois, Map<String, Object> _map) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				DatabaseRecord dr = pointed_table.getDefaultRecordConstructor().newInstance();
				for (FieldAccessor fa : pointed_table.getPrimaryKeysFieldAccessors()) {
					fa.deserialize(_ois, dr);
				}

				_map.put(getFieldName(), dr);
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else
				_map.put(getFieldName(), null);

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object deserialize(RandomInputStream _ois, Object _classInstance) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				DatabaseRecord dr = pointed_table.getDefaultRecordConstructor().newInstance();
				for (FieldAccessor fa : pointed_table.getPrimaryKeysFieldAccessors()) {
					fa.deserialize(_ois, dr);
				}
				setValue(_classInstance, dr);
				return dr;
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else
				return null;

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

}
