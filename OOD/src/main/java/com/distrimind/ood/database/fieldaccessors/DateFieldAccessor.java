
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 * 
 */
public class DateFieldAccessor extends FieldAccessor {
	private final SqlField[] sql_fields;

	protected DateFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection, Field _field,
			String parentFieldName) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, getCompatibleClasses(_field), table);
		if (!Date.class.isAssignableFrom(_field.getType()))
			throw new FieldDatabaseException("The field " + _field.getName() + " of the class "
					+ _field.getDeclaringClass().getName() + " of type " + _field.getType() + " must be a Date type.");
		sql_fields = new SqlField[1];
		sql_fields[0] = new SqlField(table_name + "." + this.getSqlFieldName(), "TIMESTAMP", null, null, isNotNull());
	}

	private static Class<?>[] getCompatibleClasses(Field field) {
		Class<?>[] res = new Class<?>[1];
		res[0] = field.getType();
		return res;
	}

	@Override
	public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException {
		if (_field_instance == null) {
			if (isNotNull())
				throw new FieldDatabaseException(
						"The given _field_instance, used to store the field " + field.getName() + " (type="
								+ field.getType().getName() + ", declaring_class=" + field.getDeclaringClass().getName()
								+ ") into the DatabaseField class " + field.getDeclaringClass().getName()
								+ ", is null and should not be (property NotNull present).");
		} else if (!(field.getType().isAssignableFrom(_field_instance.getClass())))
			throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
					+ field.getName() + " of the class " + field.getDeclaringClass().getName() + ", should be a "
					+ field.getType().getName() + " and not a " + _field_instance.getClass().getName());
		try {
			field.set(_class_instance, _field_instance);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			Timestamp res = _result_set.getTimestamp(getColmunIndex(_result_set, sql_fields[0].field));
			if (res == null && isNotNull())
				throw new DatabaseIntegrityException("Unexpected exception.");
			if (res==null)
				field.set(_class_instance, null);
			else
				field.set(_class_instance, new Date(res.getTime()));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		setValue(_class_instance, _field_instance);
		try {
			Date d = (Date) field.get(_class_instance);
			_result_set.updateTimestamp(sql_fields[0].short_field, d == null ? null : new Timestamp(d.getTime()));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			Date d = (Date) field.get(_class_instance);
			_result_set.updateTimestamp(_sft.translateField(sql_fields[0]),
					d == null ? null : new Timestamp(d.getTime()));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			Date val = (Date) field.get(_class_instance);
			if (val == null || _field_instance == null)
				return _field_instance == val;
			if ((!(field.getType().isAssignableFrom(_field_instance.getClass()))))
				return false;
			return val.equals(_field_instance);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			Timestamp val1 = _result_set.getTimestamp(_sft.translateField(sql_fields[0]));
			if (val1 == null || _field_instance == null)
				return _field_instance == val1;
			if ((!(field.getType().isAssignableFrom(_field_instance.getClass()))))
				return false;
			return new Date(val1.getTime()).equals(_field_instance);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object getValue(Object _class_instance) throws DatabaseException {
		try {
			return field.get(_class_instance);
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
			if (o==null)
				_prepared_statement.setTimestamp(_field_start, null);
			else
				_prepared_statement.setTimestamp(_field_start, new Timestamp(((Date) o).getTime()));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public SqlField[] getDeclaredSqlFields() {
		return sql_fields;
	}

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(Object _instance) throws DatabaseException {
		SqlFieldInstance[] res = new SqlFieldInstance[1];
		Date date=(Date) getValue(_instance);
		res[0] = new SqlFieldInstance(sql_fields[0], date==null?null:new Timestamp(date.getTime()));
		return res;
	}

	@Override
	public boolean isAlwaysNotNull() {
		return false;
	}

	@Override
	public boolean isComparable() {
		return true;
	}

	@Override
	public boolean canBePrimaryOrUniqueKey() {
		return false;
	}

	@Override
	public int compare(Object _r1, Object _r2) throws DatabaseException {
		try {
			Date obj1 = (Date) field.get(_r1);
			Date obj2 = (Date) field.get(_r2);
			if (obj1 == null && obj2 != null)
				return -1;
			else if (obj1 != null && obj2 == null)
				return 1;
			else if (obj1 == obj2)
				return 0;

			return obj1.compareTo(obj2);
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	public void serialize(DataOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			Date d = (Date) getValue(_class_instance);
			if (d != null) {
				_oos.writeBoolean(true);
				_oos.writeLong(d.getTime());
			} else
				_oos.writeBoolean(false);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void unserialize(DataInputStream _ois, Map<String, Object> _map) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				_map.put(getFieldName(), new Date(_ois.readLong()));
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else
				_map.put(getFieldName(), null);

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object unserialize(DataInputStream _ois, Object _classInstance) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				Date d = new Date(_ois.readLong());
				setValue(_classInstance, d);
				return d;
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else {
				setValue(_classInstance, (Object) null);
				return null;
			}

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

}
