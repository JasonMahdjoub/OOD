
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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 */
public class StringFieldAccessor extends FieldAccessor {
	protected final SqlField[] sql_fields;

	protected StringFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection, Field _field,
			String parentFieldName) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatible_classes, table);
		sql_fields = new SqlField[1];
		sql_fields[0] = new SqlField(table_name + "." + this.getSqlFieldName(),
				limit == 0 ? "VARCHAR(" + DatabaseWrapperAccessor.getVarCharLimit(sql_connection) + ")"
						: (limit < DatabaseWrapperAccessor.getVarCharLimit(sql_connection) ? "VARCHAR(" + limit + ")"
								: "CLOB(" + limit + ")"),
				null, null, isNotNull());
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
		} else if (!(_field_instance instanceof String))
			throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
					+ field.getName() + " of the class " + field.getDeclaringClass().getName()
					+ ", should be a String and not a " + _field_instance.getClass().getName());
		try {
			field.set(_class_instance, _field_instance);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			if ((!(_field_instance instanceof String)))
				return false;
			Object val = field.get(_class_instance);
			if (val == null)
				return false;
			return val.equals(_field_instance);
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			String val1 = null;
			if (_field_instance instanceof String)
				val1 = (String) _field_instance;
			String val2 = (String) _result_set.getObject(_sft.translateField(sql_fields[0]));

			assert val1 != null;
			return val1.equals(val2);
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private static final Class<?>[] compatible_classes = { String.class };

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
		SqlFieldInstance[] res = new SqlFieldInstance[1];
		res[0] = new SqlFieldInstance(sql_fields[0], getValue(_instance));
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
	public int compare(Object _r1, Object _r2) throws DatabaseException {
		try {
			Object obj1 = field.get(_r1);
			Object obj2 = field.get(_r2);
			if (obj1 == null && obj2 != null)
				return -1;
			else if (obj1 != null && obj2 == null)
				return 1;
			else if (obj1 == obj2)
				return 0;

			String val1 = (String) obj1;
			String val2 = (String) obj2;

			return val1.compareTo(val2);
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			if (sql_fields[0].type.startsWith("VARCHAR")) {
				String res = _result_set.getString(getColmunIndex(_result_set, sql_fields[0].field));
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				field.set(_class_instance, res);
			} else {
				Clob c = _result_set.getClob(getColmunIndex(_result_set, sql_fields[0].field));
				String res = c.getSubString(0, (int) c.length());
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				field.set(_class_instance, res);
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
			_prepared_statement.setString(_field_start, (String) o);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		setValue(_class_instance, _field_instance);
		try {
			_result_set.updateString(sql_fields[0].short_field, (String) field.get(_class_instance));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			_result_set.updateString(_sft.translateField(sql_fields[0]), (String) field.get(_class_instance));
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
			String s = (String) getValue(_class_instance);
			if (s != null) {
				_oos.writeInt(s.length());
				_oos.writeChars(s);
			} else
				_oos.writeInt(-1);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private String deserialize(RandomInputStream _ois) throws DatabaseException {
		try {
			int size = _ois.readInt();
			if (size > -1) {
				if (getLimit() > 0 && size > getLimit())
					throw new IOException();

				char[] b = new char[size];
				int index = 0;
				while (size-- > 0)
					b[index++] = _ois.readChar();
				return String.valueOf(b);
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else
				return null;

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void deserialize(RandomInputStream dis, Map<String, Object> _map) throws DatabaseException {
		_map.put(getFieldName(), deserialize(dis));
	}

	@Override
	public Object deserialize(RandomInputStream dis, Object _classInstance) throws DatabaseException {
		Object o=deserialize(dis);
		setValue(_classInstance, o);
		return o;
	}

}
