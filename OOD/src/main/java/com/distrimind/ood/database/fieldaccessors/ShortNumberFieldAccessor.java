
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
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.crypto.AbstractSecureRandom;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 */
public class ShortNumberFieldAccessor extends FieldAccessor {
	protected final SqlField sql_fields[];

	protected ShortNumberFieldAccessor(Class<? extends Table<?>> table_class, DatabaseWrapper _sql_connection,
			Field _field, String parentFieldName) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatible_classes, table_class);
		sql_fields = new SqlField[1];
		sql_fields[0] = new SqlField(table_name + "." + this.getFieldName(),
				Objects.requireNonNull(DatabaseWrapperAccessor.getShortType(sql_connection)), null, null, isNotNull());

	}

	@Override
	public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException {
		if (_field_instance == null && isNotNull()) {
			throw new FieldDatabaseException("The given _field_instance, used to store the field " + field.getName()
					+ " (type=" + field.getType().getName() + ", declaring_class=" + field.getDeclaringClass().getName()
					+ ") into the DatabaseField class " + field.getDeclaringClass().getName()
					+ ", is null and should not be.");
		}
		try {
			if (_field_instance == null)
				field.set(_class_instance, null);
			if (_field_instance instanceof Short)
				field.set(_class_instance, _field_instance);
			else if (_field_instance instanceof Long)
				field.set(_class_instance, ((Long) _field_instance).shortValue());
			else {
				assert _field_instance != null;
				throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName()
						+ ", should be a Short and not a " + _field_instance.getClass().getName());
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			if (_field_instance == null) {
				if (isNotNull())
					return false;
				else
					return field.get(_class_instance) == null;
			}
			if (_field_instance instanceof Short)
				return _field_instance.equals(field.get(_class_instance));
			else if (_field_instance instanceof Long)
				return Short.valueOf(((Long) _field_instance).shortValue()).equals(field.get(_class_instance));
			return false;
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			Short val1 = null;
			if (_field_instance instanceof Short)
				val1 = (Short) _field_instance;
			Short val2 = (Short) _result_set.getObject(_sft.translateField(sql_fields[0]));

			//noinspection NumberEquality
			return (val1 == null || val2 == null) ? val1 == val2 : val1.equals(val2);
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private static final Class<?>[] compatible_classes = { short.class, Short.class };

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
		SqlFieldInstance res[] = new SqlFieldInstance[1];
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

			short val1 = (Short) obj1;
			short val2 = (Short) obj2;
			return Short.compare(val1, val2);
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			Integer val = ((Integer) _result_set.getObject(getColmunIndex(_result_set, sql_fields[0].field)));
			if (val == null && isNotNull())
				throw new DatabaseIntegrityException("Unexpected exception");
			field.set(_class_instance, val == null ? null : val.shortValue());
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
			_prepared_statement.setObject(_field_start, o);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		setValue(_class_instance, _field_instance);
		try {
			_result_set.updateObject(sql_fields[0].short_field, field.get(_class_instance));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			_result_set.updateObject(_sft.translateField(sql_fields[0]), field.get(_class_instance));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean canBePrimaryOrUniqueKey() {
		return true;
	}

	@Override
	public boolean canAutoGenerateValues() {
		return true;
	}

	@Override
	public Object autoGenerateValue(AbstractSecureRandom _random) {
		return new BigInteger(getBitsNumber(), _random).shortValue();
	}

	@Override
	public boolean needToCheckUniquenessOfAutoGeneratedValues() {
		return true;
	}

	@Override
	public int getDefaultBitsNumberForAutoGeneratedValues() {
		return 15;
	}

	@Override
	public int getMaximumBitsNumberForAutoGeneratedValues() {
		return 15;
	}

	@Override
	public void serialize(DataOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			Short v = (Short) getValue(_class_instance);
			if (v == null) {
				_oos.writeBoolean(false);
			} else {
				_oos.writeBoolean(true);
				_oos.writeShort(v);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void unserialize(DataInputStream _ois, HashMap<String, Object> _map) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				_map.put(getFieldName(), _ois.readShort());
			} else if (isNotNull())
				throw new DatabaseException("field should not be null");
			else {
				_map.put(getFieldName(), null);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object unserialize(DataInputStream _ois, Object _classInstance) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				Short s = _ois.readShort();
				setValue(_classInstance, s);
				return s;
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
