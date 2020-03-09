
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

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
 * @version 1.2
 * @since OOD 1.0
 */
public class BigDecimalFieldAccessor extends FieldAccessor {
	protected final SqlField[] sql_fields;

	protected BigDecimalFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection,
			Field _field, String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatible_classes, table, severalPrimaryKeysPresentIntoTable);
		sql_fields = new SqlField[1];
		long l=getLimit();
		if (l<=0)
			l=1024;

		sql_fields[0] = new SqlField(this.table_name + "." + this.getSqlFieldName(),
				Objects.requireNonNull(DatabaseWrapperAccessor.getBigDecimalType(sql_connection, l)), null, null, isNotNull());
	}

	@Override
	public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			if (_field_instance == null) {
				if (isNotNull())
					throw new FieldDatabaseException("The given _field_instance, used to store the field "
							+ field.getName() + " (type=" + field.getType().getName() + ", declaring_class="
							+ field.getDeclaringClass().getName() + ") into the DatabaseField class "
							+ field.getDeclaringClass().getName()
							+ ", is null and should not be (property NotNull present).");
				field.set(_class_instance, null);
			} else if (_field_instance.getClass().equals(BigDecimal.class))
				field.set(_class_instance, _field_instance);
			else if (_field_instance.getClass().equals(String.class))
				field.set(_class_instance, new BigDecimal((String) _field_instance));
			else
				throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName()
						+ ", should be a BigDecimal and not a " + _field_instance.getClass().getName());
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			BigDecimal val1 = (BigDecimal) field.get(_class_instance);
			if (_field_instance == null) {
				if (isNotNull())
					return false;
				else
					return val1 == null;
			}
			BigDecimal val2;
			if (_field_instance.getClass().equals(BigDecimal.class))
				val2 = (BigDecimal) _field_instance;
			else if (_field_instance.getClass().equals(String.class))
				val2 = new BigDecimal((String) _field_instance);
			else
				return false;

			return val1.equals(val2);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@SuppressWarnings("NumberEquality")
	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			BigDecimal val1 = null;
			if (_field_instance != null) {
				if (_field_instance instanceof BigDecimal)
					val1 = (BigDecimal) _field_instance;
				else if (_field_instance instanceof String)
					val1 = new BigDecimal((String) _field_instance);
				else
					return false;
			}

			String s = _result_set.getString(_sft.translateField(sql_fields[0]));
			BigDecimal val2 = s == null ? null : new BigDecimal(s);
			return (val1 == null || val2 == null) ? val1 == val2 : val1.equals(val2);
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private static final Class<?>[] compatible_classes = { BigDecimal.class };

	@Override
	public Object getValue(Object _class_instance) throws DatabaseException {
		try {
			return field.get(_class_instance);
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
		res[0] = new SqlFieldInstance(sql_fields[0], getValue(_instance).toString());
		return res;
	}

	@Override
	public boolean isAlwaysNotNull() {
		return false;
	}

	@Override
	public int compare(Object _o1, Object _o2) throws DatabaseException {
		try {
			Object val1 = field.get(_o1);
			Object val2 = field.get(_o2);
			if (val1 == null && val2 != null)
				return -1;
			else if (val1 != null && val2 == null)
				return 1;
			else if (val1 == val2)
				return 0;
			return ((BigDecimal) val1).compareTo((BigDecimal) val2);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean isComparable() {
		return true;
	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			String s = _result_set.getString(getColmunIndex(_result_set, sql_fields[0].field_without_quote));
			BigDecimal res = s == null ? null : new BigDecimal(s);
			if (res == null && isNotNull())
				throw new DatabaseIntegrityException("Unexpected exception.");
			field.set(_class_instance, res);
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
	public void getValue(PreparedStatement _prepared_statement, int _field_start, Object _field_content)
			throws DatabaseException {
		try {
			BigDecimal bd = (BigDecimal) _field_content;
			_prepared_statement.setString(_field_start, bd == null ? null : bd.toString());
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		setValue(_class_instance, _field_instance);
		try {
			BigDecimal bd = (BigDecimal) field.get(_class_instance);
			_result_set.updateString(sql_fields[0].short_field_without_quote, bd == null ? null : bd.toString());
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			BigDecimal bd = (BigDecimal) field.get(_class_instance);
			_result_set.updateString(_sft.translateField(sql_fields[0]), bd == null ? null : bd.toString());
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean canBePrimaryOrUniqueKey() {
		return true;
	}

	@Override
	public void serialize(RandomOutputStream dos, Object _class_instance) throws DatabaseException {
		BigDecimal bd=((BigDecimal)getValue(_class_instance));
		BigInteger bi=(bd==null?null:bd.unscaledValue());

		try {
			if (bi==null)
				dos.writeInt(-1);
			else {
				byte[] tab=bi.toByteArray();
				assert tab.length>0;
				dos.writeInt(tab.length);
				dos.write(tab);
				dos.writeInt(bd.scale());
			}

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private BigDecimal deserialize(RandomInputStream dis) throws DatabaseException {
		try {
			int s = dis.readInt();
			BigDecimal o = null;
			if (s >= 0) {
				if (getLimit() > 0 && s > getLimit())
					throw new IOException();
				byte[] tab = new byte[s];
				dis.readFully(tab);
				BigInteger bi = new BigInteger(tab);
				int scale = dis.readInt();
				o = new BigDecimal(bi, scale);
			}
			if (o == null && isNotNull())
				throw new DatabaseException("field should not be null");
			return o;
		}
		catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void deserialize(RandomInputStream dis, Map<String, Object> _map) throws DatabaseException {
		_map.put(getFieldName(), deserialize(dis));
	}

	@Override
	public Object deserialize(RandomInputStream dis, Object _classInstance) throws DatabaseException {
		BigDecimal o=deserialize(dis);
		setValue(_classInstance, o);
		return o;
	}

}
