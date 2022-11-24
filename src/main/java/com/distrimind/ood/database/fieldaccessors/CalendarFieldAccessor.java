package com.distrimind.ood.database.fieldaccessors;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language 

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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class CalendarFieldAccessor extends FieldAccessor{
	protected final SqlField[] sql_fields;
	protected final boolean isVarBinary;
	private static final int MAX_SIZE_OF_TIME_ZONE_IN_BYTES=128;

	protected CalendarFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection,
										   Field _field, String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, getCompatibleClasses(_field), table, severalPrimaryKeysPresentIntoTable);
		sql_fields = new SqlField[2];
		sql_fields[0] = new SqlField(supportQuotes, table_name + "." + this.getSqlFieldName() + "_utc",
				Objects.requireNonNull(DatabaseWrapperAccessor.getLongType(sql_connection)), isNotNull());
		boolean isVarBinarySupported=DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);
		if (limit<=0)
			limit=32;
		else
			limit=Math.min(limit, MAX_SIZE_OF_TIME_ZONE_IN_BYTES);
		if (!isVarBinarySupported)
			limit*=3;
		sql_fields[1] = new SqlField(supportQuotes, table_name + "." + this.getSqlFieldName() + "_tz",
				Objects.requireNonNull(isVarBinarySupported ? DatabaseWrapperAccessor.getVarBinaryType(_sql_connection, limit)
						: DatabaseWrapperAccessor.getBigDecimalType(sql_connection, limit)), isNotNull());
		isVarBinary = DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);
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
				throw new FieldDatabaseException("The given _field_instance, used to store the field " + field.getName()
						+ " (type=" + field.getType().getName() + ", declaring_class="
						+ field.getDeclaringClass().getName() + ") into the DatabaseField class "
						+ field.getDeclaringClass().getName() + ", is null and should not be.");
		} else if (!(_field_instance instanceof Calendar))
			throw new FieldDatabaseException("The given _field_instance parameter, destined to the field "
					+ field.getName() + " of the class " + field.getDeclaringClass().getName()
					+ ", should be a Calendar and not a " + _field_instance.getClass().getName());
		try {
			field.set(_class_instance, _field_instance);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			Object o = field.get(_class_instance);
			if (o == _field_instance)
				return true;
			if (o != null)
				return o.equals(_field_instance);
			return false;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}
	private static byte[] getBytes(BigDecimal v) {
		return ByteTabFieldAccessor.getByteTab(v);

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
	public SqlField[] getDeclaredSqlFields() {
		return sql_fields;
	}

	private Object getTZSQLObject(TimeZone timeZone) {
		if (timeZone==null)
			return null;
		byte[] tz=timeZone.getID().getBytes(StandardCharsets.UTF_8);
		if (isVarBinary) {
			return tz;
		} else {
			return ByteTabFieldAccessor.getBigDecimalValue(tz);
		}
	}

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(String sqlTableName, Object _instance) throws DatabaseException {
		SqlFieldInstance[] res = new SqlFieldInstance[2];
		Calendar did = (Calendar) getValue(_instance);
		if (did==null)
		{
			res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], null);
			res[1] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[1], null);
		}
		else
		{
			res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], did.getTimeInMillis());
			res[1] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[1], getTZSQLObject(did.getTimeZone()));
		}
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
	public boolean isAlgebraic() {
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
			return ((Calendar) val1).compareTo((Calendar) val2);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	public void setValue(String sqlTableName, Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {


			Long utc;
			byte[] tz;
			if (_result_set==null) {
				utc = null;
				tz = null;
			}

			else {
				utc=_result_set.getLong(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
				if (isVarBinary) {
					tz = _result_set.getBytes(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[1])));
				} else {
					tz = getBytes(_result_set.getBigDecimal(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[1]))));
				}
			}
			if ((utc==null)!=(tz==null))
				throw new SQLException();

			if (utc==null)
				field.set(_class_instance, null);
			else {
				Calendar c=Calendar.getInstance(TimeZone.getTimeZone(new String(tz, StandardCharsets.UTF_8)));
				c.setTimeInMillis(utc);
				field.set(_class_instance, c);
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
			Calendar did = (Calendar) o;
			if (did==null)
			{
				_prepared_statement.setObject(_field_start, null);
				_prepared_statement.setObject(++_field_start, null);
			}
			else
			{
				_prepared_statement.setObject(_field_start, did.getTimeInMillis());
				_prepared_statement.setObject(++_field_start, getTZSQLObject(did.getTimeZone()));
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
	public boolean canAutoGenerateValues() {
		return false;
	}

	@Override
	public boolean needToCheckUniquenessOfAutoGeneratedValues() {
		return false;
	}

	@Override
	public Object autoGenerateValue(AbstractSecureRandom _random) throws DatabaseException {
		throw new IllegalAccessError();
	}

	@Override
	public void serialize(RandomOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			Calendar a = (Calendar) getValue(_class_instance);
			if (a == null)
			{
				_oos.writeBoolean(false);
			}
			else
			{
				_oos.writeBoolean(true);
				_oos.writeLong(a.getTimeInMillis());
				_oos.writeBytesArray(a.getTimeZone().getID().getBytes(StandardCharsets.UTF_8), false, MAX_SIZE_OF_TIME_ZONE_IN_BYTES);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private Calendar read(RandomInputStream _ois) throws IOException {
		if (_ois.readBoolean())
		{
			long utc = _ois.readLong();
			byte[] tz = _ois.readBytesArray(false, MAX_SIZE_OF_TIME_ZONE_IN_BYTES);
			Calendar c=Calendar.getInstance(TimeZone.getTimeZone(new String(tz, StandardCharsets.UTF_8)));
			c.setTimeInMillis(utc);
			return c;
		}
		else
			return null;
	}

	@Override
	public void deserialize(RandomInputStream _ois, Map<String, Object> _map) throws DatabaseException {
		try {
			_map.put(getFieldName(), read(_ois));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object deserialize(RandomInputStream _ois, Object _classInstance) throws DatabaseException {
		try {
			Calendar c=read(_ois);
			setValue(_classInstance, c);
			return c;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

}
