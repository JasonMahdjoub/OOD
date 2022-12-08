
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

package com.distrimind.ood.database.fieldaccessors;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.data_buffers.WrappedData;
import com.distrimind.util.data_buffers.WrappedSecretData;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Map;

/**
 * Byte tab field accessor
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 */
public class ByteTabFieldAccessor extends FieldAccessor {
	protected final SqlField[] sql_fields;
	private final boolean isVarBinary;
	private final boolean isBigInteger;

	public static final int defaultByteTabSize= 512;
	public static final int shortTabSizeLimit=32768;
	
	protected ByteTabFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection, Field _field,
			String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatible_classes, table, severalPrimaryKeysPresentIntoTable);
		sql_fields = new SqlField[1];
		String type;
		isVarBinary=DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);

		if (limit <= 0)
			limit = defaultByteTabSize;
		boolean isBigInteger=false;
		if (useBlob)
		{
			type=DatabaseWrapperAccessor.getBlobType(sql_connection, limit);
		}
		else if (limit <= shortTabSizeLimit) {
			if (isVarBinary)
				type = DatabaseWrapperAccessor.getVarBinaryType(_sql_connection, limit);
			else if (DatabaseWrapperAccessor.isLongVarBinarySupported(sql_connection))
				type = DatabaseWrapperAccessor.getLongVarBinaryType(_sql_connection, limit);
			else
			{
				isBigInteger=true;
				type = DatabaseWrapperAccessor.getBigDecimalType(sql_connection, limit);
			}
		} else {
			if (DatabaseWrapperAccessor.isLongVarBinarySupported(sql_connection))
				type = DatabaseWrapperAccessor.getLongVarBinaryType(_sql_connection, limit);
			else
				type=DatabaseWrapperAccessor.getBlobType(sql_connection, limit);

		}
		assert type != null;
		sql_fields[0] = new SqlField(supportQuotes, table_name + "." + this.getSqlFieldName(), type, isNotNull());

		this.isBigInteger=isBigInteger;
	}
	public static byte[] getByteTab(BigDecimal value)
	{
		if (value==null)
			return null;
		else 
		{
			byte[] tab = value.toBigInteger().toByteArray();
			if (tab.length<1)
				return null;
			if (tab.length==1)
				return new byte[0];
			byte[] res = new byte[tab.length - 1];
			System.arraycopy(tab, 1, res, 0, res.length);
			return res;
		}
	}
	public static class BD extends BigDecimal
	{
		public BD(BigInteger val) {
			super(val);
		}
	}
	public static BigDecimal getBigDecimalValue(WrappedData wd)
	{
		if (wd==null)
			return null;
		else
			return getBigDecimalValue(wd.getBytes());
	}
	public static BD getBigDecimalValue(byte[] value)
	{
		if (value==null)
			return null;
		else
		{
			byte[] tab = new byte[value.length + 1];
			tab[0]=1;
			System.arraycopy(value, 0, tab, 1, value.length);
			return new BD(new BigInteger(tab));
		}
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
			} else if (_field_instance.getClass().equals(this.getCompatibleClasses()[0]))
				field.set(_class_instance, _field_instance);
			else
				throw new FieldDatabaseException("The given _field_instance parameter, destined to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName()
						+ ", should be an array of Byte and not a " + _field_instance.getClass().getName());
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			byte[] tab1 = (byte[]) field.get(_class_instance);
			if (_field_instance == null) {
				if (isNotNull())
					return false;
				else
					return tab1 == null;
			}

			byte[] tab2;
			if (_field_instance.getClass().equals(this.getCompatibleClasses()[0])) {
				tab2 = (byte[]) _field_instance;
			}
			else if (WrappedData.class.isAssignableFrom(_field_instance.getClass()))
			{
				tab2=((WrappedData) _field_instance).getBytes();
				if (WrappedSecretData.class.isAssignableFrom(_field_instance.getClass()))
				{
					return WrappedSecretData.constantTimeAreEqual(tab1, tab2);
				}
			}
			else
				return false;

			if (tab1.length != tab2.length) {
				return false;
			}
			for (int i = 0; i < tab1.length; i++)
				if (tab1[i] != tab2[i])
					return false;
			return true;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private static final Class<?>[] compatible_classes = {byte[].class};

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
	public SqlFieldInstance[] getSqlFieldsInstances(String sqlTableName, Object _instance) throws DatabaseException {
		SqlFieldInstance[] res = new SqlFieldInstance[1];
		try
		{
			if (isVarBinary)
				res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], getValue(_instance));
			else if (isBigInteger)
			{
				res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], getBigDecimalValue((byte[])getValue(_instance)));
			}
			else {
				byte[] b = (byte[]) field.get(_instance);
				if (b == null)
					res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], null);
				else {
					Blob blob = DatabaseWrapperAccessor.getBlob(sql_connection, b);
					if (blob == null)
						res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], new ByteArrayInputStream(b));
					else
						res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], blob);
				}
			}
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
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
	public boolean isAlgebraic() {
		return false;
	}

	@Override
	public int compare(Object _r1, Object _r2) throws DatabaseException {
		throw new DatabaseException("Unexpected exception");
	}

	@Override
	public void setValue(String sqlTableName, Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			if (isVarBinary) {
				byte[] res = _result_set.getBytes(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				field.set(_class_instance, res);
			} else if (isBigInteger){
				byte[] res = getByteTab(_result_set.getBigDecimal(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0]))));
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				field.set(_class_instance, res);
			} else {
				Blob b = _result_set.getBlob(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
				byte[] res = b == null ? null : b.getBytes(1, (int) b.length());
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
			byte[] tab/*;
			if (o instanceof WrappedData)
				tab=((WrappedData) o).getBytes();
			else
				tab*/=(byte[])o;
			if (isVarBinary)
				_prepared_statement.setBytes(_field_start, tab);
			else if (isBigInteger){
				_prepared_statement.setBigDecimal(_field_start, getBigDecimalValue(tab));
			}else {
				if (o == null)
					_prepared_statement.setObject(_field_start, null);
				else {
					Blob blob = DatabaseWrapperAccessor.getBlob(sql_connection, tab);
					if (blob == null)
						_prepared_statement.setBinaryStream(_field_start, new ByteArrayInputStream(tab));
					else {
						_prepared_statement.setBlob(_field_start, blob);
					}
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
	public boolean canAutoGenerateValues() {
		return true;
	}

	@Override
	public Object autoGenerateValue(AbstractSecureRandom _random) {
		byte[] res = new byte[getBitsNumber() / 8];
		_random.nextBytes(res);
		return res;
	}

	@Override
	public boolean needToCheckUniquenessOfAutoGeneratedValues() {
		return true;
	}

	@Override
	public int getDefaultBitsNumberForAutoGeneratedValues() {
		return 128;
	}

	@Override
	public int getMaximumBitsNumberForAutoGeneratedValues() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void serialize(RandomOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			byte[] b = (byte[])getValue(_class_instance);
			_oos.writeBytesArray(b, !isAlwaysNotNull(), (int)Math.min(Integer.MAX_VALUE, getLimit()));
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private byte[] deserialize(RandomInputStream _ois) throws DatabaseException {
		try {
			byte[] b=_ois.readBytesArray(!isAlwaysNotNull(), (int)Math.min(Integer.MAX_VALUE, getLimit()));
			if (b!=null) {
				if (getLimit() > 0 && b.length > getLimit())
					throw new IOException();

				return b;
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
