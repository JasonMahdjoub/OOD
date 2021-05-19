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
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.AbstractDecentralizedIDGenerator;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.*;
import com.distrimind.util.data_buffers.WrappedData;
import com.distrimind.util.data_buffers.WrappedSecretData;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0.0
 */
public class DecentralizedValueFieldAccessor extends FieldAccessor {
	protected final SqlField[] sql_fields;
	private final boolean isVarBinary;
	private final boolean encodeExpirationUTC;

	protected DecentralizedValueFieldAccessor(Table<?> table,
											  DatabaseWrapper _sql_connection, Field _field, String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, new Class<?>[] {_field.getType()}, table, severalPrimaryKeysPresentIntoTable);
		sql_fields = new SqlField[1];
		Class<?>[] compatibleClasses = new Class<?>[]{_field.getType()};
		isVarBinary=DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);

		if (limit<=0)
		{
			if (AbstractDecentralizedID.class.isAssignableFrom(compatibleClasses[0]))
			{
				if (AbstractDecentralizedIDGenerator.class.isAssignableFrom(compatibleClasses[0]))
				{
					limit=AbstractDecentralizedIDGenerator.MAX_DECENTRALIZED_ID_SIZE_IN_BYTES;
				}
				else
					limit=AbstractDecentralizedID.MAX_DECENTRALIZED_ID_SIZE_IN_BYTES;
			}
			else {
				if (compatibleClasses[0]==IASymmetricPublicKey.class)
					limit=IASymmetricPublicKey.MAX_SIZE_IN_BYTES_OF_PUBLIC_KEY;
				else if (compatibleClasses[0]==IASymmetricPrivateKey.class)
					limit=IASymmetricPrivateKey.MAX_SIZE_IN_BYTES_OF_PRIVATE_KEY;
				else if (compatibleClasses[0]==ASymmetricPublicKey.class)
					limit=ASymmetricPublicKey.MAX_SIZE_IN_BYTES_OF_NON_HYBRID_PUBLIC_KEY;
				else if (compatibleClasses[0]==ASymmetricPrivateKey.class)
					limit=ASymmetricPrivateKey.MAX_SIZE_IN_BYTES_OF_NON_HYBRID_PRIVATE_KEY;
				else if (compatibleClasses[0]==HybridASymmetricPublicKey.class)
					limit=HybridASymmetricPublicKey.MAX_SIZE_IN_BYTES_OF_HYBRID_PUBLIC_KEY;
				else if (compatibleClasses[0]==HybridASymmetricPrivateKey.class)
					limit=HybridASymmetricPrivateKey.MAX_SIZE_IN_BYTES_OF_HYBRID_PRIVATE_KEY;
				else if (compatibleClasses[0]==ASymmetricKeyPair.class)
					limit=ASymmetricKeyPair.MAX_SIZE_IN_BYTES_OF_NON_HYBRID_KEY_PAIR;
				else if (compatibleClasses[0]==HybridASymmetricKeyPair.class)
					limit=HybridASymmetricKeyPair.MAX_SIZE_IN_BYTES_OF_HYBRID_KEY_PAIR;
				else if (compatibleClasses[0]==AbstractKeyPair.class)
					limit=AbstractKeyPair.MAX_SIZE_IN_BYTES_OF_KEY_PAIR;
				else if (compatibleClasses[0]==SymmetricSecretKey.class)
					limit=SymmetricSecretKey.MAX_SIZE_IN_BYTES_OF_SYMMETRIC_KEY;
				else
					limit=DecentralizedValue.MAX_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE;
			}

		}
		if (!isVarBinary)
			limit*=3;
		sql_fields[0] = new SqlField(supportQuotes, table_name + "." + this.getSqlFieldName(),
				Objects.requireNonNull(isVarBinary ? DatabaseWrapperAccessor.getVarBinaryType(_sql_connection, limit)
						: DatabaseWrapperAccessor.getBigDecimalType(sql_connection, limit)),
				isNotNull());
		//isVarBinary = DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);

		if (_field.isAnnotationPresent(com.distrimind.ood.database.annotations.Field.class))
			encodeExpirationUTC=_field.getAnnotation(com.distrimind.ood.database.annotations.Field.class).includeKeyExpiration();
		else
			encodeExpirationUTC=true;
	}


	private Object getSQLObject(WrappedData bytes) {
		if (bytes==null)
			return null;
		try {
			if (isVarBinary) {
				if (bytes instanceof WrappedSecretData) {
					return bytes.getBytes().clone();
				} else
					return bytes.getBytes();
			} else {
				return ByteTabFieldAccessor.getBigDecimalValue(bytes);
			}
		}
		finally {
			if (bytes instanceof WrappedSecretData)
				((WrappedSecretData) bytes).zeroize();
		}
	}

	private static byte[] getBytes(BigDecimal v) {
		return ByteTabFieldAccessor.getByteTab(v);

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
			throw new FieldDatabaseException("The given _field_instance parameter, destined to the field "
					+ field.getName() + " of the class " + field.getDeclaringClass().getName() + ", should be a "
					+ field.getType().getName() + " and not a " + _field_instance.getClass().getName());
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

	private WrappedData encode(Object instance)
	{
		if (instance==null)
			return null;
		else if (getFieldClassType()==ASymmetricPublicKey.class)
			return ((ASymmetricPublicKey) instance).encode(encodeExpirationUTC);
		else if (DecentralizedValue.class.isAssignableFrom(getFieldClassType()))
			return ((DecentralizedValue) instance).encode();
		else
			throw new IllegalAccessError();
	}

	public Object decode(byte[] tab)
	{
		if (tab==null)
			return null;
		else if (getFieldClassType()==ASymmetricPublicKey.class)
			return ASymmetricPublicKey.decode(tab, false);
		else if (DecentralizedValue.class.isAssignableFrom(getFieldClassType())) {
			return DecentralizedValue.decode(tab, sql_connection.supportNoCacheParam());
		}
		else
			throw new IllegalAccessError();
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

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(String sqlTableName, Object _instance) throws DatabaseException {
		SqlFieldInstance[] res = new SqlFieldInstance[1];
		res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], getSQLObject(encode(getValue(_instance))));
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

	@Override
	public void setValue(String sqlTableName, Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			byte[] res;
			if (isVarBinary) {
				res = _result_set.getBytes(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
			} else {
				res = getBytes(_result_set.getBigDecimal(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0]))));
			}
			if (res == null && isNotNull())
				throw new DatabaseIntegrityException("Unexpected exception. Null value was found into a not null field "
						+ this.getSqlFieldName() + " into table " + this.getTableClass().getName());
			field.set(_class_instance, res == null ? null : decode(res));
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
			WrappedData b = null;
			if (o != null) {
				b = encode(o);
			}
			if (isVarBinary)
				_prepared_statement.setBytes(_field_start, (byte[])getSQLObject(b));
			else {
				_prepared_statement.setBigDecimal(_field_start, (BigDecimal)getSQLObject(b));
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
	public boolean isDecentralizablePrimaryKey()
	{
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
	public Object autoGenerateValue(AbstractSecureRandom _random) {
		throw new IllegalAccessError();
	}

	@Override
	public void serialize(RandomOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			_oos.writeObject(getValue(_class_instance), !isNotNull());
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void deserialize(RandomInputStream _ois, Map<String, Object> _map) throws DatabaseException {
		try {
			_map.put(getFieldName(), _ois.readObject(!isNotNull(), DecentralizedValue.class));

		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public Object deserialize(RandomInputStream _ois, Object _classInstance) throws DatabaseException {
		try {
			DecentralizedValue dv=_ois.readObject(!isNotNull(), DecentralizedValue.class);
			setValue(_classInstance, dv);
			return dv;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}
}
