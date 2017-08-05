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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.AbstractDecentralizedIDGenerator;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.SecuredDecentralizedID;
import com.distrimind.util.crypto.SecureRandomType;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 1.7.0
 * 
 */
public class AbstractDencetralizedIDFieldAccessor extends FieldAccessor {
	protected final SqlField sql_fields[];
	private final boolean isVarBinary;

	protected AbstractDencetralizedIDFieldAccessor(Class<? extends Table<?>> table_class,
			DatabaseWrapper _sql_connection, Field _field, String parentFieldName) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatibleClasses, table_class);
		sql_fields = new SqlField[1];
		sql_fields[0] = new SqlField(table_name + "." + this.getFieldName(),
				DatabaseWrapperAccessor.isVarBinarySupported(sql_connection) ? "VARBINARY(70)"
						: DatabaseWrapperAccessor.getBigIntegerType(sql_connection),
				null, null, isNotNull());
		isVarBinary = DatabaseWrapperAccessor.isVarBinarySupported(sql_connection);
	}

	private static final Class<?> compatibleClasses[];
	static {
		compatibleClasses = new Class<?>[5];
		compatibleClasses[0] = AbstractDecentralizedID.class;
		compatibleClasses[1] = AbstractDecentralizedIDGenerator.class;
		compatibleClasses[2] = DecentralizedIDGenerator.class;
		compatibleClasses[3] = RenforcedDecentralizedIDGenerator.class;
		compatibleClasses[4] = SecuredDecentralizedID.class;
	}

	private static BigDecimal getBigDecimal(byte bytes[]) {
		if (bytes == null)
			return null;

		BigInteger res = BigInteger.valueOf(1);
		for (int i = 0; i < bytes.length; i++) {
			res = res.shiftLeft(8).or(BigInteger.valueOf(bytes[i] & 0xFF));
		}
		return new BigDecimal(res);
	}

	private static byte[] getBytes(BigDecimal v) {
		if (v == null)
			return null;
		BigInteger val = v.toBigInteger();

		ArrayList<Byte> res = new ArrayList<>();
		do {
			byte b = (byte) val.and(BigInteger.valueOf(0xFF)).intValue();
			val = val.shiftRight(8);
			res.add(new Byte(b));
		} while (!val.equals(BigInteger.ZERO));
		byte[] bytes = new byte[res.size() - 1];
		for (int i = 0; i < bytes.length; i++)
			bytes[i] = res.get(res.size() - i - 2).byteValue();
		return bytes;

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
		} catch (IllegalArgumentException e) {
			throw new DatabaseException("Unexpected exception.", e);
		} catch (IllegalAccessException e) {
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

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			byte[] val1 = null;
			if (_field_instance instanceof byte[])
				val1 = (byte[]) _field_instance;
			else if (_field_instance instanceof AbstractDecentralizedID)
				val1 = ((AbstractDecentralizedID) _field_instance).getBytes();

			byte[] val2 = null;

			if (isVarBinary) {
				val2 = _result_set.getBytes(_sft.translateField(sql_fields[0]));
			} else {
				val2 = getBytes(_result_set.getBigDecimal(_sft.translateField(sql_fields[0])));
			}

			if (val1 == null || val2 == null)
				return val1 == val2;
			else {
				if (val1.length != val2.length)
					return false;
				for (int i = 0; i < val1.length; i++)
					if (val1[i] != val2[i])
						return false;
				return true;
			}

		} catch (SQLException e) {
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
	public SqlField[] getDeclaredSqlFields() {
		return sql_fields;
	}

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(Object _instance) throws DatabaseException {
		SqlFieldInstance res[] = new SqlFieldInstance[1];
		if (isVarBinary)
			res[0] = new SqlFieldInstance(sql_fields[0], ((AbstractDecentralizedID) getValue(_instance)).getBytes());
		else
			res[0] = new SqlFieldInstance(sql_fields[0],
					getBigDecimal(((AbstractDecentralizedID) getValue(_instance)).getBytes()));
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
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			byte[] res = null;
			if (isVarBinary) {
				res = _result_set.getBytes(getColmunIndex(_result_set, sql_fields[0].field));
			} else {
				res = getBytes(_result_set.getBigDecimal(getColmunIndex(_result_set, sql_fields[0].field)));
			}
			if (res == null && isNotNull())
				throw new DatabaseIntegrityException("Unexpected exception. Null value was found into a not null field "
						+ this.getFieldName() + " into table " + this.getTableClass().getName());
			field.set(_class_instance, res == null ? null : AbstractDecentralizedID.instanceOf(res));
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
			byte[] b = null;
			if (o != null) {
				b = ((AbstractDecentralizedID) o).getBytes();
			}
			if (isVarBinary)
				_prepared_statement.setBytes(_field_start, b);
			else {
				_prepared_statement.setBigDecimal(_field_start, getBigDecimal(b));
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
			Object o = field.get(_class_instance);
			byte[] b = null;
			if (o != null) {
				b = ((AbstractDecentralizedID) o).getBytes();
			}
			if (isVarBinary)
				_result_set.updateBytes(sql_fields[0].short_field, b);
			else {
				_result_set.updateBigDecimal(sql_fields[0].short_field, getBigDecimal(b));
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			Object o = field.get(_class_instance);
			byte[] b = null;
			if (o != null) {
				b = ((AbstractDecentralizedID) o).getBytes();
			}
			if (isVarBinary)
				_result_set.updateBytes(_sft.translateField(sql_fields[0]), b);
			else {
				_result_set.updateBigDecimal(sql_fields[0].short_field, getBigDecimal(b));
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
	public boolean needToCheckUniquenessOfAutoGeneratedValues() {
		return false;
	}

	@Override
	public Object autoGenerateValue(Random _random) throws DatabaseException {
		try {
			if (field.getType() == AbstractDecentralizedID.class
					|| field.getType() == AbstractDecentralizedIDGenerator.class
					|| field.getType() == DecentralizedIDGenerator.class) {
				return new DecentralizedIDGenerator();
			} else if (field.getType() == RenforcedDecentralizedIDGenerator.class) {
				return new RenforcedDecentralizedIDGenerator();
			} else if (field.getType() == SecuredDecentralizedID.class) {
				return new SecuredDecentralizedID(new DecentralizedIDGenerator(),
						SecureRandomType.DEFAULT.getInstance());
			}
			throw new DatabaseException("Unexpected exception !");
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void serialize(DataOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			AbstractDecentralizedID a = (AbstractDecentralizedID) getValue(_class_instance);
			if (a != null) {
				byte b[] = a.getBytes();
				_oos.writeInt(b.length);
				_oos.write(b);
			} else {
				_oos.writeInt(-1);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public void unserialize(DataInputStream _ois, HashMap<String, Object> _map) throws DatabaseException {
		try {
			int size = _ois.readInt();
			if (size > -1) {
				byte[] b = new byte[size];
				int os = _ois.read(b);
				if (os != size)
					throw new DatabaseException(
							"read bytes insuficiant (expected size=" + size + ", obtained size=" + os + ")");
				_map.put(getFieldName(), AbstractDecentralizedID.instanceOf(b));
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
			int size = _ois.readInt();
			if (size > -1) {
				byte[] b = new byte[size];
				int os = _ois.read(b);
				if (os != size)
					throw new DatabaseException(
							"read bytes insuficiant (expected size=" + size + ", obtained size=" + os + ")");
				AbstractDecentralizedID a = AbstractDecentralizedID.instanceOf(b);
				setValue(_classInstance, a);
				return a;
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
