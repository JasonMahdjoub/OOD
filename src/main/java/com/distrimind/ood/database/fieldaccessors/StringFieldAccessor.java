
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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
import com.distrimind.ood.database.exceptions.*;
import com.distrimind.util.InvalidEncodedValue;
import com.distrimind.util.crypto.*;
import com.distrimind.util.data_buffers.WrappedSecretString;
import com.distrimind.util.data_buffers.WrappedString;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * String field accessor
 * @author Jason Mahdjoub
 * @version 1.3
 * @since OOD 1.0
 */
public class StringFieldAccessor extends FieldAccessor implements FieldAccessorWithStringPattern {
	protected final SqlField[] sql_fields;
	private final Pattern pattern;
	private final String stringPattern;

	public static final long defaultStringLengthLimit=ByteTabFieldAccessor.defaultByteTabSize;

	protected StringFieldAccessor(Table<?> table, DatabaseWrapper _sql_connection, Field _field,
			String parentFieldName, boolean severalPrimaryKeysPresentIntoTable) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, compatible_classes, table, severalPrimaryKeysPresentIntoTable);
		sql_fields = new SqlField[1];
		long l=limit;
		if (l<=0) {
			if (WrappedHashedValueInBase64StringFormat.class.isAssignableFrom(field.getType()))
			{
				l=WrappedHashedValueInBase64StringFormat.MAX_CHARS_NUMBER;
			}
			else if (WrappedEncryptedASymmetricPrivateKeyString.class.isAssignableFrom(field.getType()))
			{
				l=WrappedEncryptedASymmetricPrivateKeyString.MAX_CHARS_NUMBER;
			}
			else if (WrappedEncryptedSymmetricSecretKeyString.class.isAssignableFrom(field.getType()))
			{
				l=WrappedEncryptedSymmetricSecretKeyString.MAX_CHARS_NUMBER;
			}
			else if (WrappedHashedPasswordString.class.isAssignableFrom(field.getType()))
			{
				l=WrappedHashedPasswordString.MAX_CHARS_NUMBER;
			}
			else if (WrappedPassword.class.isAssignableFrom(field.getType()))
			{
				l=WrappedPassword.MAX_CHARS_NUMBER;
			}
			else
				l = defaultStringLengthLimit;
		}
		sql_fields[0] = new SqlField(supportQuotes, table_name + "." + this.getSqlFieldName(),
				l < DatabaseWrapperAccessor.getVarCharLimit(sql_connection)
						? "VARCHAR(" + l + ")"
						:DatabaseWrapperAccessor.getTextType(sql_connection, l),
				isNotNull());
		com.distrimind.ood.database.annotations.Field a=_field.getAnnotation(com.distrimind.ood.database.annotations.Field.class);
		if (a==null || a.regexPattern()==null || a.regexPattern().isEmpty()) {
			pattern = null;
			stringPattern = "";
		}
		else {
			stringPattern=a.regexPattern();
			pattern=Pattern.compile(stringPattern);
		}
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
		}
		else if (!(_field_instance instanceof String) && !(_field_instance instanceof WrappedString) && !(_field_instance instanceof StringBuffer) && !(_field_instance instanceof StringBuilder))
			throw new FieldDatabaseException("The given _field_instance parameter, destined to the field "
					+ field.getName() + " of the class " + field.getDeclaringClass().getName()
					+ ", should be a String and not a " + _field_instance.getClass().getName());

		try {

			field.set(_class_instance, toObject(_field_instance));
		} catch (IllegalArgumentException | IllegalAccessException | InvalidEncodedValue e) {
			throw new DatabaseException("Unexpected exception.", e);
		}
	}

	@SuppressWarnings("deprecation")
	private Object toObject(Object _field_instance) throws InvalidEncodedValue, ConstraintsNotRespectedDatabaseException {
		checkPattern(_field_instance);
		if (WrappedString.class.isAssignableFrom(field.getType())) {
			if (_field_instance instanceof StringBuffer)
				_field_instance=_field_instance.toString();
			if (_field_instance instanceof String) {
				String s = (String) _field_instance;
				if (WrappedEncryptedASymmetricPrivateKeyString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedEncryptedASymmetricPrivateKeyString(s);
				} else if (WrappedHashedValueInBase64StringFormat.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedHashedValueInBase64StringFormat(s);
				} else if (WrappedEncryptedSymmetricSecretKeyString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedEncryptedSymmetricSecretKeyString(s);
				} else if (WrappedHashedPasswordString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedHashedPasswordString(s);
				} else if (WrappedPassword.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedPassword(s);
				} else if (WrappedSecretString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedSecretString(s);
				} else
					_field_instance = new WrappedString(s);
			}
			else if (_field_instance instanceof StringBuilder) {
				StringBuilder s = (StringBuilder) _field_instance;
				if (WrappedEncryptedASymmetricPrivateKeyString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedEncryptedASymmetricPrivateKeyString(s);
				} else if (WrappedHashedValueInBase64StringFormat.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedHashedValueInBase64StringFormat(s);
				} else if (WrappedEncryptedSymmetricSecretKeyString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedEncryptedSymmetricSecretKeyString(s);
				} else if (WrappedHashedPasswordString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedHashedPasswordString(s);
				} else if (WrappedPassword.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedPassword(s);
				} else if (WrappedSecretString.class.isAssignableFrom(field.getType())) {
					_field_instance = new WrappedSecretString(s);
				} else
					_field_instance = new WrappedString(s);
			}
		}
		else if (String.class==field.getType()) {
			if (_field_instance instanceof StringBuilder)
				_field_instance=_field_instance.toString();
			else if (_field_instance instanceof StringBuffer)
				_field_instance=_field_instance.toString();
		}
		else if (StringBuilder.class==field.getType()) {
			if (_field_instance instanceof String)
				_field_instance=new StringBuilder((String)_field_instance);
			else if (_field_instance instanceof StringBuffer)
				_field_instance=new StringBuilder((StringBuffer)_field_instance);
		} else if (StringBuffer.class==field.getType()) {
			if (_field_instance instanceof String)
				_field_instance=new StringBuffer((String)_field_instance);
			else if (_field_instance instanceof StringBuilder)
				_field_instance=new StringBuffer((StringBuilder)_field_instance);
		}

		return _field_instance;
	}

	@SuppressWarnings("DuplicateExpressions")
	@Override
	public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException {
		try {
			Object o=field.get(_class_instance);
			if (o==null)
				return _field_instance==null;
			if (_field_instance==null)
				return false;
			if (WrappedString.class.isAssignableFrom(field.getType()))
			{

				if (_field_instance instanceof CharSequence)
				{
					if (WrappedSecretString.class.isAssignableFrom(field.getType())) {

						return WrappedSecretString.constantTimeAreEqual(((WrappedSecretString) o).toStringBuilder(), (CharSequence) _field_instance);
					}
					else if (_field_instance instanceof StringBuilder)
						return ((WrappedString) o).toStringBuilder().equals(_field_instance);
					else
						return o.toString().equals(_field_instance.toString());
				}
				else
					return o.equals(_field_instance);
			}
			else if (CharSequence.class.isAssignableFrom(field.getType()))
			{
				if (WrappedSecretString.class.isAssignableFrom(_field_instance.getClass()))
				{
					return WrappedSecretString.constantTimeAreEqual((CharSequence)o, ((WrappedSecretString) _field_instance).toStringBuilder());
				}
				else if (WrappedString.class.isAssignableFrom(_field_instance.getClass()))
				{
					if (StringBuilder.class.isAssignableFrom(field.getType()))
						return ((WrappedString)_field_instance).toStringBuilder().equals(o);
					else
						return o.toString().equals(_field_instance.toString());
				}
				else if (CharSequence.class.isAssignableFrom(_field_instance.getClass()))
				{
					if (field.getType()==_field_instance.getClass())
						return o.equals(_field_instance);
					else
						return o.toString().equals(_field_instance.toString());
				}
			}
			throw new DatabaseException("Unexpected exception");
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	private static final Class<?>[] compatible_classes = { String.class,
			StringBuilder.class,
			StringBuffer.class,
			WrappedString.class,
			WrappedHashedValueInBase64StringFormat.class,
			WrappedEncryptedASymmetricPrivateKeyString.class,
			WrappedEncryptedSymmetricSecretKeyString.class,
			WrappedHashedPasswordString.class,
			WrappedPassword.class,
			WrappedSecretString.class};

	@Override
	public Object getValue(Object _class_instance) throws DatabaseException {
		try {
			/*if (WrappedString.class.isAssignableFrom(field.getType()))
			{
				Object o=field.get(_class_instance);
				if (o==null)
					return null;
				else
					return o.toString();
			}
			else*/
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
	public SqlFieldInstance[] getSqlFieldsInstances(String sqlTableName, Object _instance) throws DatabaseException {
		SqlFieldInstance[] res = new SqlFieldInstance[1];
		res[0] = new SqlFieldInstance(supportQuotes, sqlTableName, sql_fields[0], getValue(_instance));
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

			if (WrappedString.class.isAssignableFrom(field.getType()))
			{
				StringBuilder val1 = ((WrappedString) obj1).toStringBuilder();
				StringBuilder val2 = ((WrappedString) obj2).toStringBuilder();
				return val1.compareTo(val2);
			}
			else if (StringBuilder.class.isAssignableFrom(field.getType())) {
				StringBuilder val1 = (StringBuilder) obj1;
				StringBuilder val2 = (StringBuilder) obj2;
				return val1.compareTo(val2);
			}
			else if (StringBuffer.class.isAssignableFrom(field.getType())) {
				StringBuffer val1 = (StringBuffer) obj1;
				StringBuffer val2 = (StringBuffer) obj2;
				return val1.compareTo(val2);
			}
			else if (StringBuffer.class.isAssignableFrom(field.getType())) {
				String val1 = (String) obj1;
				String val2 = (String) obj2;
				return val1.compareTo(val2);
			}
			else
				throw new DatabaseException("Unexpected exception");

		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}



	@Override
	public void setValue(String sqlTableName, Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			if (sql_fields[0].type.startsWith("VARCHAR")) {
				String res = _result_set.getString(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				setValue(_class_instance, res);
			} else {
				Clob c = _result_set.getClob(getColumnIndex(_result_set, getSqlFieldName(sqlTableName, sql_fields[0])));
				String res = c.getSubString(0, (int) c.length());
				if (res == null && isNotNull())
					throw new DatabaseIntegrityException("Unexpected exception.");
				setValue(_class_instance, res);
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
			String s=o==null?null:o.toString();
			checkPattern(s);
			_prepared_statement.setString(_field_start, s);
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
			Object o=getValue(_class_instance);
			CharSequence s=null;
			if (o instanceof WrappedString)
				s=((WrappedString) o).toStringBuilder();
			else if (o instanceof CharSequence)
				s=(CharSequence) o;

			if (s != null) {
				_oos.writeInt(s.length());
				for (int i=0;i<s.length();i++)
					_oos.writeChar(s.charAt(i));
			} else
				_oos.writeInt(-1);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private Object deserialize(RandomInputStream _ois) throws DatabaseException {
		try {
			int size = _ois.readInt();
			if (size > -1) {
				if (getLimit() > 0 && size > getLimit())
					throw new IOException();

				StringBuilder b = new StringBuilder(size);
				while (size-- > 0)
					b.append(_ois.readChar());
				return toObject(b);
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



	@Override
	public void checkPattern(Object value) throws PatternNotRespectedDatabaseException, InvalidEncodedValue {
		if (value==null)
			return;
		CharSequence s;
		if (value instanceof CharSequence)
			s=(CharSequence)value;
		else if (value instanceof WrappedString)
			s=((WrappedString)value).toStringBuilder();
		else
			throw new InvalidEncodedValue();
		if (pattern != null) {
			synchronized (pattern) {
				if (!pattern.matcher(s).matches())
					throw new PatternNotRespectedDatabaseException(stringPattern, s);
			}
		}
	}

	@Override
	public String getStringPattern() {
		return stringPattern;
	}
}
