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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 1.8
 */
public class ComposedFieldAccessor extends FieldAccessor {
	private final ArrayList<FieldAccessor> fieldsAccessor;
	private final Constructor<?> defaultConstructor;
	private final SqlField[] sqlFields;

	protected ComposedFieldAccessor(DatabaseWrapper _sql_connection, Table<?> table,
			Field _field, String parentFieldName, List<Class<?>> parentFields) throws DatabaseException {
		super(_sql_connection, _field, parentFieldName, new Class<?>[] { _field.getType() }, table);
		try {
			defaultConstructor = _field.getType().getDeclaredConstructor();
			if (!Modifier.isPublic(defaultConstructor.getModifiers())) {
				throw new DatabaseException("The class " + _field.getType().getCanonicalName()
						+ " must have a public default constructor ");
			}

			fieldsAccessor = FieldAccessor.getFields(_sql_connection, table, _field.getType(), getFieldName(),
					parentFields);
			ArrayList<SqlField> sfs = new ArrayList<>();

			for (FieldAccessor fa : fieldsAccessor) {
				Collections.addAll(sfs, fa.getDeclaredSqlFields());
			}
			sqlFields = new SqlField[sfs.size()];
			sfs.toArray(sqlFields);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new DatabaseException(
					"The class " + _field.getType().getCanonicalName() + " must have a public default constructor ", e);
		}

	}

	@Override
	public boolean isNotNull() {
		if (not_null)
			return true;

		for (SqlField sf : getDeclaredSqlFields())
			if (sf.not_null)
				return true;
		return false;
	}

	public ArrayList<FieldAccessor> getFieldAccessors() {
		return fieldsAccessor;
	}

	@Override
	public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException {
		if (_field_instance == null && isNotNull()) {
			throw new FieldDatabaseException("The given _field_instance, used to store the field " + field.getName()
					+ " (type=" + field.getType().getName() + ", declaring_class=" + field.getDeclaringClass().getName()
					+ ") into the DatabaseField class " + field.getDeclaringClass().getName()
					+ ", is null and should not be (property NotNull present).");
		}
		try {
			if (_field_instance == null)
				field.set(_class_instance, null);
			else if (field.getType().isAssignableFrom(_field_instance.getClass()))
				field.set(_class_instance, _field_instance);
			else
				throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "
						+ field.getName() + " of the class " + field.getDeclaringClass().getName()
						+ ", should be an instance of " + field.getType().getCanonicalName() + " and not a "
						+ _field_instance.getClass().getName());
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new DatabaseException("Unexpected exception.", e);
		}

	}

	@Override
	public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)
			throws DatabaseException {
		try {
			Object instance = defaultConstructor.newInstance();

			for (FieldAccessor fa : fieldsAccessor) {
				fa.setValue(instance, _result_set, _pointing_records);
			}
			field.set(_class_instance, instance);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set)
			throws DatabaseException {
		try {
			setValue(_class_instance, _field_instance);
			for (FieldAccessor fa : fieldsAccessor) {
				fa.updateValue(_field_instance, fa.field.get(_field_instance), _result_set);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {
			Object fi = field.get(_class_instance);
			for (FieldAccessor fa : fieldsAccessor) {
				fa.updateResultSetValue(fi, _result_set, _sft);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
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
			if (field.getType().isAssignableFrom(_field_instance.getClass())) {
				Object instance = field.get(_class_instance);

				for (FieldAccessor fa : fieldsAccessor) {
					if (!fa.equals(instance, fa.field.get(_field_instance)))
						return false;
				}
				return true;
			} else
				return false;
		} catch (Exception e) {
			throw new DatabaseException("", e);
		}
	}

	@Override
	protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)
			throws DatabaseException {
		try {

			for (FieldAccessor fa : fieldsAccessor) {
				if (!fa.equals(fa.field.get(_field_instance), _result_set, _sft))
					return false;
			}
			return true;
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
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
			for (FieldAccessor fa : fieldsAccessor) {
				fa.getValue(_field_content, _prepared_statement, _field_start);
				_field_start += fa.getDeclaredSqlFields().length;
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	public SqlField[] getDeclaredSqlFields() {
		return sqlFields;
	}

	@Override
	public SqlFieldInstance[] getSqlFieldsInstances(Object _instance) throws DatabaseException {
		try {
			SqlFieldInstance[] res = new SqlFieldInstance[getDeclaredSqlFields().length];
			int index = 0;
			for (FieldAccessor fa : fieldsAccessor) {
				SqlFieldInstance[] r = fa.getSqlFieldsInstances(fa.field.get(_instance));
				for (SqlFieldInstance sfi : r)
					res[index++] = sfi;
			}
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

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
	public boolean canBePrimaryOrUniqueKey() {
		return parentFieldName == null || parentFieldName.isEmpty();
	}

	@Override
	public int compare(Object _r1, Object _r2) {
		return 0;
	}

	@Override
	public void serialize(DataOutputStream _oos, Object _class_instance) throws DatabaseException {
		try {
			Object o = getValue(_class_instance);
			if (o == null) {
				_oos.writeBoolean(false);
			} else {
				_oos.writeBoolean(true);
				for (FieldAccessor fa : fieldsAccessor)
					fa.serialize(_oos, o);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	@Override
	public void unserialize(DataInputStream _ois, Map<String, Object> _map) throws DatabaseException {
		try {
			boolean isNotNull = _ois.readBoolean();
			if (isNotNull) {
				Object dr = this.defaultConstructor.newInstance();
				for (FieldAccessor fa : fieldsAccessor) {
					fa.unserialize(_ois, dr);
				}
				_map.put(getFieldName(), dr);
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
				Object dr = this.defaultConstructor.newInstance();
				for (FieldAccessor fa : fieldsAccessor) {
					fa.unserialize(_ois, dr);
				}
				setValue(_classInstance, dr);
				return dr;
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
