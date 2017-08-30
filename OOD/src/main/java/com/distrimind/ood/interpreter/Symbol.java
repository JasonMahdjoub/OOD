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
package com.distrimind.ood.interpreter;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseSyntaxException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class Symbol implements QueryPart {
	private SymbolType type;
	private String symbol;

	public Symbol(SymbolType type, String symbol) {
		if (type == null)
			throw new NullPointerException("type");
		if (symbol == null)
			throw new NullPointerException("symbol");

		this.type = type;
		this.symbol = symbol;

	}

	public SymbolType getType() {
		return type;
	}

	public String getSymbol() {
		return symbol;
	}

	@Override
	public Object getContent() {
		return getSymbol();
	}

	public RuleInstance getRule() {
		switch (type) {
		case ANDCONDITION:
		case ORCONDITION:
			return new RuleInstance(Rule.OPCONDITION, this);
		case EQUALOPERATOR:
		case NOTEQUALOPERATOR:
		case GREATEROPERATOR:
		case GREATEROREQUALOPERATOR:
		case LOWEROPERATOR:
		case LOWEROREQUALOPERATOR:
		case LIKE:
		case NOTLIKE:
			return new RuleInstance(Rule.OPCOMP, this);
		case IDENTIFIER:
		case NUMBER:
		case PARAMETER:
		case STRING:
		case NULL:
			return new RuleInstance(Rule.TERME, this);
		case OPEN_PARENTHESIS:
		case CLOSE_PARENTHESIS:
			return null;

		}
		return null;
	}

	@Override
	public String getBackusNaurNotation() {
		if (type == SymbolType.OPEN_PARENTHESIS || type == SymbolType.CLOSE_PARENTHESIS)
			return type.getContent();
		else
			return "<" + getType().name() + ">";
	}

	@Override
	public <T extends DatabaseRecord> boolean isMultiType(Table<T> table, Map<String, Object> parameters)
			throws DatabaseSyntaxException {
		if (getType() == SymbolType.IDENTIFIER) {
			FieldAccessor fa = table.getFieldAccessor(getSymbol());
			if (fa == null)
				throw new DatabaseSyntaxException(
						"Cannot find field " + getSymbol() + " into table " + table.getName());
			return fa.getDeclaredSqlFields().length > 1;
		} else if (getType() == SymbolType.PARAMETER) {
			Object p = parameters.get(getSymbol());
			if (p == null)
				throw new DatabaseSyntaxException("Impossible to find parameter " + getSymbol());
			FieldAccessor fa = getFieldAccessor(table, p);
			if (fa != null)
				return fa.getDeclaredSqlFields().length > 1;
			else
				throw new DatabaseSyntaxException("No field accessor corresponds to parameter " + getSymbol()
						+ " and type " + p.getClass().getName());
		} else
			return false;

	}

	@Override
	public <T extends DatabaseRecord> boolean isEqualable(Table<T> table, Map<String, Object> parameters,
			QueryPart otherPart) throws DatabaseSyntaxException {
		String valueType = getValueType(table, parameters);
		if (valueType == null)
			return false;
		String otherValueType = otherPart.getValueType(table, parameters);
		return valueType.equals(otherValueType) || otherValueType.equals(SymbolType.NULL.name());
	}

	@Override
	public <T extends DatabaseRecord> String getValueType(Table<T> table, Map<String, Object> parameters)
			throws DatabaseSyntaxException {
		if (getType() == SymbolType.IDENTIFIER) {
			FieldAccessor fa = table.getFieldAccessor(getSymbol());
			if (fa == null)
				throw new DatabaseSyntaxException(
						"Cannot find field " + getSymbol() + " into table " + table.getName());
			else if (fa.getFieldClassType() == Boolean.class)
				return "boolean";
			else if (AbstractDecentralizedID.class.isAssignableFrom(fa.getFieldClassType()))
				return "decentralizedID";
			else if (CharSequence.class.isAssignableFrom(fa.getFieldClassType()))
				return SymbolType.STRING.name();
			else if (fa.isComparable())
				return "comparable";
			else
				return fa.getFieldClassType().getName();
		} else if (getType() == SymbolType.PARAMETER) {
			Object p = parameters.get(getSymbol());
			if (p == null)
			{
				throw new DatabaseSyntaxException("Impossible to find parameter " + getSymbol());
			}
			FieldAccessor fa = getFieldAccessor(table, p);

			if (fa == null)
				throw new DatabaseSyntaxException("No field accessor corresponds to parameter " + getSymbol()
						+ " and type " + p.getClass().getName() + " with table " + table.getClass().getName());
			else if (fa.getFieldClassType() == Boolean.class)
				return "boolean";
			else if (AbstractDecentralizedID.class.isAssignableFrom(fa.getFieldClassType()))
				return "decentralizedID";
			else if (CharSequence.class.isAssignableFrom(fa.getFieldClassType()))
				return SymbolType.STRING.name();
			else if (fa.isComparable())
				return "comparable";
			else
				return fa.getFieldClassType().getName();
		} else if (getType() == SymbolType.NUMBER)
			return "comparable";
		else 
			return getType().name();

	}

	public static <T extends DatabaseRecord> FieldAccessor getFieldAccessor(Table<T> table, Object o) {
		if (o == null)
			return null;
		for (FieldAccessor fa : table.getFieldAccessors()) {
			Class<?> c = fa.getFieldClassType();

			if (c.isAssignableFrom(o.getClass()))
				return fa;
			else if (c.isPrimitive()) {
				if (c == boolean.class) {
					if (o.getClass() == Boolean.class)
						return fa;
				} else if (c == float.class) {
					if (o.getClass() == Float.class)
						return fa;
				} else if (c == short.class) {
					if (o.getClass() == Short.class)
						return fa;
				} else if (c == int.class) {
					if (o.getClass() == Integer.class)
						return fa;
				} else if (c == long.class) {
					if (o.getClass() == Long.class)
						return fa;
				} else if (c == char.class) {
					if (o.getClass() == Character.class)
						return fa;
				} else if (c == byte.class) {
					if (o.getClass() == Byte.class)
						return fa;
				} else if (c == double.class) {
					if (o.getClass() == Double.class)
						return fa;
				}
			}

		}
		for (FieldAccessor fa : table.getFieldAccessors()) {
			if (fa instanceof ForeignKeyFieldAccessor) {
				FieldAccessor res = getFieldAccessor(((ForeignKeyFieldAccessor) fa).getPointedTable(), o);
				if (res != null)
					return res;
			}

		}
		return null;
	}

	public static <T extends DatabaseRecord> FieldAccessor setFieldAccessor(Table<T> table, Object o, T record)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			DatabaseException {
		if (o == null)
			throw new IllegalAccessError();
		for (FieldAccessor fa : table.getFieldAccessors()) {
			if (fa.isAssignableTo(o.getClass())) {
				fa.setValue(record, o);
				return fa;
			}

		}
		for (FieldAccessor fa : table.getFieldAccessors()) {
			if (fa instanceof ForeignKeyFieldAccessor) {
				@SuppressWarnings("unchecked")
				Table<DatabaseRecord> otherTable = (Table<DatabaseRecord>) ((ForeignKeyFieldAccessor) fa)
						.getPointedTable();

				DatabaseRecord r = otherTable.getDefaultRecordConstructor().newInstance();
				FieldAccessor res = setFieldAccessor(otherTable, o, r);
				fa.setValue(record, r);
				return res;
			}

		}
		return null;
	}

	public boolean needParenthesis() {
		return false;
	}
}
