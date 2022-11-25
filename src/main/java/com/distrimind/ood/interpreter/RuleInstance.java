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

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseSyntaxException;
import com.distrimind.ood.database.fieldaccessors.BigDecimalFieldAccessor;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.ood.database.fieldaccessors.StringFieldAccessor;
import com.distrimind.util.Reference;
import com.distrimind.util.data_buffers.WrappedString;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class RuleInstance implements QueryPart {

	private final Rule rule;
	private final ArrayList<QueryPart> parts;

	public RuleInstance(Rule rule, ArrayList<QueryPart> parts, int off, int len) {
		if (rule == null)
			throw new NullPointerException("rule");
		this.rule = rule;
		this.parts = new ArrayList<>();
		for (int i = off; i < off + len; i++)
			this.parts.add(parts.get(i));
	}

	@Override
	public String toString() {
		return "RuleInstance{" +
				"rule=" + rule +
				", parts=" + parts +
				'}';
	}

	public RuleInstance(Rule rule, QueryPart part, QueryPart... parts) {
		if (rule == null)
			throw new NullPointerException("rule");

		this.rule = rule;
		this.parts = new ArrayList<>();
		this.parts.add(part);
		Collections.addAll(this.parts, parts);

	}

	@Override
	public Object getContent() {
		if (parts.size() == 0) {
			return null;
		} else if (parts.size() == 1)
			return parts.get(0).getContent();
		else {
			return parts;
		}
	}

	@Override
	public String getBackusNaurNotation() {
		return "<" + rule.name() + ">";
	}

	public Rule getRule() {
		return rule;
	}

	@Override
	public <T extends DatabaseRecord> boolean isMultiType(Table<T> table, Map<String, Object> parameters)
			throws DatabaseSyntaxException {
		switch (rule) {
			case FACTOR:
			case COMPARE:
			case QUERY:
			case OPCONDITION:
			case OPCOMP:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case NULLTEST:
			case INTEST:
			case ISOP:
			case INOP:
			case NULL:
				return false;
			case EXPRESSION: {
				if (parts.size()==1)
					return parts.get(0).isMultiType(table, parameters);
				else
					return false;
			}
			case WORD: {
				if (parts.size() == 1) {
					return getSymbol().isMultiType(table, parameters);
				} else {
					return false;
				}

		}

		}
		throw new IllegalAccessError();
	}

	@Override
	public <T extends DatabaseRecord> boolean isEqualable(Table<T> table, Map<String, Object> parameters,
			QueryPart otherPart) throws DatabaseSyntaxException {
		String valueType = getValueType(table, parameters);
		if (valueType == null)
			return false;
		String otherValueType = otherPart.getValueType(table, parameters);
		return valueType.equals(otherValueType) || otherValueType.equals(SymbolType.NULL.getContent());
	}

	@Override
	public <T extends DatabaseRecord> String getValueType(Table<T> table, Map<String, Object> parameters)
			throws DatabaseSyntaxException {
		switch (rule) {
			case ISOP:
			case INOP:
			case NULLTEST:
			case INTEST:
			case COMPARE:
			case OPCOMP:
			case OPCONDITION:
			case QUERY:
				return "boolean";
			case NULL:
			case WORD:
				if (parts.size() == 1)
					return parts.get(0).getValueType(table, parameters);
				else
					return parts.get(1).getValueType(table, parameters);
			case EXPRESSION:
			case FACTOR:
				if (parts.size()==1)
					return parts.get(0).getValueType(table, parameters);
				else
					return "comparable";

		}
		throw new IllegalAccessError();
	}

	@Override
	public <T extends DatabaseRecord> boolean isAlgebraic(Table<T> table, Map<String, Object> parameters)
			throws DatabaseSyntaxException {
		switch (rule) {
			case ISOP:
			case INOP:
			case NULLTEST:
			case INTEST:
			case COMPARE:
			case OPCOMP:
			case OPCONDITION:
			case QUERY:
			case NULL:
				return false;
			case ADD_OPERATOR:
			case MULTIPLY_OPERATOR:
				return true;
			case WORD:
				if (parts.size() == 1)
					return parts.get(0).isAlgebraic(table, parameters);
				else
					return parts.get(1).isAlgebraic(table, parameters);
			case EXPRESSION:
			case FACTOR:
				if (parts.size()==1)
					return parts.get(0).isAlgebraic(table, parameters);
				else
					return parts.get(0).isAlgebraic(table, parameters) && parts.get(1).isAlgebraic(table, parameters) && parts.get(2).isAlgebraic(table, parameters);

		}
		throw new IllegalAccessError();
	}

	@Override
	public boolean needParenthesis() {
		return (parts.size() == 1 && parts.get(0).needParenthesis()) || (parts.size() == 3
				&& (parts.get(0) instanceof Symbol) && ((Symbol) parts.get(0)).getType() == SymbolType.OPEN_PARENTHESIS
				&& (parts.get(2) instanceof Symbol)
				&& ((Symbol) parts.get(2)).getType() == SymbolType.CLOSE_PARENTHESIS);
	}

	public <T extends DatabaseRecord> BigDecimal getNumber(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		Object o = getComparable(table, parameters, record);
		if (o instanceof BigDecimal)
			return (BigDecimal) o;
		else
			throw new DatabaseSyntaxException(o + " of type " + o.getClass() + " is not a number !");
	}

	public <T extends DatabaseRecord> Object getComparable(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case COMPARE:
			case QUERY:
			case OPCOMP:
			case OPCONDITION:
			case ISOP:
			case INOP:
			case NULLTEST:
			case INTEST:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case NULL:
				throw new IllegalAccessError();
			case FACTOR:
			case EXPRESSION:
				if (parts.size()==1)
					return getRuleInstance(0).getComparable(table, parameters, record);
				else if (parts.size()==3)
				{
					return getAlgebraic(table, parameters, record);
				}
				else
					throw new IllegalAccessError();
			case WORD:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();
						HashSet<TableJunction> tablesJunction = new HashSet<>();
						Table.FieldAccessorValue fav = table.getFieldAccessorAndValue(record, fieldName, tablesJunction);
						if (fav == null || fav.getFieldAccessor()==null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
						if (fav.getFieldAccessor().isComparable())
							return fav.getFieldAccessor().getValue(fav.getValue()/*getDatabaseRecord(fav.getFieldAccessor(), tablesJunction, record)*/);
						else
							throw new DatabaseSyntaxException(
									"The " + fieldName + " into table " + table.getClass().getSimpleName() + " is not comparable !");
					} else if (s.getType() == SymbolType.PARAMETER) {
						Object parameter1 = parameters.get(s.getSymbol());
						if (parameter1 == null)
							throw new DatabaseSyntaxException("Cannot find parameter " + s.getSymbol());
						return parameter1;
					} else if (s.getType() == SymbolType.NUMBER)
						return new BigDecimal(s.getSymbol());
					else
						throw new DatabaseSyntaxException("Cannot get comparable with type " + s.getType());
				} else if (parts.size() == 3) {
					return getRuleInstance(1).getNumber(table, parameters, record);
				} else
					throw new IllegalAccessError();

		}
		throw new IllegalAccessError();
	}

	private BigDecimal toBigDecimal(Object o) {
		if (o instanceof Date)
		{
			return BigDecimal.valueOf(((Date) o).getTime());
		}
		else if (o instanceof Number) {
			if (o instanceof BigDecimal)
			{
				return (BigDecimal) o;
			}
			else if (o instanceof BigInteger)
				return new BigDecimal((BigInteger) o);
			else if (o instanceof Long)
				return BigDecimal.valueOf(((Number)o).longValue());
			else
				return BigDecimal.valueOf(((Number)o).doubleValue());
		}
		else
			return null;
	}



	private BigDecimal algebraicOperation(BigDecimal v1, BigDecimal v2, SymbolType symbolType) throws DatabaseException {
		if (v1==null)
			return null;
		if (v2==null)
			return null;

		switch (symbolType)
		{
			case PLUS:
				return v1.add(v2);
			case MINUS:
				return v1.subtract(v2);
			case MULTIPLY:
				return v1.multiply(v2);
			case DIVIDE:
				return v1.divide(v2, RoundingMode.HALF_UP);
			case MODULO:
				return v1.remainder(v2);
			default:
				throw new DatabaseException("Not algebraic operator "+symbolType);
		}
	}
	private <T extends DatabaseRecord>  BigDecimal getAlgebraicWith3partsFactorOrExpression(Table<T> table, Map<String, Object> parameters, T record) throws DatabaseException {
		BigDecimal o1=getRuleInstance(0).getAlgebraic(table, parameters, record);
		BigDecimal o2=getRuleInstance(2).getAlgebraic(table, parameters, record);
		RuleInstance op=getRuleInstance(1);
		switch (op.getRule())
		{
			case ADD_OPERATOR:
			case MULTIPLY_OPERATOR:
				return algebraicOperation(o1, o2, ((Symbol)op.getContent()).getType());
			default:
				throw new IllegalAccessError();
		}
	}
	public <T extends DatabaseRecord> BigDecimal getAlgebraic(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case COMPARE:
			case QUERY:
			case OPCOMP:
			case OPCONDITION:
			case ISOP:
			case INOP:
			case NULLTEST:
			case INTEST:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case NULL:
				throw new IllegalAccessError();
			case FACTOR:
			case EXPRESSION:
				if (parts.size()==1)
					return getRuleInstance(0).getAlgebraic(table, parameters, record);
				else if (parts.size()==3)
				{
					return getAlgebraicWith3partsFactorOrExpression(table, parameters, record);
				}
				else
					throw new IllegalAccessError();
			case WORD:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();
						HashSet<TableJunction> tablesJunction = new HashSet<>();
						Table.FieldAccessorValue fav = table.getFieldAccessorAndValue(record, fieldName, tablesJunction);
						if (fav == null || fav.getFieldAccessor()==null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());

						if (fav.getFieldAccessor().getDeclaredSqlFields().length==1 && fav.getFieldAccessor().isAlgebraic())
						{
							Object o=fav.getFieldAccessor().getValue(fav.getValue()/*getDatabaseRecord(fav.getFieldAccessor(), tablesJunction, record)*/);
							if (o==null)
								return null;
							BigDecimal r=toBigDecimal(o);
							if (r==null)
								throw new DatabaseException("The field "+fieldName+" must be a number");
							else
								return r;
						}
						else
							throw new DatabaseSyntaxException(
									"The " + fieldName + " into table " + table.getClass().getSimpleName() + " is not comparable !");
					} else if (s.getType() == SymbolType.PARAMETER) {
						Object parameter1 = parameters.get(s.getSymbol());
						if (parameter1 == null)
							throw new DatabaseSyntaxException("Cannot find parameter " + s.getSymbol());
						BigDecimal r=toBigDecimal(parameter1);
						if (r==null)
							throw new DatabaseSyntaxException("The parameter "+s.getSymbol()+" must be a number");
						return r;
					} else if (s.getType() == SymbolType.NUMBER)
						return new BigDecimal(s.getSymbol());
					else
						throw new DatabaseSyntaxException("Cannot get comparable with type " + s.getType());
				} else if (parts.size() == 3) {
					return getRuleInstance(1).getAlgebraic(table, parameters, record);
				} else
					throw new IllegalAccessError();

		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> Object getEquallable(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case FACTOR:
			case EXPRESSION:
				if (parts.size()==1)
					return getRuleInstance(0).getEquallable(table, parameters, record);
				else if (parts.size()==3){
					return getAlgebraicWith3partsFactorOrExpression(table, parameters, record);
				}
				else
					throw new IllegalAccessError();
			case OPCOMP:
			case OPCONDITION:
			case COMPARE:
			case QUERY:
			case ISOP:
			case INOP:
			case NULL:
			case NULLTEST:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case INTEST:
				throw new IllegalAccessError();
			case WORD:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();
						Table.FieldAccessorValue fav = table.getFieldAccessorAndValue(record, fieldName);
						if (fav == null || fav.getFieldAccessor()==null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());

						return fav;
					} else if (s.getType() == SymbolType.PARAMETER) {
						return parameters.get(s.getSymbol());
					} else if (s.getType() == SymbolType.STRING)
						return s.getContent();
					else if (s.getType()==SymbolType.NULL)
						return null;
					else
						return new BigDecimal(s.getSymbol());
				} else if (parts.size() == 3) {
					return getRuleInstance(1).getEquallable(table, parameters, record);
				} else
					throw new IllegalAccessError();
		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> Object getStringable(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseSyntaxException {
		switch (rule) {
			case COMPARE:
			case QUERY:

			case OPCOMP:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case OPCONDITION:
			case ISOP:
			case INOP:
			case NULLTEST:
			case INTEST:
				throw new IllegalAccessError();
			case FACTOR:
			case EXPRESSION:
				if (parts.size()==1)
					return getRuleInstance(0).getStringable(table, parameters, record);
				else
					throw new DatabaseSyntaxException("Expression or factor cannot be converted to string value");
			case NULL:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					return s.getContent().toString();
				}
				else
					throw new IllegalAccessError();
			case WORD:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();
						Table.FieldAccessorValue fav ;
						try {
							fav = table.getFieldAccessorAndValue(record, fieldName);
						} catch (DatabaseException e) {
							throw new DatabaseSyntaxException("", e);
						}
						if (fav == null || fav.getFieldAccessor()==null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
						if (StringFieldAccessor.class.isAssignableFrom(fav.getFieldAccessor().getClass()))
							return fav;
						else
							throw new DatabaseSyntaxException(
									"The field " + fieldName + " into table " + table.getClass().getSimpleName() + " is not a string !");
					} else if (s.getType() == SymbolType.PARAMETER) {
						Object parameter1 = parameters.get(s.getSymbol());
						if (parameter1 == null)
							throw new DatabaseSyntaxException("Cannot find parameter " + s.getSymbol());
						if (CharSequence.class.isAssignableFrom(parameter1.getClass()) || WrappedString.class.isAssignableFrom(parameter1.getClass())) {
							return parameter1.toString();
						} else
							throw new DatabaseSyntaxException("The parameter " + s.getSymbol() + " is not a string !");
					} else if (s.getType() == SymbolType.STRING || s.getType() == SymbolType.NUMBER || s.getType()==SymbolType.NULL) {

						if (s.getContent() instanceof BigDecimal)
						{
							String t=DatabaseWrapperAccessor.getBigDecimalType(table.getDatabaseWrapper(), 64);
							return t.contains("CHAR")?s.getContent().toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? BigDecimalFieldAccessor.bigDecimalToBytes((BigDecimal)s.getContent()):s.getContent();
						}
						else if (s.getContent() instanceof BigInteger)
						{
							String t=DatabaseWrapperAccessor.getBigIntegerType(table.getDatabaseWrapper(), 64);
							return t.contains("CHAR")?s.getContent().toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? ((BigInteger)s.getContent()).toByteArray():new BigDecimal(((BigInteger)s.getContent()));
						}
						else
							return s.getContent().toString();
					} else
						throw new IllegalAccessError();
				} else if (parts.size() == 3) {
					throw new DatabaseSyntaxException("Cannot convert an expression to a string");
				} else
					throw new IllegalAccessError();

		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> int compareTo(Table<T> table, T record, Object o1, Object o2)
			throws DatabaseSyntaxException {
		try {
			if (((o1==null || !Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass()))
					&& (o2!=null && Table.FieldAccessorValue.class.isAssignableFrom(o2.getClass())))
					|| ((o1==null || !BigDecimal.class.isAssignableFrom(o1.getClass()))
							&& (o2!=null && BigDecimal.class.isAssignableFrom(o2.getClass()))
							&& (o1==null || !Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass())))) {
				Object o = o1;
				o1 = o2;
				o2 = o;
			}
			if (o1!=null && Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass()))
				o1 = ((Table.FieldAccessorValue) o1).getFieldAccessor().getValue(getRecordInstance(table, record, ((Table.FieldAccessorValue) o1).getFieldAccessor()));
			if (o2!=null && Table.FieldAccessorValue.class.isAssignableFrom(o2.getClass()))
				o2 = ((Table.FieldAccessorValue) o2).getFieldAccessor().getValue(getRecordInstance(table, record, ((Table.FieldAccessorValue) o2).getFieldAccessor()));
			if (o1==null)
			{
				if (null ==o2)
					return 0;
				else 
					return -1;
			}
			else if (o2==null)
			{
				return 1;
			}
			if (BigDecimal.class.isAssignableFrom(o1.getClass())) {
				if (BigDecimal.class.isAssignableFrom(o2.getClass()))
					return ((BigDecimal) o1).compareTo((BigDecimal) o2);
				else if (Number.class.isAssignableFrom(o2.getClass())) {
					return ((BigDecimal) o1).compareTo(new BigDecimal(o2.toString()));
				}
			} else if (Number.class.isAssignableFrom(o1.getClass())) {
				if (BigDecimal.class.isAssignableFrom(o2.getClass()))
					return new BigDecimal(o1.toString()).compareTo((BigDecimal) o2);
				else
					return new BigDecimal(o1.toString()).compareTo(new BigDecimal(o2.toString()));
			}
			else if (Date.class.isAssignableFrom(o1.getClass())) {
				if (Date.class.isAssignableFrom(o2.getClass()))
					return ((Date)o1).compareTo((Date) o2);
				else if (Calendar.class.isAssignableFrom(o2.getClass()))
					return Long.compare(((Date)o1).getTime(), ((Calendar) o2).getTimeInMillis());
			}
			else if (Calendar.class.isAssignableFrom(o1.getClass())) {
				if (Calendar.class.isAssignableFrom(o2.getClass()))
					return ((Calendar)o1).compareTo((Calendar) o2);
				else if (Date.class.isAssignableFrom(o2.getClass()))
					return Long.compare(((Calendar)o1).getTimeInMillis(), ((Date) o2).getTime());
			}
		} catch (Exception e) {
			throw new DatabaseSyntaxException("Unexpected exception ! ", e);
		}
		throw new DatabaseSyntaxException("Cannot compare " + o1 + " and " + o2);

	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RuleInstance that = (RuleInstance) o;
		return rule == that.rule && Objects.equals(parts, that.parts) && Objects.equals(cachedConstructors, that.cachedConstructors);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rule, parts, cachedConstructors);
	}

	public <T extends DatabaseRecord> boolean equals(Table<T> table, T record, Object o1, Object o2)
			throws DatabaseSyntaxException {
		try {
			if (o1==null) {
				return o2 == null || ((o2 instanceof String) && o2.equals(SymbolType.NULL.getContent()))
					|| ((o2 instanceof WrappedString) && o2.toString().equals(SymbolType.NULL.getContent()));
			}
			else {
				String s1=null;
				String s2=null;
				if (o1 instanceof String)
					s1=(String)o1;
				else if (o1 instanceof WrappedString)
					s1=o1.toString();
				if (o2 instanceof String)
					s2=(String)o2;
				else if (o2 instanceof WrappedString)
					s2=o2.toString();
				if (s1!=null && s2!=null && s1.equals(SymbolType.NULL.getContent()) && s2.equals(SymbolType.NULL.getContent()))
					return true;
			}
			
			if (o2!=null && ((!Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass())
					&& Table.FieldAccessorValue.class.isAssignableFrom(o2.getClass()))
					|| (!BigDecimal.class.isAssignableFrom(o1.getClass())
							&& BigDecimal.class.isAssignableFrom(o2.getClass())
							&& !Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass())))) {
				Object o = o1;
				o1 = o2;
				o2 = o;
			}
			if (BigDecimal.class.isAssignableFrom(o1.getClass())) {

				assert o2 != null;
				if (BigDecimal.class.isAssignableFrom(o2.getClass()))
					return o1.equals(o2);
				else if (Number.class.isAssignableFrom(o2.getClass())) {
					return o1.equals(new BigDecimal(o2.toString()));
				} else {
					return false;
				}
			} else if (Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass())) {
				Table.FieldAccessorValue fav = ((Table.FieldAccessorValue) o1);
				//DatabaseRecord dr = getRecordInstance(table, record, fa);

				if (fav.getValue() == null)
					return o2 == null;
				if (CharSequence.class.isAssignableFrom(fav.getFieldAccessor().getFieldClassType())
				|| WrappedString.class.isAssignableFrom(fav.getFieldAccessor().getFieldClassType()))
				{
					if (o2 instanceof String) {
						String s = (String) o2;
						if (s.startsWith("\"") && s.endsWith("\""))
							o2 = s.substring(1, s.length() - 1);
					} else if (o2 instanceof WrappedString)
					{
						String s = o2.toString();
						if (s.startsWith("\"") && s.endsWith("\""))
							o2 = s.substring(1, s.length() - 1);
					}
					return fav.getFieldAccessor().equals(fav.getValue(), o2);
				}

				else if (fav.getFieldAccessor().isComparable() && o2 instanceof BigDecimal) {
					Object v = fav.getFieldAccessor().getValue(fav.getValue());
					if (v == null)
						return false;
					else
						return o2.equals(new BigDecimal(v.toString()));
				} else {
					return fav.getFieldAccessor().equals(fav.getValue(), o2);
				}

			} else {
				return o1.equals(o2);
			}

		} catch (Exception e) {
			throw new DatabaseSyntaxException("Unexpected exception ! ", e);
		}
	}

	private Object getRecordInstance(Table<?> table, DatabaseRecord record, FieldAccessor fa)
			throws DatabaseException {
		return getRecordInstance(table, record, fa, new HashSet<>());
	}
	private Object getRecordInstance(Table<?> table, Object record, FieldAccessor fa, HashSet<Table<?>> tablesDone)
			throws DatabaseException {
		if (fa.getTableClass() == table.getClass() || record == null)
			return table.getFieldAccessorAndValue(record, fa.getFieldName()).getValue();
			//return record;
		else {
			if (tablesDone.contains(table))
				return null;
			tablesDone.add(table);
			for (ForeignKeyFieldAccessor fkfa : table.getForeignKeysFieldAccessors()) {
				Object dr = fkfa.getValue(record);

				dr = getRecordInstance(fkfa.getPointedTable(), dr, fa, tablesDone);
				if (dr != null)
					return dr;
			}
			return null;
		}

	}

	public <T extends DatabaseRecord> boolean like(Table<T> table, T record, Object o1, Object o2)
			throws DatabaseSyntaxException {
		try {
			if (String.class.isAssignableFrom(o1.getClass()) || WrappedString.class.isAssignableFrom(o1.getClass())) {
				throw new DatabaseSyntaxException(
						"When using like operator, the first field must be a table identifier");
			} else if (Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass()) && StringFieldAccessor.class.isAssignableFrom(((Table.FieldAccessorValue)o1).getFieldAccessor().getClass())) {
				if (o2 instanceof WrappedString)
					o2=o2.toString();
				if (String.class.isAssignableFrom(o2.getClass())) {
					Object o=(((Table.FieldAccessorValue)o1).getFieldAccessor())
							.getValue(getRecordInstance(table, record, ((Table.FieldAccessorValue)o1).getFieldAccessor()));

					String s1 = (o instanceof WrappedString)?o.toString():(String) o;
					String s2 = (String) o2;

					if (s2.startsWith("\"") && s2.endsWith("\""))
						s2 = s2.substring(1, s2.length() - 1);

					Pattern p = SymbolType.convertLikeStringToPattern(s2);
					return p.matcher(s1).matches();
				} else {
					return false;
				}
			} else {
				return false;
			}

		} catch (Exception e) {
			throw new DatabaseSyntaxException("Unexpected exception ! ", e);
		}

	}

	public <T extends DatabaseRecord> boolean isIndependentFromOtherTables(Table<T> table) throws DatabaseSyntaxException {
		switch (rule) {
			case COMPARE:
				if (parts.size() == 3) {
					RuleInstance ri2 = getRuleInstance(1);
					if (ri2.getRule().equals(Rule.QUERY)) {
						return ri2.isIndependentFromOtherTables(table);
					} else {
						RuleInstance ri1 = getRuleInstance(0);
						RuleInstance ri3 = getRuleInstance(2);

						return ri1.isIndependentFromOtherTables(table) && ri3.isIndependentFromOtherTables(table);
					}
				} else
					throw new IllegalAccessError();
			case NULLTEST:
			case INTEST:
				if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					return ri1.isIndependentFromOtherTables(table);
				} else
					throw new IllegalAccessError();
			case FACTOR:
			case EXPRESSION:
			case QUERY:
				if (parts.size() == 1) {
					return getRuleInstance(0).isIndependentFromOtherTables(table);
				} else if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri3 = getRuleInstance(2);

					return ri1.isIndependentFromOtherTables(table) && ri3.isIndependentFromOtherTables(table);
				} else
					throw new IllegalAccessError();
				case NULL:
					return true;
			case WORD:

				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();

						FieldAccessor fa = table.getFieldAccessor(fieldName);

						return fa.getTableClass() == table.getClass();
					} else
						return true;
				} else if (parts.size() == 3) {
					return getRuleInstance(1).isIndependentFromOtherTables(table);
				} else
					throw new IllegalAccessError();
			case OPCOMP:
			case OPCONDITION:
			case ISOP:
			case INOP:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
				throw new IllegalAccessError();
		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> boolean isConcernedBy(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case NULLTEST:
				if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri2 = getRuleInstance(1);
					RuleInstance ri3 = getRuleInstance(2);
					if (ri3.rule != Rule.NULL)
						throw new IllegalAccessError();
					Symbol s1 = ri1.getSymbol();
					if (s1.getType() != SymbolType.IDENTIFIER)
						throw new DatabaseSyntaxException("Cannot do null comparison without identifier");
					Symbol comp = ri2.getSymbol();
					if (comp.getType() == SymbolType.IS) {
						return equals(table, record, ri1.getEquallable(table, parameters, record),
								null);
					} else if (comp.getType() == SymbolType.ISNOT) {
						return !equals(table, record, ri1.getEquallable(table, parameters, record),
								null);
					} else
						throw new IllegalAccessError();
				} else
					throw new IllegalAccessError();
			case INTEST:
				if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri2 = getRuleInstance(1);
					RuleInstance ri3 = getRuleInstance(2);
					Symbol s1 = ri1.getSymbol();
					if (s1.getType() != SymbolType.IDENTIFIER)
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operators without identifier at left");
					Symbol s3 = ri3.getSymbol();
					if (s3.getType() != SymbolType.PARAMETER)
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operator without parameter of type Collection at right");
					Object o = parameters.get(s3.getSymbol());
					if (!(o instanceof Collection))
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operator without parameter of type Collection at right");
					Collection<?> parameter=(Collection<?>)o;
					Symbol comp = ri2.getSymbol();
					if (comp.getType() == SymbolType.IN) {
						for (Object p : parameter)
						{
							if (equals(table, record, ri1.getEquallable(table, parameters, record), p))
								return true;
						}
						return false;
					} else if (comp.getType() == SymbolType.NOTIN) {
						for (Object p : parameter)
						{
							if (equals(table, record, ri1.getEquallable(table, parameters, record), p))
								return false;
						}
						return true;
					} else
						throw new IllegalAccessError();
				} else
					throw new IllegalAccessError();
			case COMPARE:
				if (parts.size() == 3) {
					RuleInstance ri2 = getRuleInstance(1);
					if (ri2.getRule().equals(Rule.QUERY)) {
						return ri2.isConcernedBy(table, parameters, record);
					} else {
						RuleInstance ri1 = getRuleInstance(0);
						RuleInstance ri3 = getRuleInstance(2);

						Symbol comp = ri2.getSymbol();
						if (comp.getType() == SymbolType.EQUALOPERATOR) {
							return equals(table, record, ri1.getEquallable(table, parameters, record),
									ri3.getEquallable(table, parameters, record));
						} else if (comp.getType() == SymbolType.NOTEQUALOPERATOR) {
							return !equals(table, record, ri1.getEquallable(table, parameters, record),
									ri3.getEquallable(table, parameters, record));
						} else if (comp.getType() == SymbolType.LIKE) {
							return like(table, record, ri1.getStringable(table, parameters, record),
									ri3.getStringable(table, parameters, record));
						} else if (comp.getType() == SymbolType.NOTLIKE) {
							return !like(table, record, ri1.getStringable(table, parameters, record),
									ri3.getStringable(table, parameters, record));
						} else {
							int c = compareTo(table, record, ri1.getComparable(table, parameters, record),
									ri3.getComparable(table, parameters, record));
							if (comp.getType() == SymbolType.LOWEROPERATOR) {
								return c < 0;
							} else if (comp.getType() == SymbolType.GREATEROPERATOR) {
								return c > 0;
							} else if (comp.getType() == SymbolType.LOWEROREQUALOPERATOR) {
								return c <= 0;
							} else if (comp.getType() == SymbolType.GREATEROREQUALOPERATOR) {
								return c >= 0;
							} else
								throw new IllegalAccessError();
						}
					}
				} else
					throw new IllegalAccessError();
			case QUERY:
				if (parts.size() == 1) {
					return getRuleInstance(0).isConcernedBy(table, parameters, record);
				} else if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri3 = getRuleInstance(2);
					Symbol comp = getRuleInstance(1).getSymbol();

					boolean vri1 = ri1.isConcernedBy(table, parameters, record);
					boolean vri2 = ri3.isConcernedBy(table, parameters, record);
					if (comp.getType() == SymbolType.ANDCONDITION)
						return vri1 && vri2;
					else if (comp.getType() == SymbolType.ORCONDITION)
						return vri1 || vri2;
					else
						throw new IllegalAccessError();
				} else
					throw new IllegalAccessError();
			case WORD:
			case NULL:
			case ISOP:
			case INOP:
			case FACTOR:
			case EXPRESSION:
			case OPCOMP:
			case OPCONDITION:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
				throw new IllegalAccessError();
		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> StringBuilder translateToSqlQuery(Table<T> table, Map<String, Object> parameters,
			Map<Integer, Object> outputParameters, Set<TableJunction> tablesJunction) throws DatabaseSyntaxException {
		return translateToSqlQuery(table, parameters, outputParameters, 1, tablesJunction);
	}

	public <T extends DatabaseRecord> StringBuilder translateToSqlQuery(Table<T> table, Map<String, Object> parameters,
			Map<Integer, Object> outputParameters, int firstParameterIndex, Set<TableJunction> tablesJunction)
			throws DatabaseSyntaxException {
		return translateToSqlQuery(table, parameters, outputParameters, new AtomicInteger(firstParameterIndex),
				tablesJunction);
	}

	public static class TableJunction {
		private final Table<?> tablePointing, tablePointed;
		private final ForeignKeyFieldAccessor fieldAccessor;

		public TableJunction(Table<?> _tablePointing, Table<?> _tablePointed, ForeignKeyFieldAccessor fieldAccessor) {
			super();
			tablePointing = _tablePointing;
			tablePointed = _tablePointed;
			this.fieldAccessor = fieldAccessor;
		}

		public Table<?> getTablePointing() {
			return tablePointing;
		}

		public Table<?> getTablePointed() {
			return tablePointed;
		}

		public ForeignKeyFieldAccessor getFieldAccessor() {
			return fieldAccessor;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof TableJunction) {
				TableJunction tj = (TableJunction) o;
				return tablePointing.equals(tj.tablePointing) && tablePointed.equals(tj.tablePointed);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return tablePointing.hashCode() + tablePointed.hashCode();
		}
	}

	private RuleInstance getRuleInstance(int index) throws DatabaseSyntaxException {
		if (parts.size()<=index)
			throw new DatabaseSyntaxException("Impossible to find a rule instance number "+index+" (parts size="+parts.size()+")");
		assert parts.size()>0;
		QueryPart qp=parts.get(index);
		if (qp instanceof RuleInstance)
			return (RuleInstance) qp;
		throw new DatabaseSyntaxException("Impossible to find a rule instance number "+index+". Found query part of type "+(qp==null?"null":qp.getClass().getName()));
	}

	private Symbol getSymbol() throws DatabaseSyntaxException {
		if (parts.size()!=1)
			throw new DatabaseSyntaxException("Impossible to find a symbol with a rule instance that contains "+parts.size()+" parts (!=1)");
		QueryPart qp=parts.get(0);
		if (qp instanceof Symbol)
			return (Symbol) qp;
		else if (qp instanceof RuleInstance)
			return ((RuleInstance) qp).getSymbol();
		else
			throw new IllegalAccessError();
	}

	<T extends DatabaseRecord> StringBuilder translateToSqlQuery(Table<T> table, Map<String, Object> parameters,
			Map<Integer, Object> outputParameters, AtomicInteger currentParameterID, Set<TableJunction> tablesJunction)
			throws DatabaseSyntaxException {
		switch (rule) {
			case COMPARE:
				if (parts.size() == 3) {
					RuleInstance ri2 = getRuleInstance(1);
					if (ri2.getRule().equals(Rule.QUERY)) {
						StringBuilder tmp = ri2.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
								tablesJunction);
						StringBuilder sb = new StringBuilder(tmp.length() + 2);
						sb.append("(");
						sb.append(tmp);
						sb.append(")");
						return sb;
					} else {
						RuleInstance ri1 = getRuleInstance(0);
						RuleInstance ri3 = getRuleInstance(2);
						Symbol comp = ri2.getSymbol();
						if (comp.getType() == SymbolType.EQUALOPERATOR || comp.getType() == SymbolType.NOTEQUALOPERATOR
								|| comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE) {
							if (!ri1.isEqualable(table, parameters, ri3)) {
								throw new DatabaseSyntaxException(
										"Cannot compare " + ri1.getContent() + " and " + ri3.getContent());
							}
							Symbol s1 = ri1.getSymbol();
							Symbol s2 = ri3.getSymbol();

							if (s1.getType() != SymbolType.IDENTIFIER && s2.getType() != SymbolType.IDENTIFIER)
								throw new DatabaseSyntaxException("Cannot do comparison between two paramters");
							FieldAccessor fa1 = null, fa2 = null;
							Object parameter1 = null, parameter2 = null;
							Reference<String> sqlTableName1 = new Reference<>();
							//Reference<String> sqlTableName2=new Reference<>();
							if (s1.getType() == SymbolType.IDENTIFIER) {
								String fieldName = s1.getSymbol();
								fa1 = table.getFieldAccessor(fieldName, tablesJunction, sqlTableName1);
								if (fa1 == null)
									throw new DatabaseSyntaxException(
											"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
							} else if (s1.getType() == SymbolType.PARAMETER) {
								parameter1 = parameters.get(s1.getSymbol());
								if (parameter1 == null)
									throw new DatabaseSyntaxException("Cannot find parameter " + s1.getSymbol());
							} else if (s1.getType() == SymbolType.NUMBER || s1.getType() == SymbolType.STRING || s1.getType() == SymbolType.NULL) {
								parameter1 = s1.getContent();
								if (parameter1 == null)
									throw new IllegalAccessError();
							} else
								throw new IllegalAccessError();

							if (s2.getType() == SymbolType.IDENTIFIER) {
								String fieldName = s2.getSymbol();
								fa2 = table.getFieldAccessor(fieldName, tablesJunction, sqlTableName1);
								if (fa2 == null)
									throw new DatabaseSyntaxException(
											"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
							} else if (s2.getType() == SymbolType.PARAMETER) {
								parameter2 = parameters.get(s2.getSymbol());

							} else if (s2.getType() == SymbolType.NUMBER || s2.getType() == SymbolType.STRING || s2.getType() == SymbolType.NULL) {
								parameter2 = s2.getContent();
								if (parameter2 == null)
									throw new IllegalAccessError();
							} else
								throw new IllegalAccessError();

							if ((s1.getType() == SymbolType.PARAMETER || s1.getType() == SymbolType.NUMBER
									|| s1.getType() == SymbolType.STRING || s1.getType() == SymbolType.NULL) && s2.getType() == SymbolType.IDENTIFIER) {
								fa1 = fa2;
								//sqlTableName1=sqlTableName2;
								fa2 = null;
								parameter2 = parameter1;
								s2 = s1;
							}
							int fieldsNumber = 0;

							assert fa1 != null;
							SqlField[] sfs = fa1.getDeclaredSqlFields();
							if (s2.getType() == SymbolType.STRING || s2.getType() == SymbolType.NUMBER) {
								if (sfs.length != 1
										|| ((comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE)
										&& s2.getType() != SymbolType.STRING))
									throw new DatabaseSyntaxException("Cannot compare " + fa1.getFieldName() + " with "
											+ s2.getType().name() + " using operator " + comp.getType().name());
								StringBuilder res = new StringBuilder();
								res.append(sqlTableName1.get())
										.append(".")
										.append(sfs[0].shortField)
										.append(comp.getType().getContent());
								assert parameter2 != null;
								if (parameter2 instanceof BigDecimal) {
									int id = currentParameterID.getAndIncrement();
									res.append("?");
									String t = DatabaseWrapperAccessor.getBigDecimalType(table.getDatabaseWrapper(), 64);
									outputParameters.put(id, t.contains("CHAR") ? parameter2.toString() : t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper())) ? BigDecimalFieldAccessor.bigDecimalToBytes((BigDecimal) parameter2) : parameter2);
								} else if (parameter2 instanceof BigInteger) {
									int id = currentParameterID.getAndIncrement();
									res.append("?");
									String t = DatabaseWrapperAccessor.getBigIntegerType(table.getDatabaseWrapper(), 64);
									outputParameters.put(id, t.contains("CHAR") ? parameter2.toString() : t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper())) ? ((BigInteger) parameter2).toByteArray() : new BigDecimal(((BigInteger) parameter2)));
								} else
									res.append(parameter2);
								return res;
							} else {
								StringBuilder res = new StringBuilder();
								if ((comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE)
										&& (!(parameter2 instanceof CharSequence) && !(parameter2 instanceof WrappedString)))
									throw new DatabaseSyntaxException("Cannot compare " + fa1.getFieldName() + " with "
											+ s2.getType().name() + " using operator " + comp.getType().name());
								if (comp.getType() == SymbolType.EQUALOPERATOR
										|| comp.getType() == SymbolType.NOTEQUALOPERATOR)
									res.append("(");
								SqlFieldInstance[] sfis = null;
								try {
									if (fa2 == null) {
										Class<?> c = fa1.getField().getDeclaringClass();
										Constructor<?> cons;
										if (Modifier.isAbstract(c.getModifiers()))
											cons = table.getDefaultRecordConstructor();
										else
											cons = getConstructor(c);
										Object value = cons.newInstance();
										fa1.setValue(value, parameter2);
										sfis = fa1.getSqlFieldsInstances(sqlTableName1.get(), value);
									}
								} catch (Exception e) {
									throw new DatabaseSyntaxException("Database exception with " + fa1.getField().getDeclaringClass(), e);
								}
								SqlField[] sfs2 = null;
								if (fa2 != null)
									sfs2 = fa2.getDeclaredSqlFields();

								for (SqlField sf : sfs) {

									++fieldsNumber;
									if (fieldsNumber > 1)
										res.append(" AND ");
									res.append(sqlTableName1.get())
											.append(".")
											.append(sf.shortField);
									res.append(comp.getType().getContent());
									if (fa2 == null) {
										boolean found = false;
										for (SqlFieldInstance sfi : sfis) {
											if (sfi.field.equals(sf.field)) {

												if (parameter2 == null) {
													res.append("NULL");
												} else {
													int id = currentParameterID.getAndIncrement();
													res.append("?");
													outputParameters.put(id, sfi.instance);
												}
												found = true;
												break;
											}
										}
										if (!found)
											throw new DatabaseSyntaxException(
													"Field " + sf.field + " not found. Unexpected error !");

									} else {
										if (fa2.getFieldName().equals(fa1.getFieldName()))
											throw new DatabaseSyntaxException(
													"Cannot compare two same fields : " + fa1.getFieldName());
										if (fa1.getClass() != fa2.getClass())
											throw new DatabaseSyntaxException(
													"Cannot compare two fields whose type is different : "
															+ fa1.getFieldName() + " and " + fa2.getFieldName());
										res.append(sqlTableName1.get())
												.append(".")
												.append(sfs2[fieldsNumber - 1].shortField);
									}
								}
								if (comp.getType() == SymbolType.EQUALOPERATOR
										|| comp.getType() == SymbolType.NOTEQUALOPERATOR)
									res.append(")");
								return res;
							}
						}
						else {
							String typeRi1=ri1.getValueType(table, parameters);
							String typeRi3=ri3.getValueType(table, parameters);
							if (!(typeRi1.equals(SymbolType.STRING.name()) || typeRi3.equals(SymbolType.STRING.name())))
							{
								if (!ri1.getValueType(table, parameters).equals("comparable"))
									throw new DatabaseSyntaxException("Left element not comparable : "+ri1.getContent());
								if (!ri3.getValueType(table, parameters).equals("comparable"))
									throw new DatabaseSyntaxException("Left element not comparable : "+ri3.getContent());
								if (comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE)
									throw new DatabaseSyntaxException("Invalid operator "+comp+" with "+typeRi1+" type with "+typeRi3+" type");

							}

							return new StringBuilder(ri1.needParenthesis()?"(":"")
									.append(ri1.translateToSqlQuery(table, parameters, outputParameters, currentParameterID, tablesJunction))
									.append(ri1.needParenthesis()?")":"")
									.append(comp.getType().getContent())
									.append(ri3.needParenthesis()?"(":"")
									.append(ri3.translateToSqlQuery(table, parameters, outputParameters, currentParameterID, tablesJunction))
									.append(ri3.needParenthesis()?")":"");
						}
					}
				} else
					throw new IllegalAccessError();
			case NULLTEST:
				if (parts.size() == 3) {
					RuleInstance ri2 = getRuleInstance(1);

					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri3 = getRuleInstance(2);
					if (ri3.rule!=Rule.NULL)
						throw new IllegalAccessError();

					Symbol comp = ri2.getSymbol();

					if (comp.getType() == SymbolType.IS || comp.getType() == SymbolType.ISNOT) {
						Symbol s1 = ri1.getSymbol();
						if (s1.getType() != SymbolType.IDENTIFIER)
							throw new DatabaseSyntaxException("Cannot do null comparison without identifier");
						String fieldName = s1.getSymbol();
						Reference<String> sqlTableName=new Reference<>();
						FieldAccessor fa1 = table.getFieldAccessor(fieldName, tablesJunction, sqlTableName);
						if (fa1 == null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());


						SqlField[] sfs = fa1.getDeclaredSqlFields();
						StringBuilder res = new StringBuilder();
						res.append("(");
						int fieldsNumber = 0;
						for (SqlField sf : sfs) {
							++fieldsNumber;
							if (fieldsNumber > 1)
								res.append(" AND ");
							res.append(sqlTableName.get())
									.append(".")
									.append(sf.shortField);
							res.append(comp.getType().getContent())
									.append(SymbolType.NULL.getContent());
						}
						res.append(")");
						return res;
					}
					else
						throw new IllegalAccessError();
				} else
					throw new IllegalAccessError();
			case INTEST:
				if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri2 = getRuleInstance(1);
					RuleInstance ri3 = getRuleInstance(2);
					Symbol s1 = ri1.getSymbol();
					if (s1.getType() != SymbolType.IDENTIFIER)
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operators without identifier at left");
					Symbol s3 = ri3.getSymbol();
					if (s3.getType() != SymbolType.PARAMETER)
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operator without parameter of type Collection at right");
					Object o = parameters.get(s3.getSymbol());
					if (!(o instanceof Collection))
						throw new DatabaseSyntaxException("Cannot use IN/NOT IN operator without parameter of type Collection at right");
					Collection<?> parameter=(Collection<?>)o;

					Symbol comp = ri2.getSymbol();
					if (comp.getType() == SymbolType.IN || comp.getType() == SymbolType.NOTIN) {


						if (parameter.size()==0)
							return new StringBuilder();
						String fieldName = s1.getSymbol();
						Reference<String> sqlTableName=new Reference<>();
						FieldAccessor fa1 = table.getFieldAccessor(fieldName, tablesJunction, sqlTableName);
						if (fa1 == null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
						SqlField[] sfs = fa1.getDeclaredSqlFields();

						if (sfs.length==1)
						{
							StringBuilder res = new StringBuilder(sfs[0].field)
									.append(comp.getType().getContent())
									.append("(");
							int parameterIndex=0;
							for (Object p : parameter) {
								if (parameterIndex > 0)
									res.append(", ");
								if (p==null)
									res.append("NULL");
								else {
									SqlFieldInstance[] sfis;
									try {
										Class<?> c = fa1.getField().getDeclaringClass();
										Constructor<?> cons;
										if (Modifier.isAbstract(c.getModifiers()))
											cons = table.getDefaultRecordConstructor();
										else
											cons = getConstructor(c);
										Object value = cons.newInstance();
										fa1.setValue(value, p);
										sfis = fa1.getSqlFieldsInstances(sqlTableName.get(), value);
									} catch (Exception e) {
										throw new DatabaseSyntaxException("Database exception with " + fa1.getField().getDeclaringClass(), e);
									}
									int id = currentParameterID.getAndIncrement();
									res.append("?");
									outputParameters.put(id, sfis[0].instance);
								}
								++parameterIndex;
							}
							res.append(")");
							return res;
						}
						else {

							String cond;
							if (comp.getType() == SymbolType.IN) {
								comp = new Symbol(SymbolType.EQUALOPERATOR, "=");
								cond = " OR ";
							} else {
								comp = new Symbol(SymbolType.NOTEQUALOPERATOR, "!=");
								cond = " AND ";
							}
							StringBuilder res = new StringBuilder("(");

							int parameterIndex = 0;
							for (Object p : parameter) {
								if (parameterIndex > 0)
									res.append(cond);
								SqlFieldInstance[] sfis;
								try {
									Class<?> c = fa1.getField().getDeclaringClass();
									Constructor<?> cons;
									if (Modifier.isAbstract(c.getModifiers()))
										cons = table.getDefaultRecordConstructor();
									else
										cons = getConstructor(c);
									Object value = cons.newInstance();
									fa1.setValue(value, p);
									sfis = fa1.getSqlFieldsInstances(sqlTableName.get(), value);
								} catch (Exception e) {
									throw new DatabaseSyntaxException("Database exception with " + fa1.getField().getDeclaringClass(), e);
								}


								res.append("(");
								int fieldsNumber = 0;
								for (SqlField sf : sfs) {


									++fieldsNumber;
									if (fieldsNumber > 1)
										res.append(" AND ");
									res.append(sqlTableName.get())
											.append(".")
											.append(sf.shortField)
											.append(comp.getType().getContent());
									boolean found = false;
									for (SqlFieldInstance sfi : sfis) {
										if (sfi.field.equals(sf.field)) {

											if (p == null) {
												res.append("NULL");
											} else {
												int id = currentParameterID.getAndIncrement();
												res.append("?");
												outputParameters.put(id, sfi.instance);
											}
											found = true;
											break;
										}
									}
									if (!found)
										throw new DatabaseSyntaxException(
												"Field " + sf.field + " not found. Unexpected error !");
								}
								res.append(")");
								++parameterIndex;
							}
							res.append(")");
							return res;
						}
					}
					else
						throw new IllegalAccessError();


				} else
					throw new IllegalAccessError();
			case FACTOR:
			case EXPRESSION:
			case QUERY:
				if (parts.size() == 1) {
					return getRuleInstance(0).translateToSqlQuery(table, parameters, outputParameters,
							currentParameterID, tablesJunction);
				} else if (parts.size() == 3) {
					RuleInstance ri1 = getRuleInstance(0);
					RuleInstance ri3 = getRuleInstance(2);
					Symbol op = getRuleInstance(1).getSymbol();
					if (rule == Rule.QUERY) {
						if (!ri1.getValueType(table, parameters).equals("boolean")
								|| !ri3.getValueType(table, parameters).equals("boolean"))
							throw new DatabaseSyntaxException(
									"Cannot make test with " + ri1.getContent() + " and " + ri3.getContent());
					} else {
						if (op.getType().isMathematicalOperator())
						{
							if (!ri1.isAlgebraic(table, parameters)
									|| !ri3.isAlgebraic(table, parameters))
								throw new DatabaseSyntaxException(
										"Cannot mul/div/add/sub/mod " + ri1.getContent() + " and " + ri3.getContent());
						}
						else {
							if (!ri1.getValueType(table, parameters).equals("comparable")
									|| !ri3.getValueType(table, parameters).equals("comparable"))
								throw new DatabaseSyntaxException(
										"Cannot compare " + ri1.getContent() + " and " + ri3.getContent());
						}
					}

					StringBuilder sb = new StringBuilder();
					/*
					 * if (ri1.needParenthesis()) sb.append("(");
					 */
					sb.append(ri1.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
							tablesJunction));
					/*
					 * if (ri1.needParenthesis()) sb.append(")");
					 */

					sb.append(" ");
					sb.append(op.getType().getContent());
					sb.append(" ");
					/*
					 * if (ri3.needParenthesis()) sb.append("(");
					 */
					sb.append(ri3.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
							tablesJunction));
					/*
					 * if (ri3.needParenthesis()) sb.append(")");
					 */
					return sb;
				} else
					throw new IllegalAccessError();

			case OPCOMP:
			case OPCONDITION:
			case ISOP:
			case MULTIPLY_OPERATOR:
			case ADD_OPERATOR:
			case INOP:
				throw new IllegalAccessError();
			case NULL: {
				if (parts.size() == 1)
					return new StringBuilder( getSymbol().getSymbol());
				else
					throw new IllegalAccessError();
			}
			case WORD:
				if (parts.size() == 1) {
					Symbol s = getSymbol();
					if (s.getType() == SymbolType.IDENTIFIER) {
						String fieldName = s.getSymbol();
						Reference<String> sqlTableName=new Reference<>();
						FieldAccessor fa = table.getFieldAccessor(fieldName, tablesJunction, sqlTableName);
						if (fa == null)
							throw new DatabaseSyntaxException(
									"Cannot find field " + fieldName + " into table " + table.getClass().getSimpleName());
						SqlField[] sfs = fa.getDeclaredSqlFields();
						if (!fa.isComparable())
							throw new IllegalAccessError();
						StringBuilder res=new StringBuilder();
						res.append(sqlTableName.get())
								.append(".")
								.append(sfs[0].shortField);
						return res;
					} else if (s.getType() == SymbolType.PARAMETER) {
						Object parameter1 = parameters.get(s.getSymbol());
						if (parameter1 == null)
							throw new DatabaseSyntaxException("Cannot find parameter " + s.getSymbol());
						SqlFieldInstance[] sfis;
						try {
							T record = table.getDefaultRecordConstructor().newInstance();
							FieldAccessor fa = Symbol.setFieldAccessor(table, parameter1, record);
							if (fa == null)
								throw new DatabaseSyntaxException(
										"Cannot find parameter type " + parameter1.getClass().getName() + " of "
												+ s.getSymbol() + " into table " + table.getClass().getName());
							if (!fa.isComparable())
								throw new DatabaseSyntaxException("Field accessor field must be comparable ");

							sfis = fa.getSqlFieldsInstances(table.getSqlTableName(), getRecordInstance(table, record, fa));
						} catch (DatabaseSyntaxException de) {
							throw de;
						} catch (Exception e) {
							throw new DatabaseSyntaxException("Database exception", e);
						}
						int id = currentParameterID.getAndIncrement();
						if (sfis == null)
							throw new DatabaseSyntaxException("Database exception");
						outputParameters.put(id, sfis[0].instance);
						return new StringBuilder("?");
					} else if (s.getType()==SymbolType.NUMBER) {
						Object n=s.getContent();
						if (n==null)
							throw new DatabaseSyntaxException(s.getContent() + " is not a number !");

						if (n instanceof BigDecimal)
						{
							StringBuilder sb=new StringBuilder("?");
							int id = currentParameterID.getAndIncrement();
							String t=DatabaseWrapperAccessor.getBigDecimalType(table.getDatabaseWrapper(), 64);
							outputParameters.put(id, t.contains("CHAR")?n.toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? BigDecimalFieldAccessor.bigDecimalToBytes((BigDecimal) n):n);
							return sb;
						}
						else if (n instanceof BigInteger)
						{
							StringBuilder sb=new StringBuilder("?");
							int id = currentParameterID.getAndIncrement();
							String t=DatabaseWrapperAccessor.getBigIntegerType(table.getDatabaseWrapper(), 64);
							outputParameters.put(id, t.contains("CHAR")?n.toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? ((BigInteger)n).toByteArray():new BigDecimal(((BigInteger)n)));
							return sb;
						}
						else
							return new StringBuilder(n.toString());
					} else
						return new StringBuilder(s.getSymbol());

				} else if (parts.size() == 3) {
					StringBuilder sb = new StringBuilder();
					sb.append("(");
					sb.append(getRuleInstance(1).translateToSqlQuery(table, parameters, outputParameters,
							currentParameterID, tablesJunction));
					sb.append(")");
					return sb;
				} else
					throw new IllegalAccessError();

		}
		throw new IllegalAccessError(""+rule);
	}

	private final Map<Class<?>, Constructor<?>> cachedConstructors=new HashMap<>();

	private Constructor<?> getConstructor(final Class<?> declaringClass) throws PrivilegedActionException {
		synchronized (cachedConstructors)
		{
			Constructor<?> c=cachedConstructors.get(declaringClass);
			if (c==null)
			{
				c=AccessController.doPrivileged((PrivilegedExceptionAction<Constructor<?>>) () -> {
					Constructor<?> c1 =declaringClass.getDeclaredConstructor();
					c1.setAccessible(true);
					return c1;

				});
				cachedConstructors.put(declaringClass, c);
			}
			return c;
		}

	}

}
