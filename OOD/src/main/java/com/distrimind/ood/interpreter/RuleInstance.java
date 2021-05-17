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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
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
		case COMPARE:
			case QUERY:
			case OPCONDITION:
			case OPCOMP:
			case NULLTEST:
			case ISOP:
			case NULL:
				return false;
		case EXPRESSION:
			return parts.get(0).isMultiType(table, parameters);
			case TERME: {
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
				return s.isMultiType(table, parameters);
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

		case EXPRESSION:
			return parts.get(0).getValueType(table, parameters);
		case ISOP:
		case NULLTEST:
		case COMPARE:
		case OPCOMP:
		case OPCONDITION:
		case QUERY:
			return "boolean";
		case NULL:
		case TERME:
			if (parts.size() == 1)
				return parts.get(0).getValueType(table, parameters);
			else
				return parts.get(1).getValueType(table, parameters);

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
			case NULLTEST:
			case NULL:
				throw new IllegalAccessError();
		case EXPRESSION:
			return ((RuleInstance) parts.get(0)).getComparable(table, parameters, record);
			case TERME:
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
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
				return ((RuleInstance) parts.get(1)).getNumber(table, parameters, record);
			} else
				throw new IllegalAccessError();

		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> Object getEquallable(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case EXPRESSION:
			return ((RuleInstance) parts.get(0)).getEquallable(table, parameters, record);
			case OPCOMP:
			case OPCONDITION:
			case COMPARE:
			case QUERY:
			case ISOP:
			case NULL:
			case NULLTEST:
			throw new IllegalAccessError();
			case TERME:
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
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
				return ((RuleInstance) parts.get(1)).getEquallable(table, parameters, record);
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
			case OPCONDITION:
			case ISOP:
			case NULLTEST:
				throw new IllegalAccessError();
		case EXPRESSION:
			return ((RuleInstance) parts.get(0)).getStringable(table, parameters, record);
			case NULL:
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
				return s.getContent().toString();
			}
			else
				throw new IllegalAccessError();
		case TERME:
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
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
					if (CharSequence.class.isAssignableFrom(parameter1.getClass())) {
						return ((CharSequence) parameter1).toString();
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
			if (o1==null)
				return o2==null || ((o2 instanceof String) && o2.equals(SymbolType.NULL.getContent()));
			else if ((o1 instanceof String) && (o2 instanceof String) && o1.equals(SymbolType.NULL.getContent()) && o2.equals(SymbolType.NULL.getContent()))
				return true;
			
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
				if (CharSequence.class.isAssignableFrom(fav.getFieldAccessor().getFieldClassType())) {
					if (o2 instanceof String) {
						String s = (String) o2;
						if (s.startsWith("\"") && s.endsWith("\""))
							o2 = s.substring(1, s.length() - 1);
					}
					return fav.getFieldAccessor().equals(fav.getValue(), o2);
				} else if (fav.getFieldAccessor().isComparable() && o2 instanceof BigDecimal) {
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
			if (String.class.isAssignableFrom(o1.getClass())) {
				throw new DatabaseSyntaxException(
						"When using like operator, the first field must be a table identifier");
			} else if (Table.FieldAccessorValue.class.isAssignableFrom(o1.getClass()) && StringFieldAccessor.class.isAssignableFrom(((Table.FieldAccessorValue)o1).getFieldAccessor().getClass())) {
				if (String.class.isAssignableFrom(o2.getClass())) {
					String s1 = (String) (((Table.FieldAccessorValue)o1).getFieldAccessor())
							.getValue(getRecordInstance(table, record, ((Table.FieldAccessorValue)o1).getFieldAccessor()));
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

	public <T extends DatabaseRecord> boolean isIndependantFromOtherTables(Table<T> table) {
		switch (rule) {
		case COMPARE:
			if (parts.size() == 3) {
				RuleInstance ri2 = (RuleInstance) parts.get(1);
				if (ri2.getRule().equals(Rule.QUERY)) {
					return ri2.isIndependantFromOtherTables(table);
				} else {
					RuleInstance ri1 = (RuleInstance) parts.get(0);
					RuleInstance ri3 = (RuleInstance) parts.get(2);

					return ri1.isIndependantFromOtherTables(table) && ri3.isIndependantFromOtherTables(table);
				}
			} else
				throw new IllegalAccessError();
			case NULLTEST:
				if (parts.size() == 3) {
					RuleInstance ri1 = (RuleInstance) parts.get(0);
					return ri1.isIndependantFromOtherTables(table);
				} else
					throw new IllegalAccessError();
		case QUERY:
			if (parts.size() == 1) {
				return ((RuleInstance) parts.get(0)).isIndependantFromOtherTables(table);
			} else if (parts.size() == 3) {
				RuleInstance ri1 = (RuleInstance) parts.get(0);
				RuleInstance ri3 = (RuleInstance) parts.get(2);

				return ri1.isIndependantFromOtherTables(table) && ri3.isIndependantFromOtherTables(table);
			} else
				throw new IllegalAccessError();
			case NULL:
				return true;
		case TERME:

			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
				if (s.getType() == SymbolType.IDENTIFIER) {
					String fieldName = s.getSymbol();

					FieldAccessor fa = table.getFieldAccessor(fieldName);

					return fa.getTableClass() == table.getClass();
				} else
					return true;
			} else if (parts.size() == 3) {
				return ((RuleInstance) parts.get(1)).isIndependantFromOtherTables(table);
			} else
				throw new IllegalAccessError();

		case EXPRESSION:
			return ((RuleInstance) parts.get(0)).isIndependantFromOtherTables(table);
		case OPCOMP:
		case OPCONDITION:
			case ISOP:
			throw new IllegalAccessError();
		}
		throw new IllegalAccessError();
	}

	public <T extends DatabaseRecord> boolean isConcernedBy(Table<T> table, Map<String, Object> parameters, T record)
			throws DatabaseException {
		switch (rule) {
			case NULLTEST:
				if (parts.size() == 3) {
					RuleInstance ri2 = (RuleInstance) parts.get(1);

					RuleInstance ri1 = (RuleInstance) parts.get(0);
					RuleInstance ri3 = (RuleInstance) parts.get(2);
					if (ri3.rule != Rule.NULL)
						throw new IllegalAccessError();
					Symbol s1 = ((Symbol)ri1.parts.get(0));
					if (s1.getType() != SymbolType.IDENTIFIER)
						throw new DatabaseSyntaxException("Cannot do null comparison without identifier");
					Symbol comp = (Symbol) ri2.parts.get(0);
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
		case COMPARE:
			if (parts.size() == 3) {
				RuleInstance ri2 = (RuleInstance) parts.get(1);
				if (ri2.getRule().equals(Rule.QUERY)) {
					return ri2.isConcernedBy(table, parameters, record);
				} else {
					RuleInstance ri1 = (RuleInstance) parts.get(0);
					RuleInstance ri3 = (RuleInstance) parts.get(2);

					Symbol comp = (Symbol) ri2.parts.get(0);
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
				return ((RuleInstance) parts.get(0)).isConcernedBy(table, parameters, record);
			} else if (parts.size() == 3) {
				RuleInstance ri1 = (RuleInstance) parts.get(0);
				RuleInstance ri3 = (RuleInstance) parts.get(2);
				Symbol comp = (Symbol) ((RuleInstance) parts.get(1)).parts.get(0);

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
		case TERME:
		case NULL:
		case ISOP:
		case EXPRESSION:
		case OPCOMP:
		case OPCONDITION:
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

	<T extends DatabaseRecord> StringBuilder translateToSqlQuery(Table<T> table, Map<String, Object> parameters,
			Map<Integer, Object> outputParameters, AtomicInteger currentParameterID, Set<TableJunction> tablesJunction)
			throws DatabaseSyntaxException {
		switch (rule) {
		case COMPARE:
			if (parts.size() == 3) {
				RuleInstance ri2 = (RuleInstance) parts.get(1);
				if (ri2.getRule().equals(Rule.QUERY)) {
					StringBuilder tmp = ri2.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
							tablesJunction);
					StringBuilder sb = new StringBuilder(tmp.length() + 2);
					sb.append("(");
					sb.append(tmp);
					sb.append(")");
					return sb;
				} else {
					RuleInstance ri1 = (RuleInstance) parts.get(0);
					RuleInstance ri3 = (RuleInstance) parts.get(2);
					Symbol comp = (Symbol) ri2.parts.get(0);

					if (comp.getType() == SymbolType.EQUALOPERATOR || comp.getType() == SymbolType.NOTEQUALOPERATOR
							|| comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE) {
						if (!ri1.isEqualable(table, parameters, ri3)) {
							throw new DatabaseSyntaxException(
									"Cannot compare " + ri1.getContent() + " and " + ri3.getContent());
						}
						// if (ri1.isMultiType(table, parameters))
						{
							Symbol s1 = ((Symbol) (((RuleInstance) ri1.parts.get(0)).parts.get(0)));
							Symbol s2 = ((Symbol) (((RuleInstance) ri3.parts.get(0)).parts.get(0)));

							if (s1.getType() != SymbolType.IDENTIFIER && s2.getType() != SymbolType.IDENTIFIER)
								throw new DatabaseSyntaxException("Cannot do comparison between two paramters");
							FieldAccessor fa1 = null, fa2 = null;
							Object parameter1 = null, parameter2 = null;
							Reference<String> sqlTableName1=new Reference<>();
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
							} else if (s1.getType() == SymbolType.NUMBER || s1.getType() == SymbolType.STRING || s1.getType()==SymbolType.NULL) {
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

							} else if (s2.getType() == SymbolType.NUMBER || s2.getType() == SymbolType.STRING || s2.getType()==SymbolType.NULL) {
								parameter2 = s2.getContent();
								if (parameter2 == null)
									throw new IllegalAccessError();
							} else
								throw new IllegalAccessError();

							if ((s1.getType() == SymbolType.PARAMETER || s1.getType() == SymbolType.NUMBER
									|| s1.getType() == SymbolType.STRING || s1.getType()==SymbolType.NULL) && s2.getType() == SymbolType.IDENTIFIER) {
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
								res.append(sfs[0].field);
								res.append(comp.getType().getContent());
								assert parameter2 != null;
								if (parameter2 instanceof BigDecimal)
								{
									int id = currentParameterID.getAndIncrement();
									res.append("?");
									String t=DatabaseWrapperAccessor.getBigDecimalType(table.getDatabaseWrapper(), 64);
									outputParameters.put(id, t.contains("CHAR")?parameter2.toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? BigDecimalFieldAccessor.bigDecimalToBytes((BigDecimal)parameter2):parameter2);
								}
								else if (parameter2 instanceof BigInteger)
								{
									int id = currentParameterID.getAndIncrement();
									res.append("?");
									String t=DatabaseWrapperAccessor.getBigIntegerType(table.getDatabaseWrapper(), 64);
									outputParameters.put(id, t.contains("CHAR")?parameter2.toString():t.contains(DatabaseWrapperAccessor.getBinaryBaseWord(table.getDatabaseWrapper()))? ((BigInteger)parameter2).toByteArray():new BigDecimal(((BigInteger)parameter2)));
								}
								else
									res.append(parameter2);
								return res;
							} else {
								StringBuilder res = new StringBuilder();
								if ((comp.getType() == SymbolType.LIKE || comp.getType() == SymbolType.NOTLIKE)
										&& (!(parameter2 instanceof CharSequence)))
									throw new DatabaseSyntaxException("Cannot compare " + fa1.getFieldName() + " with "
											+ s2.getType().name() + " using operator " + comp.getType().name());
								if (comp.getType() == SymbolType.EQUALOPERATOR
										|| comp.getType() == SymbolType.NOTEQUALOPERATOR)
									res.append("(");
								SqlFieldInstance[] sfis = null;
								try {
									if (fa2 == null) {
										Class<?> c=fa1.getField().getDeclaringClass();
										Constructor<?> cons;
										if (Modifier.isAbstract(c.getModifiers()))
											cons=table.getDefaultRecordConstructor();
										else
											cons=getConstructor(c);
										Object value=cons.newInstance();
										fa1.setValue(value, parameter2);
										sfis = fa1.getSqlFieldsInstances(sqlTableName1.get(), value);
									}
								} catch (Exception e) {
									throw new DatabaseSyntaxException("Database exception with "+fa1.getField().getDeclaringClass(), e);
								}
								SqlField[] sfs2 = null;
								if (fa2 != null)
									sfs2 = fa2.getDeclaredSqlFields();

								for (SqlField sf : sfs) {
									String sql_field_name=sqlTableName1.get()+"."+sf.shortFieldWithoutQuote;
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
											if (sfi.fieldWithoutQuote.equals(sql_field_name)) {

												if (parameter2==null)
												{
													res.append("NULL");
												}
												else
												{
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
													"Field " + sql_field_name + " not found. Unexpected error !");

									} else {
										if (fa2.getFieldName().equals(fa1.getFieldName()))
											throw new DatabaseSyntaxException(
													"Cannot compare two same fields : " + fa1.getFieldName());
										if (fa1.getClass() != fa2.getClass())
											throw new DatabaseSyntaxException(
													"Cannot compare two fields whose type is different : "
															+ fa1.getFieldName() + " and " + fa2.getFieldName());
										res.append(sfs2[fieldsNumber - 1].field);
									}
								}
								if (comp.getType() == SymbolType.EQUALOPERATOR
										|| comp.getType() == SymbolType.NOTEQUALOPERATOR)
									res.append(")");
								return res;
							}
						}
					} else {
						if (!ri1.getValueType(table, parameters).equals("comparable")
								|| !ri3.getValueType(table, parameters).equals("comparable")) {
							throw new DatabaseSyntaxException(
									"Cannot compare " + ri1.getContent() + " and " + ri3.getContent());
						}
					}
					StringBuilder res = new StringBuilder();
					StringBuilder sb = ri1.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
							tablesJunction);
					if (ri1.needParenthesis())
						res.append("(");
					res.append(sb);
					if (ri1.needParenthesis())
						res.append(")");
					res.append(comp.getType().getContent());
					sb = ri3.translateToSqlQuery(table, parameters, outputParameters, currentParameterID,
							tablesJunction);
					if (ri3.needParenthesis())
						res.append("(");
					res.append(sb);
					if (ri3.needParenthesis())
						res.append(")");
					return res;
				}
			} else
				throw new IllegalAccessError();
			case NULLTEST:
				if (parts.size() == 3) {
					RuleInstance ri2 = (RuleInstance) parts.get(1);

					RuleInstance ri1 = (RuleInstance) parts.get(0);
					RuleInstance ri3 = (RuleInstance) parts.get(2);
					if (ri3.rule!=Rule.NULL)
						throw new IllegalAccessError();

					Symbol comp = (Symbol) ri2.parts.get(0);

					if (comp.getType() == SymbolType.IS || comp.getType() == SymbolType.ISNOT) {
						/*if (!ri1.isEqualable(table, parameters, null)) {
							throw new DatabaseSyntaxException(
									"Cannot compare " + ri1.getContent() + " and " + ri3.getContent());
						}*/
						Symbol s1 = (Symbol)ri1.parts.get(0);
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

		case EXPRESSION:
		case QUERY:
			if (parts.size() == 1) {
				return ((RuleInstance) parts.get(0)).translateToSqlQuery(table, parameters, outputParameters,
						currentParameterID, tablesJunction);
			} else if (parts.size() == 3) {
				RuleInstance ri1 = (RuleInstance) parts.get(0);
				RuleInstance ri3 = (RuleInstance) parts.get(2);
				if (rule == Rule.QUERY) {
					if (!ri1.getValueType(table, parameters).equals("boolean")
							|| !ri3.getValueType(table, parameters).equals("boolean"))
						throw new DatabaseSyntaxException(
								"Cannot make test with " + ri1.getContent() + " and " + ri3.getContent());
				} else {
					if (!ri1.getValueType(table, parameters).equals("comparable")
							|| !ri3.getValueType(table, parameters).equals("comparable"))
						throw new DatabaseSyntaxException(
								"Cannot mul/div " + ri1.getContent() + " and " + ri3.getContent());
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
				Symbol comp = (Symbol) ((RuleInstance) parts.get(1)).parts.get(0);
				sb.append(" ");
				sb.append(comp.getType().getContent());
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
			throw new IllegalAccessError();
		case NULL: {
			if (parts.size() == 1)
				return new StringBuilder( ((Symbol)parts.get(0)).getSymbol());
			else
				throw new IllegalAccessError();
		}
		case TERME:
			if (parts.size() == 1) {
				Symbol s = (Symbol) parts.get(0);
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
				} else
					return new StringBuilder(s.getSymbol());

			} else if (parts.size() == 3) {
				StringBuilder sb = new StringBuilder();
				sb.append("(");
				sb.append(((RuleInstance) parts.get(1)).translateToSqlQuery(table, parameters, outputParameters,
						currentParameterID, tablesJunction));
				sb.append(")");
				return sb;
			} else
				throw new IllegalAccessError();

		}
		throw new IllegalAccessError();
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
