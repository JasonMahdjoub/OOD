/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */


package com.distrimind.ood.database.fieldaccessors;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.LoadToMemory;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.annotations.RandomPrimaryKey;
import com.distrimind.ood.database.annotations.Unique;
import com.distrimind.ood.database.exceptions.DatabaseException;




public abstract class FieldAccessor
{
    protected final DatabaseWrapper sql_connection;
    protected final Field field;
    protected final String table_name;
    protected final boolean auto_primary_key;
    protected final boolean random_primary_key;
    protected final boolean primary_key;
    protected final boolean foreign_key;
    protected final boolean unique;
    protected final boolean not_null;
    protected final long limit;
    protected final long start_value;
    protected final int bits_number;
    
    @SuppressWarnings("unchecked")
    protected FieldAccessor(DatabaseWrapper _sql_connection, Field _field) throws DatabaseException
    {
	sql_connection=_sql_connection;
	field=_field;
	table_name=Table.getName(Table.getTableClass((Class<? extends DatabaseRecord>)field.getDeclaringClass()));
	auto_primary_key=_field.isAnnotationPresent(AutoPrimaryKey.class);
	random_primary_key=field.isAnnotationPresent(RandomPrimaryKey.class);
	if (auto_primary_key && random_primary_key)
	    throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" cannot be an auto primary key and a random primary key at the same time.");
	if (auto_primary_key && !isTypeCompatible(byte.class) && !isTypeCompatible(short.class) && !isTypeCompatible(int.class) && !isTypeCompatible(long.class))
	    throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+", which is an auto primary key, must be a byte, a short, an int, or a long.");
	primary_key=field.isAnnotationPresent(PrimaryKey.class) || auto_primary_key || random_primary_key;
	foreign_key=field.isAnnotationPresent(ForeignKey.class);
	if (foreign_key && (auto_primary_key || random_primary_key))
	    throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" cannot be a foreign key and an auto primary key (or a random primary key) at the same time.");
	if (random_primary_key && !isAssignableTo(long.class) && !isAssignableTo(BigInteger.class) && !isAssignableTo(int.class))
	    throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" is a a random primary key which must be an int, a long or a BigInteger.");
	unique=field.isAnnotationPresent(Unique.class) && !auto_primary_key && !random_primary_key;
	not_null=field.isAnnotationPresent(NotNull.class) || primary_key || isAlwaysNotNull() || (isAssignableTo(BigInteger.class) && random_primary_key);
	if (foreign_key && not_null && field.getType().equals(field.getDeclaringClass()))
	    throw new DatabaseException("The field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+" points to same class. So this field cannot have the annotation NotNull.");
	
	if (primary_key && !canBePrimaryOrUniqueKey())
	    throw new DatabaseException("The field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+" of type "+field.getType().getName()+" can't be a primary key.");
	if (unique && !canBePrimaryOrUniqueKey())
	    throw new DatabaseException("The field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+" of type "+field.getType().getName()+" can't be a unique key.");
	
	if (auto_primary_key)
	{
	    start_value=_field.getAnnotation(AutoPrimaryKey.class).startValue();
	    bits_number=-1;
	    limit=-1;
	} else if (random_primary_key)
	{
	    start_value=-1;
	    int nb=_field.getAnnotation(RandomPrimaryKey.class).byteNumber();
	    if (nb==-1)
	    {
		if (isAssignableTo(long.class))
		    nb=63;
		else if (isAssignableTo(int.class))
		    nb=31;
		else if (isAssignableTo(BigInteger.class))
		    nb=128;
	    }
	    bits_number=nb;
	    if (isAssignableTo(long.class) && (bits_number>63 || bits_number<1))
		throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" is a random primary key whose long type enables at maximum 63 random bits and at minimum 1 random bit. Currently, the number of bits is "+bits_number);
	    if (isAssignableTo(int.class) && (bits_number>31 || bits_number<1))
		throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" is a random primary key whose int type enables at maximum 31 random bits and at minimum 1 random bit. Currently, the number of bits is "+bits_number);
	    if (isAssignableTo(BigInteger.class) && bits_number<1)
		throw new DatabaseException("The field "+field.getName()+" of the DatabaseRecord "+field.getDeclaringClass().getName()+" is a random primary key whose BigInteger type enables at minimum 1 random bit. Currently, the number of bits is "+bits_number);
	    limit=-1;
	}
	else if (primary_key)
	{
	    start_value=-1;
	    bits_number=-1;
	    limit=_field.getAnnotation(PrimaryKey.class).limit();
	} else if (foreign_key)
	{
	    start_value=-1;
	    bits_number=-1;
	    limit=-1;
	}
	else
	{
	    start_value=-1;
	    bits_number=-1;
	    limit=_field.getAnnotation(com.distrimind.ood.database.annotations.Field.class).limit();
	}
    }
    
    public Class<?> getFieldClassType()
    {
	return field.getType();
    }
    
    public boolean isTypeCompatible(Class<?> _cls)
    {
	for (Class<?> c : getCompatibleClasses())
	    if (c.equals(_cls))
		return true;
	return false;
    }
    public boolean isAssignableTo(Class<?> _cls)
    {
	for (Class<?> c : getCompatibleClasses())
	    if (_cls.isAssignableFrom(c) /*|| _cls.equals(c)*/)
		return true;
	return false;
    }
    public boolean isPrimaryKey()
    {
	return primary_key;
    }
    public boolean isAutoPrimaryKey()
    {
	return auto_primary_key;
    }
    public boolean isRandomPrimaryKey()
    {
	return random_primary_key;
    }
    public boolean isUnique()
    {
	return unique;
    }
    public boolean isForeignKey()
    {
	return foreign_key;
    }
    public boolean isNotNull()
    {
	return not_null;
    }
    
    public String getFieldName()
    {
	return field.getName();
    }
    public final long getLimit()
    {
	return limit;
    }
    public final long getStartValue()
    {
	return start_value;
    }
    public final int getBitsNumber()
    {
	return bits_number;
    }
    public abstract void setValue(DatabaseRecord _class_instance, Object _field_instance)  throws DatabaseException;
    public final void setValue(DatabaseRecord _class_instance, ResultSet _result_set)  throws DatabaseException
    {
	setValue(_class_instance, _result_set, null);
    }
    public abstract void setValue(DatabaseRecord _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records)  throws DatabaseException;
    public abstract void updateValue(DatabaseRecord _class_instance, Object _field_instance, ResultSet _result_set) throws DatabaseException;
    protected abstract void updateResultSetValue(DatabaseRecord _class_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException;
    public abstract boolean equals(DatabaseRecord _class_instance, Object _field_instance)  throws DatabaseException;
    protected abstract boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft)  throws DatabaseException;
    public final boolean equals(Object _field_instance, ResultSet _result_set) throws DatabaseException
    {
	return equals(_field_instance, _result_set, new SqlFieldTranslation(this));
    }
    
    public abstract Class<?>[] getCompatibleClasses();
    public abstract Object getValue(DatabaseRecord _class_instance) throws DatabaseException;
    public abstract void getValue(DatabaseRecord _class_instance, PreparedStatement _prepared_statement, int _field_start)  throws DatabaseException;
    
    public abstract SqlField[] getDeclaredSqlFields();
    public abstract SqlFieldInstance[] getSqlFieldsInstances(DatabaseRecord _instance)  throws DatabaseException;
    public abstract boolean isAlwaysNotNull();
    public abstract boolean isComparable();
    public abstract boolean canBePrimaryOrUniqueKey();
    
    public abstract int compare(DatabaseRecord r1, DatabaseRecord _r2) throws DatabaseException;
    
    public Field getField()
    {
	return field;
    }
    
    public static ArrayList<FieldAccessor> getFields(DatabaseWrapper _sql_connection, Class<? extends Table<?>> _table_class) throws DatabaseException
    {
	Class<? extends DatabaseRecord> database_record_class=Table.getDatabaseRecord(_table_class);
	ArrayList<FieldAccessor> res=new ArrayList<FieldAccessor>();
	try
	{
	    if (!checkCircularityWithPrimaryForeignKeys(database_record_class))
		throw new DatabaseException("The class/table "+_table_class.getSimpleName()+" has a problem of circularity with other tables, through primary foreign keys !");
	    if (!checkCircularityWithNotNullForeignKeys(database_record_class))
		throw new DatabaseException("The class/table "+_table_class.getSimpleName()+" has a problem of circularity with other tables, through not null foreign keys !");
		
	    FieldAccessPrivilegedAction fapa=new FieldAccessPrivilegedAction(database_record_class);
	    
	    for (java.lang.reflect.Field f : AccessController.doPrivileged(fapa))
	    {
		if (f.isAnnotationPresent(com.distrimind.ood.database.annotations.Field.class) || f.isAnnotationPresent(PrimaryKey.class) || f.isAnnotationPresent(ForeignKey.class) || f.isAnnotationPresent(AutoPrimaryKey.class) || f.isAnnotationPresent(RandomPrimaryKey.class))
		{
		    Class<?> type=f.getType();
		    if (f.isAnnotationPresent(ForeignKey.class))
		    {
			
			if (!DatabaseRecord.class.isAssignableFrom(type))
			    throw new IllegalAccessError("The class "+database_record_class.getSimpleName()+" contains a foreign key ("+type.getName()+") which does not point to a DatabaseRecord class");
			if (!type.getSimpleName().equals("Record"))
			    throw new IllegalAccessError("The class "+database_record_class.getSimpleName()+" contains a foreign key which points to a DatabaseRecord class whose name ("+type.getName()+") is not equal to 'Record'.");
			@SuppressWarnings("unchecked")
			Class<? extends DatabaseRecord> type2=(Class<? extends DatabaseRecord>)type;
			
			Class<? extends Table<?>> t=Table.getTableClass(type2);
			if (!t.getPackage().equals(_table_class.getPackage()))
			    throw new DatabaseException("The class "+database_record_class.getName()+" contains a foreign key which points to a DatabaseRecord ("+type.getName()+") which have not the same package of the considered table.");
			if (!t.isAnnotationPresent(LoadToMemory.class) && _table_class.isAnnotationPresent(LoadToMemory.class))
			    throw new IllegalAccessError("The Table "+t.getSimpleName()+" is not loaded into memory whereas the table "+_table_class.getSimpleName()+" is ! It is a problem since the table "+_table_class.getSimpleName()+" has a foreign key which points to "+t.getSimpleName());
			res.add(new ForeignKeyFieldAccessor(_sql_connection, f));
		    }
		    else
		    {
			if (type.equals(boolean.class))
			    res.add(new booleanFieldAccessor(f));
			else if (type.equals(byte.class))
			    res.add(new byteFieldAccessor(_sql_connection, f));
			else if (type.equals(short.class))
			    res.add(new shortFieldAccessor(_sql_connection, f));
			else if (type.equals(char.class))
			    res.add(new charFieldAccessor(f));
			else if (type.equals(int.class))
			    res.add(new intFieldAccessor(_sql_connection, f));
			else if (type.equals(long.class))
			    res.add(new longFieldAccessor(_sql_connection, f));
			else if (type.equals(float.class))
			    res.add(new floatFieldAccessor(_sql_connection, f));
			else if (type.equals(double.class))
			    res.add(new doubleFieldAccessor(_sql_connection, f));
			else if (type.equals(String.class))
			    res.add(new StringFieldAccessor(_sql_connection, f));
			else if (type.equals(class_array_byte))
			    res.add(new ByteTabFieldAccessor(_sql_connection, f));
			else if (type.equals(Boolean.class))
			    res.add(new BooleanNumberFieldAccessor(f));
			else if (type.equals(Byte.class))
			    res.add(new ByteNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(Short.class))
			    res.add(new ShortNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(Character.class))
			    res.add(new CharacterNumberFieldAccessor(f));
			else if (type.equals(Integer.class))
			    res.add(new IntegerNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(Long.class))
			    res.add(new LongNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(Float.class))
			    res.add(new FloatNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(Double.class))
			    res.add(new DoubleNumberFieldAccessor(_sql_connection, f));
			else if (type.equals(BigDecimal.class))
			    res.add(new BigDecimalFieldAccessor(_sql_connection, f));
			else if (type.equals(BigInteger.class))
			    res.add(new BigIntegerFieldAccessor(_sql_connection, f));
			else if (Calendar.class.isAssignableFrom(type))
			    res.add(new CalendarFieldAccessor(_sql_connection, f));
			else if (Date.class.isAssignableFrom(type))
			    res.add(new DateFieldAccessor(f));
			else if (Serializable.class.isAssignableFrom(type))
			    res.add(new SerializableFieldAccessor(_sql_connection, f));
			else
			    throw new DatabaseException("The field "+f.getName()+" of the class "+database_record_class.getName()+" have a type which can't be used on the SqlJet database ");
		    }
		}
	    }
	}
	catch(PrivilegedActionException e)
	{
	    throw new DatabaseException("Impossible to access to fields of the class "+database_record_class.getName(), e);
	}
	
	return res;
    }
    
    
    private static boolean checkCircularityWithNotNullForeignKeys(Class<?> _original_class) throws PrivilegedActionException
    {
	ArrayList<Class<?>> list_classes=new ArrayList<Class<?>>();
	
	FieldAccessPrivilegedAction fapa=new FieldAccessPrivilegedAction(_original_class);
	java.lang.reflect.Field fields[]=AccessController.doPrivileged(fapa);
	
	for (java.lang.reflect.Field f : fields)
	{
	    if (f.isAnnotationPresent(ForeignKey.class))
	    {
		if (f.isAnnotationPresent(NotNull.class))
		{
		    Class<?> new_class=f.getType();
		    if (DatabaseRecord.class.isAssignableFrom(new_class))
		    {
			if (!checkCircularityWithNotNullForeignKeys(_original_class, list_classes, new_class))
			    return false;
		    }
		}
	    }
	}
	return true;
    }
    private static boolean checkCircularityWithNotNullForeignKeys(Class<?> _original_class, List<Class<?>> _list_classes, Class<?> _new_class) throws PrivilegedActionException
    {
	if (_original_class.equals(_new_class))
	    return false;
	if (_list_classes.contains(_new_class))
	    return true;
	_list_classes.add(_new_class);
	
	FieldAccessPrivilegedAction fapa=new FieldAccessPrivilegedAction(_new_class);
	java.lang.reflect.Field fields[]=AccessController.doPrivileged(fapa);
	
	for (java.lang.reflect.Field f : fields)
	{
	    if (f.isAnnotationPresent(ForeignKey.class))
	    {
		if (f.isAnnotationPresent(NotNull.class))
		{
		    Class<?> new_class=f.getType();
		    if (DatabaseRecord.class.isAssignableFrom(new_class))
		    {
			if (!checkCircularityWithNotNullForeignKeys(_original_class, _list_classes, new_class))
			    return false;
		    }
		}
	    }
	}
	
	return true;
    }
    private static boolean checkCircularityWithPrimaryForeignKeys(Class<?> _original_class) throws PrivilegedActionException
    {
	ArrayList<Class<?>> list_classes=new ArrayList<Class<?>>();
	
	FieldAccessPrivilegedAction fapa=new FieldAccessPrivilegedAction(_original_class);
	java.lang.reflect.Field fields[]=AccessController.doPrivileged(fapa);
	
	for (java.lang.reflect.Field f : fields)
	{
	    if (f.isAnnotationPresent(ForeignKey.class) && f.isAnnotationPresent(PrimaryKey.class))
	    {
		Class<?> new_class=f.getType();
		if (DatabaseRecord.class.isAssignableFrom(new_class))
		{
		    if (!checkCircularityWithPrimaryForeignKeys(_original_class, list_classes, new_class))
			return false;
		}
	    }
	}
	return true;
    }
    private static boolean checkCircularityWithPrimaryForeignKeys(Class<?> _original_class, List<Class<?>> _list_classes, Class<?> _new_class) throws PrivilegedActionException
    {
	if (_original_class.equals(_new_class))
	    return false;
	if (_list_classes.contains(_new_class))
	    return true;
	_list_classes.add(_new_class);
	
	FieldAccessPrivilegedAction fapa=new FieldAccessPrivilegedAction(_new_class);
	java.lang.reflect.Field fields[]=AccessController.doPrivileged(fapa);
	
	for (java.lang.reflect.Field f : fields)
	{
	    if (f.isAnnotationPresent(ForeignKey.class) && f.isAnnotationPresent(PrimaryKey.class))
	    {
		Class<?> new_class=f.getType();
		if (DatabaseRecord.class.isAssignableFrom(new_class))
		{
		    if (!checkCircularityWithPrimaryForeignKeys(_original_class, _list_classes, new_class))
			return false;
		}
	    }
	}
	
	return true;
    }
    private static Class<?> class_array_byte=(new byte[0]).getClass();
    public final static boolean equals(Object val1, Object val2) throws DatabaseException
    {
	if (val1==val2)
	    return true;
	if (val1==null)
	    return false;
	if (val2==null)
	    return false;
	if (val1.getClass().equals(String.class))
	{
	    if (val2.getClass().equals(String.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(BigDecimal.class))
		return val2.equals(new BigDecimal((String)val1));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(class_array_byte))
	{
	    if (val2.getClass().equals(class_array_byte))
		return tabEquals((byte[])val1, (byte[])val2);
	    else if (val2.getClass().equals(BigInteger.class))
		return val2.equals(new BigInteger((byte[])val1));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Boolean.class))
	{
	    if (val2.getClass().equals(Boolean.class))
	    {
		return val1.equals(val2);
	    }
	    else if (val2.getClass().equals(Long.class))
	    {
		return val1.equals(new Boolean(((Long)val2).longValue()!=0));
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Byte.class))
	{
	    if (val2.getClass().equals(Byte.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Long.class))
		return val1.equals(new Byte(((Long)val2).byteValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Short.class))
	{
	    if (val2.getClass().equals(Short.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Long.class))
		return val1.equals(new Short(((Long)val2).shortValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Character.class))
	{
	    if (val2.getClass().equals(Character.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Long.class))
		return val1.equals(new Character((char)((Long)val2).longValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Integer.class))
	{
	    if (val2.getClass().equals(Integer.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Long.class))
		return val1.equals(new Integer(((Long)val2).intValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Long.class))
	{
	    if (val2.getClass().equals(Long.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Boolean.class))
		return val2.equals(new Boolean(((Long)val1).longValue()!=0));
	    else if (val2.getClass().equals(Integer.class))
	    {
		return val1.equals(new Long(((Integer)val2).longValue()));
	    }
	    else if (val2.getClass().equals(Byte.class))
		return val1.equals(new Long(((Byte)val2).byteValue()));
	    else if (val2.getClass().equals(Short.class))
		return val1.equals(new Long(((Short)val2).shortValue()));
	    else if (val2.getClass().equals(Character.class))
		return val1.equals(new Long(((Character)val2).charValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(Float.class))
	{
	    if (val2.getClass().equals(Float.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Double.class))
		return val1.equals(new Float(((Double)val2).floatValue()));
	    else
		throw new DatabaseException("Unexpected exception.");
	    
	}
	else if (val1.getClass().equals(Double.class))
	{
	    if (val2.getClass().equals(Double.class))
		return val1.equals(val2);
	    else if (val2.getClass().equals(Float.class))
	    {
		return ((Double)val1).floatValue()==((Float)val2).floatValue();
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(BigDecimal.class))
	{
	    if (val2.getClass().equals(String.class))
		return val1.equals(new BigDecimal((String)val2));
	    else if (val2.getClass().equals(BigDecimal.class))
		return val1.equals((BigDecimal)val2);
	    else if (val2.getClass().equals(BigInteger.class))
		return val1.equals(new BigDecimal((BigInteger)val2));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (val1.getClass().equals(BigInteger.class))
	{
	    if (val2.getClass().equals(String.class))
		return val1.equals(new BigInteger((String)val2));
	    else if (val2.getClass().equals(BigInteger.class))
		return val1.equals((BigInteger)val2);
	    else if (val2.getClass().equals(BigDecimal.class))
		return val2.equals(new BigDecimal((BigInteger)val1));
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (Calendar.class.isAssignableFrom(val1.getClass()))
	{
	    if (Calendar.class.isAssignableFrom(val2.getClass()))
	    {
		return val1.equals(val2);
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (Timestamp.class.isAssignableFrom(val1.getClass()))
	{
	    if (Timestamp.class.isAssignableFrom(val2.getClass()))
	    {
		return val2.equals(val1);
	    }
	    else if (Date.class.isAssignableFrom(val2.getClass()))
	    {
		return val1.equals(new Timestamp(((Date)val2).getTime()));
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (Date.class.isAssignableFrom(val1.getClass()))
	{
	    if (Timestamp.class.isAssignableFrom(val2.getClass()))
	    {
		return val2.equals(new Timestamp(((Date)val1).getTime()));
	    }
	    else if (Date.class.isAssignableFrom(val2.getClass()))
	    {
		return val1.equals(val2);
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else if (Serializable.class.isAssignableFrom(val1.getClass()))
	{
	    if (Serializable.class.isAssignableFrom(val2.getClass()))
	    {
		return val1.equals(val2);
	    }
	    else
		throw new DatabaseException("Unexpected exception.");
	}
	else 
	    throw new DatabaseException("Unexpected exception.");
    }
    private static boolean tabEquals(byte[] tab1, byte[] tab2)
    {
	if (tab1.length!=tab2.length)
	    return false;
	for (int i=0;i<tab1.length;i++)
	    if (tab1[i]!=tab2[i])
		return false;
	return true;
    }
    
    protected static class SqlFieldTranslation
    {
	private HashMap<String, String> sql_fields=new HashMap<>();
	private final FieldAccessor field_accessor;
	public SqlFieldTranslation(FieldAccessor fa, SqlField _sql_field[])
	{
	    field_accessor=fa;
	    for (SqlField sf : fa.getDeclaredSqlFields())
	    {
		if (sf.pointed_field!=null)
		    sql_fields.put(sf.pointed_field, sf.short_field);
		else
		    sql_fields.put(sf.field, sf.short_field);
	    }
	}
	
	public SqlFieldTranslation(FieldAccessor fa, SqlFieldTranslation _sft)
	{
	    field_accessor=fa;
	    for (SqlField sf : fa.getDeclaredSqlFields())
	    {
		SqlField sf_pointing_founded=null;
		for (SqlField sf_pointing : _sft.field_accessor.getDeclaredSqlFields())
		{
		    if (sf_pointing.pointed_field.equals(sf.field))
		    {
			sf_pointing_founded=sf_pointing;
			break;
		    }
		}
		String t=_sft.translateField(sf_pointing_founded);
		if (sf_pointing_founded==null || t==null)
		{
		    if (sf.pointed_field!=null)
			sql_fields.put(sf.pointed_field, sf.short_field);
		    else
			sql_fields.put(sf.field, sf.short_field);
		}
		else
		{
		    if (sf.pointed_field!=null)
			sql_fields.put(sf.pointed_field, t);
		    else
			sql_fields.put(sf.field, t);
		}
	    }
	}

	public SqlFieldTranslation(FieldAccessor fa)
	{
	    field_accessor=fa;
	    for (SqlField sf : fa.getDeclaredSqlFields())
	    {
		sql_fields.put(sf.field, sf.short_field);
	    }
	}
	
	public String translateField(SqlField sf)
	{
	    return sql_fields.get(sf.field);
	}
	
	
	
    }
    
    protected static final class FieldAccessPrivilegedAction
	implements PrivilegedExceptionAction<Field[]>
    {
	private final Class<?> m_cls;

	public FieldAccessPrivilegedAction(Class<?> _cls)
	{
	    m_cls=_cls;
	}
	
	public Field[] run () throws Exception
	{
	    Field fields[]=m_cls.getDeclaredFields();
	    for (java.lang.reflect.Field f : fields)
	    {
		f.setAccessible(true);
	    }

	    return fields;
	}
	
	
	
}
    
    
}
