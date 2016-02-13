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

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;

public class DateFieldAccessor extends FieldAccessor
{
    private final SqlField sql_fields[];
    protected final Class<?> compatible_classes[];
    protected DateFieldAccessor(Field _field) throws DatabaseException
    {
	super(null, _field);
	if (!Date.class.isAssignableFrom(_field.getType()))
	    throw new FieldDatabaseException("The field "+_field.getName()+" of the class "+_field.getDeclaringClass().getName()+" of type "+_field.getType()+" must be a Date type.");
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), "TIMESTAMP", null, null);
	compatible_classes=new Class<?>[1];
	compatible_classes[0]=field.getType();
	
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	if (_field_instance==null)
	{
	    if (isNotNull())
		throw new FieldDatabaseException("The given _field_instance, used to store the field "+field.getName()+" (type="+field.getType().getName()+", declaring_class="+field.getDeclaringClass().getName()+") into the DatabaseField class "+field.getDeclaringClass().getName()+", is null and should not be (property NotNull present).");
	}
	else if (!(field.getType().isAssignableFrom(_field_instance.getClass())))
	    throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be a "+field.getType().getName()+" and not a "+_field_instance.getClass().getName());
	try
	{
	    field.set(_class_instance, _field_instance);
	}
	catch(IllegalArgumentException e)
	{
	    throw new DatabaseException("Unexpected exception.",e);
	}
	catch(IllegalAccessException e)
	{
	    throw new DatabaseException("Unexpected exception.",e);
	}
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records) throws DatabaseException
    {
	try
	{
	    Timestamp res=_result_set.getTimestamp(sql_fields[0].short_field);
	    if (res==null && isNotNull())
		throw new DatabaseIntegrityException("Unexpected exception.");
	    field.set(_class_instance, new Date(res.getTime()));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public void updateValue(DatabaseRecord _class_instance, Object _field_instance, ResultSet _result_set) throws DatabaseException
    {
	setValue(_class_instance, _field_instance);
	try
	{
	    Date d=(Date)field.get(_class_instance);
	    _result_set.updateTimestamp(sql_fields[0].short_field, d==null?null:new Timestamp(d.getTime()));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    protected void updateResultSetValue(DatabaseRecord _class_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException
    {
	try
	{
	    Date d=(Date)field.get(_class_instance);
	    _result_set.updateTimestamp(_sft.translateField(sql_fields[0]), d==null?null:new Timestamp(d.getTime()));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public boolean equals(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    Date val=(Date)field.get(_class_instance);
	    if (val==null || _field_instance==null)
		return _field_instance==val;
	    if ((!(field.getType().isAssignableFrom(_field_instance.getClass()))))
		return false;
	    return val.equals(_field_instance);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException
    {
	try
	{
	    Timestamp val1=_result_set.getTimestamp(_sft.translateField(sql_fields[0]));
	    if (val1==null || _field_instance==null)
		return _field_instance==val1;
	    if ((!(field.getType().isAssignableFrom(_field_instance.getClass()))))
		return false;
	    return new Date(val1.getTime()).equals(_field_instance);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public Class<?>[] getCompatibleClasses()
    {
	return compatible_classes;
    }

    @Override
    public Object getValue(DatabaseRecord _class_instance) throws DatabaseException
    {
	try
	{
	    return field.get(_class_instance);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public void getValue(DatabaseRecord _class_instance, PreparedStatement _prepared_statement, int _field_start) throws DatabaseException
    {
	try
	{
	    _prepared_statement.setTimestamp(_field_start, new Timestamp(((Date)field.get(_class_instance)).getTime()));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public SqlField[] getDeclaredSqlFields()
    {
	return sql_fields;
    }

    @Override
    public SqlFieldInstance[] getSqlFieldsInstances(DatabaseRecord _instance) throws DatabaseException
    {
	SqlFieldInstance res[]=new SqlFieldInstance[1];
	res[0]=new SqlFieldInstance(sql_fields[0], getValue(_instance));
	return res;
    }

    @Override
    public boolean isAlwaysNotNull()
    {
	return false;
    }

    @Override
    public boolean isComparable()
    {
	return true;
    }

    @Override
    public boolean canBePrimaryOrUniqueKey()
    {
	return false;
    }

    @Override
    public int compare(DatabaseRecord _r1, DatabaseRecord _r2) throws DatabaseException
    {
	try
	{
	    Date obj1=(Date)field.get(_r1);
	    Date obj2=(Date)field.get(_r2);
	    if (obj1==null && obj2!=null)
		return -1;
	    else if (obj1!=null && obj2==null)
		return 1;
	    else if (obj1==obj2)
		return 0;
	
	    return obj1.compareTo(obj2);
	}
	catch(Exception e)
	{
	    throw new DatabaseException("", e);
	}
    }
	

}
