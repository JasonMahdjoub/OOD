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



package ood.database.fieldaccessors;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import ood.database.DatabaseRecord;
import ood.database.SqlField;
import ood.database.SqlFieldInstance;
import ood.database.exceptions.DatabaseException;
import ood.database.exceptions.FieldDatabaseException;


public class booleanFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];
    
    protected booleanFieldAccessor(Field _field) throws DatabaseException
    {
	super(null, _field);
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), "BOOLEAN", null, null);
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	if (_field_instance==null)
	{
	    throw new FieldDatabaseException("The given _field_instance, used to store the field "+field.getName()+" (type="+field.getType().getName()+", declaring_class="+field.getDeclaringClass().getName()+") into the DatabaseField class "+field.getDeclaringClass().getName()+", is null and should not be.");
	}
	try
	{
	    if (_field_instance instanceof Boolean)
		field.setBoolean(_class_instance, ((Boolean)_field_instance).booleanValue());
	    else
		throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be a Boolean and not a "+_field_instance.getClass().getName());
	}
	catch(IllegalArgumentException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	catch(IllegalAccessException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public boolean equals(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    if (_field_instance==null)
		return false;
	    else if (_field_instance instanceof Boolean)
		return field.getBoolean(_class_instance)==((Boolean)_field_instance).booleanValue();
	    else if (_field_instance instanceof Long)
		return field.getBoolean(_class_instance)==(((Long)_field_instance).longValue()!=0);
	    else 
		return false;
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
	    if (_field_instance==null)
		return false;
	    Boolean val1=null;
	    if (_field_instance instanceof Boolean)
		val1=(Boolean)_field_instance;
	    if (val1==null)
		return false;
	    boolean val2=_result_set.getBoolean(_sft.translateField(sql_fields[0]));
	    
	    return val1.booleanValue()==val2;
	}
	catch(SQLException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    
    @Override
    public Class<?> [] getCompatibleClasses()
    {
	return compatible_classes;
    }

    private static final Class<?>[] compatible_classes={boolean.class, Boolean.class};

    @Override
    public Object getValue(DatabaseRecord _class_instance) throws DatabaseException
    {
	try
	{
	    return new Boolean(field.getBoolean(_class_instance));
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
    public boolean isAlwaysNutNull()
    {
	return true;
    }

    @Override
    public boolean isComparable()
    {
	return false;
    }

    @Override
    public int compare(DatabaseRecord _r1, DatabaseRecord _r2) throws DatabaseException
    {
	throw new DatabaseException("Unexpected exception");
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records) throws DatabaseException
    {
	try
	{
	    field.setBoolean(_class_instance, _result_set.getBoolean(sql_fields[0].field));
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
	    _prepared_statement.setBoolean(_field_start, field.getBoolean(_class_instance));
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
	    _result_set.updateBoolean(sql_fields[0].field, field.getBoolean(_class_instance));
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
	    _result_set.updateBoolean(_sft.translateField(sql_fields[0]), field.getBoolean(_class_instance));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public boolean canBePrimaryOrUniqueKey()
    {
	return false;
    }
    
}
