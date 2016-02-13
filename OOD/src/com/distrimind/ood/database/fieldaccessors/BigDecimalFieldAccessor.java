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
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;

public class BigDecimalFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];

    protected BigDecimalFieldAccessor(DatabaseWrapper _sql_connection, Field _field) throws DatabaseException
    {
	super(_sql_connection, _field);
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(this.table_name+"."+this.getFieldName(), sql_connection.getBigDecimalType(), null, null);
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    if (_field_instance==null)
	    {
		if (isNotNull())
		    throw new FieldDatabaseException("The given _field_instance, used to store the field "+field.getName()+" (type="+field.getType().getName()+", declaring_class="+field.getDeclaringClass().getName()+") into the DatabaseField class "+field.getDeclaringClass().getName()+", is null and should not be (property NotNull present).");
		field.set(_class_instance, null);
	    }
	    else if (_field_instance.getClass().equals(BigDecimal.class))
		field.set(_class_instance, _field_instance);
	    else if (_field_instance.getClass().equals(String.class))
		field.set(_class_instance, new BigDecimal((String)_field_instance));
	    else
		throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be a BigDecimal and not a "+_field_instance.getClass().getName());
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
	    BigDecimal val1=(BigDecimal)field.get(_class_instance);
	    if (_field_instance==null)
	    {
		if (isNotNull())
		    return false;
		else 
		    return val1==null;
	    }
	    BigDecimal val2;
	    if (_field_instance.getClass().equals(BigDecimal.class))
		val2=(BigDecimal)_field_instance;
	    else if (_field_instance.getClass().equals(String.class))
		val2=new BigDecimal((String)_field_instance);
	    else
		return false;
	    
	    return val1.equals(val2);
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
	    BigDecimal val1=null;
	    if (_field_instance!=null)
	    {
		if (_field_instance instanceof BigDecimal)
		    val1=(BigDecimal)_field_instance;
		else if (_field_instance instanceof String)
		    val1=new BigDecimal((String)_field_instance);
		else
		    return false;
	    }
		
	    String s=_result_set.getString(_sft.translateField(sql_fields[0]));
	    BigDecimal val2=s==null?null:new BigDecimal(s);
	    return (val1==null|| val2==null)?val1==val2:val1.equals(val2);
	}
	catch(SQLException e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    
    @Override
    public Class<?>[] getCompatibleClasses()
    {
	return compatible_classes;
    }
    
    private static final Class<?>[] compatible_classes={BigDecimal.class};


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
    public SqlField[] getDeclaredSqlFields()
    {
	return sql_fields;
    }

    @Override
    public SqlFieldInstance[] getSqlFieldsInstances(DatabaseRecord _instance) throws DatabaseException
    {
	SqlFieldInstance[] res=new SqlFieldInstance[1];
	res[0]=new SqlFieldInstance(sql_fields[0], getValue(_instance));
	return res;
    }

    @Override
    public boolean isAlwaysNotNull()
    {
	return false;
    }

    @Override
    public int compare(DatabaseRecord _o1, DatabaseRecord _o2) throws DatabaseException
    {
	try
	{
	    Object val1=field.get(_o1);
	    Object val2=field.get(_o2);
	    if (val1==null && val2!=null)
		return -1;
	    else if (val1!=null && val2==null)
		return 1;
	    else if (val1==val2)
		return 0;
	    return ((BigDecimal)val1).compareTo((BigDecimal)val2);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public boolean isComparable()
    {
	return true;
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records) throws DatabaseException
    {
	try
	{
	    String s=_result_set.getString(sql_fields[0].short_field);
	    BigDecimal res=s==null?null:new BigDecimal(s);
	    if (res==null && isNotNull())
		throw new DatabaseIntegrityException("Unexpected exception.");
	    field.set(_class_instance, res);
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
	    BigDecimal bd=(BigDecimal)field.get(_class_instance);
	    _prepared_statement.setString(_field_start, bd==null?null:bd.toString());
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
	    BigDecimal bd=(BigDecimal)field.get(_class_instance);
	    _result_set.updateString(sql_fields[0].short_field, bd==null?null:bd.toString());
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
	    BigDecimal bd=(BigDecimal)field.get(_class_instance);
	    _result_set.updateString(_sft.translateField(sql_fields[0]), bd==null?null:bd.toString());
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }

    @Override
    public boolean canBePrimaryOrUniqueKey()
    {
	return true;
    }
}
