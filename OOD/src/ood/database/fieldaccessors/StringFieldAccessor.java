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
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import ood.database.DatabaseRecord;
import ood.database.SqlField;
import ood.database.SqlFieldInstance;
import ood.database.exceptions.DatabaseException;
import ood.database.exceptions.DatabaseIntegrityException;
import ood.database.exceptions.FieldDatabaseException;


public class StringFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];
    
    protected StringFieldAccessor(Field _field) throws DatabaseException
    {
	super(null, _field);
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), limit==0?"VARCHAR(16777216)":(limit<4096?"VARCHAR("+limit+")":"CLOB("+limit+")"), null, null);
    }

    @Override
    public void setValue(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	if (_field_instance==null)
	{
	    if (isNotNull())
		throw new FieldDatabaseException("The given _field_instance, used to store the field "+field.getName()+" (type="+field.getType().getName()+", declaring_class="+field.getDeclaringClass().getName()+") into the DatabaseField class "+field.getDeclaringClass().getName()+", is null and should not be (property NotNull present).");
	}
	else if (!(_field_instance instanceof String))
	    throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be a String and not a "+_field_instance.getClass().getName());
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
    public boolean equals(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    if ((!(_field_instance instanceof String)))
		return false;
	    Object val=field.get(_class_instance);
	    if (val==null)
		return _field_instance==null;
	    return val.equals(_field_instance);
	}
	catch(Exception e)
	{
	    throw new DatabaseException("", e);
	}
    }

    @Override
    protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException
    {
	try
	{
	    String val1=null;
	    if (_field_instance instanceof String)
		val1=(String)_field_instance;
	    String val2=(String)_result_set.getObject(_sft.translateField(sql_fields[0]));
	    
	    return val1.equals(val2);
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

    private static final Class<?>[] compatible_classes={String.class};

    @Override
    public Object getValue(DatabaseRecord _class_instance) throws DatabaseException
    {
	try
	{
	    return field.get(_class_instance);
	}
	catch(Exception e)
	{
	    throw new DatabaseException("",e);
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
	return false;
    }
    @Override
    public boolean isComparable()
    {
	return true;
    }

    @Override
    public int compare(DatabaseRecord _r1, DatabaseRecord _r2) throws DatabaseException
    {
	try
	{
	    Object obj1=field.get(_r1);
	    Object obj2=field.get(_r2);
	    if (obj1==null && obj2!=null)
		return -1;
	    else if (obj1!=null && obj2==null)
		return 1;
	    else if (obj1==obj2)
		return 0;
		
	    String val1=(String)obj1;
	    String val2=(String)obj2;
	    
	    return val1.compareTo(val2);
	}
	catch(Exception e)
	{
	    throw new DatabaseException("", e);
	}
    }
    @Override
    public void setValue(DatabaseRecord _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records) throws DatabaseException
    {
	try
	{
	    if (sql_fields[0].type.startsWith("VARCHAR"))
	    {
		String res=_result_set.getString(sql_fields[0].field);
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
		field.set(_class_instance, res);
	    }
	    else
	    {
		Clob c=_result_set.getClob(sql_fields[0].field);
		String res=c.getSubString(0, (int)c.length());
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
		field.set(_class_instance, res);
	    }
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
	    _prepared_statement.setString(_field_start, (String)field.get(_class_instance));
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
	    _result_set.updateString(sql_fields[0].field, (String)field.get(_class_instance));
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
	    _result_set.updateString(_sft.translateField(sql_fields[0]), (String)field.get(_class_instance));
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
