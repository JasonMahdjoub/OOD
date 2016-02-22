/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@distri-mind.fr)) Copyright (c)
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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.sql.Blob;
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

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 1.5
 */
public class ByteTabConvertibleFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];
    private final ByteTabObjectConverter converter;
    private final Class<?> compatible_classes[];
    
    protected ByteTabConvertibleFieldAccessor(DatabaseWrapper _sql_connection, Field _field, ByteTabObjectConverter converter) throws DatabaseException
    {
	super(_sql_connection, _field);
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), (limit==0)?(sql_connection.isVarBinarySupported()?"VARBINARY("+16777216+")":"BLOB"):((limit>16777216 || !sql_connection.isVarBinarySupported())?("BLOB("+limit+")"):("VARBINARY("+limit+")")), null, null);
	this.converter=converter;
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
	else if (!(field.getType().equals(_field_instance.getClass())))
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
    public boolean equals(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    byte tab1[]=(byte[])field.get(_class_instance);
	    if (_field_instance==null)
	    {
		if (isNotNull())
		    return false;
		else 
		    return tab1==null;
	    }
	    byte tab2[];
	    if (_field_instance.getClass().equals(this.getCompatibleClasses()[0]))
		tab2=(byte[])_field_instance;
	    else
		return false;
	    
	    if (tab1.length!=tab2.length)
	    {
		return false;
	    }
	    for (int i=0;i<tab1.length;i++)
		if (tab1[i]!=tab2[i])
		    return false;
	    return true;
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
	    byte[] val1=null;
	    if (_field_instance instanceof byte[])
		val1=(byte[])_field_instance;
	    
	    byte[] val2=null;
	    
	    if (sql_fields[0].type.startsWith("VARBINARY"))
	    {
		val2=_result_set.getBytes(_sft.translateField(sql_fields[0]));
	    }
	    else
	    {
		Blob b=_result_set.getBlob(_sft.translateField(sql_fields[0]));
		val2=b==null?null:b.getBytes(1, (int)b.length());
	    }
	    
	    if (val1==null|| val2==null)
		return val1==val2;
	    else
	    {
		if (val1.length!=val2.length)
		    return false;
		for (int i=0;i<val1.length;i++)
		    if (val1[i]!=val2[i])
			return false;
		return true;
	    }
	    
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
	    byte[] res=null;
	    if (sql_fields[0].type.startsWith("VARBINARY"))
	    {
		res=_result_set.getBytes(sql_fields[0].short_field);
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
	    }
	    else
	    {
		Blob b=_result_set.getBlob(sql_fields[0].short_field);
		res=b==null?null:b.getBytes(1, (int)b.length());
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
		
	    }
	    if (res==null)
		field.set(_class_instance, null);
	    else
	    {
		Object o=converter.getObject(field.getType(), (byte[])res);
		if (o==null)
		    throw new FieldDatabaseException("The given ByteTabObjectConverter should produce an object of type "+field.getType().getCanonicalName()+" and not a null reference. This concern the affectation of the field "+field.getName()+" into the class "+field.getDeclaringClass().getCanonicalName()); 
		if (!(field.getType().isAssignableFrom(o.getClass())))
		    throw new FieldDatabaseException("The given ByteTabObjectConverter does not produce an object of type "+field.getType().getCanonicalName()+" but an object of type "+o.getClass().getCanonicalName()+". This concern the affectation of the field "+field.getName()+" into the class "+field.getDeclaringClass().getCanonicalName());
		field.set(_class_instance, o);
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
	    getValue(field.get(_class_instance), _prepared_statement, _field_start);
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
    }
    @Override
    public void getValue(Object o, PreparedStatement _prepared_statement, int _field_start) throws DatabaseException
    {
	try
	{
	    byte[] b=null;
	    if (o!=null)
	    {
		b=converter.getByte(o);
		if (b==null)
		    throw new FieldDatabaseException("The given ByteTabObjectConverter should produce an byte tab and not a null reference. This concern the affectation of the field "+field.getName()+" into the class "+field.getDeclaringClass().getCanonicalName());
	    }
	    if (sql_fields[0].type.startsWith("VARBINARY"))
		_prepared_statement.setBytes(_field_start, b);
	    else
	    {
		Blob blob=sql_connection.getBlob(b);
		if (blob==null && b!=null)
		    _prepared_statement.setBinaryStream(_field_start, new ByteArrayInputStream(b));
		else
		    _prepared_statement.setBlob(_field_start, blob);
	    }
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
	    Object o=field.get(_class_instance);
	    byte[] b=null;
	    if (o!=null)
	    {
		b=converter.getByte(o);
		if (b==null)
		    throw new FieldDatabaseException("The given ByteTabObjectConverter should produce an byte tab and not a null reference. This concern the affectation of the field "+field.getName()+" into the class "+field.getDeclaringClass().getCanonicalName());
	    }
	    if (sql_fields[0].type.startsWith("VARBINARY"))
		_result_set.updateBytes(sql_fields[0].short_field, b);
	    else
	    {
		Blob blob=sql_connection.getBlob(b);
		if (blob==null && b!=null)
		    _result_set.updateBinaryStream(sql_fields[0].short_field, new ByteArrayInputStream(b));
		else
		    _result_set.updateBlob(sql_fields[0].short_field, blob);
	    }
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
	    Object o=field.get(_class_instance);
	    byte[] b=null;
	    if (o!=null)
	    {
		b=converter.getByte(o);
		if (b==null)
		    throw new FieldDatabaseException("The given ByteTabObjectConverter should produce an byte tab and not a null reference. This concern the affectation of the field "+field.getName()+" into the class "+field.getDeclaringClass().getCanonicalName());
	    }
	    if (sql_fields[0].type.startsWith("VARBINARY"))
		_result_set.updateBytes(_sft.translateField(sql_fields[0]), b);
	    else
	    {
		Blob blob=sql_connection.getBlob(b);
		if (blob==null && b!=null)
		    _result_set.updateBinaryStream(_sft.translateField(sql_fields[0]), new ByteArrayInputStream(b));    
		else
		    _result_set.updateBlob(_sft.translateField(sql_fields[0]), blob);
	    }
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
