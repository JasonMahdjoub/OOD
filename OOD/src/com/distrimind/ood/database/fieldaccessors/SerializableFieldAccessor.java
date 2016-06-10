
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
 * @version 1.1
 * @since OOD 1.0
 */
public class SerializableFieldAccessor extends FieldAccessor
{
    protected final Class<?> compatible_classes[];
    protected final SqlField sql_fields[];
    protected Method compareTo_method;
    protected SerializableFieldAccessor(DatabaseWrapper _sql_connection, Field _field) throws DatabaseException
    {
	super(_sql_connection, _field);
	if (!Serializable.class.isAssignableFrom(field.getType()))
	    throw new FieldDatabaseException("The given field "+field.getName()+" of type "+field.getType().getName()+" must be a serializable field.");
	compatible_classes=new Class<?>[1];
	compatible_classes[0]=field.getType();
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), sql_connection.getSerializableType(), null, null);
	
	if (Comparable.class.isAssignableFrom(field.getType()))
	{
	    try
	    {
		Method m=field.getType().getDeclaredMethod("compareTo", field.getType());
		if (Modifier.isPublic(m.getModifiers()))
		    compareTo_method=m;
		else
		    compareTo_method=null;
	    }
	    catch(Exception e)
	    {
		compareTo_method=null;
	    }
	}
	else
	    compareTo_method=null;
	
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
	    Blob b=_result_set.getBlob(sql_fields[0].short_field);
	    try(ByteArrayInputStream bais=new ByteArrayInputStream(b.getBytes(1, (int)b.length())))
	    {
		try(ObjectInputStream ois=new ObjectInputStream(bais))
		{
		    Object res=ois.readObject();
		    if (res==null && isNotNull())
			throw new DatabaseIntegrityException("Unexpected exception.");
		    field.set(_class_instance, res);
		}
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
	    if (sql_connection.getSerializableType().equals("BLOB"))
	    {
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    try(ObjectOutputStream os=new ObjectOutputStream(baos))
		    {
			os.writeObject(field.get(_class_instance));
		
			try(ByteArrayInputStream bais=new ByteArrayInputStream(baos.toByteArray()))
			{
			    _result_set.updateBlob(sql_fields[0].short_field, bais);
			}
		    }
		}    
	    }
	    else
		_result_set.updateObject(sql_fields[0].short_field, field.get(_class_instance));
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
	    if (sql_connection.getSerializableType().equals("BLOB"))
	    {
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    try(ObjectOutputStream os=new ObjectOutputStream(baos))
		    {
			os.writeObject(field.get(_class_instance));
		
			try(ByteArrayInputStream bais=new ByteArrayInputStream(baos.toByteArray()))
			{
			    _result_set.updateBlob(_sft.translateField(sql_fields[0]), bais);
			}
		    }
		}    
	    }
	    else
		_result_set.updateObject(_sft.translateField(sql_fields[0]), field.get(_class_instance));
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
	    Object val=field.get(_class_instance);
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
	    Blob b=_result_set.getBlob(sql_fields[0].short_field);
	    try(ByteArrayInputStream bais=new ByteArrayInputStream(b.getBytes(1, (int)b.length())))
	    {
		try(ObjectInputStream ois=new ObjectInputStream(bais))
		{
		    Object val1=ois.readObject();
		    if (val1==null || _field_instance==null)
			return _field_instance==val1;
		    if ((!(field.getType().isAssignableFrom(_field_instance.getClass()))))
			return false;
		    return val1.equals(_field_instance);
		}
	    }
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
	    if (sql_connection.getSerializableType().equals("BLOB"))
	    {
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    try(ObjectOutputStream os=new ObjectOutputStream(baos))
		    {
			os.writeObject(o);
		
			try(ByteArrayInputStream bais=new ByteArrayInputStream(baos.toByteArray()))
			{
			    _prepared_statement.setBlob(_field_start, bais);
			}
		    }
		}    
	    }
	    else
		_prepared_statement.setObject(_field_start, o);
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
	return compareTo_method!=null;
    }

    @Override
    public int compare(DatabaseRecord _r1, DatabaseRecord _r2) throws DatabaseException
    {
	if (!isComparable())
	    throw new DatabaseException("The field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+" of type "+field.getType().getName()+" is not a comparable field.");
	else
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
	    
		return ((Integer)compareTo_method.invoke(obj1, obj2)).intValue();
	    }
	    catch(Exception e)
	    {
		throw new DatabaseException("", e);
	    }
	}
    }

    @Override
    public boolean canBePrimaryOrUniqueKey()
    {
	return false;
    }
}
