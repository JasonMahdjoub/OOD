
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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 1.5
 */
public class ByteTabConvertibleFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];
    private final ByteTabObjectConverter converter;
    private final boolean isVarBinary;
    
    protected ByteTabConvertibleFieldAccessor(Class<? extends Table<?>> table_class, DatabaseWrapper _sql_connection, Field _field, String parentFieldName, ByteTabObjectConverter converter) throws DatabaseException
    {
	super(_sql_connection, _field,parentFieldName, getCompatibleClasses(_field), table_class);
	sql_fields=new SqlField[1];
	
	String type=null;
	long l=limit;
	if (l<=0)
	    l=16777216l;
	if (l<=4096)
	{
	    if (DatabaseWrapperAccessor.isVarBinarySupported(sql_connection))
		type="VARBINARY("+l+")";
	    else if (DatabaseWrapperAccessor.isLongVarBinarySupported(sql_connection))
		type="LONGVARBINARY("+l+")";
	    else
		type="BLOB("+l+")";
	}
	else
	{
	    if (DatabaseWrapperAccessor.isLongVarBinarySupported(sql_connection))
		type="LONGVARBINARY("+l+")";
	    else
		type="BLOB("+l+")";
	}
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), type, null, null, isNotNull());
	
	isVarBinary=type.startsWith("VARBINARY") || type.startsWith("LONGVARBINARY");
	this.converter=converter;
    }
    private static Class<?>[] getCompatibleClasses(Field field)
    {
	Class<?>[] res=new Class<?>[1];
	res[0]=field.getType();
	return res;
    }

    @Override
    public void setValue(Object _class_instance, Object _field_instance) throws DatabaseException
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
    public boolean equals(Object _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    Object o=field.get(_class_instance);
	    if (o==_field_instance)
		return true;
	    if (o!=null)
		return o.equals(_field_instance);
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
	    byte[] val1=null;
	    if (_field_instance instanceof byte[])
		val1=(byte[])_field_instance;
	    
	    byte[] val2=null;
	    
	    if (isVarBinary)
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
    public Object getValue(Object _class_instance) throws DatabaseException
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
    public SqlFieldInstance[] getSqlFieldsInstances(Object _instance) throws DatabaseException
    {
	SqlFieldInstance res[]=new SqlFieldInstance[1];
	res[0]=new SqlFieldInstance(sql_fields[0], converter.getByte(getValue(_instance)));
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
    public int compare(Object _r1, Object _r2) throws DatabaseException
    {
	throw new DatabaseException("Unexpected exception");
    }

    @Override
    public void setValue(Object _class_instance, ResultSet _result_set, ArrayList<DatabaseRecord> _pointing_records) throws DatabaseException
    {
	try
	{
	    byte[] res=null;
	    if (isVarBinary)
	    {
		res=_result_set.getBytes(getColmunIndex(_result_set, sql_fields[0].field));
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
	    }
	    else
	    {
		Blob b=_result_set.getBlob(getColmunIndex(_result_set, sql_fields[0].field));
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
    public void getValue(Object _class_instance, PreparedStatement _prepared_statement, int _field_start) throws DatabaseException
    {
	try
	{
	    getValue(_prepared_statement, _field_start, field.get(_class_instance));
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
    }
    @Override
    public void getValue(PreparedStatement _prepared_statement, int _field_start, Object o) throws DatabaseException
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
	    if (isVarBinary)
		_prepared_statement.setBytes(_field_start, b);
	    else
	    {
		if (b==null)
		    _prepared_statement.setObject(_field_start, null);
		else
		{
		    Blob blob=DatabaseWrapperAccessor.getBlob(sql_connection, b);
		    if (blob==null && b!=null)
			_prepared_statement.setBinaryStream(_field_start, new ByteArrayInputStream(b));
		    else
			_prepared_statement.setBlob(_field_start, blob);
		}
	    }
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    @Override
    public void updateValue(Object _class_instance, Object _field_instance, ResultSet _result_set) throws DatabaseException
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
	    if (isVarBinary)
		_result_set.updateBytes(sql_fields[0].short_field, b);
	    else
	    {
		if (b==null)
		    _result_set.updateObject(sql_fields[0].short_field, null);
		else
		{
		    Blob blob=DatabaseWrapperAccessor.getBlob(sql_connection, b);
		    if (blob==null && b!=null)
			_result_set.updateBinaryStream(sql_fields[0].short_field, new ByteArrayInputStream(b));
		    else
			_result_set.updateBlob(sql_fields[0].short_field, blob);
		}
	    }
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
    }
    
    @Override
    protected void updateResultSetValue(Object _class_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException
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
	    if (isVarBinary)
		_result_set.updateBytes(_sft.translateField(sql_fields[0]), b);
	    else
	    {
		if (b==null)
		    _result_set.updateObject(_sft.translateField(sql_fields[0]), null);
		else
		{
		    Blob blob=DatabaseWrapperAccessor.getBlob(sql_connection, b);
		    if (blob==null && b!=null)
			_result_set.updateBinaryStream(_sft.translateField(sql_fields[0]), new ByteArrayInputStream(b));    
		    else
			_result_set.updateBlob(_sft.translateField(sql_fields[0]), blob);
		}
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

    @Override
    public void serialize(DataOutputStream _oos, Object _class_instance) throws DatabaseException
    {
	try
	{
	    Object o=getValue(_class_instance);
	    
	    if (o!=null)
	    {
		byte[] b=converter.getByte(o);		
		_oos.writeInt(b.length);
		_oos.write(b);
	    }
	    else
	    {
		_oos.writeInt(-1);
	    }
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }



    @Override
    public void unserialize(DataInputStream _ois, HashMap<String, Object> _map) throws DatabaseException
    {
	try
	{
	    int size=_ois.readInt();
	    if (size>-1)
	    {
		byte[] b=new byte[size];
		int os=_ois.read(b);
		if (os!=size)
		    throw new DatabaseException("read bytes insuficiant (expected size="+size+", obtained size="+os+")");
		
		_map.put(getFieldName(), converter.getObject(field.getType(), b));
	    }
	    else if (isNotNull())
		throw new DatabaseException("field should not be null");
	    else
		_map.put(getFieldName(), null);	
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    @Override
    public Object unserialize(DataInputStream _ois, Object _classInstance) throws DatabaseException
    {
	try
	{
	    int size=_ois.readInt();
	    if (size>-1)
	    {
		byte[] b=new byte[size];
		int os=_ois.read(b);
		if (os!=size)
		    throw new DatabaseException("read bytes insuficiant (expected size="+size+", obtained size="+os+")");
		Object o=converter.getObject(field.getType(), b);
		setValue(_classInstance, o);
		return o;
	    }
	    else if (isNotNull())
		throw new DatabaseException("field should not be null");
	    else
	    {
		setValue(_classInstance, (Object)null);
		return null;
	    }
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }    
    
}
