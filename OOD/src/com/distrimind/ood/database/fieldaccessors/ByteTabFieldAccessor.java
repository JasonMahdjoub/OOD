
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
import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

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
public class ByteTabFieldAccessor extends FieldAccessor
{
    protected final SqlField sql_fields[];
    private final boolean isVarBinary;
    protected ByteTabFieldAccessor(DatabaseWrapper _sql_connection, Field _field) throws DatabaseException
    {
	super(_sql_connection, _field, compatible_classes);
	sql_fields=new SqlField[1];
	sql_fields[0]=new SqlField(table_name+"."+this.getFieldName(), (limit==0)?(sql_connection.isVarBinarySupported()?"VARBINARY("+16777216+")":"BLOB"):((limit>4096 || !sql_connection.isVarBinarySupported())?("BLOB("+limit+")"):("VARBINARY("+limit+")")), null, null);
	isVarBinary=sql_fields[0].type.startsWith("VARBINARY");
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
	    else if (_field_instance.getClass().equals(this.getCompatibleClasses()[0]))
		field.set(_class_instance, _field_instance);
	    else
		throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be an array of Byte and not a "+_field_instance.getClass().getName());
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
    
    private static final Class<?>[] compatible_classes={(new byte[0]).getClass()};


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
	    if (isVarBinary)
	    {
		byte[] res=_result_set.getBytes(sql_fields[0].short_field);
		if (res==null && isNotNull())
		    throw new DatabaseIntegrityException("Unexpected exception.");
		field.set(_class_instance, res);
	    }
	    else
	    {
		Blob b=_result_set.getBlob(sql_fields[0].short_field);
		byte[] res=b==null?null:b.getBytes(1, (int)b.length());
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
	    if (isVarBinary)
		_prepared_statement.setBytes(_field_start, (byte[])o);
	    else
	    {
		Blob blob=sql_connection.getBlob((byte[])o);
		if (blob==null && o!=null)
		    _prepared_statement.setBinaryStream(_field_start, new ByteArrayInputStream((byte[])o));
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
	    if (isVarBinary)
		_result_set.updateBytes(sql_fields[0].short_field, (byte[])field.get(_class_instance));
	    else
	    {
		byte[] b=(byte[])field.get(_class_instance);
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
	    if (isVarBinary)
		_result_set.updateBytes(_sft.translateField(sql_fields[0]), (byte[])field.get(_class_instance));
	    else
	    {
		byte[] b=(byte[])field.get(_class_instance);
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
    
    @Override
    public boolean canAutoGenerateValues()
    {
	return true;
    }

    @Override
    public Object autoGenerateValue(Random _random) 
    {
	byte[] res=new byte[getBitsNumber()/8];
	_random.nextBytes(res);
	return res;
    }

    @Override
    public boolean needToCheckUniquenessOfAutoGeneratedValues()
    {
	return true;
    }

    @Override
    public int getDefaultBitsNumberForAutoGeneratedValues()
    {
	return 128;
    }

    @Override
    public int getMaximumBitsNumberForAutoGeneratedValues()
    {
	return Integer.MAX_VALUE;
    }
}
