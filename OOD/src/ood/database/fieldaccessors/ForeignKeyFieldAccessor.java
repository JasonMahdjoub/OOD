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
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import ood.database.DatabaseRecord;
import ood.database.HSQLDBWrapper;
import ood.database.SqlField;
import ood.database.SqlFieldInstance;
import ood.database.Table;
import ood.database.exceptions.DatabaseException;
import ood.database.exceptions.FieldDatabaseException;


public class ForeignKeyFieldAccessor extends FieldAccessor
{
    protected SqlField sql_fields[]=null;
    protected ArrayList<FieldAccessor> linked_primary_keys=null; 
    protected String linked_table_name=null;
    protected Table<? extends DatabaseRecord> pointed_table=null;
    private final Class<?>[] compatible_classes;
    
    private static Method get_record_method;
    static
    {
	try
	{
	    get_record_method=Table.class.getDeclaredMethod("getRecordFromPointingRecord", (new SqlFieldInstance[1]).getClass(), (new ArrayList<DatabaseRecord>()).getClass());
	    get_record_method.setAccessible(true);
	}
	catch (SecurityException e)
	{
	    System.err.println("Impossible to access to the function getRecordFromPointingRecord of the class Table. This is an inner bug of MadKitGroupExtension. Please contact the developers. Impossible to continue. See the next error :");
	    e.printStackTrace();
	    System.exit(-1);
	}
	catch (NoSuchMethodException e)
	{
	    System.err.println("Impossible to found to the function getRecordFromPointingRecord of the class Table. This is an inner bug of MadKitGroupExtension. Please contact the developers. Impossible to continue. See the next error :");
	    e.printStackTrace();
	    System.exit(-1);
	}
	
    }
    
    
    protected ForeignKeyFieldAccessor(HSQLDBWrapper _sql_connection, Field _field) throws DatabaseException
    {
	super(_sql_connection, _field);
	if (!DatabaseRecord.class.isAssignableFrom(_field.getType()))
	    throw new DatabaseException("The field "+_field.getName()+" of the class "+_field.getDeclaringClass().getName()+" is not a DatabaseRecord.");
	if (!field.getType().getPackage().equals(field.getDeclaringClass().getPackage()))
	    throw new DatabaseException("The package of the pointed DatabaseRecord class "+field.getType().getName()+", is not the same then the package of the containing class "+field.getDeclaringClass().getName()+" (Foregin key="+field.getName()+").");
	compatible_classes=new Class<?>[1];
	compatible_classes[0]=field.getType();
	
    }
    public void initialize() throws DatabaseException
    {
	if (sql_fields==null)
	{
	    @SuppressWarnings("unchecked")
	    Class<? extends DatabaseRecord> c=(Class<? extends DatabaseRecord>)field.getType();
	    pointed_table=Table.getTableInstance(sql_connection, Table.getTableClass(c));
	    linked_primary_keys=pointed_table.getPrimaryKeysFieldAccessors();
	    linked_table_name=pointed_table.getName();
	    
	    ArrayList<SqlField> sql_fields=new ArrayList<SqlField>();
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		if (fa.isForeignKey())
		{
		    ((ForeignKeyFieldAccessor)fa).initialize();
		}
		for (SqlField sf : fa.getDeclaredSqlFields())
		{
		    sql_fields.add(new SqlField(table_name+"."+this.getFieldName()+"__"+pointed_table.getName()+"_"+sf.short_field, sf.type, pointed_table.getName(), sf.field));
		}
	    }
	    this.sql_fields=new SqlField[sql_fields.size()];
	    for (int i=0;i<sql_fields.size();i++)
		this.sql_fields[i]=sql_fields.get(i);	    
	}
    }
    
    
    public Table<? extends DatabaseRecord> getPointedTable()
    {
	return pointed_table;
    }
    @Override
    public void setValue(DatabaseRecord _class_instance, Object _field_instance) throws DatabaseException
    {
	try
	{
	    if (_field_instance==null)
	    {
		if (isNotNull())
		    throw new FieldDatabaseException("The given _field_instance, used to store the field "+field.getName()+" (type="+field.getType().getName()+", declaring_class="+field.getDeclaringClass().getName()+") into the DatabaseRecord class "+field.getDeclaringClass().getName()+", is null and should not be (property NotNull present).");
	    }
	    else if (!(_field_instance.getClass().equals(field.getType())))
		throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", should be a "+field.getType().getName()+" and not a "+_field_instance.getClass().getName());
	    if (_field_instance==_class_instance)
		throw new FieldDatabaseException("The given _field_instance parameter, destinated to the field "+field.getName()+" of the class "+field.getDeclaringClass().getName()+", is the same reference than the correspondant table (autoreference).");
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
	if (_field_instance!=null && !(_field_instance.getClass().equals(field.getType())))
	    return false;
	try
	{
	    
	    if (_field_instance==null && isNotNull())
		return false;
	    DatabaseRecord val1=(DatabaseRecord)field.get(_class_instance);
	    DatabaseRecord val2=(DatabaseRecord)_field_instance;
	    if (val1==val2)
		return true;
	    if (val1==null || val2==null)
		return false;
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		if (!fa.equals(val1, fa.getValue(val2)))
		{
		    return false;
		}
	    }
	    return true;
	}
	catch(Exception e)
	{
	    throw new DatabaseException("",e);
	}
	
    }

    @Override
    protected boolean equals(Object _field_instance, ResultSet _result_set, SqlFieldTranslation _sft) throws DatabaseException
    {
	if (_field_instance!=null && !(_field_instance.getClass().equals(field.getType())))
	    return false;
	    
	DatabaseRecord val1=(DatabaseRecord)_field_instance;
	    
	for (FieldAccessor fa : linked_primary_keys)
	{
	    if (!fa.equals(val1, _result_set, new SqlFieldTranslation(fa, _sft)))
		return false;
	}
	return true;
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
	Object val=this.getValue(_instance);
	SqlFieldInstance res[]=new SqlFieldInstance[sql_fields.length];
	if (val==null)
	{
	    for (int i=0;i<sql_fields.length;i++)
		res[i]=new SqlFieldInstance(sql_fields[i], null);
	}
	else
	{
	    int i=0;
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		SqlFieldInstance linked_sql_field_instances[]=fa.getSqlFieldsInstances((DatabaseRecord)val);
		for (SqlFieldInstance sfi : linked_sql_field_instances)
		{
		    res[i++]=new SqlFieldInstance(table_name+"."+this.getFieldName()+"__"+pointed_table.getName()+"_"+sfi.short_field, sfi.type, linked_table_name, sfi.field, sfi.instance);
		}
	    }
	}
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
	    ArrayList<DatabaseRecord> list=_pointing_records==null?new ArrayList<DatabaseRecord>():_pointing_records;
	    list.add(_class_instance);
	    SqlField sfs[]=getDeclaredSqlFields();
	    SqlFieldInstance sfis[]=new SqlFieldInstance[sfs.length];
	    for (int i=0;i<sfs.length;i++)
	    {
		sfis[i]=new SqlFieldInstance(sfs[i], _result_set.getObject(sfs[i].field));
	    }
	    field.set(_class_instance, get_record_method.invoke(getPointedTable(), sfis, list));
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
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		DatabaseRecord dr=(DatabaseRecord)field.get(_class_instance);
		if (dr==null)
		{
		    for (int i=0;i<this.sql_fields.length;i++)
		    {
			_prepared_statement.setObject(_field_start++, null);
		    }
		}
		else
		{
		    fa.getValue(dr, _prepared_statement, _field_start);
		    _field_start+=fa.getDeclaredSqlFields().length;
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
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		DatabaseRecord dr=(DatabaseRecord)field.get(_class_instance);
		if (dr==null)
		{
		    for (SqlField sf : sql_fields)
		    {
			_result_set.updateObject(sf.field, null);
		    }
		}
		else
		{
		    fa.updateResultSetValue(dr, _result_set, new SqlFieldTranslation(this));
		}
		
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
	    for (FieldAccessor fa : linked_primary_keys)
	    {
		DatabaseRecord dr=(DatabaseRecord)field.get(_class_instance);
		if (dr==null)
		{
		    for (SqlField sf : sql_fields)
		    {
			_result_set.updateObject(_sft.translateField(sf), null);
		    }
		}
		else
		{
		    fa.updateResultSetValue(dr, _result_set, new SqlFieldTranslation(this, _sft));
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
}
