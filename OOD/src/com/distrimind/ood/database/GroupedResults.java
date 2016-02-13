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

package com.distrimind.ood.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;

/**
 * This class enables to group results according given table fields. It is equivalent to the function "GROUP BY" in the SQL language.
 * 
 * To get an instance of this class, call the functions {@link com.distrimind.ood.database.Table#getGroupedResults(String...)} or {@link com.distrimind.ood.database.Table#getGroupedResults(Collection, String...)}.
 * 
 * @author Jason Mahdoub
 * @since 1.0
 * @version 1.0
 * @param <T> The type of the DatabaseRecord.
 */
public final class GroupedResults<T extends DatabaseRecord>
{
    private class Field
    {
	private final String field_name;
	private final ArrayList<FieldAccessor> fields=new ArrayList<>();
	
	public Field(String _field_name) throws ConstraintsNotRespectedDatabaseException
	{
	    field_name=_field_name;
	    ArrayList<String> strings=splitPoint(field_name);
	    Table<?> current_table=GroupedResults.this.table;
	    
	    for (int i=0;i<strings.size();i++)
	    {
		if (current_table==null)
		    throw new ConstraintsNotRespectedDatabaseException("The field "+field_name+" does not exists.");
		String f=strings.get(i);
		FieldAccessor founded_field=null;
		for (FieldAccessor fa : current_table.getFieldAccessors())
		{
		    if (fa.getFieldName().equals(f))
		    {
			founded_field=fa;
			break;
		    }
		}
		if (founded_field==null)
		    throw new ConstraintsNotRespectedDatabaseException("The field "+f+" does not exist into the class/table "+current_table.getClass().getName());
		
		fields.add(founded_field);
		
		if (founded_field.isForeignKey())
		    current_table=((ForeignKeyFieldAccessor)founded_field).getPointedTable();
		else
		    current_table=null;
	    }
	}
	public String getName()
	{
	    return field_name;
	}
	private ArrayList<String> splitPoint(String s)
	{
	    ArrayList<String> res=new ArrayList<String>(10);
	    int last_index=0;
	    for (int i=0;i<s.length();i++)
	    {
		if (s.charAt(i)=='.')
		{
		    if (i!=last_index)
		    {
			res.add(s.substring(last_index, i));
		    }
		    last_index=i+1;
		}
	    }
	    if (s.length()!=last_index)
	    {
		res.add(s.substring(last_index));
	    }
	
	    return res;
	 }
	
	public boolean equals(T o1, Object o2) throws DatabaseException
	{
	    DatabaseRecord r1=o1;
	    for (int i=0;i<fields.size()-1;i++)
	    {
		if (r1==null)
		    return r1==o2;
		ForeignKeyFieldAccessor f=(ForeignKeyFieldAccessor)fields.get(i);
		r1=(DatabaseRecord)f.getValue(r1);
	    }
	    if (r1==null)
		return r1==o2;
	    FieldAccessor fa=fields.get(fields.size()-1);
	    return fa.equals(r1, o2);
	}
	
	public Object getValue(T o) throws DatabaseException
	{
	    DatabaseRecord r=o;
	    for (int i=0;i<fields.size()-1;i++)
	    {
		if (r==null)
		    return null;
		ForeignKeyFieldAccessor f=(ForeignKeyFieldAccessor)fields.get(i);
		r=(DatabaseRecord)f.getValue(r);
	    }
	    if (r==null)
		return null;
	    FieldAccessor fa=fields.get(fields.size()-1);
	    return fa.getValue(r);
	}
	
    }
    
    
    
    
    private Class<T> class_record;
    protected final Table<T> table;
    protected final ArrayList<Field> group_definition=new ArrayList<Field>();
    private final ArrayList<Group> groups=new ArrayList<Group>();
    
    @SuppressWarnings("unchecked")
    private GroupedResults(DatabaseWrapper _sql_conncection, Collection<T> _records, Class<T> _class_record, String ..._fields) throws DatabaseException
    {
	class_record=null;
	
	class_record=_class_record;
	
	table=(Table<T>) _sql_conncection.getTableInstance(Table.getTableClass(class_record));
	
	if (_fields.length==0)
	    throw new ConstraintsNotRespectedDatabaseException("It must have at mean one field to use.");
	
	for (int i=0;i<_fields.length;i++)
	{
	    group_definition.add(new Field(_fields[i]));
	}
	
	if (_records!=null)
	    addRecords(_records);
    }
    
    /**
     * Add a record and sort it according the group definition.
     * @param _record the record
     * @throws DatabaseException if a database exception occurs
     */
    public void addRecord(T _record) throws DatabaseException
    {
	for (Group g : groups)
	{
	    if (g.addRecord(_record))
		return;
	}
	groups.add(new Group(_record));
    }
    
    /**
     * Add records and sort them according the group definition.
     * @param _records the records to add
     * @throws DatabaseException if a database exception occurs
     */
    public void addRecords(Collection<T> _records) throws DatabaseException
    {
	for (T r : _records)
	{
	    addRecord(r);
	}
    }
    
    /**
     * Returns the list of groups each containing a list of records.
     * @return the groups.
     */
    public ArrayList<Group> getGroupedResults()
    {
	return groups;
    }
    
    /**
     * This class represent a group (according a set of table fields) of records. It is relative to the use of the class {@link com.distrimind.ood.database.GroupedResults}.
     * @author Jason Mahdjoub
     * @since 1.0
     * @version 1.0
     *
     */
    public class Group
    {
	protected final HashMap<String, Object> group;
	protected final int hash_code;
	protected final ArrayList<T> results;
	
	
	protected Group(HashMap<String, Object> _group)
	{
	    group=_group;
	    hash_code=group.hashCode();
	    results=new ArrayList<T>();
	}
	protected Group(T _record) throws DatabaseException
	{
	    group=new HashMap<>();
	    for (Field fa : group_definition)
	    {
		group.put(fa.getName(), fa.getValue(_record));
	    }
	    
	    hash_code=group.hashCode();
	    results=new ArrayList<T>();
	    results.add(_record);
	}
	
	protected boolean addRecord(T _record) throws DatabaseException
	{
	    for (Field fa : group_definition)
	    {
		if (!fa.equals(_record, group.get(fa.getName())))
		    return false;
	    }
	    results.add(_record);
	    return true;
	}
	
	/**
	 * Returns the identity of the group by association a set of table fields with their instance.
	 * @return identity of the group
	 */
	public HashMap<String, Object> getGroupIdentity()
	{
	    return group;
	}
	
	
	@Override 
	public int hashCode()
	{
	    return hash_code;
	}
	
	@SuppressWarnings("unchecked")
	@Override 
	public boolean equals(Object o)
	{
	    if (o instanceof GroupedResults.Group)
		return equals((GroupedResults<T>.Group)o);
	    else
		return false;
	}
	
	public boolean equals(Group _group)
	{
	    for (String s : group.keySet())
	    {
		if (!group.get(s).equals(_group.group.get(s)))
		    return false;
	    }
	    return true;
	}
	
	
	/**
	 * Returns the records associated to this group.
	 * @return the records associated to this group.
	 */
	public ArrayList<T> getResults()
	{
	    return results;
	}
	
    }
    
}
