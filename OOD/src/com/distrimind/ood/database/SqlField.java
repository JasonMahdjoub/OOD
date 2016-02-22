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


package com.distrimind.ood.database;

/**
 * This class represent a field description into the Sql database. The user should not use this class.
 * @author Jason Mahdjoub
 * @version 1.0
 */
public class SqlField
{
    
    	/**
    	 * the name of the Sql field appended with the name of the Sql Table.
    	 */
	public final String field;
	
	/**
	 * the type of the Sql field.
	 */
	public final String type;
	
	/**
	 * The name of the pointed Sql table, if this field is a foreign key
	 */
	public final String pointed_table;
	
	/**
	 * The name of the pointed Sql field appended with its Sql Table, if this field is a foreign key
	 */
	public final String pointed_field;
	
	/**
	 * the name of the Sql field not appended with the name of the Sql Table.
	 */
	public final String short_field;
	
	/**
	 * The name of the pointed Sql field not appended with its Sql Table, if this field is a foreign key
	 */
	public final String short_pointed_field;
	
	
	
	public int sql_position=-1;
	/**
	 * Constructor
	 * @param _field the name of the Sql field. 
	 * @param _type the type of the Sql field.
	 * @param _pointed_field The name of the pointed Sql field, if this field is a foreign key
	 * @param _pointed_table The name of the pointed Sql table, if this field is a foreign key
	 */
	public SqlField(String _field, String _type, String _pointed_table, String _pointed_field)
	{
	    field=_field.toUpperCase();
	    type=_type.toUpperCase();
	    pointed_table=_pointed_table==null?null:_pointed_table.toUpperCase();
	    pointed_field=_pointed_field==null?null:_pointed_field.toUpperCase();
	    
	    int index=-1;
	    for (int i=0;i<field.length();i++)
	    {
		if (field.charAt(i)=='.')
		{
		    index=i+1;
		    break;
		}
	    }
	    if (index!=-1)
		short_field=field.substring(index);
	    else
		short_field=field;

	    if (pointed_field!=null)
	    {
		index=-1;
		for (int i=0;i<pointed_field.length();i++)
		{
		    if (pointed_field.charAt(i)=='.')
		    {
			index=i+1;
			break;
		    }
		}
		if (index!=-1)
		    short_pointed_field=pointed_field.substring(index);
		else
		    short_pointed_field=pointed_field;
	    }
	    else
		short_pointed_field=null;
	}
}
