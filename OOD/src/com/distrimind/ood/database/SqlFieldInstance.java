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

/**
 * This class represent a field description with its instance into the SqlJet database. The user should not use this class.
 * @author Jason Mahdjoub
 * @version 1.0
 */

public class SqlFieldInstance extends SqlField
{
    /**
     * the field instance
     */
    public final Object instance;
    
    /**
     * Constructor
     * @param _field the name of the SqlJet field. 
     * @param _type the type of the SqlJet field.
     * @param _pointed_field The name of the pointed Sql field, if this field is a foreign key
     * @param _pointed_table The name of the pointed SqlJet table, if this field is a foreign key
     * @param _instance The field instance
     */
    public SqlFieldInstance(String _field, String _type, String _pointed_table, String _pointed_field, Object _instance)
    {
	super(_field, _type, _pointed_table, _pointed_field);
	instance=_instance;
    }
    /**
     * Constructor
     * @param _sql_field The description of the SqlJet field.
     * @param _instance The field instance.
     */
    public SqlFieldInstance(SqlField _sql_field, Object _instance)
    {
	super(_sql_field.field, _sql_field.type, _sql_field.pointed_table, _sql_field.pointed_field);
	instance=_instance;
    }

}
