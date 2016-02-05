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
import java.util.Calendar;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;


public class CalendarFieldAccessor extends SerializableFieldAccessor
{

    protected CalendarFieldAccessor(Field _field) throws DatabaseException
    {
	super(_field);
	if (!Calendar.class.isAssignableFrom(_field.getType()))
	    throw new FieldDatabaseException("The field "+_field.getName()+" of the class "+_field.getDeclaringClass().getName()+" of type "+_field.getType()+" must be a Calendar type."); 
    }

}
