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


package com.distrimind.ood.database.exceptions;

import com.distrimind.ood.database.Table;

/**
 * This exception is generated when the loaded SqlJet database does not correspond to the correspondent table classes.
 * @author Jason Mahdjoub
 * @version 1.0
 *
 */
public class DatabaseVersionException extends DatabaseException
{
    public DatabaseVersionException(Table<?> _table, String _additional_message)
    {
	super("The table "+_table.getName()+" exists into the database but does not corresponds to the class "+_table.getName()+" coded into this program. This is a problem of Version. "+_additional_message);
    }

    /**
     * 
     */
    private static final long serialVersionUID = -4067663818886835510L;

}
