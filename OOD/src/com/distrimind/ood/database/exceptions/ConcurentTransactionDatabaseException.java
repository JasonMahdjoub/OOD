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

/**
 * This exception is generated when the user attempt to access in an nested way, two functions of the database. 
 * It is not generated when two threads access to the database. Typically, this exception occurs in some cases when the user call a database function into the classes {@link com.distrimind.ood.database.AlterRecordFilter} and {@link com.distrimind.ood.database.Filter}.
 * Nested queries are authorized, but write operations are limited (for example, it is impossible to write to the same table with nested queries).
 *   
 * @author Jason Mahdjoub
 * @version 1.1
 *
 */
public class ConcurentTransactionDatabaseException extends DatabaseException
{

    public ConcurentTransactionDatabaseException(String message)
    {
	super(message);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 5359800859903751326L;

}
