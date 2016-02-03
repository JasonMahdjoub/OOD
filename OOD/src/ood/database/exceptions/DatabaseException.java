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



package ood.database.exceptions;

import java.sql.SQLException;

/**
 * This exception is the generic exception used if a problem into the database occurs
 * @author Jason Mahdjoub
 * @version 1.0
 *
 */
public class DatabaseException extends Exception
{
    /**
     * 
     */
    private static final long serialVersionUID = 1629424573316997573L;

    public DatabaseException(String _message)
    {
	super(_message);
    }
    public DatabaseException(String _message, Exception e)
    {
	super(_message, e);
    }
    
    public static DatabaseException getDatabaseException(Exception e)
    {
	if (e==null)
	    return null;
	if (DatabaseException.class.isAssignableFrom(e.getClass()))
	    return (DatabaseException)e;
	Throwable res=e.getCause();
	while (res!=null)
	{
	    if (DatabaseException.class.isAssignableFrom(res.getClass()))
		return (DatabaseException)res;
	    res=res.getCause();
	}
	
	if (e instanceof SQLException)
	{
	    return new DatabaseException("A Sql exception occurs", e);
	}
	else
	    return new DatabaseException("Unexpected exception", e);
    }
}
