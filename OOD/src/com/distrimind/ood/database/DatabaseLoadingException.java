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

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * This exception occurs when a SqlJet database loading occurs
 * @author Jason Mahdjoub
 * @version 1.0
 */
public class DatabaseLoadingException extends DatabaseException
{

    public DatabaseLoadingException(String _message)
    {
	super(_message);
    }
    public DatabaseLoadingException(String _message, Exception e)
    {
	super(_message, e);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 6220211115965771697L;
    
}
