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
 * This interface is used to filter results or queries into the database. 
 * User must inherit this filter to include or not every record instance into the considered query.
 * This filter can be used for example by the functions {@link com.distrimind.ood.database.Table#getRecords(Filter)} or {@link com.distrimind.ood.database.Table#removeRecords(Filter)}.
 * @author Jason Mahdjoub
 * @version 1.0
 * @param <T> the record type which correspond to its database class.
 */
public interface Filter<T extends DatabaseRecord>
{
    /**
     * this function is called for every instance record present on the database. 
     * It returns true if the instance must be considered, false else. 
     * @param _record the instance
     * @return true if the instance must be used throw this filter, false else
     * @throws DatabaseException if a database exception occurs
     */
    public abstract boolean nextRecord(T _record) throws DatabaseException;
    
}
