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

import java.util.Iterator;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * Defines a table iterator. 
 * 
 * This iterator must be closed when it becomes unused. 
 * Without being closed, the table will become locked.
 * Note that this interface implements {@link AutoCloseable} interface. 
 * 
 * @author Jason Mahdjoub
 *
 * @param <DR> The database record type 
 * @version 1.0
 * @since OOD 1.5
 */
public interface TableIterator<DR extends DatabaseRecord> extends Iterator<DR>, AutoCloseable
{

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws DatabaseException;
}
