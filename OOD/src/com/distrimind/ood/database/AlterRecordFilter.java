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

import java.util.Map;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * This interface is used to filter records which have to be altered or deleted into the database. 
 * User must inherit this filter to include or not every record instance into the considered query.
 * This filter must be used with the function {@link com.distrimind.ood.database.Table#updateRecords(AlterRecordFilter)}.
 * @author Jason Mahdjoub
 * @version 1.0
 * @param <T> the record type which correspond to its database class.
 */
public abstract class AlterRecordFilter<T extends DatabaseRecord>
{
    private boolean to_delete=false;
    private boolean to_delete_with_cascade=false;
    
    private Map<String, Object> modifications=null;
    private boolean modificationFromRecordInstance=false;
    
    /**
     * This function is called for every instance record present on the database.
     * The user must call functions {@link #update(Map)}, {@link #remove()}, or {@link #removeWithCascade()} into this function, in order to alter, remove or remove with cascade the current record. 
     * Modifications are applied after the end of this function. 
     * 
     * @param _record the instance 
     * @throws DatabaseException if a database exception occurs
     */
    public abstract void nextRecord(T _record) throws DatabaseException;
    
    /**
     * Must be called into the function {@link #nextRecord(DatabaseRecord)}. This function aims to remove the current record. Do not works if the current record is pointed by other records.
     * 
     */
    protected final void remove()
    {
	to_delete=true;
	to_delete_with_cascade=false;
    }
    
    /**
     * Must be called into the function {@link #nextRecord(DatabaseRecord)}. This function aims to remove the current record and all records pointing to it.
     * 
     */
    protected final void removeWithCascade()
    {
	to_delete=false;
	to_delete_with_cascade=true;
    }

    /**
     * Must be called into the function {@link #nextRecord(DatabaseRecord)}. This function aims to alter the current record. 
     *  
     * @param fields a map containing the fields to alter with the given record. Note that primary keys, and unique keys cannot be altered with this filter. To do that, please use the function {@link com.distrimind.ood.database.Table#updateRecord(DatabaseRecord, Map)}. 
     */
    protected final void update(Map<String, Object> fields)
    {
	modifications=fields;
    }
    
    /**
     * Must be called into the function {@link #nextRecord(DatabaseRecord)}. This function aims to alter the current record. 
     *  
     */
    protected final void update()
    {
	modificationFromRecordInstance=true;
    }
    
    
    /**
     * Reset the modifications to do into the current record.
     */
    final void reset()
    {
	to_delete=false;
	to_delete_with_cascade=false;
	modifications=null;
    }
    
    /**
     * @return true if the current record has to be removed without cascade.
     */
    final boolean hasToBeRemoved()
    {
	return to_delete;
    }
    
    /**
     * @return true if the current record has to be removed with cascade.
     */
    final boolean hasToBeRemovedWithCascade()
    {
	return to_delete_with_cascade;
    }
    
    
    /**
     * @return the modifications to be done into the current record.
     */
    final Map<String, Object> getModifications()
    {
	return modifications;
    }
    
    final boolean isModificatiedFromRecordInstance()
    {
	return modificationFromRecordInstance;
    }
}
