
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
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
public abstract class AlterRecordFilter<T extends Object>
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
     * Must be called into the function {@link #nextRecord(Object)}. This function aims to remove the current record. Do not works if the current record is pointed by other records.
     * 
     */
    protected final void remove()
    {
	to_delete=true;
	to_delete_with_cascade=false;
    }
    
    /**
     * Must be called into the function {@link #nextRecord(Object)}. This function aims to remove the current record and all records pointing to it.
     * 
     */
    protected final void removeWithCascade()
    {
	to_delete=false;
	to_delete_with_cascade=true;
    }

    /**
     * Must be called into the function {@link #nextRecord(Object)}. This function aims to alter the current record. 
     *  
     * @param fields a map containing the fields to alter with the given record. Note that primary keys, and unique keys cannot be altered with this filter. To do that, please use the function {@link com.distrimind.ood.database.Table#updateRecord(Object, Map)}. 
     */
    protected final void update(Map<String, Object> fields)
    {
	modifications=fields;
    }
    
    /**
     * Must be called into the function {@link #nextRecord(Object)}. This function aims to alter the current record. 
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
	modificationFromRecordInstance=false;
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
