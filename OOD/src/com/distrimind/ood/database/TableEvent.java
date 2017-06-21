
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

import java.util.HashMap;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class TableEvent<T extends DatabaseRecord> extends DatabaseEvent
{
    private final int id;
    private final DatabaseEventType type;
    
    private final T oldDatabaseRecord;
    private final T newDatabaseRecord;
    private final boolean force;
    private HashMap<String, Object> mapKeys=null;
    private boolean oldAlreadyPresent=false;
    
    
    TableEvent(int id, DatabaseEventType type, T oldDatabaseRecord, T newDatabaseRecord, boolean force)
    {
	if (type==null)
	    throw new NullPointerException("type");
	if (oldDatabaseRecord==null && type!=DatabaseEventType.ADD)
	    throw new NullPointerException("oldDatabaseRecord");
	if (newDatabaseRecord==null && type!=DatabaseEventType.REMOVE && type!=DatabaseEventType.REMOVE_WITH_CASCADE)
	    throw new NullPointerException("bewDatabaseRecord");
	this.id=id;
	this.type=type;
	this.oldDatabaseRecord=oldDatabaseRecord;
	this.newDatabaseRecord=newDatabaseRecord;
	this.force=force;
    }
    TableEvent(int id, DatabaseEventType type, T oldDatabaseRecord, T newDatabaseRecord, boolean force, HashMap<String, Object> mapKeys, boolean oldAlreadyPresent)
    {
	this(id, type, oldDatabaseRecord, newDatabaseRecord, force);
	this.mapKeys=mapKeys;
	this.oldAlreadyPresent=oldAlreadyPresent;
    }
    
    boolean isOldAlreadyPresent()
    {
	return oldAlreadyPresent;
    }
    
    HashMap<String, Object> getMapKeys()
    {
	return mapKeys;
    }
    
    boolean isForce()
    {
	return force;
    }
    
    public int getID()
    {
	return id;
    }
    
    
    public DatabaseEventType getType()
    {
	return type;
    }

    public T getOldDatabaseRecord()
    {
	return oldDatabaseRecord;
    }
    
    public T getNewDatabaseRecord()
    {
	return newDatabaseRecord;
    }
    
    public Table<T> getTable(DatabaseWrapper wrapper) throws DatabaseException
    {
	@SuppressWarnings("unchecked")
	Table<T> tableInstance = (Table<T>) wrapper.getTableInstance(Table.getTableClass(oldDatabaseRecord.getClass()));
	return tableInstance;
    }
}

