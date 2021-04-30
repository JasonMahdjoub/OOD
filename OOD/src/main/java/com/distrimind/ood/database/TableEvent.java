
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language

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

import com.distrimind.util.DecentralizedValue;

import java.util.HashMap;
import java.util.Set;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class TableEvent<T extends DatabaseRecord> extends DatabaseEvent {
	private final int id;
	private final DatabaseEventType type;

	private final T oldDatabaseRecord;
	private final T newDatabaseRecord;
	private final boolean forced;
	private transient final Set<DecentralizedValue> hostsDestination;
	private transient HashMap<String, Object> mapKeys = null;
	private transient boolean oldAlreadyPresent = false;
	private Table<? extends T> table;


	TableEvent(int id, DatabaseEventType type, Table<? extends T> table, T oldDatabaseRecord, T newDatabaseRecord,
			Set<DecentralizedValue> resentTo) {
		if (type == null)
			throw new NullPointerException("type");
		if (table==null)
			throw new NullPointerException();
		if (oldDatabaseRecord == null && type != DatabaseEventType.ADD && type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
			throw new NullPointerException("oldDatabaseRecord");
		if (newDatabaseRecord == null && type != DatabaseEventType.REMOVE
				&& type != DatabaseEventType.REMOVE_WITH_CASCADE
				&& type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
			throw new NullPointerException("bewDatabaseRecord");
		if (type==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE &&
				(oldDatabaseRecord!=null || newDatabaseRecord!=null))
			throw new IllegalArgumentException();
		this.id = id;
		this.type = type;
		this.table=table;
		this.oldDatabaseRecord = oldDatabaseRecord;
		this.newDatabaseRecord = newDatabaseRecord;
		this.forced = resentTo != null && !resentTo.isEmpty();
		this.hostsDestination = forced ? resentTo : null;
	}

	TableEvent(int id, DatabaseEventType type, Table<? extends T> table, T oldDatabaseRecord, T newDatabaseRecord,
			Set<DecentralizedValue> resentTo, HashMap<String, Object> mapKeys, boolean oldAlreadyPresent) {
		if (type == null)
			throw new NullPointerException("type");
		if (table==null)
			throw new NullPointerException();
		if (oldDatabaseRecord == null && type != DatabaseEventType.ADD && (mapKeys == null || mapKeys.isEmpty())
				&& type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
			throw new NullPointerException("oldDatabaseRecord");
		if (newDatabaseRecord == null && type != DatabaseEventType.REMOVE
				&& type != DatabaseEventType.REMOVE_WITH_CASCADE
				&& type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
			throw new NullPointerException("bewDatabaseRecord");
		if (type==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE &&
				(oldDatabaseRecord!=null || newDatabaseRecord!=null || (mapKeys != null && !mapKeys.isEmpty())))
			throw new IllegalArgumentException();
		this.id = id;
		this.type = type;
		this.table=table;
		this.oldDatabaseRecord = oldDatabaseRecord;
		this.newDatabaseRecord = newDatabaseRecord;
		this.forced = resentTo != null && !resentTo.isEmpty();
		this.hostsDestination = forced ? resentTo : null;
		this.oldAlreadyPresent = oldAlreadyPresent;
		this.mapKeys = mapKeys;
	}

	Set<DecentralizedValue> getHostsDestination() {
		return hostsDestination;
	}

	@Override
	public String toString() {
		return "TableEvent[" + type + ", forced=" + forced + ", old=" + oldDatabaseRecord + ", new=" + newDatabaseRecord
				+ "]";
	}

	boolean isOldAlreadyPresent() {
		return oldAlreadyPresent;
	}

	HashMap<String, Object> getMapKeys() {
		return mapKeys;
	}

	boolean isForced() {
		return forced;
	}

	public int getID() {
		return id;
	}

	public DatabaseEventType getType() {
		return type;
	}

	public T getOldDatabaseRecord() {
		return oldDatabaseRecord;
	}

	public T getNewDatabaseRecord() {
		return newDatabaseRecord;
	}

	public Table<? extends T> getTable() {
		return table;
	}

	void setTable(Table<? extends T> table) {
		this.table = table;
	}
}
