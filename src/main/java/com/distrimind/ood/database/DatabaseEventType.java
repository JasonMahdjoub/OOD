
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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

import java.util.ArrayList;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public enum DatabaseEventType {
	UPDATE((byte) 1, true, true),
	ADD((byte) 2, true, false),
	REMOVE((byte) 4, false, true),
	REMOVE_WITH_CASCADE((byte) 8, false, true),
	REMOVE_ALL_RECORDS_WITH_CASCADE((byte) 16, false, false);


	private final byte type;
	private final boolean hasNewValue;
	private final boolean hasOldValue;

	DatabaseEventType(byte type, boolean hasNewValue, boolean hasOldValue) {
		this.type = type;
		this.hasNewValue = hasNewValue;
		this.hasOldValue = hasOldValue;
	}

	public boolean hasOldValue() {
		return hasOldValue;
	}

	public boolean needsNewValue() {
		return hasNewValue;
	}

	public byte getByte() {
		return type;
	}

	public static DatabaseEventType getEnum(byte type) {
		for (DatabaseEventType e : DatabaseEventType.values())
			if (e.type == type)
				return e;
		return null;
	}

	public static ArrayList<DatabaseEventType> getEnums(byte type) {
		ArrayList<DatabaseEventType> list = new ArrayList<>(DatabaseEventType.values().length);
		for (DatabaseEventType e : DatabaseEventType.values())
			if ((e.type & type) == e.type)
				list.add(e);
		return list;
	}

}
