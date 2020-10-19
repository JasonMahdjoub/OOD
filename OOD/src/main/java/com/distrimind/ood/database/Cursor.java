package com.distrimind.ood.database;
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

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.0.0
 */
public class Cursor<T extends DatabaseRecord> {
	public static final int DEFAULT_CACHE_SIZE=64;
	private Table<T> table;
	private String whereClause;
	private int cacheSize;
	private int position;
	private ArrayList<T> records;
	private long recordsNumber;
	private Map<String, Object> parameters;
	private Boolean ascendant;
	private String[] fields;

	Cursor(Table<T> table, String whereClause, Map<String, Object> parameters, int cacheSize, Boolean ascendant, String ... fields) {
		if (table==null)
			throw new NullPointerException();
		if (cacheSize<2)
			throw new IllegalArgumentException("The cache size must be greater than 2");
		this.table = table;
		this.whereClause=whereClause;
		this.cacheSize=cacheSize;
		this.position=-1;
		this.records=null;
		this.recordsNumber=-1;
		this.parameters=parameters==null? new HashMap<>():parameters;
		this.ascendant=ascendant;
		this.fields=fields;
	}

	public long getTotalRecordsNumber() throws DatabaseException {
		if (recordsNumber==-1)
		{
			if (whereClause==null)
				recordsNumber=table.getRecordsNumber();
			else
				recordsNumber=table.getRecordsNumber(whereClause, parameters);
		}
		return recordsNumber;
	}
	public void refreshData()
	{
		recordsNumber=-1;
		this.position=-1;
		this.records=null;
	}

	private void refreshData(int position) throws DatabaseException {
		this.position=position-cacheSize/2;
		if (ascendant==null) {
			if (whereClause==null)
				records = table.getPaginatedRecords(position, cacheSize);
			else
				records = table.getPaginatedRecords(position, cacheSize, whereClause, parameters);
		}
		else {
			records = table.getPaginatedOrderedRecords(position, cacheSize, whereClause, parameters, ascendant, fields);
		}
	}
	public T getRecord(int position) throws DatabaseException {
		if (this.position==-1 || position<this.position || position>this.position+cacheSize)
		{
			refreshData(position);
		}
		int pos=position-this.position;
		if (pos<0 || pos>=records.size())
			return null;
		return records.get(pos);
	}

}
