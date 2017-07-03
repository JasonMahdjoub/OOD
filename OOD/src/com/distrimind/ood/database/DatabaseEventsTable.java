
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

import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseEventsTable extends Table<DatabaseEventsTable.Record>
{

    static class Record extends DatabaseRecord
    {
	@NotNull
	@AutoPrimaryKey
	private int id;
	@NotNull
	@ForeignKey
	DatabaseTransactionEventsTable.Record transaction;
	
	@Field
	private int position;
	
	@Field
	private byte type;
	@NotNull
	@Field(limit=400)
	private String concernedTable;
	@NotNull
	@Field
	private byte[] concernedSerializedPrimaryKey;
	
	@Field(limit=Integer.MAX_VALUE)
	private byte[] concernedSerializedOldNonPK;
	Record()
	{
	    
	}
	<T extends DatabaseRecord> Record(DatabaseTransactionEventsTable.Record transaction, TableEvent<T> _de, DatabaseWrapper wrapper, boolean includeOldNonPKFields) throws DatabaseException
	{
	    if (transaction==null)
		throw new NullPointerException("transaction");
	    if (_de==null)
		throw new NullPointerException("_de");
	    if (wrapper==null)
		throw new NullPointerException("wrapper");
	    this.transaction=transaction;
	    type=_de.getType().getByte();
	    Table<T> table=_de.getTable(wrapper);
	    concernedTable=table.getClass().getName();
	    if (_de.getMapKeys()!=null)
	    {
		concernedSerializedPrimaryKey=table.serializePrimaryKeys(_de.getMapKeys());
	    }
	    else if (_de.getOldDatabaseRecord()!=null)
		concernedSerializedPrimaryKey=table.serializePrimaryKeys(_de.getOldDatabaseRecord());
	    else
		concernedSerializedPrimaryKey=table.serializePrimaryKeys(_de.getNewDatabaseRecord());
	    if (_de.getOldDatabaseRecord()!=null && includeOldNonPKFields)
		concernedSerializedOldNonPK=table.serializeFieldsNonPK(_de.getOldDatabaseRecord());
	    
	}
	
	DatabaseTransactionEventsTable.Record getTransaction()
	{
	    return transaction;
	}
	void setTransaction(DatabaseTransactionEventsTable.Record _transaction)
	{
	    transaction = _transaction;
	}
	byte getType()
	{
	    return type;
	}
	void setType(byte _type)
	{
	    type = _type;
	}
	String getConcernedTable()
	{
	    return concernedTable;
	}
	void setConcernedTable(String _concernedTable)
	{
	    concernedTable = _concernedTable;
	}
	byte[] getConcernedSerializedPrimaryKey()
	{
	    return concernedSerializedPrimaryKey;
	}
	void setConcernedSerializedPrimaryKey(byte[] _concernedSerializedPrimaryKey)
	{
	    concernedSerializedPrimaryKey = _concernedSerializedPrimaryKey;
	}
	byte[] getConcernedSerializedOldNonPK()
	{
	    return concernedSerializedOldNonPK;
	}
	void setConcernedSerializedOldNonPK(byte[] _concernedSerializedOldNonPK)
	{
	    concernedSerializedOldNonPK = _concernedSerializedOldNonPK;
	}
	int getId()
	{
	    return id;
	}
	
	int getPosition()
	{
	    return position;
	}
	void setPosition(int order)
	{
	    this.position=order;
	}
	
    }

    protected DatabaseEventsTable() throws DatabaseException
    {
	super();
    }


}
