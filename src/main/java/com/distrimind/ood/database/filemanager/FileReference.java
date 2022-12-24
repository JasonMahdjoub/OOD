package com.distrimind.ood.database.filemanager;
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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;
import com.distrimind.util.io.SecureExternalizable;

import java.io.IOException;

/**
 * The serialization of this class must always give the same binary array
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 3.0.0
 */
public abstract class FileReference implements SecureExternalizable {
	static final int MAX_FILE_REFERENCE_SIZE_IN_BYTES=2048;
	private transient FileTable.Record record=null;
	private transient FileTable fileTable=null;

	void linkToDatabase(FileTable fileTable, FileTable.Record record) {
		this.record = record;
		this.fileTable = fileTable;
	}

	@Override
	public boolean equals(Object o)
	{
		if (o==null)
			return false;
		if (this.getClass()==o.getClass())
		{
			if (record!=null) {
				FileReference fr = (FileReference) o;
				if (fr.record != null) {
					return record.getFileId() == fr.record.getFileId();
				}
			}
			return equalsImplementation(o);
		}
		else
			return false;
	}

	protected abstract boolean equalsImplementation(Object o);
	@Override
	public abstract int hashCode();
	@Override
	public abstract String toString();

	public abstract long lengthInBytes() throws IOException;
	public boolean delete() throws IOException
	{
		if (record==null)
		{
			return deleteImplementation();
		}
		else
		{

			try {
				if (record.referenceNumber<=0) {
					fileTable.removeRecord(record);
					record=null;
					return false;
				}
				else if (--record.referenceNumber==0) {
					deleteImplementation();
					fileTable.removeRecord(record);
					record=null;
				}
				else {
					fileTable.updateRecord(record, "referenceNumber", record.referenceNumber);
				}

				return true;
			} catch (DatabaseException e) {
				throw new IOException(e);
			}
		}
	}
	protected abstract boolean deleteImplementation() throws IOException;

	public RandomInputStream getRandomInputStream() throws IOException
	{
		return getRandomInputStreamImplementation();
	}

	protected abstract RandomInputStream getRandomInputStreamImplementation() throws IOException;

	public RandomOutputStream getRandomOutputStream() throws IOException
	{
		if (record!=null)
		{
			try {
				fileTable.updateRecord(record, "lastModificationDateUTC", System.currentTimeMillis());
			} catch (DatabaseException e) {
				throw new IOException(e);
			}
		}
		return getRandomOutputStreamImplementation();
	}
	protected abstract RandomOutputStream getRandomOutputStreamImplementation() throws IOException;

	public void save(RandomInputStream in) throws IOException
	{
		try(RandomOutputStream ros=getRandomOutputStream())
		{
			in.transferTo(ros);
		}
	}
}
