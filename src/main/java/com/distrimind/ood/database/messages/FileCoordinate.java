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
package com.distrimind.ood.database.messages;

import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;

import java.io.IOException;

/**
 * Database backup file coordinate
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class FileCoordinate
{
	public enum Boundary
	{
		UPPER_LIMIT,
		LOWER_LIMIT
	}
	private final long timeStamp;
	private final Boundary boundary;

	public FileCoordinate(long timeStampUTCNotIncluded, Boundary boundary) {
		if (boundary==null)
			throw new NullPointerException();
		this.timeStamp = timeStampUTCNotIncluded;
		this.boundary = boundary;
	}

	public long getTimeStampUTC() {
		return timeStamp;
	}

	public Boundary getBoundary() {
		return boundary;
	}

	@Override
	public String toString() {
		return "FileCoordinate{" +
				"timeStamp=" + timeStamp +
				", boundary=" + boundary +
				'}';
	}
	void write(SecuredObjectOutputStream out) throws IOException {
		out.writeLong(timeStamp);
		out.writeByte(boundary.ordinal());
	}
	static FileCoordinate read(SecuredObjectInputStream in) throws IOException {
		long ts =in.readLong();
		byte b=in.readByte();
		for (FileCoordinate.Boundary cb : FileCoordinate.Boundary.values())
		{
			if (cb.ordinal()==b)
			{
				return new FileCoordinate(ts, cb);
			}
		}
		return null;
	}
}
