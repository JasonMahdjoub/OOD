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
package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.Table;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class AskForMetaDataPerFileToCentralDatabaseBackup extends DatabaseEvent implements DatabaseEventToSend, ChannelMessageDestinedToCentralDatabaseBackup, SecureExternalizable {
	private DecentralizedValue hostSource;
	private DecentralizedValue channelHost;
	private FileCoordinate fileCoordinate;
	private String packageString;
	@SuppressWarnings("unused")
	private AskForMetaDataPerFileToCentralDatabaseBackup()
	{

	}
	public AskForMetaDataPerFileToCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue channelHost, FileCoordinate fileCoordinate, String packageString) {
		if (hostSource==null)
			throw new NullPointerException();
		if (channelHost ==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		if (fileCoordinate==null)
			throw new NullPointerException();
		this.hostSource = hostSource;
		this.channelHost = channelHost;
		this.fileCoordinate = fileCoordinate;
		this.packageString=packageString;
	}

	public String getPackageString() {
		return packageString;
	}

	@Override
	public DecentralizedValue getChannelHost() {
		return channelHost;
	}

	public FileCoordinate getFileCoordinate() {
		return fileCoordinate;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public int getInternalSerializedSize() {
		return 9+SerializationTools.getInternalSize(hostSource)+
				SerializationTools.getInternalSize(channelHost)+
				SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
		out.writeObject(channelHost, false);
		fileCoordinate.write(out);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class);
		channelHost =in.readObject(false, DecentralizedValue.class);
		fileCoordinate=FileCoordinate.read(in);
		if (fileCoordinate==null)
			throw new MessageExternalizationException(Integrity.FAIL);
		packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}

	@Override
	public String toString() {
		return "AskForMetaDataPerFileToCentralDatabaseBackup{" +
				"hostSource=" + DatabaseWrapper.toString(hostSource) +
				", channelHost=" + DatabaseWrapper.toString(channelHost) +
				", fileCoordinate=" + fileCoordinate +
				", packageString='" + packageString + '\'' +
				'}';
	}
}
