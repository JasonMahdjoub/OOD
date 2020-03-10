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

import java.io.File;
import java.nio.charset.Charset;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMariaDatabaseFactory extends CommonMySQLDatabaseFactory<DistantMariaDBWrapper> {
	public DistantMariaDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password) {
		super(urlLocation, port, databaseName, user, password);
	}

	public DistantMariaDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, Charset characterEncoding, CommonMySQLWrapper.SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) {
		super(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
	}

	public DistantMariaDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password, String mysqlParams) {
		super(urlLocation, port, databaseName, user, password, mysqlParams);
	}

	public DistantMariaDatabaseFactory() {
	}

	@Override
	protected DistantMariaDBWrapper newWrapperInstance() throws DatabaseException {
		Charset cs=getCharacterEncoding();
		String css=null;
		if (cs.name().contains("-")) {
			for (String s : cs.aliases()) {
				if (!s.contains("-")) {
					css = s;
					break;
				}
			}
			if (css==null)
				css=cs.name();
		}
		else
			css=cs.name();
		if (mysqlParams==null)
			return new DistantMariaDBWrapper(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
		else
			return new DistantMariaDBWrapper(urlLocation, port, databaseName, user, password, mysqlParams);
	}
}
