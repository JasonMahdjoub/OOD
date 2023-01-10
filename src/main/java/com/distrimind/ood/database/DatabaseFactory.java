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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProviderFactory;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.properties.MultiFormatProperties;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * 
 * Database initializer
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 2.0.0
 */
public abstract class DatabaseFactory<DW extends DatabaseWrapper> extends MultiFormatProperties {

	private volatile transient DW wrapper;
	protected EncryptionProfileProviderFactory signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup;
	protected EncryptionProfileProviderFactory encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup;
	protected EncryptionProfileProviderFactory protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages;
	protected SecureRandomType randomType=SecureRandomType.DEFAULT;
	protected byte[] randomNonce= "fsg24778kqQVEZYogt1°F_:x=,aért16PHMXv;t/+$eùŜF21F0".getBytes();
	protected byte[] randomPersonalizationString= "SGJST92?GI1N4: carz:fg *£f14902é\"§e'!(y)ĝùa(!h,qàényŝtufx".getBytes();
	protected DatabaseConfigurations databaseConfigurations;

	private transient DatabaseLifeCycles databaseLifeCycles=null;
	private transient boolean createDatabasesIfNecessaryAndCheckIt=true;

	private ScheduledPoolExecutor defaultPoolExecutor=null;
	private Object context=null;

	public DatabaseLifeCycles getDatabaseLifeCycles() {
		return databaseLifeCycles;
	}

	public void setDatabaseLifeCycles(DatabaseLifeCycles databaseLifeCycles) {
		this.databaseLifeCycles = databaseLifeCycles;
	}

	public boolean isCreateDatabasesIfNecessaryAndCheckIt() {
		return createDatabasesIfNecessaryAndCheckIt;
	}

	public void setCreateDatabasesIfNecessaryAndCheckIt(boolean createDatabasesIfNecessaryAndCheckIt) {
		this.createDatabasesIfNecessaryAndCheckIt = createDatabasesIfNecessaryAndCheckIt;
	}

	public AbstractSecureRandom getSecureRandom() throws DatabaseException {
		try {
			return randomType.getSingleton(randomNonce, randomPersonalizationString );
		} catch (NoSuchAlgorithmException | NoSuchProviderException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	public EncryptionProfileProviderFactory getSignatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup() {
		return signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup;
	}

	public EncryptionProfileProviderFactory getEncryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup() {
		return encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup;
	}
	public void setEncryptionProfileProviders(EncryptionProfileProviderFactory protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages,
											  SecureRandomType randomType, byte[] randomNonce, byte[] randomPersonalizationString) {
		setEncryptionProfileProviders(null, null, protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages, randomType,
				randomNonce, randomPersonalizationString);
	}
	public void setEncryptionProfileProviders(EncryptionProfileProviderFactory protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages,
											  SecureRandomType randomType) {
		setEncryptionProfileProviders(null, null, protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages, randomType);
	}
	public void setEncryptionProfileProviders(EncryptionProfileProviderFactory signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
											  EncryptionProfileProviderFactory encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
											  EncryptionProfileProviderFactory protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages,
											  SecureRandomType randomType) {
		setEncryptionProfileProviders(signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
				encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages, randomType, this.randomNonce, this.randomPersonalizationString);
	}
	public void setEncryptionProfileProviders(EncryptionProfileProviderFactory signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
											  EncryptionProfileProviderFactory encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
											  EncryptionProfileProviderFactory protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages,
											  SecureRandomType randomType, byte[] randomNonce, byte[] randomPersonalizationString) {
		if ((signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null)!=(encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup==null))
			throw new NullPointerException();
		if (protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null)
			throw new NullPointerException();
		if (randomType==null)
			throw new NullPointerException();
		this.signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup = signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup;
		this.encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup = encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup;
		this.protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages=protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages;
		this.randomType=randomType;
		this.randomNonce=randomNonce;
		this.randomPersonalizationString=randomPersonalizationString;
	}

	public EncryptionProfileProviderFactory getProtectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages() {
		return protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages;
	}


	public SecureRandomType getRandomType() {
		return randomType;
	}

	public byte[] getRandomNonce() {
		return randomNonce;
	}


	public byte[] getRandomPersonalizationString() {
		return randomPersonalizationString;
	}

	public DatabaseConfigurations getDatabaseConfigurations() {
		return databaseConfigurations;
	}

	public void setDatabaseConfigurations(DatabaseConfigurations databaseConfigurations) throws DatabaseException {
		if (databaseConfigurations==null)
			throw new NullPointerException();
		if (wrapper!=null)
			throw new DatabaseException("Cannot change configuration when database wrapper is already instantiated !");
		this.databaseConfigurations = databaseConfigurations;
	}

	protected DatabaseFactory() throws DatabaseException {
		super(null);
		databaseConfigurations=new DatabaseConfigurations();
	}
	protected DatabaseFactory(DatabaseConfigurations databaseConfigurations) throws DatabaseException {
		super(null);
		if (databaseConfigurations==null)
			databaseConfigurations=new DatabaseConfigurations();
		this.databaseConfigurations = databaseConfigurations;
	}

	public final DW getDatabaseWrapperSingleton() throws DatabaseException {
		if (wrapper == null) {
			synchronized (this) {
				if (wrapper == null)
					wrapper = newWrapperInstance();
			}
		}
		return wrapper;
	}

	public void closeSingletonIfOpened()
	{
		synchronized (this)
		{
			if (wrapper!=null)
			{
				wrapper.close();
			}
		}
	}

	public ScheduledPoolExecutor getDefaultPoolExecutor() {
		return defaultPoolExecutor;
	}

	public void setDefaultPoolExecutor(ScheduledPoolExecutor defaultPoolExecutor) {
		this.defaultPoolExecutor = defaultPoolExecutor;
	}

	final DW newWrapperInstance() throws DatabaseException
	{
		DW res= newWrapperInstanceImpl();
		res.getDatabaseConfigurationsBuilder().databaseWrapperLoaded();
		return res;
	}

	protected abstract DW newWrapperInstanceImpl() throws DatabaseException;


	public abstract void deleteDatabase() throws DatabaseException;

	public Object getContext() {
		return context;
	}

	public void setContext(Object context) {
		if (context==null)
			throw new NullPointerException();
		this.context = context;
	}


}
