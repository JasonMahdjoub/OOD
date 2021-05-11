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

import com.distrimind.ood.database.centraldatabaseapi.*;
import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.messages.*;
import com.distrimind.ood.database.tests.TestDatabaseToOperateActionIntoDecentralizedNetwork;
import com.distrimind.ood.database.tests.TestRevertToOldVersionIntoDecentralizedNetwork;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.FileTools;
import com.distrimind.util.crypto.*;
import com.distrimind.util.data_buffers.WrappedData;
import com.distrimind.util.io.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * @author Jason
 * @version 1.0
 * @since OOD 2.5.0
 */
public abstract class CommonDecentralizedTests {
	public static final Level networkLogLevel=Level.INFO;
	public static final Level databaseLogLevel=Level.INFO;

	private static final Method isLocallyDecentralized;
	static{
		Method m=null;
		try {
			m= Table.class.getDeclaredMethod("isLocallyDecentralized");
			m.setAccessible(true);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		isLocallyDecentralized=m;
	}



	boolean isLocallyDecentralized(Table<?> table)
	{
		try {
			return (boolean)isLocallyDecentralized.invoke(table);
		} catch (IllegalAccessException | InvocationTargetException e) {
			e.printStackTrace();
			System.exit(-1);
			return false;
		}
	}

	protected CommonDecentralizedTests() {

	}
	private void loadCertificates() throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
		this.random=SecureRandomType.DEFAULT.getSingleton(null);
		this.centralDatabaseBackupKeyPair=ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair();

		peerKeyPairUsedWithCentralDatabaseBackupCertificate=ASymmetricAuthenticatedSignatureType.DEFAULT.getKeyPairGenerator(random).generateKeyPair();
		SymmetricSecretKey secretKeyForSignature=SymmetricAuthenticatedSignatureType.DEFAULT.getKeyGenerator(random).generateKey();
		SymmetricSecretKey secretKeyForEncryption=SymmetricEncryptionType.DEFAULT.getKeyGenerator(random).generateKey();
		((EncryptionProfileCollection)this.signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup).putProfile((short)0, MessageDigestType.DEFAULT, peerKeyPairUsedWithCentralDatabaseBackupCertificate.getASymmetricPublicKey(),
				peerKeyPairUsedWithCentralDatabaseBackupCertificate.getASymmetricPrivateKey(), null,null, false, true );
		((EncryptionProfileCollection)this.encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup).putProfile((short)0, MessageDigestType.DEFAULT, null,
				null, secretKeyForSignature,secretKeyForEncryption, false, true );
		secretKeyForSignature=SymmetricAuthenticatedSignatureType.DEFAULT.getKeyGenerator(random).generateKey();
		((EncryptionProfileCollection)this.protectedSignatureProfileProviderForAuthenticatedP2PMessages).putProfile((short)0, MessageDigestType.DEFAULT, null, null, secretKeyForSignature,null, false, true );
		peerCertificate =new CentralDatabaseBackupCertificate(centralDatabaseBackupKeyPair, peerKeyPairUsedWithCentralDatabaseBackupCertificate.getASymmetricPublicKey());

	}

	public static class DistantDatabaseEvent {
		private final byte[] eventToSend;
		private final byte[] joinedData;
		private final DecentralizedValue hostDest;

		DistantDatabaseEvent(DatabaseWrapper wrapper, DatabaseEventToSend eventToSend) throws DatabaseException {
			try {
				try (RandomByteArrayOutputStream baos = new RandomByteArrayOutputStream()) {
					baos.writeObject(eventToSend, false);

					baos.flush();

					this.eventToSend = baos.getBytes();
				}
				if (eventToSend instanceof P2PBigDatabaseEventToSend) {
					P2PBigDatabaseEventToSend b = (P2PBigDatabaseEventToSend) eventToSend;
					final AtomicReference<RandomByteArrayOutputStream> baos = new AtomicReference<>();
					try (OutputStreamGetter osg = new OutputStreamGetter() {

						@Override
						public RandomOutputStream initOrResetOutputStream() {
							if (baos.get() != null)
								baos.get().close();
							baos.set(new RandomByteArrayOutputStream());
							return baos.get();
						}

						@Override
						public void close() {
							if (baos.get() != null)
								baos.get().close();
						}
					}) {
						b.exportToOutputStream(wrapper, osg);
						baos.get().flush();
						this.joinedData = baos.get().getBytes();
					}
				} else if (eventToSend instanceof BigDataEventToSendWithCentralDatabaseBackup) {
					try (RandomByteArrayOutputStream out = new RandomByteArrayOutputStream(); RandomInputStream ris = ((BigDataEventToSendWithCentralDatabaseBackup) eventToSend).getPartInputStream()) {
						ris.transferTo(out);
						out.flush();
						this.joinedData = out.getBytes();
					}
				} else
					this.joinedData = null;
				if (eventToSend instanceof P2PDatabaseEventToSend)
					hostDest = ((P2PDatabaseEventToSend) eventToSend).getHostDestination();
				else if (eventToSend instanceof MessageComingFromCentralDatabaseBackup)
					hostDest = ((MessageComingFromCentralDatabaseBackup) eventToSend).getHostDestination();
				else
					hostDest = null;
			}catch (Exception e)
			{
				throw DatabaseException.getDatabaseException(e);
			}
		}

		public DatabaseEventToSend getDatabaseEventToSend() throws IOException, ClassNotFoundException {
			try (RandomByteArrayInputStream bais = new RandomByteArrayInputStream(eventToSend)) {
				DatabaseEventToSend res=bais.readObject(false, DatabaseEventToSend.class);
				if (res instanceof BigDataEventToSendWithCentralDatabaseBackup) {
					Assert.assertNotNull(this.joinedData);
					((BigDataEventToSendWithCentralDatabaseBackup) res).setPartInputStream(new RandomByteArrayInputStream(joinedData));
				}
				return res;
			}
		}

		public RandomInputStream getInputStream() {
			return new RandomByteArrayInputStream(joinedData);
		}

		public DecentralizedValue getHostDestination() {
			return hostDest;
		}
	}
	static File centralDatabaseBackupDirectory=new File("centralDatabaseBackup");
	/*public class DatabaseBackup
	{
		private final String packageString;
		private final DecentralizedValue channelHost;

		public DatabaseBackup(String packageString, DecentralizedValue channelHost) {
			this.packageString = packageString;
			this.channelHost = channelHost;
		}

		private final HashMap<Long, EncryptedDatabaseBackupMetaDataPerFile> metaDataPerFile=new HashMap<>();
		private final HashMap<Long, File> fileBackupLocations=new HashMap<>();
		public void addFileBackupPart(EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws IOException {
			assert message.getMetaData().getPackageString().equals(packageString);
			if (!fileBackupLocations.containsKey(message.getMetaData().getFileTimestampUTC()))
			{
				File f=getFile(message.getHostSource(), message.getMetaData().getPackageString(), message.getMetaData().getFileTimestampUTC(),message.getMetaData().isReferenceFile());

				try(RandomFileOutputStream out=new RandomFileOutputStream(f))
				{
					message.getPartInputStream().transferTo(out);
				}
				message.getPartInputStream().close();
				fileBackupLocations.put(message.getMetaData().getFileTimestampUTC(), f);
				metaDataPerFile.put(message.getMetaData().getFileTimestampUTC(), message.getMetaData());
			}
			else
				Assert.fail();
		}

		public EncryptedDatabaseBackupMetaDataPerFile getEncryptedDatabaseBackupMetaDataPerFile(long timeStamp)
		{
			return metaDataPerFile.get(timeStamp);
		}

		public EncryptedBackupPartComingFromCentralDatabaseBackup getEncryptedBackupPartComingFromCentralDatabaseBackup(DecentralizedValue hostDestination, long timeStamp) throws FileNotFoundException {
			EncryptedDatabaseBackupMetaDataPerFile metaData=metaDataPerFile.get(timeStamp);
			if (metaData==null)
				return null;
			File f=fileBackupLocations.get(timeStamp);
			assert f!=null;
			return new EncryptedBackupPartComingFromCentralDatabaseBackup(channelHost, hostDestination, metaData, new RandomFileInputStream(f));
		}
		public long getLastFileBackupPartUTC()
		{
			long res=Long.MIN_VALUE;
			for (long utc : fileBackupLocations.keySet()) {
				if (utc > res)
					res = utc;
			}
			return res;
		}
		public EncryptedDatabaseBackupMetaDataPerFile getBackupMetaDataPerFile(FileCoordinate fileCoordinate)
		{
			Map.Entry<Long, EncryptedDatabaseBackupMetaDataPerFile> found = null;
			if (fileCoordinate.getBoundary()== FileCoordinate.Boundary.UPPER_LIMIT) {

				for (Map.Entry<Long, EncryptedDatabaseBackupMetaDataPerFile> e : metaDataPerFile.entrySet()) {
					if (e.getKey() < fileCoordinate.getTimeStamp()) {
						if (found == null || found.getKey() < e.getKey())
							found = e;
					}
				}

			}
			else if (fileCoordinate.getBoundary()== FileCoordinate.Boundary.LOWER_LIMIT) {
				for (Map.Entry<Long, EncryptedDatabaseBackupMetaDataPerFile> e : metaDataPerFile.entrySet()) {
					if (e.getKey() > fileCoordinate.getTimeStamp()) {
						if (found == null || found.getKey() > e.getKey())
							found = e;
					}
				}
			}
			else
				throw new IllegalAccessError();
			return found == null ? null : found.getValue();
		}
		private void received(AskForDatabaseBackupPartDestinedToCentralDatabaseBackup message) throws FileNotFoundException, DatabaseException {
			EncryptedDatabaseBackupMetaDataPerFile m=getBackupMetaDataPerFile(message.getFileCoordinate());
			if (m!=null)
				sendMessageFromCentralDatabaseBackup(getEncryptedBackupPartComingFromCentralDatabaseBackup(message.getHostSource(), m.getFileTimestampUTC()));
		}
		private void received(AskForMetaDataPerFileToCentralDatabaseBackup message) throws DatabaseException {
			EncryptedDatabaseBackupMetaDataPerFile m=getBackupMetaDataPerFile(message.getFileCoordinate());
			if (m!=null)
				sendMessageFromCentralDatabaseBackup(new EncryptedMetaDataFromCentralDatabaseBackup(message.getHostSource(), channelHost, m));

		}

		public File getDirectory(DecentralizedValue host, String packageString)
		{
			File res=new File(centralDatabaseBackupDirectory, host.encodeString()+File.separator+packageString.replace('.', File.separatorChar));
			FileTools.checkFolderRecursive(res);
			return res;
		}
		public File getFile(DecentralizedValue host, String packageString, long timeStamp, boolean reference)
		{
			return new File(getDirectory(host, packageString), (reference?"refbackup":"backup")+timeStamp+".data");
		}


	}
	public class DatabaseBackupPerHost
	{
		private final Map<String, DatabaseBackup> databaseBackupPerPackage=new HashMap<>();
		private byte[] lastValidatedAndEncryptedID=null;
		private final Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedDistantID=new HashMap<>();
		private boolean connected=false;

		public DatabaseBackupPerHost(DecentralizedValue channelHost) {
		}

		private byte[] received(EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws IOException, DatabaseException {
			DatabaseBackup dbb=databaseBackupPerPackage.get(message.getMetaData().getPackageString());
			if (dbb==null)
				databaseBackupPerPackage.put(message.getMetaData().getPackageString(), dbb=new DatabaseBackup(message.getMetaData().getPackageString(), message.getHostSource()));
			byte[] res=null;
			if (dbb.getLastFileBackupPartUTC()<message.getMetaData().getFileTimestampUTC()) {
				res=lastValidatedAndEncryptedID = message.getLastValidatedAndEncryptedID();

			}
			dbb.addFileBackupPart(message);
			sendMessageFromCentralDatabaseBackup(new EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup(message.getHostSource(), message.getMetaData().getFileTimestampUTC(), message.getMetaData().getLastTransactionTimestampUTC(), message.getMetaData().getPackageString()));
			return res;
		}
		private void received(AskForDatabaseBackupPartDestinedToCentralDatabaseBackup message) throws FileNotFoundException, DatabaseException {
			DatabaseBackup dbb=databaseBackupPerPackage.get(message.getPackageString());
			if (dbb!=null)
				dbb.received(message);
		}
		private void received(AskForMetaDataPerFileToCentralDatabaseBackup message) throws DatabaseException {
			DatabaseBackup dbb=databaseBackupPerPackage.get(message.getPackageString());
			if (dbb!=null)
				dbb.received(message);
		}
		private void received(LastValidatedDistantTransactionDestinedToCentralDatabaseBackup message)
		{
			lastValidatedAndEncryptedDistantID.put(message.getChannelHost(), message.getEncryptedLastValidatedDistantID());
		}
		private void received(DistantBackupCenterConnexionInitialisation message) {
			lastValidatedAndEncryptedDistantID.putAll(message.getEncryptedDistantLastValidatedIDs());
			/*for (Database d : listDatabase)
			{
				if (d.hostID.equals(message.getHostSource()))
					continue;
				sendMessageFromCentralDatabaseBackup(new LastIDCorrectionFromCentralDatabaseBackup(message.getHostSource(), lastValidatedAndEncryptedDistantID.get(d.hostID)), true);
			}
		}

	}*/
	public class CentralDatabaseBackupReceiverPerPeer extends com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupReceiverPerPeer
	{

		public CentralDatabaseBackupReceiverPerPeer(CentralDatabaseBackupReceiver centralDatabaseBackupReceiver, DatabaseWrapper wrapper) {
			super(centralDatabaseBackupReceiver, wrapper);
		}

		@Override
		protected void sendMessageFromThisCentralDatabaseBackup(MessageComingFromCentralDatabaseBackup message) throws DatabaseException {
			try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream())
			{
				out.writeObject(message, false);
				out.flush();
				RandomInputStream ris=null;
				if (message instanceof BigDataEventToSendWithCentralDatabaseBackup)
				{
					ris=((BigDataEventToSendWithCentralDatabaseBackup) message).getPartInputStream();
				}
				try(RandomByteArrayInputStream  in=new RandomByteArrayInputStream(out.getBytes()))
				{
					message=in.readObject(false, MessageComingFromCentralDatabaseBackup.class);
					if (ris!=null)
						((BigDataEventToSendWithCentralDatabaseBackup) message).setPartInputStream(ris);
				}
			} catch (ClassNotFoundException | IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}

			for (Database d2 : listDatabase)
			{
				if (d2.hostID.equals(message.getHostDestination()))
				{
					centralDatabaseBackupMessageSent=true;
					if (d2.isConnected() && d2.dbwrapper.getSynchronizer().isInitializedWithCentralBackup())
						d2.getReceivedDatabaseEvents().add(new DistantDatabaseEvent(db2.dbwrapper, message));
					else
						Assert.fail(""+message.getClass()+", is connected : "+d2.isConnected());
					break;
				}
			}
		}

		@Override
		protected void sendMessageFromOtherCentralDatabaseBackup(DecentralizedValue centralDatabaseBackupID, MessageComingFromCentralDatabaseBackup message) {
			Assert.fail();
		}

		@Override
		protected EncryptionProfileProvider getEncryptionProfileProviderToValidateCertificateOrGetNullIfNoValidProviderIsAvailable(com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate certificate) {
			if (!isValidCertificate(certificate))
				return null;
			return new EncryptionProfileProvider() {
				@Override
				public MessageDigestType getMessageDigest(short keyID, boolean duringDecryptionPhase) throws IOException {
					if (keyID==0)
						return MessageDigestType.DEFAULT;
					throw new MessageExternalizationException(Integrity.FAIL);
				}

				@Override
				public IASymmetricPrivateKey getPrivateKeyForSignature(short keyID)  {
					return null;
				}

				@Override
				public IASymmetricPublicKey getPublicKeyForSignature(short keyID) throws IOException {
					if (keyID==0)
						return certificate.getCertifiedAccountPublicKey();
					throw new IOException();
				}

				@Override
				public SymmetricSecretKey getSecretKeyForSignature(short keyID, boolean duringDecryptionPhase) {
					return null;
				}

				@Override
				public SymmetricSecretKey getSecretKeyForEncryption(short keyID, boolean duringDecryptionPhase) {
					return null;
				}

				@Override
				public boolean isValidProfileID(short id) {
					return id==0;
				}

				@Override
				public Short getValidProfileIDFromPublicKeyForSignature(IASymmetricPublicKey publicKeyForSignature) {
					if (certificate.getCertifiedAccountPublicKey().equals(publicKeyForSignature))
						return 0;
					return null;
				}

				@Override
				public short getDefaultKeyID() {
					return 0;
				}
			};
		}


		protected boolean isValidCertificate(com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate certificate) {
			if (certificate instanceof CentralDatabaseBackupCertificate)
			{
				return ((CentralDatabaseBackupCertificate) certificate).getCentralDatabaseBackupPublicKey().equals(centralDatabaseBackupReceiver.getCentralID())
						&& ((CentralDatabaseBackupCertificate) certificate).isValidSignature();
			}
			return false;
		}


		@Override
		public FileReference getFileReference(EncryptedDatabaseBackupMetaDataPerFile encryptedDatabaseBackupMetaDataPerFile) {
			return new FileReferenceForTests(encryptedDatabaseBackupMetaDataPerFile.getFileTimestampUTC());
		}

		@Override
		public long getDurationInMsBeforeRemovingDatabaseBackup() {
			return 4L*30L*24L*60L*60L*1000L;
		}
	}
	static class CentralDatabaseBackupCertificate extends com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate
	{
		private IASymmetricPublicKey centralDatabaseBackupPublicKey, certifiedAccountPublicKey;
		private byte[] signature;
		@SuppressWarnings("unused")
		CentralDatabaseBackupCertificate()
		{

		}
		public CentralDatabaseBackupCertificate(AbstractKeyPair<?, ?> centralDatabaseBackupKeyPair, IASymmetricPublicKey certifiedAccountPublicKey) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
			this.centralDatabaseBackupPublicKey = centralDatabaseBackupKeyPair.getASymmetricPublicKey();
			this.certifiedAccountPublicKey = certifiedAccountPublicKey;
			ASymmetricAuthenticatedSignerAlgorithm signer=new ASymmetricAuthenticatedSignerAlgorithm(centralDatabaseBackupKeyPair.getASymmetricPrivateKey());
			signer.init();
			WrappedData wd=certifiedAccountPublicKey.encode();
			signer.update(wd.getBytes() );
			signature= signer.getSignature();

		}

		public IASymmetricPublicKey getCentralDatabaseBackupPublicKey()
		{
			return centralDatabaseBackupPublicKey;
		}

		public boolean isValidSignature()
		{
			try {
				ASymmetricAuthenticatedSignatureCheckerAlgorithm checker=new ASymmetricAuthenticatedSignatureCheckerAlgorithm(centralDatabaseBackupPublicKey);
				checker.init(signature);
				WrappedData wd=certifiedAccountPublicKey.encode();
				checker.update(wd.getBytes() );
				return checker.verify();
			} catch (NoSuchProviderException | NoSuchAlgorithmException | IOException e) {
				return false;
			}
		}

		@Override
		public IASymmetricPublicKey getCertifiedAccountPublicKey() {
			return certifiedAccountPublicKey;
		}

		@Override
		public int getInternalSerializedSize() {
			return SerializationTools.getInternalSize(centralDatabaseBackupPublicKey)
					+SerializationTools.getInternalSize(certifiedAccountPublicKey)
					+SerializationTools.getInternalSize(signature, ASymmetricAuthenticatedSignatureType.MAX_ASYMMETRIC_SIGNATURE_SIZE);
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeObject(centralDatabaseBackupPublicKey, false);
			out.writeObject(certifiedAccountPublicKey, false);
			out.writeBytesArray(signature, false, ASymmetricAuthenticatedSignatureType.MAX_ASYMMETRIC_SIGNATURE_SIZE);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
			centralDatabaseBackupPublicKey=in.readObject(false);
			certifiedAccountPublicKey=in.readObject(false);
			signature=in.readBytesArray(false, ASymmetricAuthenticatedSignatureType.MAX_ASYMMETRIC_SIGNATURE_SIZE);
		}
	}
	static class FileReferenceForTests implements FileReference
	{
		private transient File file;
		private long fileTimeStamp;
		@SuppressWarnings("unused")
		FileReferenceForTests()
		{

		}
		FileReferenceForTests(long fileTimeStamp)
		{
			this.fileTimeStamp=fileTimeStamp;
			initFile();
		}
		private void initFile()
		{
			if (!centralDatabaseBackupDirectory.exists())
				if (!centralDatabaseBackupDirectory.mkdir())
					throw new IllegalAccessError();
			file=new File(centralDatabaseBackupDirectory, fileTimeStamp+".backup");
		}
		@Override
		public boolean equals(Object o) {
			return o instanceof FileReferenceForTests && ((FileReferenceForTests) o).fileTimeStamp==fileTimeStamp;
		}

		@Override
		public int hashCode() {
			return Long.hashCode(fileTimeStamp);
		}

		@Override
		public String toString() {
			return "FileBackup-"+fileTimeStamp;
		}

		@Override
		public long lengthInBytes() {
			return file.length();
		}

		@Override
		public boolean delete() {
			return file.delete();
		}

		@Override
		public RandomInputStream getRandomInputStream() throws IOException {
			return new RandomFileInputStream(file);
		}

		@Override
		public RandomOutputStream getRandomOutputStream() throws IOException {
			return new RandomFileOutputStream(file);
		}

		@Override
		public int getInternalSerializedSize() {
			return 8;
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeLong(fileTimeStamp);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException {
			fileTimeStamp=in.readLong();
			initFile();
		}
	}

	public class CentralDatabaseBackupReceiver extends com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupReceiver
	{

		public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper, DecentralizedValue centralID) throws DatabaseException {
			super(wrapper, centralID);
			ClientCloudAccountTable.Record r=new ClientCloudAccountTable.Record((short)10, CommonDecentralizedTests.this.peerKeyPairUsedWithCentralDatabaseBackupCertificate.getASymmetricPublicKey());
			clientCloudAccountTable.addRecord(r);
		}

		@Override
		protected CentralDatabaseBackupReceiverPerPeer newCentralDatabaseBackupReceiverPerPeerInstance(DatabaseWrapper wrapper) {
			return new CentralDatabaseBackupReceiverPerPeer(this, wrapper);
		}
	}





	public static class Database implements AutoCloseable, DatabaseNotifier {
		private final DatabaseWrapper dbwrapper;
		private volatile boolean connected;
		private final AbstractDecentralizedID hostID;
		private final ArrayList<DatabaseEvent> localEvents;
		private final List<CommonDecentralizedTests.DistantDatabaseEvent> eventsReceivedStack;
		private TableAlone tableAlone;
		private TablePointed tablePointed;
		private TablePointing tablePointing;
		private UndecentralizableTableA1 undecentralizableTableA1;
		private UndecentralizableTableB1 undecentralizableTableB1;
		private volatile boolean newDatabaseEventDetected = false;
		private volatile boolean replaceWhenCollisionDetected = false;
		private volatile CommonDecentralizedTests.DetectedCollision collisionDetected = null;

		private volatile TablePointed.Record recordPointed = null;
		private TablePointing.Record recordPointingNull = null, recordPointingNotNull = null;
		private TableAlone.Record recordAlone = null;
		private List<CommonDecentralizedTests.Anomaly> anomalies;


		public Database(DatabaseWrapper dbwrapper) throws DatabaseException {
			this.dbwrapper = dbwrapper;
			connected = false;
			hostID = new DecentralizedIDGenerator();
			localEvents = new ArrayList<>();
			eventsReceivedStack = Collections.synchronizedList(new LinkedList<>());




		}


		void initStep2() throws DatabaseException {
			tableAlone = dbwrapper.getTableInstance(TableAlone.class);
			tablePointed = dbwrapper.getTableInstance(TablePointed.class);
			tablePointing = dbwrapper.getTableInstance(TablePointing.class);
			Assert.assertNotNull(tableAlone.getDatabaseWrapper().getDatabaseConfiguration(TablePointed.class.getPackage()));
			Assert.assertNotNull(tablePointing.getDatabaseWrapper().getDatabaseConfiguration(TablePointed.class.getPackage()));
			Assert.assertNotNull(tablePointed.getDatabaseWrapper().getDatabaseConfiguration(TablePointed.class.getPackage()));
			Assert.assertNotNull(tableAlone.getDatabaseWrapper().getDatabaseConfiguration(TablePointed.class.getPackage()).getBackupConfiguration());
			undecentralizableTableA1 = dbwrapper
					.getTableInstance(UndecentralizableTableA1.class);
			undecentralizableTableB1 = dbwrapper
					.getTableInstance(UndecentralizableTableB1.class);
			anomalies = Collections.synchronizedList(new ArrayList<>());

			tableAlone.setDatabaseAnomaliesNotifier(Database.this::anomalyDetected);
			tableAlone.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TableAlone.Record, Table<TableAlone.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TableAlone.Record> concernedTable, HashMap<String, Object> keys, TableAlone.Record newValues, TableAlone.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);
				}

				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}
			});
			tablePointed.setDatabaseAnomaliesNotifier(Database.this::anomalyDetected);
			tablePointed.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TablePointed.Record, Table<TablePointed.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TablePointed.Record> concernedTable, HashMap<String, Object> keys, TablePointed.Record newValues, TablePointed.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);
				}
				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}

			});
			tablePointing.setDatabaseAnomaliesNotifier(Database.this::anomalyDetected);
			tablePointing.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TablePointing.Record, Table<TablePointing.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TablePointing.Record> concernedTable, HashMap<String, Object> keys, TablePointing.Record newValues, TablePointing.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);

				}
				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}

			});
		}


		public TableAlone getTableAlone() {
			return tableAlone;
		}

		public TablePointed getTablePointed() {
			return tablePointed;
		}

		public TablePointing getTablePointing() {
			return tablePointing;
		}

		public UndecentralizableTableA1 getUndecentralizableTableA1() {
			return undecentralizableTableA1;
		}

		public UndecentralizableTableB1 getUndecentralizableTableB1() {
			return undecentralizableTableB1;
		}

		public boolean isConnected() {
			return connected;
		}

		public void setConnected(boolean _connected) {
			connected = _connected;
		}

		public DatabaseWrapper getDbwrapper() {
			return dbwrapper;
		}

		@Override
		public void close() {
			dbwrapper.close();
		}

		public AbstractDecentralizedID getHostID() {
			return hostID;
		}

		public ArrayList<DatabaseEvent> getLocalEvents() {
			return localEvents;
		}

		public void clearPendingEvents() {
			synchronized (CommonDecentralizedTests.class) {
				localEvents.clear();
				this.eventsReceivedStack.clear();
				newDatabaseEventDetected = false;
				collisionDetected = null;
				anomalies.clear();
			}
		}

		public boolean isNewDatabaseEventDetected() {
			return newDatabaseEventDetected;
		}

		public void setNewDatabaseEventDetected(boolean newDatabaseEventDetected) {
			this.newDatabaseEventDetected = newDatabaseEventDetected;
		}

		public List<CommonDecentralizedTests.DistantDatabaseEvent> getReceivedDatabaseEvents() {
			return this.eventsReceivedStack;
		}



		@Override
		public void newDatabaseEventDetected(DatabaseWrapper _wrapper) {
			newDatabaseEventDetected = true;
		}

		@Override
		public void startNewSynchronizationTransaction() {

		}

		@Override
		public void endSynchronizationTransaction() {

		}

		@Override
		public void hostDisconnected(DecentralizedValue hostID) {

		}

		@Override
		public void hostConnected(DecentralizedValue hostID) {

		}


		private boolean collisionDetected(DecentralizedValue _distantPeerID,
										  DecentralizedValue _intermediatePeer, DatabaseEventType _type, Table<?> _concernedTable,
										  HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues) {
			synchronized (CommonDecentralizedTests.class) {
				collisionDetected = new CommonDecentralizedTests.DetectedCollision(_distantPeerID, _intermediatePeer, _type, _concernedTable,
						_keys, _newValues, _actualValues);
				return replaceWhenCollisionDetected;
			}
		}

		public void setReplaceWhenCollisionDetected(boolean _replaceWhenCollisionDetected) {
			replaceWhenCollisionDetected = _replaceWhenCollisionDetected;
		}
		public void resetCollisions()
		{
			collisionDetected=null;
		}

		public boolean isReplaceWhenCollisionDetected() {
			return replaceWhenCollisionDetected;
		}

		public CommonDecentralizedTests.DetectedCollision getDetectedCollision() {
			return collisionDetected;
		}

		public TablePointed.Record getRecordPointed() {
			return recordPointed;
		}

		public void setRecordPointed(TablePointed.Record _recordPointed) {
			recordPointed = _recordPointed;
		}

		public TablePointing.Record getRecordPointingNull() {
			return recordPointingNull;
		}

		public TablePointing.Record getRecordPointingNotNull() {
			return recordPointingNotNull;
		}

		public void setRecordPointingNull(TablePointing.Record _recordPointing) {
			recordPointingNull = _recordPointing;
		}

		public void setRecordPointingNotNull(TablePointing.Record _recordPointing) {
			recordPointingNotNull = _recordPointing;
		}

		public TableAlone.Record getRecordAlone() {
			return recordAlone;
		}

		public void setRecordAlone(TableAlone.Record _recordAlone) {
			recordAlone = _recordAlone;
		}

		public List<CommonDecentralizedTests.Anomaly> getAnomalies() {
			return anomalies;
		}


		private void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID,
									 DatabaseWrapper.SynchronizationAnomalyType _type, Table<?> _concernedTable, Map<String, Object> _primary_keys,
									 DatabaseRecord _record) {
			anomalies.add(new CommonDecentralizedTests.Anomaly(distantPeerID, intermediatePeerID, _type, _concernedTable, _primary_keys, _record));
		}


	}

	public static class Anomaly {

		final DecentralizedValue distantPeerID;
		final DecentralizedValue intermediatePeerID;
		final Table<?> table;
		final Map<String, Object> keys;
		final DatabaseRecord record;
		final DatabaseWrapper.SynchronizationAnomalyType type;

		public Anomaly(DecentralizedValue _distantPeerID, DecentralizedValue _intermediatePeerID, DatabaseWrapper.SynchronizationAnomalyType type,
					   Table<?> _table, Map<String, Object> _keys, DatabaseRecord _record) {
			super();
			distantPeerID = _distantPeerID;
			intermediatePeerID = _intermediatePeerID;
			this.type=type;
			table = _table;
			keys = _keys;
			record = _record;
		}

		@Override
		public String toString()
		{
			return "Anomaly-"+type;
		}
	}

	public static class DetectedCollision {
		final DecentralizedValue distantPeerID;
		final DecentralizedValue intermediatePeer;
		final DatabaseEventType type;
		final Table<?> concernedTable;
		final HashMap<String, Object> keys;
		final DatabaseRecord newValues;
		final DatabaseRecord actualValues;

		public DetectedCollision(DecentralizedValue _distantPeerID, DecentralizedValue _intermediatePeer,
								 DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys,
								 DatabaseRecord _newValues, DatabaseRecord _actualValues) {
			super();
			distantPeerID = _distantPeerID;
			intermediatePeer = _intermediatePeer;
			type = _type;
			concernedTable = _concernedTable;
			keys = _keys;
			newValues = _newValues;
			actualValues = _actualValues;
		}

	}



	protected volatile CommonDecentralizedTests.Database db1 = null, db2 = null, db3 = null, db4 = null;
	protected final ArrayList<CommonDecentralizedTests.Database> listDatabase = new ArrayList<>(3);
	protected AbstractKeyPair<?, ?> centralDatabaseBackupKeyPair;
	private DatabaseWrapper centralDatabaseBackupDatabase=null;
	protected CentralDatabaseBackupReceiver centralDatabaseBackupReceiver=null;
	protected AbstractSecureRandom random;
	protected final EncryptionProfileProviderFactory signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup=new EncryptionProfileCollection(){};
	protected final EncryptionProfileProviderFactory encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup=new EncryptionProfileCollection(){};
	private CentralDatabaseBackupCertificate peerCertificate;
	private AbstractKeyPair<?, ?> peerKeyPairUsedWithCentralDatabaseBackupCertificate;
	protected int accessNumberInProtectedEncriptionProfile =0;
	protected final EncryptionProfileProviderFactory protectedSignatureProfileProviderForAuthenticatedP2PMessages =new EncryptionProfileCollection(){
		@Override
		public SymmetricSecretKey getSecretKeyForSignature(short keyID, boolean duringDecryptionPhase) throws IOException {
			++accessNumberInProtectedEncriptionProfile;
			return super.getSecretKeyForSignature(keyID, duringDecryptionPhase);
		}

	};



	public abstract DatabaseFactory<?> getDatabaseFactoryInstanceForCentralDatabaseBackupReceiver() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseFactory<?> getDatabaseFactoryInstance1() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseFactory<?> getDatabaseFactoryInstance2() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseFactory<?> getDatabaseFactoryInstance3() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseFactory<?> getDatabaseFactoryInstance4() throws IllegalArgumentException, DatabaseException;

	public abstract void removeDatabaseFiles1();

	public abstract void removeDatabaseFiles2();

	public abstract void removeDatabaseFiles3();

	public abstract void removeDatabaseFiles4();
	public abstract void removeCentralDatabaseFiles();

	public BackupConfiguration getBackupConfiguration()
	{
		return null;
	}
	public boolean canInitCentralBackup()
	{
		return false;
	}

	protected Set<DecentralizedValue> getPeersToSynchronize(CommonDecentralizedTests.Database db)
	{
		Set<DecentralizedValue> peers=new HashSet<>();
		for (CommonDecentralizedTests.Database dbOther : listDatabase) {
			if (db!=dbOther)
				peers.add(dbOther.getHostID());
		}
		return peers;
	}

	protected void addConfiguration(CommonDecentralizedTests.Database db) throws DatabaseException {
		db.getDbwrapper().getSynchronizer().setNotifier(db);
		db.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
		db.getDbwrapper().setMaxTransactionEventsKeptIntoMemory(3);

		db.getDbwrapper().getDatabaseConfigurationsBuilder()
				.setLocalPeerIdentifier(db.getHostID(), sendIndirectTransactions(), true)
				.setCentralDatabaseBackupCertificate(peerCertificate)
				.addConfiguration(new DatabaseConfiguration(
							new DatabaseSchema(TablePointed.class.getPackage()),
							canInitCentralBackup()?DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE:DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION,
							getPeersToSynchronize(db), getBackupConfiguration(),
							true),
						false)
				.commit();
		Assert.assertNotNull(db.getDbwrapper().getDatabaseConfiguration(TablePointed.class.getPackage()));


		db.initStep2();
	}
	void initCentralDatabaseBackup() throws DatabaseException {
		if (canInitCentralBackup()) {
			this.centralDatabaseBackupDatabase = getDatabaseFactoryInstanceForCentralDatabaseBackupReceiver().getDatabaseWrapperSingleton();
			this.centralDatabaseBackupReceiver = new CentralDatabaseBackupReceiver(centralDatabaseBackupDatabase, centralDatabaseBackupKeyPair.getASymmetricPublicKey());
			this.centralDatabaseBackupDatabase.setNetworkLogLevel(networkLogLevel);

		}
	}
	@BeforeClass
	public void loadDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException, IOException {
		unloadDatabase();
		loadCertificates();
		initCentralDatabaseBackup();
		DatabaseFactory<?> df= getDatabaseFactoryInstance1();
		df.setEncryptionProfileProviders(signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup, encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedSignatureProfileProviderForAuthenticatedP2PMessages, SecureRandomType.DEFAULT);
		db1 = new CommonDecentralizedTests.Database(df.getDatabaseWrapperSingleton());
		df= getDatabaseFactoryInstance2();
		df.setEncryptionProfileProviders(signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup, encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedSignatureProfileProviderForAuthenticatedP2PMessages, SecureRandomType.DEFAULT);
		db2 = new CommonDecentralizedTests.Database(df.getDatabaseWrapperSingleton());
		df= getDatabaseFactoryInstance3();
		df.setEncryptionProfileProviders(signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup, encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedSignatureProfileProviderForAuthenticatedP2PMessages, SecureRandomType.DEFAULT);
		db3 = new CommonDecentralizedTests.Database(df.getDatabaseWrapperSingleton());
		listDatabase.add(db1);
		listDatabase.add(db2);
		listDatabase.add(db3);
		db1.getDbwrapper().setNetworkLogLevel(networkLogLevel);
		db2.getDbwrapper().setNetworkLogLevel(networkLogLevel);
		db3.getDbwrapper().setNetworkLogLevel(networkLogLevel);
		db1.getDbwrapper().setDatabaseLogLevel(databaseLogLevel);
		db2.getDbwrapper().setDatabaseLogLevel(databaseLogLevel);
		db3.getDbwrapper().setDatabaseLogLevel(databaseLogLevel);

		for (CommonDecentralizedTests.Database db : listDatabase) {
			addConfiguration(db);
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());

		}

		Assert.assertTrue(isLocallyDecentralized(db1.getDbwrapper().getTableInstance(TableAlone.class)));
		Assert.assertTrue(isLocallyDecentralized(db1.getDbwrapper().getTableInstance(TablePointed.class)));
		Assert.assertTrue(isLocallyDecentralized(db1.getDbwrapper().getTableInstance(TablePointing.class)));
		Assert.assertFalse(isLocallyDecentralized(db1.getDbwrapper().getTableInstance(UndecentralizableTableA1.class)));
		//Assert.assertFalse(db1.getDbwrapper().getTableInstance(UndecentralizableTableB1.class).isLocallyDecentralizable());
	}

	public void unloadDatabase1() {
		if (db1 != null) {
			try {
				db1.close();
			} finally {
				db1 = null;
			}
		}
		removeDatabaseFiles1();
	}

	public void unloadDatabase2() {
		if (db2 != null) {
			try {
				db2.close();
			} finally {
				db2 = null;
			}
		}
		removeDatabaseFiles2();
	}

	public void unloadDatabase3() {
		if (db3 != null) {
			try {
				db3.close();
			} finally {
				db3 = null;
			}
		}
		removeDatabaseFiles3();
	}

	public void unloadDatabase4() {
		if (db4 != null) {
			try {
				db4.close();
				listDatabase.remove(db4);
			} finally {
				db4 = null;
			}
		}
		removeDatabaseFiles4();
	}

	@BeforeClass
	public void createCentralBackupDirectory()
	{
		if (centralDatabaseBackupDirectory.exists())
			FileTools.deleteDirectory(centralDatabaseBackupDirectory);
		Assert.assertTrue(centralDatabaseBackupDirectory.mkdir());
	}
	@AfterClass
	public void unloadDatabase()  {
		try {
			unloadDatabase1();
		} finally {
			try {
				unloadDatabase2();
			} finally {
				try {
					unloadDatabase3();
				} finally {
					try {
						unloadDatabase4();
					} finally {
						listDatabase.clear();
						if (centralDatabaseBackupDatabase!=null) {
							centralDatabaseBackupDatabase.close();
							centralDatabaseBackupDatabase = null;
						}
						if (centralDatabaseBackupDirectory.exists())
							FileTools.deleteDirectory(centralDatabaseBackupDirectory);
						removeCentralDatabaseFiles();
					}
				}
			}
		}

	}

	@SuppressWarnings("deprecation")
	@Override
	public void finalize() {
		unloadDatabase();
	}

	@AfterMethod
	public void cleanPendedEvents() {
		synchronized (CommonDecentralizedTests.class) {
			for (CommonDecentralizedTests.Database db : listDatabase) {
				//Assert.assertEquals(db.localEvents.size(), 0);
				db.clearPendingEvents();
			}
		}
	}



	protected void sendDistantDatabaseEvent(CommonDecentralizedTests.DistantDatabaseEvent event, P2PDatabaseEventToSend p2pe) throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			if (db.getHostID().equals(event.getHostDestination())) {
				if (db.isConnected()) {
					if (!(p2pe instanceof HookSynchronizeRequest) && !(p2pe instanceof CompatibleDatabasesP2PMessage))
						Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(), p2pe.toString());
					db.getReceivedDatabaseEvents().add(event);
				}
				else
					Assert.fail();
				break;
			}
		}
	}

	protected boolean checkMessages(CommonDecentralizedTests.Database db) throws Exception {
		boolean changed = false;
		while (!db.getReceivedDatabaseEvents().isEmpty()) {
			changed = true;
			final CommonDecentralizedTests.DistantDatabaseEvent dde = db.getReceivedDatabaseEvents().remove(0);

			DatabaseEventToSend event = dde.getDatabaseEventToSend();

			if (event instanceof P2PBigDatabaseEventToSend) {
				try (InputStreamGetter is = new InputStreamGetter() {

					private RandomInputStream actual=null;

					@Override
					public RandomInputStream initOrResetInputStream() throws IOException {
						if (actual!=null)
							actual.close();
						return actual=dde.getInputStream();
					}

					@Override
					public void close() throws IOException {
						if (actual!=null)
							actual.close();
					}
				})
				{
					db.getDbwrapper().getSynchronizer().received((P2PBigDatabaseEventToSend) event, is);
				}
			} else {
				db.getDbwrapper().getSynchronizer().received(event);
			}
		}

		return changed;
	}


	protected boolean checkMessages() throws Exception {
		synchronized (CommonDecentralizedTests.class) {
			boolean changed = false;
			for (CommonDecentralizedTests.Database db : listDatabase) {
				changed |= checkMessages(db);
			}
			return changed;
		}
	}

	private boolean centralDatabaseBackupMessageSent=false;

	protected void exchangeMessages() throws Exception {
		synchronized (CommonDecentralizedTests.class) {
			boolean loop = true;
			while (loop) {
				loop = false;
				centralDatabaseBackupMessageSent=false;
				for (CommonDecentralizedTests.Database db : listDatabase) {
					/*if (canInitCentralBackup())
					{
						boolean newDBVersion=db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations().stream().anyMatch(dc -> dc.getDatabaseSchema().getPackage().equals(UndecentralizableTableA1V2.class.getPackage()));
						BackupRestoreManager brm=db.getDbwrapper().getBackupRestoreManager(newDBVersion?UndecentralizableTableA1V2.class.getPackage():UndecentralizableTableA1.class.getPackage());
						long l=brm.getLastFileTimestampUTC(false);
						if (l!=brm.getLastFileTimestampUTC(true)) {
							long delta = getBackupConfiguration().getMaxBackupFileAgeInMs() - System.currentTimeMillis() +l+1;
							if (delta > 0)
								Thread.sleep(delta);
						}
					}*/
					DatabaseEvent e = db.getDbwrapper().getSynchronizer().nextEvent();
					if (e != null) {

						loop = true;
						if (e instanceof MessageDestinedToCentralDatabaseBackup)
						{
							if (db.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup() || e instanceof DisconnectCentralDatabaseBackup)
								centralDatabaseBackupReceiver.received((MessageDestinedToCentralDatabaseBackup)new CommonDecentralizedTests.DistantDatabaseEvent(db.getDbwrapper(), (MessageDestinedToCentralDatabaseBackup)e).getDatabaseEventToSend());


						}
						else if (e instanceof P2PDatabaseEventToSend) {
							P2PDatabaseEventToSend es = (P2PDatabaseEventToSend) e;

							Assert.assertEquals(es.getHostSource(), db.getHostID());
							Assert.assertNotEquals(es.getHostDestination(), db.getHostID(), ""+es);
							if (db.isConnected()) {
								Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(), e.toString());
								sendDistantDatabaseEvent(new CommonDecentralizedTests.DistantDatabaseEvent(db.getDbwrapper(), es), es);
							} else
								Assert.fail();// TODO really ?
						} else {
							db.getLocalEvents().add(e);
						}
					}
				}
				loop |= checkMessages();
				loop |= centralDatabaseBackupMessageSent;
			}
		}
		//if (db1.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
		checkCentralBackupSynchronization();
	}

	void checkCentralBackupSynchronization() throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			for (Database d : listDatabase) {
				if (d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
					checkCentralBackupSynchronization(d);
			}
			for (Database d : listDatabase) {
				if (d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
					checkCentralBackupSynchronizationWithOtherPeers(d);
			}
		}
	}
	void checkCentralBackupSynchronization(Database d) throws DatabaseException {
		DatabaseWrapper dw=d.getDbwrapper();
		Assert.assertEquals(dw.getSynchronizer().backupDatabasePartsSynchronizingWithCentralDatabaseBackup.size(), 0);
		if (dw.getSynchronizer().isInitializedWithCentralBackup())
		{
			for (DatabaseConfiguration dc : dw.getLoadedDatabaseConfigurations()) {

				if (dc.isSynchronizedWithCentralBackupDatabase())
				{
					Assert.assertEquals(dc.getBackupConfiguration().getMaxBackupFileAgeInMs(), 1000);
					BackupRestoreManager brm=dw.getBackupRestoreManager(dc.getDatabaseSchema().getPackage());
					ClientTable.Record clientRecord=centralDatabaseBackupDatabase.getTableInstance(ClientTable.class).getRecord("clientID", d.hostID);
					DatabaseBackupPerClientTable databaseBackupPerClientTable=centralDatabaseBackupDatabase.getTableInstance(DatabaseBackupPerClientTable.class);
					DatabaseBackupPerClientTable.Record databaseRecord=databaseBackupPerClientTable.getRecord("client", clientRecord, "packageString", dc.getDatabaseSchema().getPackage().getName());
					if (databaseRecord!=null) {
						List<EncryptedBackupPartReferenceTable.Record> records = centralDatabaseBackupDatabase.getTableInstance(EncryptedBackupPartReferenceTable.class).getRecords("database=%db", "db", databaseRecord);
						List<Long> list = brm.getFinalTimestamps();
						for (int i=0;i<list.size();i++) {
							long l=list.get(i);
							Assert.assertTrue(records.stream().anyMatch(r -> r.getFileTimeUTC() == l) || i== list.size()-1 /*&& records.size()==list.size()-1*/,
									"l=" + l + ", i="+i+", list.size=" + list.size() + ", records.size=" + records.size()+", databasePackage="+dc.getDatabaseSchema().getPackage()+", db="+(d==db1?1:(d==db2?2:3))+", list="+list+(records.size()>0?", lastRecord="+records.get(records.size()-1).getFileTimeUTC():""));
						}
						for (EncryptedBackupPartReferenceTable.Record r : records) {
							if (!list.contains(r.getFileTimeUTC()))
								Assert.assertTrue(list.get(0) > r.getFileTimeUTC(), "l=" + r.getFileTimeUTC() + ", list.size=" + list.size() + ", hm.size=" + records.size());
						}
					}
				}
			}
		}
	}
	protected boolean actualGenerateDirectConflict=false;
	void checkCentralBackupSynchronizationWithOtherPeers(Database d) throws DatabaseException {
		DatabaseWrapper dw=d.getDbwrapper();
		if (dw.getSynchronizer().isInitializedWithCentralBackup())
		{
			for (DatabaseConfiguration dc : dw.getLoadedDatabaseConfigurations()) {
				if (dc.isSynchronizedWithCentralBackupDatabase())
				{
					for (Database dother : listDatabase)
					{
						if (!dother.getDbwrapper().getLoadedDatabaseConfigurations().contains(dc))
							continue;
						if (dother!=d)
						{
							if (d.getDbwrapper().getSynchronizer().isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(dother.hostID))
							{
								BackupRestoreManager brmo=dother.getDbwrapper().getBackupRestoreManager(dc.getDatabaseSchema().getPackage());
								Assert.assertNotNull(brmo, dc.getDatabaseSchema().getPackage().getName());
								if (!(this instanceof TestRevertToOldVersionIntoDecentralizedNetwork)) {
									List<Long> finalTimeStamps = brmo.getFinalTimestamps();
									Long lastID = null;
									if (finalTimeStamps.size() > 0) {
										long ts = finalTimeStamps.get(finalTimeStamps.size() - 1);
										if (!brmo.isReference(ts))
											lastID = brmo.getDatabaseBackupMetaDataPerFile(ts, false).getLastTransactionID();
									}
									if (lastID == null && finalTimeStamps.size() > 1) {
										long ts = finalTimeStamps.get(finalTimeStamps.size() - 2);
										lastID = brmo.getDatabaseBackupMetaDataPerFile(ts, brmo.isReference(ts)).getLastTransactionID();
									}
									if (lastID != null && !actualGenerateDirectConflict) {
										//lastID = Long.MIN_VALUE;
										Assert.assertTrue(
												d.getDbwrapper().getSynchronizer().getLastValidatedDistantIDSynchronization(dother.hostID) >= lastID, "lastID=" + lastID + " ; lastOtherID=" + d.getDbwrapper().getSynchronizer().getLastValidatedDistantIDSynchronization(dother.hostID));
									}
								}
							}
						}
					}
				}
			}
		}
	}


	protected void checkAllDatabaseInternalDataUsedForSynchro() throws Exception {

		for (CommonDecentralizedTests.Database db : listDatabase) {
			checkDatabaseInternalDataUsedForSynchro(db);
		}
	}

	protected void checkDatabaseInternalDataUsedForSynchro(CommonDecentralizedTests.Database db) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);

			Assert.assertEquals(db.getDbwrapper().getTransactionsTable().getRecords().size(), 0, "db:"+db.getHostID());
			Assert.assertEquals(db.getDbwrapper().getDatabaseEventsTable().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseHooksTable().getRecords().size(), listDatabase.size());
			/*if (db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size()>0)
			{
				System.out.println(listDatabase.indexOf(db));
				if (db==db1)
				{
					Assert.assertEquals(db2.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
				}
			}*/
			Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
		}

	}

	protected boolean sendIndirectTransactions()
	{
		return true;
	}

	protected void connectLocal(CommonDecentralizedTests.Database db, boolean connectToCentral) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {

			if (!db.isConnected()) {
				if (canInitCentralBackup() && connectToCentral)
					db.getDbwrapper().getSynchronizer().centralDatabaseBackupAvailable();
				db.setConnected(true);
				assert db.getDbwrapper().getSynchronizer().isInitialized();
			}
			/*if (!db.isConnected()) {
				db.getDbwrapper().getSynchronizer().initLocalHostID(db.getHostID(), sendIndirectTransactions());
				if (canInitCentralBackup())
					db.getDbwrapper().getSynchronizer().initConnexionWithDistantBackupCenter(random, encryptionProfileProvider);
				db.setConnected(true);
			}*/
		}
	}

	protected void connectDistant(CommonDecentralizedTests.Database db, CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {

		synchronized (CommonDecentralizedTests.class) {
			if (db.isConnected()) {
				for (CommonDecentralizedTests.Database otherdb : listDatabase) {
					if (otherdb != db && otherdb.isConnected()) {
						db.getDbwrapper().getSynchronizer().peerConnected(otherdb.getHostID());
						//otherdb.getDbwrapper().getSynchronizer().peerConnected(db.getHostID());
						/*db.getDbwrapper().getSynchronizer().initHook(otherdb.getHostID(), otherdb.getDbwrapper()
								.getSynchronizer().getLastValidatedDistantIDSynchronization(db.getHostID()));*/
						// otherdb.getDbwrapper().getSynchronizer().initHook(db.getHostID(),
						// db.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(otherdb.getHostID()));
					}
				}
			}
		}

	}
	protected void connectSelectedDatabase(CommonDecentralizedTests.Database... listDatabase) throws Exception {
		connectSelectedDatabase(false, listDatabase);
	}
	protected void connectSelectedDatabase(boolean connectToCentralDatabase, CommonDecentralizedTests.Database... listDatabase)
			throws Exception {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectLocal(db, connectToCentralDatabase);
		}
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectDistant(db, listDatabase);
		}
		exchangeMessages();
	}
	protected void connectAllDatabase() throws Exception {
		connectAllDatabase(null, true);
	}
	protected void connectAllDatabase(Collection<DecentralizedValue> notInitializedWithOtherPeers, boolean connectToCentral) throws Exception {
		CommonDecentralizedTests.Database[] dbs = new CommonDecentralizedTests.Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectLocal(db, connectToCentral);
		}
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectDistant(db, dbs);
		}
		exchangeMessages();

		for (CommonDecentralizedTests.Database db : listDatabase) {
			Assert.assertTrue(db.isConnected());
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());
		}
		for (CommonDecentralizedTests.Database db : listDatabase) {
			for (CommonDecentralizedTests.Database otherdb : listDatabase) {
				if (db==otherdb)
					Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()));
				else
					Assert.assertEquals(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()),
							(notInitializedWithOtherPeers==null || (!notInitializedWithOtherPeers.contains(otherdb.getHostID()) && !notInitializedWithOtherPeers.contains(db.getHostID()))) && db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDistantPeers().contains(otherdb.getHostID()), db.getHostID().toString()+";"+otherdb.getHostID());

			}
		}
	}

	protected void connectCentralDatabaseBackupWithConnectedDatabase() throws Exception {
		for (Database d : listDatabase)
		{
			assert d.getDbwrapper().getSynchronizer().isInitialized(d.hostID);
			d.getDbwrapper().getSynchronizer().centralDatabaseBackupAvailable();
			d.setConnected(true);
			/*if (!d.getDbwrapper().getSynchronizer().isInitialized(d.hostID)) {
				d.dbwrapper.getSynchronizer().initLocalHostID(d.hostID, true);
				d.setConnected(true);
			}
			d.dbwrapper.getSynchronizer().initConnexionWithDistantBackupCenter(random, encryptionProfileProvider);*/
		}
		exchangeMessages();
	}

	protected void addDatabasePackageToSynchronizeWithCentralDatabaseBackup(Package _package) throws Exception {
		connectAllDatabase();
		//listDatabase.get(0).getDbwrapper().getSynchronizer().synchronizeDatabasePackageWithCentralBackup(_package);
		exchangeMessages();
	}

	protected void disconnectCentralDatabaseBakcup() throws Exception {
		for (Database d : listDatabase)
		{
			d.dbwrapper.getSynchronizer().centralDatabaseBackupDisconnected();
		}
		exchangeMessages();
	}

	protected void disconnect(CommonDecentralizedTests.Database db, CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			if (db.isConnected()) {
				db.setConnected(false);
				db.dbwrapper.getSynchronizer().centralDatabaseBackupDisconnected();

				//db.getDbwrapper().getSynchronizer().disconnectHook(db.getHostID());
				for (CommonDecentralizedTests.Database dbother : listDatabase) {

					if (dbother != db) {
						db.getDbwrapper().getSynchronizer().peerDisconnected(dbother.getHostID());
						dbother.getDbwrapper().getSynchronizer().peerDisconnected(db.getHostID());
					}
				}
				Assert.assertFalse(db.isConnected());
			}
		}
	}

	protected void disconnectSelectedDatabase(CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			disconnect(db, listDatabase);
	}

	protected void disconnectAllDatabase() throws DatabaseException {
		CommonDecentralizedTests.Database[] dbs = new CommonDecentralizedTests.Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		disconnectSelectedDatabase(dbs);
	}

	protected TableAlone.Record generatesTableAloneRecord() throws DatabaseException {
		TableAlone.Record ralone = new TableAlone.Record();
		ralone.id = new DecentralizedIDGenerator();
		try {
			ralone.id2 = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		ralone.value = generateString();
		return ralone;

	}

	@SuppressWarnings("SameParameterValue")
	protected void addTableAloneRecord(CommonDecentralizedTests.Database db, boolean first) throws DatabaseException {
		TableAlone.Record ralone = generatesTableAloneRecord();

		db.getTableAlone().addRecord(((Table<TableAlone.Record>) db.getTableAlone()).getMap(ralone, true, true));
		if (first)
			db.setRecordAlone(ralone);
	}

	protected void addUndecentralizableTableA1Record(CommonDecentralizedTests.Database db) throws DatabaseException {
		UndecentralizableTableA1.Record record = new UndecentralizableTableA1.Record();
		StringBuilder sb=new StringBuilder(db.getHostID().toString());
		for (int i = 0; i < 10; i++) {
			sb.append('a' + ((int) (Math.random() * 52)));
		}
		record.value=sb.toString();
		db.getUndecentralizableTableA1().addRecord(record);

	}

	protected void addUndecentralizableTableB1Record(CommonDecentralizedTests.Database db) throws DatabaseException {
		UndecentralizableTableA1.Record record = new UndecentralizableTableA1.Record();
		StringBuilder sb=new StringBuilder(db.getHostID().toString());

		for (int i = 0; i < 10; i++) {
			sb.append( 'a' + ((int) (Math.random() * 52)));
		}
		record.value=sb.toString();
		record = db.getUndecentralizableTableA1().addRecord(record);
		UndecentralizableTableB1.Record record2 = new UndecentralizableTableB1.Record();
		record2.pointing = record;
		db.getUndecentralizableTableB1().addRecord(record2);
	}

	protected String generateString() {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			res.append('a' + ((int) (Math.random() * 52)));
		}
		return res.toString();
	}

	protected TablePointed.Record generatesTablePointedRecord() {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();
		return rpointed;
	}

	protected TablePointing.Record generatesTablePointingRecord(TablePointed.Record rpointed) throws DatabaseException {
		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing1.table2 = Math.random() < 0.5 ? null : rpointed;
		return rpointing1;
	}

	@SuppressWarnings("SameParameterValue")
	protected void addTablePointedAndPointingRecords(CommonDecentralizedTests.Database db, boolean first) throws DatabaseException {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();

		rpointed = db.getTablePointed().addRecord(rpointed);

		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

		rpointing1.table2 = null;
		rpointing1 = db.getTablePointing().addRecord(rpointing1);
		TablePointing.Record rpointing2 = new TablePointing.Record();
		try {
			rpointing2.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing2.table2 = rpointed;
		rpointing2 = db.getTablePointing().addRecord(rpointing2);
		if (first) {
			db.setRecordPointingNull(rpointing1);
			db.setRecordPointingNotNull(rpointing2);
		}
	}

	protected void addElements(CommonDecentralizedTests.Database db) throws DatabaseException {
		addTableAloneRecord(db, true);
		addTablePointedAndPointingRecords(db, true);
		addUndecentralizableTableA1Record(db);
		addUndecentralizableTableB1Record(db);
	}

	protected void addElements() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			addElements(db);
		}
	}


	@Test
	public void testAddFirstElements() throws DatabaseException {
		addElements();

	}

	@Test(dependsOnMethods = { "testAddFirstElements" })
	public void testInit() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			/*db.getDbwrapper().getSynchronizer().setNotifier(db);
			db.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
			db.getDbwrapper().setMaxTransactionEventsKeptIntoMemory(3);
			Set<DecentralizedValue> peers=new HashSet<>();
			for (CommonDecentralizedTests.Database dbOther : listDatabase) {
				if (db!=dbOther)
					peers.add(db.getHostID());
			}
			db.getDbwrapper().getDatabaseConfigurationsBuilder()
					.setLocalPeerIdentifier(db.getHostID(), sendIndirectTransactions(), true)
					.addConfiguration(new DatabaseConfiguration(new DatabaseSchema(TablePointed.class.getPackage()), db.canInitCentralBackup?DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE:DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION, peers, db.backupConfiguration, true), false)
					.commit();*/

					/*.getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(),
					TablePointed.class.getPackage());*/
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());
		}


/*		for (CommonDecentralizedTests.Database db : listDatabase) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					db.getDbwrapper().getDatabaseConfigurationsBuilder().syn
					AbstractHookRequest har = db.getDbwrapper().getSynchronizer().askForHookAddingAndSynchronizeDatabase(
							other.getHostID(), false, TablePointed.class.getPackage());
					har = other.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
					db.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
				}
			}
			break;
		}*/

	}

	private void testAllConnected() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			Assert.assertTrue(db.isConnected());
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(), db.getHostID().toString());
			/*if (!db.getDbwrapper().getSynchronizer().isInitialized()) {
				Assert.assertNotNull(db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getLocalPeer());
			}*/

			for (CommonDecentralizedTests.Database otherdb : listDatabase) {
				if (db==otherdb)
					Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()));
				else
					Assert.assertEquals(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()),
							db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDistantPeers().contains(otherdb.getHostID()), db.getHostID().toString());
			}
			DatabaseHooksTable.Record r = db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).getLocalDatabaseHost();
			if (r.getDatabasePackageNames() == null) {

				db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).localHost = null;
				r = db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).getLocalDatabaseHost();
				if (r.getDatabasePackageNames()==null)
				{
					Assert.assertEquals(db, db4, ""+listDatabase.indexOf(db));
					Assert.assertEquals(db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfiguration(TableAlone.class.getPackage().getName()).getDistantPeersThatCanBeSynchronizedWithThisDatabase().size(), 0);
				}
				else
					Assert.fail();
			}
			if (db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfiguration(TableAlone.class.getPackage().getName()).getDistantPeersThatCanBeSynchronizedWithThisDatabase().size()>0)
				Assert.assertTrue(r.getDatabasePackageNames().contains(TableAlone.class.getPackage().getName()));

		}
	}
	public void testAllDisconnected() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			Assert.assertFalse(db.isConnected());
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(), db.getHostID().toString());
			Assert.assertEquals(db.getDbwrapper().getSynchronizer().initializedHooks.size(), 1, ""+db.getDbwrapper().getSynchronizer().initializedHooks);

			for (CommonDecentralizedTests.Database otherdb : listDatabase) {
				if (db==otherdb)
					Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()));
				else
					Assert.assertFalse(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()),db.getHostID().toString());
			}
			DatabaseHooksTable.Record r = db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).getLocalDatabaseHost();
			if (r.getDatabasePackageNames() == null) {

				db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).localHost = null;
				r = db.getDbwrapper().getTableInstance(DatabaseHooksTable.class).getLocalDatabaseHost();
				if (r.getDatabasePackageNames()==null)
				{
					Assert.assertEquals(db, db4, ""+listDatabase.indexOf(db));
					Assert.assertEquals(db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfiguration(TableAlone.class.getPackage().getName()).getDistantPeersThatCanBeSynchronizedWithThisDatabase().size(), 0);
				}
				else
					Assert.fail();
			}
			if (db.getDbwrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfiguration(TableAlone.class.getPackage().getName()).getDistantPeersThatCanBeSynchronizedWithThisDatabase().size()>0)
				Assert.assertTrue(r.getDatabasePackageNames().contains(TableAlone.class.getPackage().getName()));

		}
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testAllConnect() throws Exception {
		connectAllDatabase();
		exchangeMessages();
		testAllConnected();
		disconnectAllDatabase();
		testAllDisconnected();
		connectAllDatabase();
		exchangeMessages();
		testAllConnected();

	}

	@Test(dependsOnMethods = {"testAllConnect"})
	public void testOldElementsAddedBeforeAddingSynchroSynchronized()
			throws Exception {
		exchangeMessages();
		testSynchronisation();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

		/*if (getBackupConfiguration()!=null) {
			for (Database db : listDatabase)
				db.getDbwrapper().getBackupRestoreManager(TableAlone.class.getPackage()).createBackupReference();
		}*/
	}

	private static final int numberEvents=40;

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"testOldElementsAddedBeforeAddingSynchroSynchronized" })
	public void testSynchroBetweenTwoPeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
										   boolean peersInitiallyConnected)
			throws Exception {
		for (TableEvent<DatabaseRecord> event : provideTableEventsForSynchro())
			testSynchroBetweenPeersImpl(2, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroAfterTestsBetweenTwoPeers() throws DatabaseException {
		testSynchronisation();
	}



	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroBetweenThreePeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											 boolean peersInitiallyConnected)
			throws Exception {
		for (TableEvent<DatabaseRecord> event : provideTableEventsForSynchro())
			testSynchroBetweenPeersImpl(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeers" })
	public void testSynchroAfterTestsBetweenThreePeers() throws DatabaseException {
		testSynchronisation();
	}
	/*
	 * @Test(dependsOnMethods={"testAllConnect"}) public void
	 * testInitDatabaseNetwork() throws DatabaseException { for (Database db :
	 * listDatabase) {
	 *
	 * db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(
	 * ), TablePointed.class.getPackage()); } }
	 */

	/*
	 * @Test(dependsOnMethods={"testInit"}) public void
	 * testAddSynchroBetweenDatabase() throws DatabaseException,
	 * ClassNotFoundException, IOException { for (Database db : listDatabase) { for
	 * (Database otherdb : listDatabase) { if (otherdb!=db) {
	 * db.getDbwrapper().getSynchronizer().addHookForDistantHost(otherdb.getHostID()
	 * ,TablePointed.class.getPackage());
	 * otherdb.getDbwrapper().getSynchronizer().addHookForDistantHost(db.getHostID()
	 * ,TablePointed.class.getPackage()); } } } exchangeMessages(); }
	 */

	protected void testSynchronisation(CommonDecentralizedTests.Database db) throws DatabaseException {

		for (TableAlone.Record r : db.getTableAlone().getRecords()) {

			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TableAlone.Record otherR = other.getTableAlone().getRecord("id", r.id, "id2", r.id2);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointed.Record r : db.getTablePointed().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointed.Record otherR = other.getTablePointed().getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointing.Record r : db.getTablePointing().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointing.Record otherR = other.getTablePointing().getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					if (r.table2 == null)
						Assert.assertNull(otherR.table2);
					else
						Assert.assertEquals(otherR.table2.value, r.table2.value);
				}
			}
		}
		if (!(this instanceof TestDatabaseToOperateActionIntoDecentralizedNetwork)) {
			for (UndecentralizableTableA1.Record r : db.getUndecentralizableTableA1().getRecords()) {
				for (CommonDecentralizedTests.Database other : listDatabase) {
					if (other != db) {
						UndecentralizableTableA1.Record otherR = other.getUndecentralizableTableA1().getRecord("id",
								r.id);
						if (otherR != null) {
							Assert.assertNotEquals(otherR.value, r.value);
						}
					}
				}
			}
			for (UndecentralizableTableB1.Record r : db.getUndecentralizableTableB1().getRecords()) {
				for (CommonDecentralizedTests.Database other : listDatabase) {
					if (other != db) {
						UndecentralizableTableB1.Record otherR = other.getUndecentralizableTableB1().getRecord("id", r.id);
						if (otherR != null) {
							if (r.pointing == null)
								Assert.assertNull(otherR.pointing);
							else
								Assert.assertNotEquals(otherR.pointing.value, r.pointing.value);
						}
					}
				}
			}
		}
	}

	protected void testSynchronisation() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			testSynchronisation(db);
	}

	protected ArrayList<TableEvent<DatabaseRecord>> provideTableEventsForThreads() throws DatabaseException {
		return provideTableEvents(db1.getDbwrapper(), (int)(5.0+Math.random()*10.0));
	}
	protected ArrayList<TableEvent<DatabaseRecord>> provideTableEventsForSynchro() throws DatabaseException {
		return provideTableEvents(db1.getDbwrapper(), (int)(numberEvents+Math.random()*10.0));
	}
	@SuppressWarnings({"unchecked", "RedundantCast"})
	protected ArrayList<TableEvent<DatabaseRecord>> provideTableEvents(DatabaseWrapper wrapper, int number) throws DatabaseException {
		ArrayList<TableEvent<DatabaseRecord>> res = new ArrayList<>();
		ArrayList<TablePointed.Record> pointedRecord = new ArrayList<>();
		ArrayList<DatabaseRecord> livingRecords = new ArrayList<>();

		while (number > 0) {
			TableEvent<DatabaseRecord> te = null;

			switch ((int) (Math.random() * 4)) {

				case 0: {

					DatabaseRecord record = null;
					switch ((int) (Math.random() * 3)) {
						case 0:
							record = generatesTableAloneRecord();
							break;
						case 1:
							record = generatesTablePointedRecord();
							break;
						case 2:
							if (pointedRecord.isEmpty())
								record = generatesTablePointedRecord();
							else
								record = generatesTablePointingRecord(
										pointedRecord.get((int) (Math.random() * pointedRecord.size())));
							break;
					}
					assert record != null;

					te = new TableEvent<>(-1, DatabaseEventType.ADD, (Table<DatabaseRecord>)wrapper.getTableInstance(Table.getTableClass(
							record.getClass())),null, record, null);
					livingRecords.add(record);

				}
				break;
				case 1: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					te = new TableEvent<>(-1, DatabaseEventType.REMOVE, (Table<DatabaseRecord>)wrapper.getTableInstance(Table.getTableClass(
							record.getClass())), record, null, null);
					Assert.assertTrue(livingRecords.remove(record));

					//noinspection SuspiciousMethodCalls
					pointedRecord.remove(record);
				}
				break;
				case 2: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					te = new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, (Table<DatabaseRecord>)wrapper.getTableInstance(Table.getTableClass(
							record.getClass())), record, null, null);
					Assert.assertTrue(livingRecords.remove(record));
					//noinspection SuspiciousMethodCalls
					pointedRecord.remove(record);
				}
				break;
				case 3: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					DatabaseRecord recordNew = null;
					if (record instanceof TableAlone.Record) {
						TableAlone.Record r = generatesTableAloneRecord();
						r.id = ((TableAlone.Record) record).id;
						r.id2=((TableAlone.Record)record).id2;
						recordNew = r;
					} else if (record instanceof TablePointed.Record) {
						TablePointed.Record r = generatesTablePointedRecord();
						r.id = ((TablePointed.Record) record).id;
						recordNew = r;
					} else if (record instanceof TablePointing.Record) {
						TablePointing.Record r = new TablePointing.Record();
						r.id = ((TablePointing.Record) record).id;
						if (pointedRecord.isEmpty())
							continue;
						r.table2 = pointedRecord.get((int) (Math.random() * pointedRecord.size()));
						recordNew = r;
					}
					te = new TableEvent<>(-1, DatabaseEventType.UPDATE, (Table<DatabaseRecord>)wrapper.getTableInstance(Table.getTableClass(
							record.getClass())), record, recordNew, null);
				}
				break;

			}
			if (te != null) {
				res.add(te);
				--number;
			}
		}

		return res;
	}

	@DataProvider(name = "provideDataForSynchroBetweenTwoPeers")
	public Object[][] provideDataForSynchroBetweenTwoPeers()  {
		//int numberEvents = 40;
		Object[][] res = new Object[2 * 3][];
		int index = 0;

		for (boolean exceptionDuringTransaction : new boolean[] { false, true }) {
			boolean[] gdc = exceptionDuringTransaction ? new boolean[] { false } : new boolean[] { false, true };
			for (boolean generateDirectConflict : gdc) {
				for (boolean peersInitiallyConnected : new boolean[] { true, false }) {
					//List<TableEvent<DatabaseRecord>> l=provideTableEvents(numberEvents);
					//assert l.size()==numberEvents;
					//for (TableEvent<DatabaseRecord> te : l) {
						res[index++] = new Object[] {exceptionDuringTransaction,
								generateDirectConflict, peersInitiallyConnected};
					//}
				}
			}
		}
		assert index==res.length;
		return res;
	}

	protected void proceedEvent(final CommonDecentralizedTests.Database db, final boolean exceptionDuringTransaction,
							  final List<TableEvent<DatabaseRecord>> events) throws DatabaseException {
		proceedEvent(db, exceptionDuringTransaction, events, false);
	}

	protected void proceedEvent(final CommonDecentralizedTests.Database db, final boolean exceptionDuringTransaction,
							  final List<TableEvent<DatabaseRecord>> events, final boolean manualKeys) throws DatabaseException {
		db.getDbwrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

			@Override
			public Void run() throws Exception {
				int indexException = exceptionDuringTransaction ? ((int) (Math.random() * events.size())) : -1;
				for (int i = 0; i < events.size(); i++) {
					TableEvent<DatabaseRecord> te = events.get(i);
					te.setTable(getTable(db, te));
					proceedEvent(te, indexException == i, manualKeys);
				}
				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset() {

			}
		});

	}


	@SuppressWarnings("unchecked")
	protected void proceedEvent(final TableEvent<DatabaseRecord> event,
								boolean exceptionDuringTransaction, boolean manualKeys) throws Exception {
		Table<DatabaseRecord> table=(Table<DatabaseRecord>)event.getTable();
		switch (event.getType()) {

			case ADD:

				table.addRecord(event.getNewDatabaseRecord());
				break;
			case REMOVE:
				table.removeRecord(event.getOldDatabaseRecord());
				break;
			case REMOVE_WITH_CASCADE:
				table.removeRecordWithCascade(event.getOldDatabaseRecord());
				break;
			case UPDATE:
				table.updateRecord(event.getNewDatabaseRecord());
				break;

		}
		if (exceptionDuringTransaction)
			throw new Exception(""+event);

	}

	protected void testEventSynchronized(CommonDecentralizedTests.Database db, List<TableEvent<DatabaseRecord>> levents, boolean synchronizedOk)
			throws DatabaseException {
		ArrayList<TableEvent<DatabaseRecord>> l = new ArrayList<>(levents.size());
		for (TableEvent<DatabaseRecord> te : levents) {
			switch (te.getType()) {
				case ADD:
				case UPDATE:
				case REMOVE: {

					for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
						TableEvent<DatabaseRecord> te2 = it.next();
						DatabaseRecord dr1 = te.getOldDatabaseRecord() == null ? te.getNewDatabaseRecord()
								: te.getOldDatabaseRecord();
						DatabaseRecord dr2 = te2.getOldDatabaseRecord() == null ? te2.getNewDatabaseRecord()
								: te2.getOldDatabaseRecord();
						if (dr1 == null)
							throw new IllegalAccessError();
						if (dr2 == null)
							throw new IllegalAccessError();
						Table<DatabaseRecord> table = getTable(db, te2);
						if (te.getTable().getClass().getName().equals(table.getClass().getName())
								&& table.equals(dr1, dr2)) {
							it.remove();
							break;
						}
					}
					break;
				}
				case REMOVE_WITH_CASCADE: {

					Table<DatabaseRecord> tablePointed = null;
					TablePointed.Record recordRemoved = null;
					for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
						TableEvent<DatabaseRecord> te2 = it.next();
						Table<DatabaseRecord> table = getTable(db, te2);
						DatabaseRecord dr1 = te.getOldDatabaseRecord() == null ? te.getNewDatabaseRecord()
								: te.getOldDatabaseRecord();
						DatabaseRecord dr2 = te2.getOldDatabaseRecord() == null ? te2.getNewDatabaseRecord()
								: te2.getOldDatabaseRecord();
						if (dr1 == null)
							throw new IllegalAccessError();
						if (dr2 == null)
							throw new IllegalAccessError();

						if (te.getTable().getClass().getName().equals(table.getClass().getName())
								&& table.equals(dr1, dr2)) {

							it.remove();
							if (table.getClass().getName().equals(TablePointed.class.getName())) {
								recordRemoved = (TablePointed.Record) te2.getNewDatabaseRecord();
								tablePointed = table;
							}
							break;
						}

					}
					if (recordRemoved != null) {
						for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
							TableEvent<DatabaseRecord> te2 = it.next();
							Table<DatabaseRecord> table = getTable(db, te2);
							if (table.getClass().getName().equals(TablePointing.class.getName())) {
								TablePointing.Record tp = te2.getOldDatabaseRecord() == null
										? (TablePointing.Record) te2.getNewDatabaseRecord()
										: (TablePointing.Record) te2.getOldDatabaseRecord();
								if (tp.table2 != null && tablePointed.equals(tp.table2, recordRemoved))
									it.remove();
							}
						}

					}
					break;
				}
			}
			l.add(te);
		}
		for (TableEvent<DatabaseRecord> te : l)
			testEventSynchronized(db, te, synchronizedOk);
	}

	protected Map<String, Object> getMapPrimaryKeys(Table<DatabaseRecord> table, DatabaseRecord record)
			throws DatabaseException {
		Map<String, Object> res = new HashMap<>();
		for (FieldAccessor fa : table.getPrimaryKeysFieldAccessors()) {
			res.put(fa.getFieldName(), fa.getValue(record));
		}
		return res;
	}

	@SuppressWarnings("unchecked")
	Table<DatabaseRecord> getTable(CommonDecentralizedTests.Database db, TableEvent<DatabaseRecord> event) throws DatabaseException {
		return (Table<DatabaseRecord>)db.getDbwrapper().getTableInstance(Table.getTableClass(event.getNewDatabaseRecord()==null?event.getOldDatabaseRecord().getClass():event.getNewDatabaseRecord().getClass()));
	}

	protected void testEventSynchronized(CommonDecentralizedTests.Database db, TableEvent<DatabaseRecord> event, boolean synchronizedOk)
			throws DatabaseException {

		if (event.getType() == DatabaseEventType.ADD || event.getType() == DatabaseEventType.UPDATE) {
			Table<DatabaseRecord> table = getTable(db, event);

			DatabaseRecord dr = table.getRecord(getMapPrimaryKeys(table, event.getNewDatabaseRecord()));
			if (synchronizedOk)
				Assert.assertNotNull(dr, event.getType().name() + " ; " + table);
			Assert.assertEquals(table.equalsAllFields(dr, event.getNewDatabaseRecord()), synchronizedOk,
					"Concerned event=" + event+", table event="+dr+", type="+event.getType()+", table="+table);
		} else if (event.getType() == DatabaseEventType.REMOVE
				|| event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE) {
			Table<DatabaseRecord> table = getTable(db, event);
			DatabaseRecord dr = table.getRecord(getMapPrimaryKeys(table, event.getOldDatabaseRecord()));
			if (synchronizedOk)
				Assert.assertNull(dr);
		} else
			throw new IllegalAccessError();
	}

	protected void testCollision(CommonDecentralizedTests.Database db, TableEvent<DatabaseRecord> event, CommonDecentralizedTests.DetectedCollision collision)
			throws DatabaseException {
		Table<DatabaseRecord> table = getTable(db, event);
		Assert.assertNotNull(collision);
		Assert.assertEquals(collision.type, event.getType());
		Assert.assertEquals(collision.concernedTable.getSqlTableName(), table.getSqlTableName(), event.getTable().getSqlTableName());
		Assert.assertNotEquals(collision.distantPeerID, db.getHostID());
		switch (event.getType()) {
			case ADD:
				Assert.assertNotNull(collision.actualValues);
				Assert.assertNull(event.getOldDatabaseRecord());
				Assert.assertTrue(table.equals(event.getNewDatabaseRecord(), collision.actualValues));
				Assert.assertTrue(table.equalsAllFields(event.getNewDatabaseRecord(), collision.newValues));
				break;
			case REMOVE:

				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNull(event.getNewDatabaseRecord());
				Assert.assertNull(collision.newValues);
				if (collision.actualValues != null) {
					Assert.assertTrue(table.equals(event.getOldDatabaseRecord(), collision.actualValues));
				}
				break;
			case REMOVE_WITH_CASCADE:
				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNull(event.getNewDatabaseRecord());
				Assert.assertNull(collision.newValues);
				Assert.assertNull(collision.actualValues);
				break;
			case UPDATE:
				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNotNull(event.getNewDatabaseRecord());
				Assert.assertNotNull(collision.newValues);
				// Assert.assertNull(collision.actualValues);
				break;

		}
	}

	protected DatabaseRecord clone(DatabaseRecord record) {
		if (record == null)
			return null;

		if (record instanceof TableAlone.Record) {
			TableAlone.Record r = (TableAlone.Record) record;
			return r.clone();
		} else if (record instanceof TablePointing.Record) {
			TablePointing.Record r = (TablePointing.Record) record;
			return r.clone();
		} else if (record instanceof TablePointed.Record) {
			TablePointed.Record r = (TablePointed.Record) record;
			return r.clone();
		} else if (record instanceof UndecentralizableTableA1.Record) {
			UndecentralizableTableA1.Record r = (UndecentralizableTableA1.Record) record;
			return r.clone();
		} else if (record instanceof UndecentralizableTableB1.Record) {
			UndecentralizableTableB1.Record r = (UndecentralizableTableB1.Record) record;
			return r.clone();
		} else
			throw new IllegalAccessError("Unkown type " + record.getClass());
	}

	protected TableEvent<DatabaseRecord> clone(TableEvent<DatabaseRecord> event) {
		return new TableEvent<>(event.getID(), event.getType(),event.getTable(), clone(event.getOldDatabaseRecord()),
				clone(event.getNewDatabaseRecord()), event.getHostsDestination());
	}

	protected List<TableEvent<DatabaseRecord>> clone(List<TableEvent<DatabaseRecord>> events) {
		ArrayList<TableEvent<DatabaseRecord>> res = new ArrayList<>(events.size());
		for (TableEvent<DatabaseRecord> te : events)
			res.add(clone(te));
		return res;
	}

	protected void testSynchroBetweenPeersImpl(int peersNumber, boolean exceptionDuringTransaction,
											   boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {

		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(peersNumber);
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));
		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (exceptionDuringTransaction) {
			if (peersInitiallyConnected) {
				connectSelectedDatabase(concernedDatabase);
				exchangeMessages();
				for (int i = 1; i < peersNumber; i++) {
					CommonDecentralizedTests.Database db = concernedDatabase[i];
					db.setNewDatabaseEventDetected(false);
				}
			}

			CommonDecentralizedTests.Database db = concernedDatabase[0];
			try {
				proceedEvent(db, true, levents);
				Assert.fail();
			} catch (Exception ignored) {

			}

			if (!peersInitiallyConnected)
				connectSelectedDatabase(concernedDatabase);

			exchangeMessages();

			for (int i = 1; i < peersNumber; i++) {
				db = concernedDatabase[i];
				if (peersInitiallyConnected && !canInitCentralBackup())
					Assert.assertFalse(db.isNewDatabaseEventDetected(), "" + db.getLocalEvents());
				testEventSynchronized(db, event, false);
			}

			disconnectSelectedDatabase(concernedDatabase);
		} else {

			if (generateDirectConflict) {
				int i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					db.setReplaceWhenCollisionDetected(i++ != 0);
					proceedEvent(db, false, clone(levents), true);

				}
				connectSelectedDatabase(concernedDatabase);
				exchangeMessages();
				i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					if (!canInitCentralBackup()) {
						Assert.assertTrue(db.isNewDatabaseEventDetected());

						CommonDecentralizedTests.DetectedCollision dcollision = db.getDetectedCollision();

						Assert.assertNotNull(dcollision, "i=" + (i));
						testCollision(db, event, dcollision);
						Assert.assertTrue(db.getAnomalies().isEmpty() || !sendIndirectTransactions(), db.getAnomalies().toString());
					}
					db.getAnomalies().clear();
					++i;
				}

			} else {
				if (peersInitiallyConnected) {
					connectSelectedDatabase(concernedDatabase);
					for (CommonDecentralizedTests.Database db : concernedDatabase)
						db.setNewDatabaseEventDetected(false);
				}

				CommonDecentralizedTests.Database db = concernedDatabase[0];
				proceedEvent(db, false, levents);

				if (!peersInitiallyConnected)
					connectSelectedDatabase(concernedDatabase);

				exchangeMessages();
				Assert.assertTrue(db.getAnomalies().isEmpty());

				for (int i = 1; i < concernedDatabase.length; i++) {
					db = concernedDatabase[i];
					Assert.assertNull(db.getDetectedCollision());
					Assert.assertTrue(db.getAnomalies().isEmpty());
					//Assert.assertTrue(db.isNewDatabaseEventDetected());
					testEventSynchronized(db, event, true);

				}

			}
			disconnectSelectedDatabase(concernedDatabase);
			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				testEventSynchronized(db, event, false );
				db.clearPendingEvents();
			}

			connectAllDatabase();
			exchangeMessages();

			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				// DetectedCollision collision=db.getDetectedCollision();
				// Assert.assertNotNull(collision, "Database N°"+i);
				Assert.assertTrue(db.getAnomalies().isEmpty());
				Assert.assertTrue(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, event, true);

			}


		}
		testSynchronisation();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	protected void testTransactionBetweenPeers(int peersNumber, boolean peersInitiallyConnected,
											 List<TableEvent<DatabaseRecord>> levents, boolean threadTest)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(listDatabase.size());
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));

		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (peersInitiallyConnected && !threadTest)
			connectSelectedDatabase(concernedDatabase);

		CommonDecentralizedTests.Database db = concernedDatabase[0];

		proceedEvent(db, false, levents);

		if (!peersInitiallyConnected && !threadTest)
			connectSelectedDatabase(concernedDatabase);

		exchangeMessages();
		Assert.assertTrue(db.getAnomalies().isEmpty());

		for (int i = 1; i < peersNumber; i++) {
			db = concernedDatabase[i];

			Assert.assertNull(db.getDetectedCollision());
			if (!threadTest)
				Assert.assertTrue(db.isNewDatabaseEventDetected());
			if (!threadTest)
				testEventSynchronized(db, levents, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

		}

		for (int i = peersNumber; i < listDatabase.size(); i++) {
			db = listDatabase.get(i);

			if (!threadTest)
				testEventSynchronized(db, levents, false);
		}
		if (!threadTest) {
			disconnectSelectedDatabase(concernedDatabase);
			connectAllDatabase();
			exchangeMessages();

			for (int i = 2; i < listDatabase.size(); i++) {
				db = listDatabase.get(i);

				Assert.assertNull(db.getDetectedCollision());
				Assert.assertTrue(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, levents, true);
				Assert.assertTrue(db.getAnomalies().isEmpty());

			}

			testSynchronisation();
			disconnectAllDatabase();
			checkAllDatabaseInternalDataUsedForSynchro();
		}

	}

	public Object[][] provideDataForTransactionBetweenTwoPeers2() {
		Object[][] res = new Object[2][];
		int index = 0;
		for (boolean peersInitiallyConnected : new boolean[] { true, false }) {
			//for (int i = 0; i < numberTransactions; i++) {
				res[index++] = new Object[] {peersInitiallyConnected };

			//}
		}

		return res;
	}

	@DataProvider(name = "provideDataSynchroBetweenThreePeers")
	public Object[][] provideDataSynchroBetweenThreePeers() {
		return provideDataForSynchroBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionBetweenTwoPeers")
	public Object[][] provideDataForTransactionBetweenTwoPeers() {
		return provideDataForTransactionBetweenTwoPeers2();
	}
	@DataProvider(name = "provideDataForTransactionBetweenTwoPeersForRestorationTests")
	public Object[][] provideDataForTransactionBetweenTwoPeersForRestorationTests() {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionBetweenThreePeers")
	public Object[][] provideDataForTransactionBetweenThreePeers()  {
		return provideDataForTransactionBetweenTwoPeers();
	}
	@DataProvider(name = "provideDataForTransactionBetweenThreePeersForRestorationTests")
	public Object[][] provideDataForTransactionBetweenThreePeersForRestorationTests() {
		return provideDataForTransactionBetweenTwoPeersForRestorationTests();
	}

	@DataProvider(name = "provideDataForTransactionSynchros")
	public Object[][] provideDataForTransactionSynchros() {
		return provideDataForTransactionBetweenTwoPeers();
	}


	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection")
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnection() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}



	protected void testTransactionsSynchrosWithIndirectConnection(boolean peersInitiallyConnected,
																  List<TableEvent<DatabaseRecord>> levents, boolean multiThread)
			throws Exception {
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };

		if (peersInitiallyConnected && !multiThread) {
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
		}

		CommonDecentralizedTests.Database db = listDatabase.get(0);
		proceedEvent(db, false, levents);

		if (!peersInitiallyConnected && !multiThread) {
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
		}

		exchangeMessages();

		Assert.assertTrue(db.getAnomalies().isEmpty());

		db = listDatabase.get(1);
		Assert.assertNull(db.getDetectedCollision());
		if (!multiThread)
			Assert.assertTrue(db.isNewDatabaseEventDetected());
		if (!multiThread)
			testEventSynchronized(db, levents, true);
		Assert.assertTrue(db.getAnomalies().isEmpty());

		db = listDatabase.get(2);
		Assert.assertNull(db.getDetectedCollision());
		if (!multiThread)
			Assert.assertTrue(db.isNewDatabaseEventDetected());
		Assert.assertTrue(db.getAnomalies().isEmpty());
		if (!multiThread)
			testEventSynchronized(db, levents, true);

		if (!multiThread) {
			disconnectSelectedDatabase(segmentA);
			disconnectSelectedDatabase(segmentB);

			connectAllDatabase();
			exchangeMessages();
			disconnectAllDatabase();
		}

	}



}
