package com.distrimind.ood.database;
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

import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.messages.AbstractEncryptedBackupPartComingFromCentralDatabaseBackup;
import com.distrimind.ood.database.messages.EncryptedBackupPartDestinedToCentralDatabaseBackup;
import com.distrimind.ood.database.messages.SynchronizationPlanMessageComingFromCentralDatabaseBackup;
import com.distrimind.ood.i18n.DatabaseMessages;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.FileTools;
import com.distrimind.util.Reference;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;
import com.distrimind.util.progress_monitors.ProgressMonitorDM;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 2.0
 * @since MaDKitLanEdition 2.0.0
 */
public class BackupRestoreManager {

	private final static int LAST_BACKUP_UTC_POSITION=0;
	//private final static int RECORDS_INDEX_POSITION=LAST_BACKUP_UTC_POSITION+8;
	private final static int LIST_CLASSES_POSITION=LAST_BACKUP_UTC_POSITION+26;
	public static final int MIN_TRANSACTION_SIZE_IN_BYTES=38;

	private ArrayList<Long> fileReferenceTimeStamps;
	private ArrayList<Long> fileTimeStamps;
	private final File backupDirectory;
	private final BackupConfiguration backupConfiguration;
	private final DatabaseConfiguration databaseConfiguration;
	private final ArrayList<Class<? extends Table<?>>> classes;
	private static final Pattern fileReferencePattern = Pattern.compile("^backup-ood-([1-9][0-9]*)\\.dreference$");
	private static final Pattern fileIncrementPattern = Pattern.compile("^backup-ood-([1-9][0-9]*)\\.dincrement$");

	private final File computeDatabaseReference;
	private final DatabaseWrapper databaseWrapper;
	private final boolean passive;
	private boolean temporaryDisabled=false;
	private final Package dbPackage;
	private final boolean generateRestoreProgressBar;
	private volatile long lastCurrentRestorationFileUsed=Long.MIN_VALUE;
	private volatile Long maxDateUTC=null;

	//private volatile long currentBackupReferenceUTC=Long.MAX_VALUE;


	File getLastFile()
	{
		if (fileTimeStamps.size()==0)
			return null;
		long l=fileTimeStamps.get(fileTimeStamps.size()-1);
		return getFile(l, isReferenceFile(l));
	}
	BackupRestoreManager(DatabaseWrapper databaseWrapper, File backupDirectory, DatabaseConfiguration databaseConfiguration, boolean passive) throws DatabaseException {
		this(databaseWrapper, backupDirectory, databaseConfiguration, databaseConfiguration.getBackupConfiguration(), passive);
	}
	BackupRestoreManager(DatabaseWrapper databaseWrapper, File backupDirectory, DatabaseConfiguration databaseConfiguration, BackupConfiguration backupConfiguration, boolean passive) throws DatabaseException {
		if (backupDirectory==null)
			throw new NullPointerException();
		if (backupDirectory.exists() && backupDirectory.isFile())
			throw new IllegalArgumentException();
		if (databaseConfiguration==null)
			throw new NullPointerException();
		if (databaseWrapper==null)
			throw new NullPointerException();
		if (backupConfiguration==null)
			throw new NullPointerException();
		this.passive=passive;
		this.databaseConfiguration=databaseConfiguration;
		this.databaseWrapper=databaseWrapper;
		this.dbPackage=databaseConfiguration.getDatabaseSchema().getPackage();
		FileTools.checkFolderRecursive(backupDirectory);
		this.backupDirectory=backupDirectory;
		this.backupConfiguration=backupConfiguration;
		classes=databaseConfiguration.getDatabaseSchema().getSortedTableClasses();

		this.computeDatabaseReference=new File(this.backupDirectory, "compute_database_new_reference.query");
		if (this.computeDatabaseReference.exists() && this.computeDatabaseReference.isDirectory())
			throw new IllegalArgumentException();
		if (this.backupConfiguration.backupProgressMonitorParameters==null) {
			ProgressMonitorParameters p=new ProgressMonitorParameters(String.format(DatabaseMessages.BACKUP_DATABASE.toString(), databaseConfiguration.getDatabaseSchema().getPackage().toString()), null, 0, 100);
			p.setMillisToDecideToPopup(1000);
			p.setMillisToPopup(1000);
			this.backupConfiguration.setBackupProgressMonitorParameters(p);
		}
		generateRestoreProgressBar= this.backupConfiguration.restoreProgressMonitorParameters == null;
		scanFiles();
		if (checkTablesHeader(getFileForBackupReference()) && !databaseWrapper.sql_database.get(dbPackage).isEmpty())
			createIfNecessaryNewBackupReference();
	}

	private void generateProgressBarParameterForRestoration(long timeUTC)
	{
		if (generateRestoreProgressBar)
		{
			ProgressMonitorParameters p=new ProgressMonitorParameters(String.format(DatabaseMessages.RESTORE_DATABASE.toString(), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz").format(new Date(timeUTC)),databaseConfiguration.getDatabaseSchema().getPackage().toString()), null, 0, 100);
			p.setMillisToDecideToPopup(1000);
			p.setMillisToPopup(1000);
			this.backupConfiguration.setRestoreProgressMonitorParameters(p);

		}
	}

	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	private void scanFiles() {

		fileTimeStamps=new ArrayList<>();
		fileReferenceTimeStamps=new ArrayList<>();
		try {
			File[] files = this.backupDirectory.listFiles();


			if (files == null)
				return;
			for (File f : files) {
				if (f.isDirectory())
					continue;
				Matcher m = fileIncrementPattern.matcher(f.getName());
				if (m.matches()) {
					try {
						long timeStamp = Long.parseLong(m.group(1));
						fileTimeStamps.add(timeStamp);
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}

				} else {
					m = fileReferencePattern.matcher(f.getName());
					if (m.matches()) {
						try {
							long timeStamp = Long.parseLong(m.group(1));
							fileTimeStamps.add(timeStamp);
							fileReferenceTimeStamps.add(timeStamp);
						} catch (NumberFormatException e) {
							e.printStackTrace();
						}

					}
				}

			}
			Collections.sort(fileTimeStamps);
			Collections.sort(fileReferenceTimeStamps);
		}
		finally {
			cleanCache();

		}
	}


	public File getBackupDirectory() {
		return backupDirectory;
	}
	private File getFile(long timeStamp, boolean backupReference)
	{
		return getFile(timeStamp, backupReference, false);
	}
	private File getFile(long timeStamp, boolean backupReference, boolean tmp)
	{
		String name="backup-ood-"+timeStamp+(backupReference?".dreference":".dincrement");
		if (tmp)
			name=name+".tmp";
		return new File(backupDirectory, name);
	}
	File getFile(long timeStamp)
	{
		return getFile(timeStamp, isReferenceFile(timeStamp));
	}

	boolean isReference(long timeStamp)
	{
		return isReferenceFile(timeStamp);
	}

	private boolean isReferenceFile(long ts)
	{
		for (int i=fileReferenceTimeStamps.size()-1;i>=0;i--)
		{
			Long v=fileReferenceTimeStamps.get(i);
			if (v ==ts)
				return true;
			if (v <ts)
				return false;
		}
		return false;
	}

	public long getLastFileReferenceTimestampUTC(boolean returnsOnlyValidatedFiles)
	{
		synchronized (this)
		{
			if (fileReferenceTimeStamps.size()==0)
				return Long.MIN_VALUE;
			else {
				long res=fileReferenceTimeStamps.get(fileReferenceTimeStamps.size() - 1);
				if (!returnsOnlyValidatedFiles || isPartFull(res, getFile(res, true)))
					return res;

				if (fileReferenceTimeStamps.size()==1)
					return Long.MIN_VALUE;
				else
					return fileReferenceTimeStamps.get(fileReferenceTimeStamps.size() - 2);
			}
		}
	}

	public long getLastFileTimestampUTC(boolean returnsOnlyValidatedFiles)
	{
		synchronized (this)
		{
			if (fileTimeStamps.size()==0)
				return Long.MIN_VALUE;
			else {
				long res=fileTimeStamps.get(fileTimeStamps.size() - 1);
				if (!returnsOnlyValidatedFiles || isPartFull(res))
					return res;

				if (fileTimeStamps.size()==1)
					return Long.MIN_VALUE;
				else
					return fileTimeStamps.get(fileTimeStamps.size() - 2);
			}
		}
	}



	public long getNearestFileUTCFromGivenTimeNotIncluded(long utc)
	{
		synchronized (this) {
			//ArrayList<File> res = new ArrayList<>(fileTimeStamps.size());
			int s = fileTimeStamps.size();

			if (s > 0) {
				long ts = fileTimeStamps.get(s - 1);
				if (!isPartFull(ts))
					--s;
			}
			for (int i = 0; i < s; i++) {
				long ts = fileTimeStamps.get(i);
				if (ts > utc) {
					return ts;
				}

			}
			return Long.MIN_VALUE;
		}
	}
	public List<File> getFinalFilesFromGivenTime(long utc)
	{
		synchronized (this) {
			ArrayList<File> res = new ArrayList<>(fileTimeStamps.size());
			int s = fileTimeStamps.size();

			if (s > 0) {
				long ts = fileTimeStamps.get(s - 1);
				if (!isPartFull(ts))
					--s;
			}
			for (int i = 0; i < s; i++) {
				long ts = fileTimeStamps.get(i);
				if (ts >= utc)
					res.add(getFile(ts, isReferenceFile(ts)));

			}
			return res;
		}
	}
	public File getFinalFile(long fileTimeStamp, boolean referenceFile)
	{
		checkFile(fileTimeStamp, referenceFile);
		File f=getFile(fileTimeStamp, referenceFile);
		if (fileTimeStamps.get(fileTimeStamps.size()-1)==fileTimeStamp && !isPartFull(fileTimeStamp, f))
			throw new IllegalArgumentException("File is not final");
		return f;
	}
	List<Long> getFinalTimestamps()
	{
		synchronized (this) {
			ArrayList<Long> res = new ArrayList<>(fileTimeStamps.size());
			int s = fileTimeStamps.size();

			if (s > 0) {
				long ts = fileTimeStamps.get(s - 1);

				if (!isPartFull(ts))
					--s;
			}
			for (int i = 0; i < s; i++) {
				long ts = fileTimeStamps.get(i);

				res.add(ts);
			}
			return res;
		}
	}

	public ArrayList<Long> getFileTimeStamps() {
		return fileTimeStamps;
	}

	public boolean hasNonFinalFiles()
	{
		return fileTimeStamps.size()>0 && !isPartFull(fileTimeStamps.get(fileTimeStamps.size()-1));
	}
	public List<File> getFinalFiles()
	{
		synchronized (this) {
			ArrayList<File> res = new ArrayList<>(fileTimeStamps.size());

			for (long ts : getFinalTimestamps()) {
				res.add(getFile(ts, isReferenceFile(ts)));
			}
			return res;
		}
	}

	private boolean isPartFull(long timeStamp)
	{
		return isPartFull(timeStamp, null);

	}
	private boolean isPartFull(long timeStamp, File file)
	{
		if (timeStamp==lastCurrentRestorationFileUsed)
			return true;
		if (timeStamp < System.currentTimeMillis() - backupConfiguration.getMaxBackupFileAgeInMs())
			return true;
		if (file==null)
			file=getFile(timeStamp, isReferenceFile(timeStamp));
		return file.length()>=backupConfiguration.getMaxBackupFileSizeInBytes();
	}

	private File initNewFileForBackupIncrement(long dateUTC) throws DatabaseException {
		return initNewFile(dateUTC, false);
	}
	private File initNewFile(long dateUTC, boolean referenceFile) throws DatabaseException {
		if (fileTimeStamps.size()>0 && dateUTC<fileTimeStamps.get(fileTimeStamps.size()-1))
			throw new DatabaseException("Invalid backup state");
		File file=getFile(dateUTC, referenceFile);
		try {
			if (!file.createNewFile())
				throw new DatabaseException("Impossible to create file : "+file);
			fileTimeStamps.add(dateUTC);
			if (referenceFile)
				fileReferenceTimeStamps.add(dateUTC);
			cleanCache();
			return file;
		} catch (IOException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

	}
	private File initNewFileForBackupReference(long dateUTC) throws DatabaseException {
		return initNewFile(dateUTC, true);
	}

	private BufferedRandomOutputStream getFileForBackupIncrementOrCreateIt(Reference<Long> fileTimeStamp, Reference<Long> firstTransactionID/*, AtomicReference<RecordsIndex> recordsIndex*/) throws DatabaseException {
		File res=null;

		if (fileTimeStamps.size()>0)
		{
			Long timeStamp=fileTimeStamps.get(fileTimeStamps.size()-1);
			boolean reference=isReferenceFile(timeStamp);
			File file=getFile(timeStamp, reference);
			if (!isPartFull(timeStamp, file)) {
				res = file;
				fileTimeStamp.set(timeStamp);
			}
		}
		try {
			final int maxBufferSize=backupConfiguration.getMaxStreamBufferSizeForTransaction();
			final int maxBuffersNumber=backupConfiguration.getMaxStreamBufferNumberForTransaction();
			if (res==null) {
				fileTimeStamp.set(System.currentTimeMillis());
				res = initNewFileForBackupIncrement(fileTimeStamp.get());

				BufferedRandomOutputStream out=new BufferedRandomOutputStream(new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber);
				firstTransactionID.set(null);
				saveHeader(out, fileTimeStamp.get(), false/*, null, recordsIndex*/);
				return out;
			}
			else {

				BufferedRandomOutputStream out=new BufferedRandomOutputStream(new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber);
				RandomInputStream ris=out.getUnbufferedRandomInputStream();
				ris.seek(LAST_BACKUP_UTC_POSITION+8);
				if (ris.readBoolean())
					firstTransactionID.set(ris.readLong());
				else
					firstTransactionID.set(null);
				//recordsIndex.set(new RecordsIndex(out.getUnbufferedRandomInputStream()));
				positionFileForNewEvent(out);
				return out;
			}
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}


	}

	private File getFileForBackupReference()  {
		if (fileReferenceTimeStamps.size()>0)
		{
			Long timeStamp=fileReferenceTimeStamps.get(fileReferenceTimeStamps.size()-1);
			return getFile(timeStamp, true);
		}

		return null;

	}

	/**
	 * Tells if the manager is ready for backup new database events
	 * @return true if the manager is ready for backup new database events
	 */
	public boolean isReady()
	{
		synchronized (this) {
			return fileTimeStamps.size() > 0 && !computeDatabaseReference.exists() && !temporaryDisabled;
		}
	}

	private void backupRecordEvent(RandomOutputStream out, Table<?> table, DatabaseRecord oldRecord, DatabaseRecord newRecord, DatabaseEventType eventType/*, RecordsIndex index*/) throws DatabaseException {
		try {
			int start=(int)out.currentPosition();
			out.writeByte(eventType.getByte());

			int tableIndex=classes.indexOf(table.getClass());
			if (tableIndex<0)
				throw new IOException();
			out.writeUnsignedInt16Bits(tableIndex);
			if (eventType!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE) {
				byte[] pks = table.serializeFieldsWithUnknownType(newRecord == null ? oldRecord : newRecord, true, false, false);
				out.writeBytesArray(pks, false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);


				switch (eventType) {

					case ADD:
					case UPDATE: {
						if (!table.isPrimaryKeysAndForeignKeysSame() && table.getForeignKeysFieldAccessors().size() > 0) {
							out.writeBoolean(true);
							byte[] fks = table.serializeFieldsWithUnknownType(newRecord, false, true, false);
							out.writeBytesArray(fks, false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
						} else
							out.writeBoolean(false);

						byte[] nonkeys = table.serializeFieldsWithUnknownType(newRecord, false, false, true);
						if (nonkeys == null || nonkeys.length == 0) {
							out.writeBoolean(false);
						} else {
							out.writeBoolean(true);
							out.writeBytesArray(nonkeys, false, Table.MAX_NON_KEYS_SIZE_IN_BYTES);
						}

						break;
					}
					case REMOVE:
					case REMOVE_WITH_CASCADE: {
						break;
					}
					default:
						throw new IllegalAccessError();
				}
			}
			out.writeInt(start);
			/*index.writeRecord(out, start, pks, 0, pks.length);*/


		} catch (DatabaseException | IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private void saveTablesHeader(RandomOutputStream out) throws DatabaseException {
		try {
			if (classes.size()>Short.MAX_VALUE)
				throw new DatabaseException("Too much tables");
			int dataPosition=(int)(10+classes.size()*2+out.currentPosition());
			List<byte[]> l=new ArrayList<>(classes.size());
			for (Class<? extends Table<?>> aClass : classes) {
				byte[] tab=aClass.getName().getBytes(StandardCharsets.UTF_8);
				l.add(tab);
				dataPosition+=tab.length;
			}
			out.writeInt(dataPosition);
			out.writeShort(classes.size());
			for (byte[] t : l) {
				out.writeShort(t.length);
				out.write(t);
			}
			out.writeInt(-1);
			if (out.currentPosition()!=dataPosition)
				throw new DatabaseException("Unexpected exception");
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	private void saveHeader(RandomOutputStream out, long backupUTC, boolean referenceFile/*, Long firstTransactionID, AtomicReference<RecordsIndex> recordsIndex*/) throws DatabaseException {

		try {
			out.writeLong(backupUTC);
			//first transaction id
			//if (firstTransactionID==null) {
			out.writeBoolean(false);
			out.writeLong(0);
			//last transaction id
			out.writeLong(0);
			/*}
			else
			{
				out.writeBoolean(true);
				out.writeLong(firstTransactionID);
				//last transaction id
				out.writeLong(firstTransactionID);

			}*/
			//recordsIndex.set(new RecordsIndex(backupConfiguration.getMaxIndexSize(), out));

			if (referenceFile) {
				out.writeBoolean(true);
				saveTablesHeader(out);
			}
			else {
				out.writeBoolean(false);
				out.writeInt(-1);
			}
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}


	private void positionFileForNewEvent(RandomOutputStream out) throws DatabaseException {
		try {
			out.seek(out.length());
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	private File currentFileReference=null;
	private long currentFileReferenceUTC=Long.MIN_VALUE;
	private List<Class<? extends Table<?>>> currentClassesList=new ArrayList<>();
	//private DatabaseWrapper.TransactionsInterval transactionsInterval=null;

	private long lastBackupEventUTC=Long.MIN_VALUE;

	private void cleanCache()  {
		maxDateUTC=null;
		lastBackupEventUTC=Long.MIN_VALUE;

	}
	static List<Class<? extends Table<?>>> extractClassesList(RandomInputStream rfis) throws DatabaseException {
		return extractClassesList(rfis, null);
	}
	private static List<Class<? extends Table<?>>> extractClassesList(RandomInputStream rfis, BackupRestoreManager backupRestoreManager) throws DatabaseException {
		try {
			if (backupRestoreManager != null) {
				if (rfis.currentPosition() != 0)
					rfis.seek(0);
				backupRestoreManager.lastBackupEventUTC = rfis.readLong();
			}
			else
				rfis.seek(8);
			if (!rfis.readBoolean()) {
				rfis.seek(16);
			}

			rfis.seek(LIST_CLASSES_POSITION/*RecordsIndex.getListClassPosition(rfis)*/ + 4);

			int s = rfis.readShort();
			if (s < 0)
				throw new IOException();

			ArrayList<Class<? extends Table<?>>> currentClassesList = new ArrayList<>(s);
			byte[] tab = new byte[Short.MAX_VALUE];
			for (int i = 0; i < s; i++) {
				int l = rfis.readShort();
				rfis.readFully(tab, 0, l);
				String className = new String(tab, 0, l, StandardCharsets.UTF_8);
				@SuppressWarnings("unchecked")
				Class<? extends Table<?>> c = (Class<? extends Table<?>>) Class.forName(className);
				currentClassesList.add(c);
			}

			return currentClassesList;
		}
		catch (IOException | ClassCastException | ClassNotFoundException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}
	private List<Class<? extends Table<?>>> extractClassesList(File file, long fileTimeStamp, boolean lastFileReference) throws DatabaseException {

		try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
			List<Class<? extends Table<?>>> currentClassesList=extractClassesList(rfis, lastFileReference?this:null);
			if (lastFileReference) {
				currentFileReferenceUTC = fileTimeStamp;
				currentFileReference=file;
				this.currentClassesList=currentClassesList;
			}
			return currentClassesList;
		} catch (IOException | ClassCastException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

	}

	private List<Class<? extends Table<?>>> extractClassesList(File file) throws DatabaseException {
		assert file!=null;
		assert fileReferenceTimeStamps.size()>0;
		long last = fileReferenceTimeStamps.get(fileReferenceTimeStamps.size() - 1);
		File lastFile=getFile(last, true);
		if (fileReferenceTimeStamps.size()>1 && file.length()<LIST_CLASSES_POSITION+6)
		{
			last = fileReferenceTimeStamps.get(fileReferenceTimeStamps.size() - 2);
			lastFile=getFile(last, true);
		}
		boolean lastFileUpdating=file.equals(lastFile);
		if (last != currentFileReferenceUTC || !lastFileUpdating)
		{
			return extractClassesList(file, last, lastFileUpdating);
		}

		return currentClassesList;
	}

	private long extractLastBackupEventUTC(File file) throws DatabaseException {
		boolean lastFileReference=file.equals(currentFileReference);
		if (lastFileReference && lastBackupEventUTC!=Long.MIN_VALUE)
			return lastBackupEventUTC;
		else
		{
			try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
				long res=rfis.readLong();
				if (lastFileReference)
					lastBackupEventUTC=res;
				return res;
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}


	private boolean checkTablesHeader(File file) throws DatabaseException {

		boolean ok=true;
		if (file!=null && file.exists())
		{
			List<Class<? extends Table<?>>> currentClassesList=extractClassesList(file);
			ok= currentClassesList.size() == classes.size();
			for (int i=0;ok && i<currentClassesList.size();i++)
			{
				if (!currentClassesList.get(i).equals(classes.get(i))) {
					ok = false;
					break;
				}

			}
		}
		if (!ok)
		{
			activateBackupReferenceCreation(true);
		}
		return ok;
	}

	private int saveTransactionHeader(RandomOutputStream out, long backupTime) throws DatabaseException {
		try {
			int nextTransactionReference = (int) out.currentPosition();
			if (nextTransactionReference<=0)
				throw new InternalError();
			//next transaction
			out.writeInt(-1);
			//transactionID
			out.writeBoolean(false);
			out.writeLong(0);
			//transaction time stamp
			out.writeLong(backupTime);
			out.writeInt(-1);
			return nextTransactionReference;
		}
		catch(IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
	}




	private void saveTransactionQueue(RandomOutputStream out, int nextTransactionReference, long transactionUTC, Long firstTransactionID, Long lastTransactionID/*, RecordsIndex index*/) throws DatabaseException {
		try {
			out.writeByte(-1);
			//backup previous transaction reference
			out.writeInt(nextTransactionReference);
			int nextTransaction=(int)out.currentPosition();


			out.seek(LAST_BACKUP_UTC_POSITION);
			//last transaction utc
			out.writeLong(transactionUTC);
			if (lastTransactionID!=null) {
				if (firstTransactionID == null) {
					out.writeBoolean(true);
					out.writeLong(lastTransactionID);
				} else
					out.seek(LAST_BACKUP_UTC_POSITION + 17);
				out.writeLong(lastTransactionID);
			}
			//next transaction of previous transaction
			out.seek(nextTransactionReference);
			out.writeInt(nextTransaction);
			if (lastTransactionID!=null) {
				out.writeBoolean(true);
				out.writeLong(lastTransactionID);
			}
			else {
				out.writeBoolean(false);
			}

			//index.flush(out);
		}
		catch(IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

	}


	/**
	 * Create a backup reference
	 * @return the time UTC of the backup reference
	 * @throws DatabaseException if a problem occurs
	 */
	@SuppressWarnings("UnusedReturnValue")
	public long createBackupReference() throws DatabaseException
	{
		final AtomicLong globalNumberOfSavedRecords=new AtomicLong(0);
		boolean notify=false;
		try {
			databaseWrapper.lockRead();
			Thread.sleep(1);
			synchronized (this) {
				int oldLength = 0;
				long oldLastFile;
				if (fileTimeStamps.size() == 0)
					oldLastFile = Long.MAX_VALUE;
				else {
					oldLastFile = fileTimeStamps.get(fileTimeStamps.size() - 1);
					File file = getFile(oldLastFile, isReferenceFile(oldLastFile));
					oldLength = (int) file.length();
				}
				long curTime = System.currentTimeMillis();
				while (curTime == oldLastFile) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					curTime = System.currentTimeMillis();
				}
				final long backupTime = curTime;
				//currentBackupReferenceUTC=curTime;
				final AtomicLong currentBackupTime = new AtomicLong(backupTime);

				try {
					try {
						if (!computeDatabaseReference.exists()) {
							if (!computeDatabaseReference.createNewFile())
								throw new DatabaseException("Impossible to create file " + computeDatabaseReference);
						} else if (computeDatabaseReference.length() >= 16) {
							try (FileInputStream fis = new FileInputStream(currentFileReference); DataInputStream dis = new DataInputStream(fis)) {
								long s = dis.readLong();
								if (s >= 0) {
									long fileRef = dis.readLong();
									for (Long l : fileTimeStamps) {
										if (l == fileRef) {
											boolean reference = isReferenceFile(l);
											try (RandomFileOutputStream rfos = new RandomFileOutputStream(getFile(l, reference), RandomFileOutputStream.AccessMode.READ_AND_WRITE)) {
												rfos.setLength(fileRef);
											}
										} else if (fileRef < l) {
											boolean reference = isReferenceFile(l);
											//noinspection ResultOfMethodCallIgnored
											getFile(l, reference).delete();
											fileReferenceTimeStamps.remove(l);
										}
									}
								}
							}
							scanFiles();
						}
						final int maxBufferSize = backupConfiguration.getMaxStreamBufferSizeForBackupRestoration();
						final int maxBuffersNumber = backupConfiguration.getMaxStreamBufferNumberForBackupRestoration();
						//DecentralizedValue localHostID=databaseWrapper.getSynchronizer().getLocalHostID();


						File file = initNewFileForBackupReference(currentBackupTime.get());
						final AtomicReference<RandomOutputStream> rout = new AtomicReference<>(new BufferedRandomOutputStream(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber));
						final ProgressMonitorDM progressMonitor = backupConfiguration.getProgressMonitorForBackup();
						long t = 0;
						if (progressMonitor != null) {
							for (Class<? extends Table<?>> c : classes) {
								final Table<?> table = databaseWrapper.getTableInstance(c);
								t += table.getRecordsNumber();
							}
							progressMonitor.setMinimum(0);
							progressMonitor.setMaximum(1000);
						}
						final long totalRecords = t;
						try {
							//final AtomicReference<RecordsIndex> index=new AtomicReference<>(null);
							saveHeader(rout.get(), currentBackupTime.get(), true/*, null, index*/);

							final AtomicInteger nextTransactionReference = new AtomicInteger(saveTransactionHeader(rout.get(), currentBackupTime.get()));



							for (Class<? extends Table<?>> c : classes) {
								final Table<?> table = databaseWrapper.getTableInstance(c);


								globalNumberOfSavedRecords.set(databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
									long originalPosition = rout.get().currentPosition();
									long numberOfSavedRecords=globalNumberOfSavedRecords.get();
									long startPosition=0;
									RandomOutputStream out = rout.get();

									@Override
									public Long run() throws Exception {

										table.getPaginatedRecordsWithUnknownType(startPosition==0?-1:startPosition, startPosition==0?-1:Long.MAX_VALUE, new Filter<DatabaseRecord>() {



											@Override
											public boolean nextRecord(DatabaseRecord _record) throws DatabaseException {
												backupRecordEvent(out, table, null, _record, DatabaseEventType.ADD/*, index.get()*/);
												try {
													originalPosition = out.currentPosition();
													++startPosition;
													if (progressMonitor != null) {

														++numberOfSavedRecords;
														progressMonitor.setProgress((int) (((numberOfSavedRecords+1) * 1000) / totalRecords));
													}

													if (out.currentPosition() >= backupConfiguration.getMaxBackupFileSizeInBytes()) {
														saveTransactionQueue(rout.get(), nextTransactionReference.get(), currentBackupTime.get(), null, null/*, index.get()*/);
														out.close();

														//index.set(null);

														long curTime = System.currentTimeMillis();
														while (curTime == currentBackupTime.get()) {
															try {
																//noinspection BusyWait
																Thread.sleep(1);
															} catch (InterruptedException e) {
																e.printStackTrace();
															}
															curTime = System.currentTimeMillis();
														}
														currentBackupTime.set(curTime);
														File file = initNewFileForBackupIncrement(curTime);
														rout.set(out = new BufferedRandomOutputStream(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber));
														saveHeader(out, curTime, false/*, null, index*/);
														nextTransactionReference.set(saveTransactionHeader(out, currentBackupTime.get()));
														originalPosition = out.currentPosition();

													}
												} catch (IOException e) {
													throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
												}

												return false;
											}
										});
										return numberOfSavedRecords;
									}

									@Override
									public TransactionIsolation getTransactionIsolation() {
										return TransactionIsolation.TRANSACTION_SERIALIZABLE;
									}

									@Override
									public boolean doesWriteData() {
										return false;
									}

									@Override
									public void initOrReset() throws Exception {
										rout.get().setLength(originalPosition);
									}
								}));

							}
							saveTransactionQueue(rout.get(), nextTransactionReference.get(), currentBackupTime.get(), null, null/*, index.get()*/);

						} finally {
							if (progressMonitor != null) {
								progressMonitor.setProgress(1000);
							}
							rout.get().close();
						}
						cleanOldBackups();
						if (!computeDatabaseReference.delete())
							throw new DatabaseException("Impossible to delete file " + computeDatabaseReference);

					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}


				} catch (DatabaseException e) {
					deleteDatabaseFilesFromReferenceToLastFile(oldLastFile, oldLength);
					scanFiles();
					throw e;
				}

				notify=true;
				return backupTime;
			}
		} catch (InterruptedException e) {
			throw DatabaseException.getDatabaseException(e);
		} finally {
			cleanCache();
			databaseWrapper.unlockRead();
			if (notify) {
				databaseWrapper.getSynchronizer().checkForNewBackupFilePartToSendToCentralDatabaseBackup(dbPackage);
				DatabaseLifeCycles lifeCycles=databaseWrapper.getDatabaseConfigurationsBuilder().getLifeCycles();
				if (lifeCycles!=null)
					lifeCycles.newDatabaseBackupFileCreated(this);
			}
		}
	}

	/**
	 * Clean old backups
	 * @throws DatabaseException if a problem occurs
	 */
	public void cleanOldBackups() throws DatabaseException {
		synchronized (this) {
			if (lastCurrentRestorationFileUsed!=Long.MIN_VALUE)
				return;
			long limitUTC=System.currentTimeMillis()-backupConfiguration.getMaxBackupDurationInMs();
			long concretLimitUTC=Long.MIN_VALUE;
			for (int i=fileReferenceTimeStamps.size()-2;i>=0;i--)
			{
				Long l=fileReferenceTimeStamps.get(i);
				if (l<limitUTC) {
					int istart=fileTimeStamps.indexOf(l);
					int iend=fileTimeStamps.indexOf(fileReferenceTimeStamps.get(i+1))-1;
					if (istart<0)
						throw new IllegalAccessError();
					if (iend<0)
						throw new IllegalAccessError();
					if (iend<istart)
						throw new IllegalAccessError("istart="+istart+", iend="+istart);
					long lastTimeUTC=fileTimeStamps.get(iend);
					long limit=extractLastBackupEventUTC(getFile(lastTimeUTC, istart==iend));
					if ( limit>= limitUTC) {
						continue;
					}

					concretLimitUTC = lastTimeUTC;
					break;
				}
			}
			if (concretLimitUTC!=Long.MIN_VALUE) {
				deleteDatabaseFilesFromReferenceToFirstFile(concretLimitUTC);
			}
		}
	}


	@SuppressWarnings("SameParameterValue")
	private void activateBackupReferenceCreation(boolean backupNow) throws DatabaseException {
		synchronized (this) {
			try {
				if (!computeDatabaseReference.createNewFile())
					throw new DatabaseException("Impossible to create file " + computeDatabaseReference);
				if (backupNow)
					createIfNecessaryNewBackupReference();
			} catch (IOException e) {
				throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
			}
		}
	}

	boolean doesCreateNewBackupReference()
	{
		return !passive && !temporaryDisabled && (!isReady() || fileReferenceTimeStamps.size()==0 || doesCreateNewBackupReference(fileReferenceTimeStamps.get(fileReferenceTimeStamps.size()-1)));
	}

	private boolean doesCreateNewBackupReference(long lastBackupReferenceTimeUTC)
	{
		DatabaseLifeCycles lifeCycles=databaseWrapper.getDatabaseConfigurationsBuilder().getLifeCycles();
		if (lifeCycles==null)
			return backupConfiguration.mustCreateNewBackupReference(lastBackupReferenceTimeUTC);
		else
			return lifeCycles.mustCreateNewBackupReference(backupConfiguration, lastBackupReferenceTimeUTC);
	}

	private void deleteDatabaseFilesFromReferenceToLastFile(long firstFileReference, int oldLength) throws DatabaseException {
		if (firstFileReference==Long.MAX_VALUE)
			return;
		try {
			for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
				Long l = it.next();
				if (l > firstFileReference || (l == firstFileReference && oldLength <= 0)) {
					boolean reference = isReferenceFile(l);
					File f = getFile(l, reference);

					if (!f.delete())
						throw new IllegalStateException();
					if (reference)
						fileReferenceTimeStamps.remove(l);
					it.remove();
				}
			}
			if (oldLength > 0) {
				if (fileTimeStamps.size() == 0)
					throw new DatabaseException("Reference not found");
				long l = fileTimeStamps.get(fileTimeStamps.size() - 1);
				if (l != firstFileReference)
					throw new DatabaseException("Reference not found");
				boolean isReference = isReferenceFile(l);
				File file = getFile(l, isReference);
				try (RandomFileOutputStream out = new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE)) {
					out.setLength(oldLength);
				} catch (IOException e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}
		}
		finally {
			cleanCache();
		}

		//scanFiles();
	}
	private void deleteDatabaseFilesFromReferenceToFirstFile(long fileReference)
	{

		for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				File f = getFile(l, isReferenceFile(l));

				if (!f.delete()) {
					System.err.println("Impossible to delete file : " + f);
					if (f.exists())
						System.err.println("The file already exists");
					else
						System.err.println("The file does not exists");
				}
				it.remove();
			}
			else
				break;
		}
		for (Iterator<Long> it = fileReferenceTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				it.remove();
			}
			else
				break;
		}

		cleanCache();
		//scanFiles();
	}

	/**
	 * Gets the older backup event UTC time
	 * @return the older backup event UTC time
	 */
	public long getFirstTransactionUTCInMs()
	{
		synchronized (this) {
			return fileTimeStamps.size()>0?fileTimeStamps.get(0):Long.MAX_VALUE;
		}
	}
	public long getFirstFileReferenceUTCInMs()
	{
		synchronized (this) {
			return fileReferenceTimeStamps.size()>0?fileReferenceTimeStamps.get(0):Long.MAX_VALUE;
		}
	}
	public long getFirstFileUTCInMs()
	{
		synchronized (this) {
			return fileTimeStamps.size()>0?fileTimeStamps.get(0):Long.MAX_VALUE;
		}
	}

	boolean hasBackupReference()
	{
		return fileReferenceTimeStamps.size()>0;
	}

	public Long getFirstValidatedTransactionUTCInMs() throws DatabaseException {
		synchronized (this) {
			if (fileTimeStamps.size()>1)
				return fileTimeStamps.get(0);
			else if (fileTimeStamps.size()>0)
			{
				long ts=fileTimeStamps.get(0);
				File file=getFile(ts);
				if (isPartFull(ts, file))
				{
					try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
						if (rfis.readLong()!=ts)
							return ts;
						if (rfis.readBoolean())
							return ts;
						return Long.MIN_VALUE;
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			}

			return Long.MIN_VALUE;

		}
	}


	/**
	 * Gets the younger backup event UTC time
	 * @return the younger backup event UTC time
	 * @throws DatabaseException if a problem occurs
	 */
	public long getLastTransactionUTCInMS() throws DatabaseException {
		synchronized (this) {
			Long m=maxDateUTC;
			if (m==null) {

				if (fileTimeStamps.size() > 0 && fileReferenceTimeStamps.size() > 0) {
					long ts = fileTimeStamps.get(fileTimeStamps.size() - 1);
					m=maxDateUTC=extractLastBackupEventUTC(getFile(ts, fileReferenceTimeStamps.get(fileReferenceTimeStamps.size() - 1).equals(ts)));
				}
				else
					m=maxDateUTC=Long.MIN_VALUE;
			}
			return m;
		}
	}
	private final byte[] recordBuffer=new byte[1<<24-1];
	/**
	 * Restore the database to the nearest given date UTC
	 * @param dateUTCInMs the UTC time in milliseconds
	 *
	 * @return true if the given time corresponds to an available backup. False is chosen if the given time is too old to find a corresponding historical into the backups. In this previous case, it is the nearest backup that is chosen.
	 * @throws DatabaseException if a problem occurs
	 */
	@SuppressWarnings("UnusedReturnValue")
	public boolean restoreDatabaseToDateUTC(long dateUTCInMs) throws DatabaseException {
		return restoreDatabaseToDateUTC(dateUTCInMs, true);
	}

	private void checkFile(long timeStamp, boolean backupReference)
	{
		if (backupReference) {
			if (!fileReferenceTimeStamps.contains(timeStamp))
				throw new IllegalArgumentException();
		} else {
			if (!fileTimeStamps.contains(timeStamp))
				throw new IllegalArgumentException();
			else if (fileReferenceTimeStamps.contains(timeStamp))
				throw new IllegalArgumentException();
		}
	}

	EncryptedBackupPartDestinedToCentralDatabaseBackup getEncryptedFilePartWithMetaData(DecentralizedValue fromHostIdentifier, long timeStamp, boolean backupReference, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		RandomCacheFileOutputStream out=getEncryptedFilePart(timeStamp, backupReference, random, encryptionProfileProvider);
		try {
			DatabaseBackupMetaDataPerFile metaData=getDatabaseBackupMetaDataPerFile(timeStamp, backupReference);
			EncryptedDatabaseBackupMetaDataPerFile encryptedMetaData=new EncryptedDatabaseBackupMetaDataPerFile(dbPackage.getName(), metaData, random, encryptionProfileProvider);

			Long lid=metaData.getLastTransactionID();
			if (lid==null) {
				lid=databaseWrapper.getTransactionIDTable().getLastTransactionID();
			}

			return new EncryptedBackupPartDestinedToCentralDatabaseBackup(fromHostIdentifier, encryptedMetaData, out.getRandomInputStream(), EncryptionTools.encryptID(lid, random, encryptionProfileProvider));
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}



	DatabaseBackupMetaDataPerFile getDatabaseBackupMetaDataPerFile(long timeStamp, boolean backupReference) throws DatabaseException {
		File file=getFinalFile(timeStamp, backupReference);
		List<TransactionMetaData> transactionsMetaData=new ArrayList<>();

		try (RandomFileInputStream in = new RandomFileInputStream(file)) {
			positionForDataRead(in, backupReference);

			while (in.available()>0) {
				int startTransaction = (int) in.currentPosition();
				int nextTransaction = in.readInt();
				if (nextTransaction < 0)
					break;
				if (nextTransaction<=in.currentPosition())
					throw new DatabaseException("curPos="+in.currentPosition()+", nextTransaction="+in.currentPosition());
				if (in.readBoolean()) {
					long transactionID=in.readLong();
					long currentTransactionUTC = in.readLong();
					transactionsMetaData.add(new TransactionMetaData(currentTransactionUTC, transactionID, startTransaction));
				}

				in.seek(nextTransaction);
			}
			return new DatabaseBackupMetaDataPerFile(timeStamp, backupReference, transactionsMetaData);
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}
	long getLastTransactionID() throws DatabaseException {
		return getLastTransactionIDBeforeGivenTimeStampIndex(fileTimeStamps.size());
	}
	private long getLastTransactionIDBeforeGivenTimeStamp(long timeStamp) throws DatabaseException {
		return getLastTransactionIDBeforeGivenTimeStampIndex(fileTimeStamps.indexOf(timeStamp));
	}
	private long getLastTransactionIDBeforeGivenTimeStampIndex(int i) throws DatabaseException {
		assert i>=0;
		while (--i>=0) {
			File f=getFile(fileTimeStamps.get(i));
			try(RandomFileInputStream fis=new RandomFileInputStream(f))
			{
				fis.seek(LAST_BACKUP_UTC_POSITION+8);
				if (fis.readBoolean()) {
					fis.skipNBytes(8);
					return fis.readLong();
				}
			}
			catch (IOException e)
			{
				throw DatabaseException.getDatabaseException(e);
			}
		}
		return Long.MIN_VALUE;
	}
	private long getTransactionID(File f) {
		if (f.length()<LAST_BACKUP_UTC_POSITION+17)
			return Long.MIN_VALUE;
		try(RandomFileInputStream fis=new RandomFileInputStream(f))
		{
			fis.seek(LAST_BACKUP_UTC_POSITION+8);
			if (fis.readBoolean()) {
				fis.skipNBytes(8);
				return fis.readLong();
			}
			else
				return Long.MIN_VALUE;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return Long.MIN_VALUE;
		}
	}

	boolean importEncryptedBackupPartComingFromCentralDatabaseBackup(AbstractEncryptedBackupPartComingFromCentralDatabaseBackup backupPart, EncryptionProfileProvider encryptionProfileProvider, @SuppressWarnings("SameParameterValue") boolean replaceExistingFilePart) throws DatabaseException {
		try {
			Integrity i = backupPart.getMetaData().checkSignature(encryptionProfileProvider);
			if (i != Integrity.OK) {
				throw new MessageExternalizationException(i);
			}
			File f=getFile(backupPart.getMetaData().getFileTimestampUTC(), backupPart.getMetaData().isReferenceFile(), false);
			boolean exists=f.exists();
			if (!exists || replaceExistingFilePart){

				try {
					File fileDest=f;
					f = getFile(backupPart.getMetaData().getFileTimestampUTC(), backupPart.getMetaData().isReferenceFile(), true);
					try(RandomFileOutputStream out=new RandomFileOutputStream(f)) {
						EncryptionTools.decode(encryptionProfileProvider, backupPart.getPartInputStream(), out);
					}
					finally {
						backupPart.getPartInputStream().close();
					}

					if (exists) {
						if (!fileDest.delete())
							throw new DatabaseException("Impossible to remove file "+fileDest);
					}
					if (!f.renameTo(fileDest)) {
						//noinspection ResultOfMethodCallIgnored
						f.delete();
						throw new DatabaseException("Impossible to remame file " + f);
					}
					assert fileDest.exists();

					scanFiles();

					return true;
				}
				catch(IOException e)
				{
					if (f.exists())
						//noinspection ResultOfMethodCallIgnored
						f.delete();
					throw e;
				}

			}
			else
				backupPart.getPartInputStream().close();
			return false;
		}
		catch (IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

	}

	public RandomCacheFileOutputStream getEncryptedFilePart(long timeStamp, boolean backupReference, AbstractSecureRandom random, EncryptionProfileProvider profileProvider) throws DatabaseException {
		try {
			RandomCacheFileOutputStream randomCacheFileOutputStream = RandomCacheFileCenter.getSingleton().getNewBufferedRandomCacheFileOutputStream(true, RandomFileOutputStream.AccessMode.READ_AND_WRITE, BufferedRandomInputStream.DEFAULT_MAX_BUFFER_SIZE, 1);
			EncryptionTools.encode(random, profileProvider, new RandomFileInputStream(getFinalFile(timeStamp, backupReference)), randomCacheFileOutputStream);
			return randomCacheFileOutputStream;
		}
		catch (IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
	}


	/**
	 * Restore the database to the last known backup
	 * @return true if a backup is available. False no backup is available.
	 * @throws DatabaseException if a problem occurs
	 */
	public boolean restoreDatabaseToLastKnownBackup() throws DatabaseException {
		return restoreDatabaseToDateUTC(Long.MAX_VALUE, false);
	}

	void restoreDatabaseToLastKnownBackupFromEmptyDatabase(SynchronizationPlanMessageComingFromCentralDatabaseBackup synchronizationPlanMessageComingFromCentralDatabaseBackup, Long timeOfRestoration) throws DatabaseException {
		restoreDatabaseToDateUTC(timeOfRestoration==null?Long.MAX_VALUE:timeOfRestoration, false, timeOfRestoration!=null, synchronizationPlanMessageComingFromCentralDatabaseBackup);
	}

	/**
	 * Restore the database to the nearest given date UTC
	 * @param dateUTCInMs the UTC time in milliseconds
	 * @param chooseNearestBackupIfNoBackupMatch if set to true, and when no backup was found at the given date/time, than choose the older backup
	 * @return true if the given time corresponds to an available backup. False is chosen if the given time is too old to find a corresponding historical into the backups. In this previous case, and if the param <code>chooseNearestBackupIfNoBackupMatch</code>is set to true, than it is the nearest backup that is chosen. Else no restoration is done.
	 * @throws DatabaseException if a problem occurs
	 */
	public boolean restoreDatabaseToDateUTC(long dateUTCInMs, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
		return restoreDatabaseToDateUTC(dateUTCInMs, chooseNearestBackupIfNoBackupMatch, true);
	}
	boolean restoreDatabaseToDateUTC(long dateUTCInMs, boolean chooseNearestBackupIfNoBackupMatch, boolean notifyOtherPeers) throws DatabaseException {
		return restoreDatabaseToDateUTC(dateUTCInMs, chooseNearestBackupIfNoBackupMatch, notifyOtherPeers, null);
	}
	boolean restoreDatabaseToDateUTC(long dateUTCInMs, boolean chooseNearestBackupIfNoBackupMatch, boolean notifyOtherPeers, SynchronizationPlanMessageComingFromCentralDatabaseBackup synchronizationPlanMessageComingFromCentralDatabaseBackup) throws DatabaseException {
		boolean notify=false;
		try
		{
			databaseWrapper.lockWrite();
			final Long lastTransactionID=synchronizationPlanMessageComingFromCentralDatabaseBackup==null?null:getLastTransactionID();
			if (lastTransactionID!=null)
			{
				databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Object>() {
					@Override
					public Object run() throws Exception {
						final long lastLocalTID=databaseWrapper.getTransactionIDTable().getLastTransactionID()-1;
						DatabaseHooksTable dht=databaseWrapper.getDatabaseHooksTable();




						//Map<DecentralizedValue, LastValidatedLocalAndDistantID> lastValidatedIDsPerHost=synchronizationPlanMessageComingFromCentralDatabaseBackup.getLastValidatedIDsPerHost(databaseWrapper.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup());
						Map<DecentralizedValue, Long> lastValidatedIDsPerHost=synchronizationPlanMessageComingFromCentralDatabaseBackup.getLastValidatedIDsPerHost(databaseWrapper.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup());
						dht.updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
							@Override
							public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
								if ((_record.getLastValidatedLocalTransactionID()==lastLocalTID || _record.getLastValidatedLocalTransactionID()==-1) && lastLocalTID<lastTransactionID)
								{
									update("lastValidatedLocalTransactionID", lastTransactionID);
								}
								if (_record.getHostID().equals(synchronizationPlanMessageComingFromCentralDatabaseBackup.getSourceChannel()))
								{
									if (_record.getLastValidatedDistantTransactionID() == -1) {
										update("lastValidatedDistantTransactionID", lastTransactionID);
									}
								}
								else {
									Long lastValidatedDistantId = lastValidatedIDsPerHost.get(_record.getHostID());
									if (lastValidatedDistantId!=null && _record.getLastValidatedDistantTransactionID() == -1) {
										update("lastValidatedDistantTransactionID", lastValidatedDistantId);
									}
								}
							}
						}, "concernsDatabaseHost=%c", "c", false);
						return null;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_READ_COMMITTED;
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
			int oldVersion;
			int newVersion;
			File currentFile;
			ProgressMonitorDM pg;
			long s;
			ArrayList<Table<?>> tbls/*, tbls2*/;
			boolean reference=true;
			LinkedList<Long> listIncrements;
			boolean newVersionLoaded=false;
			boolean res = true;

			long lastTransactionUTCInMs;
			final long restorationTimeUTCInMS=System.currentTimeMillis();
			synchronized (this) {
				if (temporaryDisabled)
					return false;
				if (fileReferenceTimeStamps.size() == 0)
					return false;
				lastTransactionUTCInMs=getLastTransactionUTCInMS();
				databaseWrapper.checkMinimumValidatedTransactionIds();
				if (synchronizationPlanMessageComingFromCentralDatabaseBackup!=null && !notifyOtherPeers)
					temporaryDisabled=true;
				if (lastTransactionUTCInMs>Long.MIN_VALUE && lastTransactionUTCInMs<=dateUTCInMs) {
					temporaryDisabled = true;
					dateUTCInMs=lastTransactionUTCInMs;
				}
				oldVersion = databaseWrapper.getCurrentDatabaseVersion(dbPackage);

				newVersion = oldVersion + 1;
				while (databaseWrapper.doesVersionExists(dbPackage, newVersion)) {
					++newVersion;
					if (newVersion < 0)
						newVersion = 0;
					if (newVersion == oldVersion)
						throw new DatabaseException("No more database version available");
				}
				long startFileReference = Long.MIN_VALUE;
				for (int i = fileReferenceTimeStamps.size() - 1; i >= 0; i--) {
					if (fileReferenceTimeStamps.get(i) <= dateUTCInMs) {
						startFileReference = fileReferenceTimeStamps.get(i);
						break;
					}
				}

				if (startFileReference == Long.MIN_VALUE) {
					res = false;
					if (chooseNearestBackupIfNoBackupMatch) {
						startFileReference = fileReferenceTimeStamps.get(0);
						dateUTCInMs = startFileReference;
					} else
						return false;
				}
				lastCurrentRestorationFileUsed = startFileReference;

				try {
					currentFile = getFile(startFileReference, true);
					listIncrements = new LinkedList<>();
					//noinspection ForLoopReplaceableByForEach
					for (int i = 0; i < fileTimeStamps.size(); i++) {
						Long l = fileTimeStamps.get(i);
						if (l > startFileReference) {
							if (isReferenceFile(l))
								break;
							lastCurrentRestorationFileUsed = l;

							listIncrements.add(l);
						}
					}

					if (!checkTablesHeader(currentFile))
						throw new DatabaseException("The database backup is incompatible with current database tables");

					databaseWrapper.loadDatabase(databaseConfiguration, newVersion, null);
					newVersionLoaded = true;
					tbls = new ArrayList<>();
					for (Class<? extends Table<?>> c : classes) {
						Table<?> t = databaseWrapper.getTableInstance(c, newVersion);
						tbls.add(t);
					}

					s = 0;
					generateProgressBarParameterForRestoration(dateUTCInMs);
					pg = backupConfiguration.getProgressMonitorForRestore();
					File f = null;
					if (pg != null) {
						for (Long l : listIncrements) {
							f = getFile(l, f == null);
							s += f.length();
						}
						pg.setMinimum(0);
						pg.setMaximum(1000);
					}

				} catch (Exception e) {
					lastCurrentRestorationFileUsed=Long.MIN_VALUE;
					if (newVersionLoaded)
						databaseWrapper.deleteDatabase(databaseConfiguration, false, newVersion, true);
					throw DatabaseException.getDatabaseException(e);
				}
			}
			final long totalSize = s;
			long progressPosition = 0;
			final ProgressMonitorDM progressMonitor=pg;
			final ArrayList<Table<?>> tables=tbls;
			final int maxBufferSize = backupConfiguration.getMaxStreamBufferSizeForBackupRestoration();
			final int maxBuffersNumber = backupConfiguration.getMaxStreamBufferNumberForBackupRestoration();
			final Reference<Boolean> addResetDB=new Reference<>(true);
			try{
				databaseWrapper.getSynchronizer().startExtendedTransaction();



				fileloop:while (currentFile!=null)
				{
					if (!currentFile.exists())
						throw new DatabaseException("Backup file not found : "+currentFile);
					long previousTransactionUTC=Long.MIN_VALUE;
					try(RandomInputStream in=new BufferedRandomInputStream(new RandomFileInputStream(currentFile), maxBufferSize, maxBuffersNumber))
					{
						positionForDataRead(in, reference);
						reference=false;
						while (in.available()>0) {
							int startTransaction=(int)in.currentPosition();
							int nextTransaction=in.readInt();

							in.skipBytes(9);
							long currentTransactionUTC=in.readLong();
							if (currentTransactionUTC<previousTransactionUTC)
								throw new IOException();
							previousTransactionUTC=currentTransactionUTC;
							if (currentTransactionUTC>dateUTCInMs)
								break fileloop;
							if (in.readInt()!=-1)
								throw new IOException();

							final long dataTransactionStartPosition=in.currentPosition();
							final long pp=progressPosition;
							progressPosition=databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
								long progressPosition=pp;
								@Override
								public Long run() throws Exception {
									if (addResetDB.get())
									{
										addResetDB.set(false);

										for (int i=tables.size()-1;i>=0;i--)
										{
											Table<?> t=tables.get(i);
											try (Table.Lock ignored = new Table.WriteLock(t)) {
												t.removeAllRecordsWithCascadeImpl(true);
											}
											catch (Exception e) {
												throw DatabaseException.getDatabaseException(e);
											}

										}
									}
									for(;;) {

										int startRecord = (int) in.currentPosition();
										byte eventTypeCode = in.readByte();
										if (eventTypeCode == -1)
											return progressPosition;
										DatabaseEventType eventType = DatabaseEventType.getEnum(eventTypeCode);
										if (eventType == null)
											throw new IOException();
										int tableIndex = in.readUnsignedShort();
										if (tableIndex >= tables.size())
											throw new IOException();
										Table<?> table = tables.get(tableIndex);
										//Table<?> oldTable = oldTables.get(tableIndex);
										if (eventType==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
										{
											table.removeAllRecordsWithCascade();
										}
										else {
											int s=in.readBytesArray(recordBuffer, 0, false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
											
											switch (eventType) {
												case ADD: {
													assert eventTypeCode == 2;
													HashMap<String, Object> hm = new HashMap<>();
													table.deserializeFields(hm, recordBuffer, 0, s, true, false, false);
													HashMap<String, Object> hmpk=hm;
													if (in.readBoolean()) {
														hmpk=new HashMap<>(hm);
														s=in.readBytesArray(recordBuffer, 0, false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
														table.deserializeFields(hm, recordBuffer, 0, s, false, true, false);
													}

													if (in.readBoolean()) {
														if (hmpk==hm)
															hmpk=new HashMap<>(hm);
														s=in.readBytesArray(recordBuffer, 0, false, Table.MAX_NON_KEYS_SIZE_IN_BYTES);
														table.deserializeFields(hm, recordBuffer, 0, s, false, false, true);
													}

													try {
														 table.addUntypedRecord(hm);
														//newRecord = table.addUntypedRecord(hm, drRecord == null, null);
													} catch (ConstraintsNotRespectedDatabaseException ignored) {
														//TODO this exception occurs sometimes but should not. See why.
														DatabaseRecord newRecord = table.getRecord(hmpk);
														for (FieldAccessor fa : table.getFieldAccessors()) {
															if (!fa.isPrimaryKey()) {
																fa.setValue(newRecord, hm.get(fa.getFieldName()));
															}
														}
														table.updateUntypedRecord(newRecord, true, null);
													}


												}
												break;
												case UPDATE: {
													DatabaseRecord dr = table.getNewRecordInstance(false);
													table.deserializeFields(dr, recordBuffer, 0, s, true, false, false);

													if (in.readBoolean()) {
														s = in.readUnsignedInt24Bits();
														in.readFully(recordBuffer, 0, s);
														table.deserializeFields(dr, recordBuffer, 0, s, false, true, false);
													}

													if (in.readBoolean()) {
														s = in.readInt();
														if (s < 0)
															throw new IOException();
														if (s > 0) {
															in.readFully(recordBuffer, 0, s);

															table.deserializeFields(dr, recordBuffer, 0, s, false, false, true);
														}
													}
													table.updateUntypedRecord(dr, true, null);

												}
												break;
												case REMOVE: {
													HashMap<String, Object> pks = new HashMap<>();
													table.deserializeFields(pks, recordBuffer, 0, s, true, false, false);
													if (!table.removeRecord(pks))
														throw new IOException();
												}
												break;
												case REMOVE_WITH_CASCADE: {
													HashMap<String, Object> pks = new HashMap<>();
													table.deserializeFields(pks, recordBuffer, 0, s, true, false, false);
													if (!table.removeRecordWithCascade(pks))
														throw new IOException();

												}
												break;
												default:
													throw new IllegalAccessError();


											}
										}
										if (in.readInt() != startRecord)
											throw new IOException();
										if (progressMonitor != null && totalSize != 0) {
											progressPosition += in.currentPosition() - startRecord;
											progressMonitor.setProgress((int) (((progressPosition+1) * 1000) / totalSize));
										}

									}
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
								public void initOrReset() throws Exception {
									in.seek(dataTransactionStartPosition);
								}
							});
							if (nextTransaction<0)
								break;
							if (in.readInt()!=startTransaction)
								throw new IOException();

						}

					}

					if (listIncrements.size()>0)
						currentFile=getFile(listIncrements.removeFirst(), false);
					else
						currentFile=null;

				}

				databaseWrapper.validateNewDatabaseVersionAndDeleteOldVersion(databaseConfiguration, oldVersion, newVersion);
				databaseWrapper.getSynchronizer().validateExtendedTransaction();
				//createBackupReference();
				notify=true;
				if (notifyOtherPeers) {
					databaseWrapper.getSynchronizer().notifyOtherPeersThatDatabaseRestorationWasDone(dbPackage, dateUTCInMs,restorationTimeUTCInMS, lastTransactionUTCInMs);
				}
				else {
					databaseWrapper.getSynchronizer().cleanTransactionsAfterRestoration(dbPackage.getName(), dateUTCInMs, restorationTimeUTCInMS, lastTransactionUTCInMs, false, chooseNearestBackupIfNoBackupMatch);
				}
				if (lastTransactionID!=null) {
					databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>(){
						@Override
						public Void run() throws Exception {
							if (databaseWrapper.getTransactionIDTable().getLastTransactionID() < lastTransactionID+1) {
								databaseWrapper.getTransactionIDTable().setLastTransactionID(lastTransactionID < 0 ? 0 : lastTransactionID + 1);
							}
							return null;
						}

						@Override
						public TransactionIsolation getTransactionIsolation() {
							return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
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

				return res;

			} catch (Exception e) {
				databaseWrapper.getSynchronizer().cancelExtendedTransaction();
				databaseWrapper.deleteDatabase(databaseConfiguration, false, newVersion, false);
				throw DatabaseException.getDatabaseException(e);
			}
			finally {
				if (progressMonitor != null) {
					progressMonitor.setProgress(1000);
				}
				lastCurrentRestorationFileUsed=Long.MIN_VALUE;
				cleanCache();
				if (notify)
					databaseWrapper.getSynchronizer().checkForNewBackupFilePartToSendToCentralDatabaseBackup(dbPackage);

			}
		}
		finally {
			synchronized (this) {
				temporaryDisabled = false;
			}
			databaseWrapper.unlockWrite();
		}
	}
	static void positionForDataRead(RandomInputStream in, boolean reference) throws DatabaseException {
		try {

			if (reference) {
				in.seek(LIST_CLASSES_POSITION);
				int dataPosition = in.readInt();
				in.seek(dataPosition);
			}
			else
				in.seek(LIST_CLASSES_POSITION+4);
		}
		catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}


	boolean createIfNecessaryNewBackupReference() throws DatabaseException {
		if (doesCreateNewBackupReference())
		{
			createBackupReference();
			return true;
		}
		else
			return false;

	}
	boolean createNewBackupReferenceIfDatabaseIsEmpty() throws DatabaseException {
		if (fileTimeStamps.size()==0) {

			return createIfNecessaryNewBackupReference();
		}
		else {
			return false;
		}
	}

	void eraseEmptyBackup() throws DatabaseException {
		if (fileTimeStamps.size()==1 && getLastTransactionUTCInMS()==Long.MIN_VALUE)
		{
			File f=getFile(fileTimeStamps.get(0));
			if (!f.delete())
				System.err.println("Impossible to delete file "+f);
			scanFiles();
		}
	}

	AbstractTransaction startTransaction() throws DatabaseException {
		synchronized (this) {
			if (fileTimeStamps.size()==0 && !temporaryDisabled && !computeDatabaseReference.exists()) {
				new TransactionToValidateByABackupReference();
			}

			if (!isReady()) {
				return null;
			}
			int oldLength;
			long oldLastFile;
			if (fileTimeStamps.size() == 0)
				throw new InternalError();
			else {
				oldLastFile = fileTimeStamps.get(fileTimeStamps.size() - 1);
				File file = getFile(oldLastFile, isReferenceFile(oldLastFile));
				oldLength = (int) file.length();
			}

			long last = getLastTransactionUTCInMS();
			Reference<Long> fileTimeStamp = new Reference<>();
			Reference<Long> firstTransactionID=new Reference<>(null);
			RandomOutputStream rfos = getFileForBackupIncrementOrCreateIt(fileTimeStamp, firstTransactionID);
			return new Transaction(fileTimeStamp.get(), last, rfos, oldLastFile, oldLength, firstTransactionID.get());
		}

	}

	/**
	 *
	 * @return true if no backup was done
	 */
	public boolean isEmpty() {
		synchronized (this) {
			return this.fileReferenceTimeStamps.size() == 0;
		}
	}
	abstract static class AbstractTransaction
	{
		abstract void cancelTransaction() throws DatabaseException;

		abstract long getTransactionUTC();

		abstract void validateTransaction(Long transactionID) throws DatabaseException;
		abstract void backupRecordEvent(TableEvent<?> _de) throws DatabaseException;
		abstract void cancelTransaction(long backupPosition) throws DatabaseException;
		abstract long getBackupPosition() throws DatabaseException;
	}
	class TransactionToValidateByABackupReference extends AbstractTransaction
	{

		@Override
		void cancelTransaction()  {

		}

		@Override
		long getTransactionUTC() {
			return 0;
		}

		@Override
		void validateTransaction(Long transactionID) throws DatabaseException {
			createBackupReference();
		}

		@Override
		void backupRecordEvent(TableEvent<?> _de) throws DatabaseException {

		}

		@Override
		void cancelTransaction(long backupPosition)  {

		}

		@Override
		long getBackupPosition()  {
			return 0;
		}
	}
	class Transaction extends AbstractTransaction
	{
		long lastTransactionUTC;
		int transactionsNumber=0;
		RandomOutputStream out;
		private boolean closed=false;
		private long transactionUTC;
		private final int nextTransactionReference;
		private final long fileTimeStamp;
		private final int oldLength;
		private final long oldLastFile;
		private final Long firstTransactionID;


		private Transaction(long fileTimeStamp, long lastTransactionUTC, RandomOutputStream out, /*RecordsIndex index, */long oldLastFile, int oldLength, Long firstTransactionID) throws DatabaseException {
			//this.index=index;
			this.fileTimeStamp=fileTimeStamp;
			this.lastTransactionUTC = lastTransactionUTC;
			this.out=out;
			this.oldLastFile=oldLastFile;
			this.oldLength=oldLength;
			this.firstTransactionID=firstTransactionID;

			transactionUTC=System.currentTimeMillis();
			while(transactionUTC==lastTransactionUTC)
			{
				try {
					//noinspection BusyWait
					Thread.sleep(1);
					transactionUTC=System.currentTimeMillis();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (transactionUTC<lastTransactionUTC)
				throw new InternalError(transactionUTC+";"+lastTransactionUTC);
			nextTransactionReference=saveTransactionHeader(out, transactionUTC);
			try(FileOutputStream fos=new FileOutputStream(computeDatabaseReference); DataOutputStream dos=new DataOutputStream(fos))
			{
				dos.writeLong(out.currentPosition());
				dos.writeLong(fileTimeStamp);
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}
		@Override
		final long getBackupPosition() throws DatabaseException {
			try {
				return out.currentPosition();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}


		@Override
		final void cancelTransaction(long backupPosition) throws DatabaseException
		{
			try {
				out.setLength(backupPosition);
				lastBackupEventUTC=Long.MIN_VALUE;
				//transactionsInterval=null;
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		@Override
		final void cancelTransaction() throws DatabaseException
		{
			try {
				synchronized (BackupRestoreManager.this) {

					if (closed)
						return;
					try {
						out.close();
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}

					deleteDatabaseFilesFromReferenceToLastFile(oldLastFile, oldLength);

					if (!computeDatabaseReference.delete())
						throw new DatabaseException("Impossible to delete file : " + computeDatabaseReference);


					closed = true;

				}
			}
			finally {
				lastBackupEventUTC=Long.MIN_VALUE;
			}

		}

		@Override
		public long getTransactionUTC() {
			return transactionUTC;
		}

		@Override
		final void validateTransaction(Long transactionID) throws DatabaseException
		{
			try {
				synchronized (BackupRestoreManager.this) {

					if (closed)
						return;
					if (transactionsNumber==0) {
						cancelTransaction();
						return;
					}
					saveTransactionQueue(out, nextTransactionReference, transactionUTC, firstTransactionID, /*transactionToSynchronize ? */transactionID /*: null/*, index*/);

					try {

						out.close();
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}
					BackupRestoreManager.this.lastBackupEventUTC = Long.MIN_VALUE;
					if (!computeDatabaseReference.delete())
						throw new DatabaseException("Impossible to delete file : " + computeDatabaseReference);
					closed = true;
				}
				if (fileTimeStamp != oldLastFile) {
					databaseWrapper.getSynchronizer().checkForNewBackupFilePartToSendToCentralDatabaseBackup(dbPackage);
				}
				else {
					if (isPartFull(fileTimeStamp))
						databaseWrapper.getSynchronizer().checkForNewBackupFilePartToSendToCentralDatabaseBackup(dbPackage);
					else
						databaseWrapper.getSynchronizer().notifyNewEventIfNecessary();
				}
			}
			finally {
				Long m=maxDateUTC;
				if (m==null)
					maxDateUTC=transactionUTC;
				else {
					if (m==lastBackupEventUTC)
						lastBackupEventUTC=Long.MIN_VALUE;
					maxDateUTC = Math.max(m, transactionUTC);
				}
			}
			if (!createIfNecessaryNewBackupReference()) {
				DatabaseLifeCycles lifeCycles = databaseWrapper.getDatabaseConfigurationsBuilder().getLifeCycles();
				if (lifeCycles != null)
					lifeCycles.newDatabaseBackupFileCreated(BackupRestoreManager.this);
			}

		}

		@Override
		final void backupRecordEvent(TableEvent<?> _de) throws DatabaseException {

			if (closed)
				return;
			++transactionsNumber;

			BackupRestoreManager.this.backupRecordEvent(out, _de.getTable(), _de.getOldDatabaseRecord(), _de.getNewDatabaseRecord(), _de.getType()/*, index*/);
		}

	}

	int getPartFilesCount()
	{
		return fileTimeStamps.size();
	}

	int getReferenceFileCount()
	{
		return fileReferenceTimeStamps.size();
	}



}
