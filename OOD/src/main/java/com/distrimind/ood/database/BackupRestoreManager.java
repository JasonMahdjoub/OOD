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
import com.distrimind.ood.i18n.DatabaseMessages;
import com.distrimind.util.FileTools;
import com.distrimind.util.io.RandomFileInputStream;
import com.distrimind.util.io.RandomFileOutputStream;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import java.io.*;
import java.lang.ref.Reference;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public class BackupRestoreManager {

	private final static int LAST_BACKUP_UTC_POSITION=0;
	private final static int LAST_REMOVE_WITH_CASCADE_POSITION=LAST_BACKUP_UTC_POSITION+8;
	private final static int LIST_CLASSES_POSITION=LAST_REMOVE_WITH_CASCADE_POSITION+8;


	private ArrayList<Long> fileReferenceTimeStamps;
	private ArrayList<Long> fileTimeStamps;
	private final File backupDirectory;
	private final BackupConfiguration backupConfiguration;
	private final DatabaseConfiguration databaseConfiguration;
	private final List<Class<? extends Table<?>>> classes;
	private static final Pattern fileReferencePattern = Pattern.compile("^backup-ood-([1-9][0-9])*\\.dreference$");
	private static final Pattern fileIncrementPattern = Pattern.compile("^backup-ood-([1-9][0-9])*\\.dincrement");

	private final File computeDatabaseReference;
	private DatabaseWrapper databaseWrapper;
	private final boolean passive;
	private final Package dbPackage;

	File getLastFile()
	{
		if (fileTimeStamps.size()==0)
			return null;
		long l=fileTimeStamps.get(fileTimeStamps.size()-1);
		return getFile(l, fileReferenceTimeStamps.contains(l));
	}

	BackupRestoreManager(DatabaseWrapper databaseWrapper, File backupDirectory, DatabaseConfiguration databaseConfiguration, boolean passive) throws DatabaseException {
		if (backupDirectory==null)
			throw new NullPointerException();
		if (backupDirectory.exists() && backupDirectory.isFile())
			throw new IllegalArgumentException();
		if (databaseConfiguration==null)
			throw new NullPointerException();
		if (databaseWrapper==null)
			throw new NullPointerException();
		this.passive=passive;
		this.databaseConfiguration=databaseConfiguration;
		this.databaseWrapper=databaseWrapper;
		this.dbPackage=databaseConfiguration.getPackage();
		FileTools.checkFolderRecursive(backupDirectory);
		this.backupDirectory=backupDirectory;
		this.backupConfiguration=databaseConfiguration.getBackupConfiguration();
		classes=new ArrayList<>(databaseConfiguration.getTableClasses());
		Collections.sort(classes, new Comparator<Class<? extends Table<?>>>() {
			@Override
			public int compare(Class<? extends Table<?>> o1, Class<? extends Table<?>> o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		this.computeDatabaseReference=new File(this.backupDirectory, "computeDatabaseNewReference.query");
		if (this.computeDatabaseReference.exists() && this.computeDatabaseReference.isDirectory())
			throw new IllegalArgumentException();
		if (this.backupConfiguration.getProgressMonitor()==null) {
			ProgressMonitorParameters p=new ProgressMonitorParameters(String.format(DatabaseMessages.BACKUP_DATABASE.toString(), databaseConfiguration.getPackage().toString()), null, 0, 100);
			p.setMillisToDecideToPopup(1000);
			p.setMillisToPopup(1000);
			this.backupConfiguration.setProgressMonitorParameters(p);
		}
		scanFiles();
		if (checkTablesHeader(getFileForBackupReference()))
			createIfNecessaryNewBackupReference();
	}

	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	private void scanFiles()
	{
		fileTimeStamps=new ArrayList<>();
		fileReferenceTimeStamps=new ArrayList<>();
		File []files=this.backupDirectory.listFiles();
		if (files==null)
			return;
		for (File f : files)
		{
			if (f.isDirectory())
				continue;
			Matcher m=fileIncrementPattern.matcher(f.getName());
			if (m.matches())
			{
				try {
					long timeStamp = Long.parseLong(m.group(1));
					fileTimeStamps.add(timeStamp);
				}
				catch(NumberFormatException e)
				{
					e.printStackTrace();
				}

			}
			else
			{
				m=fileReferencePattern.matcher(f.getName());
				if (m.matches())
				{
					try {
						long timeStamp = Long.parseLong(m.group(1));
						fileTimeStamps.add(timeStamp);
						fileReferenceTimeStamps.add(timeStamp);
					}
					catch(NumberFormatException e)
					{
						e.printStackTrace();
					}

				}
			}

		}
		Collections.sort(fileTimeStamps);
		Collections.sort(fileReferenceTimeStamps);
	}


	public File getBackupDirectory() {
		return backupDirectory;
	}

	private File getFile(long timeStamp, boolean backupReference)
	{
		return new File(backupDirectory, "nativeBackup-ood-"+timeStamp+(backupReference?".dreference":".dincrement"));
	}

	private boolean isPartFull(long timeStamp, File file)
	{
		if (file.length()>=backupConfiguration.getMaxBackupFileSizeInBytes())
			return true;
		return timeStamp < System.currentTimeMillis() - backupConfiguration.getMaxBackupFileAgeInMs();
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

			return file;
		} catch (IOException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

	}
	private File initNewFileForBackupReference(long dateUTC) throws DatabaseException {
		return initNewFile(dateUTC, true);
	}

	private RandomFileOutputStream getFileForBackupIncrementOrCreateIt(AtomicLong fileTimeStamp) throws DatabaseException {
		File res=null;

		if (fileTimeStamps.size()>0)
		{
			Long timeStamp=fileTimeStamps.get(fileTimeStamps.size()-1);
			boolean reference=fileReferenceTimeStamps.contains(timeStamp);
			File file=getFile(timeStamp, reference);
			if (!isPartFull(timeStamp, file)) {
				res = file;
				fileTimeStamp.set(timeStamp);
			}
		}
		try {
			if (res==null) {
				fileTimeStamp.set(Math.max(fileTimeStamps.size()>0?fileTimeStamps.get(fileTimeStamps.size()-1)+1:Long.MIN_VALUE, System.currentTimeMillis()));
				res = initNewFileForBackupIncrement(fileTimeStamp.get());

				RandomFileOutputStream out=new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE);
				saveHeader(out, fileTimeStamp.get(), false);
				return out;
			}
			else {
				RandomFileOutputStream out=new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE);
				positionateFileForNewEvent(out);
				return out;
			}
		} catch (FileNotFoundException e) {
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
	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isReady()
	{
		synchronized (this) {
			return fileTimeStamps.size() > 0 && !computeDatabaseReference.exists();
		}
	}

	private boolean backupRecordEvent(RandomOutputStream out, Table<?> table, DatabaseRecord record, DatabaseEventType eventType) throws DatabaseException {
		try {
			out.write(eventType.getByte());
			@SuppressWarnings("SuspiciousMethodCalls")
			int tableIndex=classes.indexOf(table.getClass());
			if (tableIndex<0)
				throw new IOException();
			out.writeUnsignedShort(tableIndex);
			switch(eventType)
			{
				case ADD:
				{
					byte[] pks=table.serializeFieldsWithUnknownType(record, true, false, false);
					out.writeUnsignedShortInt(pks.length);
					out.write(pks);
					if (!table.isPrimaryKeysAndForeignKeysSame() && table.getForeignKeysFieldAccessors().size()>0) {

						byte[] fks=table.serializeFieldsWithUnknownType(record, false, true, false);
						out.writeUnsignedShortInt(fks.length);
						out.write(fks);
					}
					else
						out.writeUnsignedShortInt(-1);

					byte[] nonkeys=table.serializeFieldsWithUnknownType(record, false,false, true);
					if (nonkeys==null || nonkeys.length==0)
					{
						out.writeUnsignedShortInt(-1);
					}
					else
					{
						out.writeUnsignedShortInt(nonkeys.length);
						out.write(nonkeys);
					}
					return false;
				}
				case REMOVE:
				{

				}
					break;
				case REMOVE_WITH_CASCADE:
				{
					return true;
				}
				case UPDATE:
				{

				}
					break;
			}



		} catch (DatabaseException | IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	private void saveTablesHeader(RandomOutputStream out) throws DatabaseException {
		try {
			if (classes.size()>Short.MAX_VALUE)
				throw new DatabaseException("Too much tables");
			int dataPosition=6+classes.size()*2;
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
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	private void saveHeader(RandomOutputStream out, long backupUTC, boolean referenceFile) throws DatabaseException {

		try {
			out.writeLong(backupUTC);
			out.writeLong(-1);
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

		if (referenceFile)
			saveTablesHeader(out);

	}
	private void positionateFileForNewEvent(RandomOutputStream out) throws DatabaseException {
		try {
			out.seek(out.length());
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	private File currentFileReference=null;
	private List<Class<? extends Table<?>>> currentClassesList=null;
	private int currentDataPos=-1;
	private long lastBackupEventUTC=-1;

	private List<Class<? extends Table<?>>> extractClassesList(File file) throws DatabaseException {
		if (currentClassesList==null || currentFileReference!=file)
		{

			try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
				lastBackupEventUTC=rfis.readLong();
				//noinspection ResultOfMethodCallIgnored
				rfis.skip(LIST_CLASSES_POSITION-8);
				currentDataPos=rfis.readInt();
				int s=rfis.readShort();
				if (s<0)
					throw new IOException();

				currentClassesList=new ArrayList<>(s);
				byte[] tab=new byte[Short.MAX_VALUE];
				for (int i=0;i<s;i++)
				{
					int l=rfis.readShort();
					rfis.readFully(tab, 0, l);
					String className=new String(tab, 0, l, StandardCharsets.UTF_8);
					@SuppressWarnings("unchecked")
					Class<? extends Table<?>> c=(Class<? extends Table<?>>)Class.forName(className);
					currentClassesList.add(c);
				}
			} catch (IOException | ClassCastException | ClassNotFoundException e) {
				throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
			}
			currentFileReference=file;
		}
		return currentClassesList;
	}

	private long extractLastBackupEventUTC(File file) throws DatabaseException {
		if (file==currentFileReference)
			return lastBackupEventUTC;
		else
		{
			try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
				return rfis.readLong();
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
				if (!currentClassesList.get(i).equals(classes.get(i)))
				{
					ok=false;
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
			out.writeInt(-1);
			out.writeBoolean(false);
			out.writeLong(backupTime);

			return nextTransactionReference;
		}
		catch(IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
	}
	private void saveTransactionQueue(RandomOutputStream out, int nextTransactionReference, boolean containsRemoveWithCascade, long transactionUTC) throws DatabaseException {
		try {
			int nextTransaction=(int)out.currentPosition();
			out.seek(LAST_BACKUP_UTC_POSITION);
			out.writeLong(transactionUTC);
			out.seek(nextTransactionReference);
			out.writeInt(nextTransaction);
			if (containsRemoveWithCascade)
				out.writeBoolean(true);
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
		synchronized (this) {

			final long backupTime = System.currentTimeMillis();
			final AtomicLong currentBackupTime = new AtomicLong(backupTime);


			try
			{
				if (!computeDatabaseReference.exists()) {
					if (!computeDatabaseReference.createNewFile())
						throw new DatabaseException("Impossible to create file " + computeDatabaseReference);
				}
				else if (computeDatabaseReference.length()>=16)
				{
					try(FileInputStream fis=new FileInputStream(currentFileReference);DataInputStream dis=new DataInputStream(fis))
					{
						long s=dis.readLong();
						if (s>=0)
						{
							long fileRef=dis.readLong();
							for (Long l : fileTimeStamps)
							{
								if (l==fileRef)
								{
									boolean reference=fileReferenceTimeStamps.contains(l);
									try(RandomFileOutputStream rfos=new RandomFileOutputStream(getFile(l, reference)))
									{
										rfos.setLength(fileRef);
									}
								}
								else if (fileRef<l)
								{
									boolean reference=fileReferenceTimeStamps.contains(l);
									//noinspection ResultOfMethodCallIgnored
									getFile(l, reference).delete();
									fileReferenceTimeStamps.remove(l);
								}
							}
						}
					}
				}

				File file = initNewFileForBackupReference(currentBackupTime.get());
				final AtomicReference<RandomFileOutputStream> rout=new AtomicReference<>(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE));
				try {

					saveHeader(rout.get(), currentBackupTime.get(), true);
					final AtomicInteger nextTransactionReference=new AtomicInteger(saveTransactionHeader(rout.get(), currentBackupTime.get()));

					for (Class<? extends Table<?>> c : classes) {
						final Table<?> table = databaseWrapper.getTableInstance(c);
						table.getPaginedRecordsWithUnknownType(-1, -1, new Filter<DatabaseRecord>() {
							RandomFileOutputStream out=rout.get();
							@Override
							public boolean nextRecord(DatabaseRecord _record) throws DatabaseException {
								backupRecordEvent(out, table, _record, DatabaseEventType.ADD);
								try {
									if (out.currentPosition()>=backupConfiguration.getMaxBackupFileSizeInBytes())
									{
										saveTransactionQueue(rout.get(), nextTransactionReference.get(), false, currentBackupTime.get());
										out.flush();
										out.close();

										currentBackupTime.set(Math.max(currentBackupTime.get()+1, System.currentTimeMillis()));
										File file=initNewFileForBackupIncrement(System.currentTimeMillis());
										rout.set(out=new RandomFileOutputStream(file));
										saveHeader(out, currentBackupTime.get(), false);
										nextTransactionReference.set(saveTransactionHeader(rout.get(), currentBackupTime.get()));

									}
								} catch (IOException e) {
									throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
								}

								return false;
							}
						});
					}
					saveTransactionQueue(rout.get(), nextTransactionReference.get(), false, currentBackupTime.get());
				}
				finally {
					rout.get().flush();
					rout.get().close();
				}
				//scanFiles();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}


			try {

				cleanOldBackups();
				if (!computeDatabaseReference.delete())
					throw new DatabaseException("Impossible to delete file " + computeDatabaseReference);
			} catch (DatabaseException e) {
				deleteDatabaseFilesFromReferenceToLastFile(backupTime);
				throw e;
			}
			finally {
				scanFiles();
			}
			return backupTime;
		}

	}

	/**
	 * Clean old backups
	 *
	 */
	public void cleanOldBackups()
	{
		synchronized (this) {
			long limitUTC=System.currentTimeMillis()-backupConfiguration.getMaxBackupDurationInMs();
			long concretLimitUTC=Long.MIN_VALUE;
			global:for (int i=fileReferenceTimeStamps.size()-2;i>=0;i--)
			{
				Long l=fileReferenceTimeStamps.get(i);
				if (l<limitUTC) {
					int istart=fileTimeStamps.indexOf(l)+1;
					int iend=fileTimeStamps.indexOf(fileReferenceTimeStamps.get(i+1));
					if (iend<0)
						throw new IllegalAccessError();
					long limit=l;
					for (int j=istart;j<iend;j++) {
						limit=fileTimeStamps.get(j);
						if ( limit>= limitUTC) {
							continue global;
						}

					}

					concretLimitUTC = limit;
					break;
				}
			}
			if (concretLimitUTC!=Long.MIN_VALUE) {
				deleteDatabaseFilesFromReferenceToFirstFile(concretLimitUTC);
			}
		}
	}


	private void activateBackupReferenceCreation(boolean backupNow) throws DatabaseException {
		synchronized (this) {
			try {
				if (!computeDatabaseReference.createNewFile())
					throw new DatabaseException("Impossible to create file " + computeDatabaseReference);
				createIfNecessaryNewBackupReference();
			} catch (IOException e) {
				throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
			}
		}
	}

	boolean doesCreateNewBackupReference()
	{
		return !passive && !isReady();
	}

	private void deleteDatabaseFilesFromReferenceToLastFile(long transactionReference) throws DatabaseException {
		for (Iterator<Long> it = fileReferenceTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l >= transactionReference) {
				File f = getFile(l, true);
				//noinspection ResultOfMethodCallIgnored
				f.delete();
				it.remove();
			}
		}
		for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l >= transactionReference) {
				File f = getFile(l, false);
				//noinspection ResultOfMethodCallIgnored
				f.delete();
				it.remove();
			}
		}
		if (fileTimeStamps.size()>0)
		{
			long l=fileTimeStamps.get(fileTimeStamps.size()-1);
			boolean isReference=fileReferenceTimeStamps.contains(l);
			File file=getFile(l, isReference);
			try(RandomFileOutputStream out=new RandomFileOutputStream(file))
			{
				RandomInputStream ris=out.getRandomInputStream();
				if (isReference)
				{
					long prevDate=ris.readLong();
					int posLastRemoveWithCascade=-1;
					if (ris.skip(LIST_CLASSES_POSITION-8)!=LIST_CLASSES_POSITION-8)
						throw new DatabaseException("Invalid file", new IOException());
					int dataPosition=ris.readInt();
					ris.seek(dataPosition);

					while(ris.currentPosition()<ris.length())
					{
						int currentPos=(int)ris.currentPosition();
						int nextTrPos=ris.readInt();
						boolean removeWithCascade=ris.readBoolean();
						long date=ris.readLong();

						if (date>=transactionReference)
						{
							long newLength=ris.currentPosition()-6;
							ris.close();
							out.setLength(newLength);
							out.seek(LAST_BACKUP_UTC_POSITION);
							out.writeLong(prevDate);
							out.writeInt(posLastRemoveWithCascade);
							break;
						}
						else {
							if (removeWithCascade)
								posLastRemoveWithCascade=currentPos;
							prevDate=date;
							ris.seek(nextTrPos);
						}
					}
				}

			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
	}
	private void deleteDatabaseFilesFromReferenceToFirstFile(long fileReference)
	{
		for (Iterator<Long> it = fileReferenceTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				File f = getFile(l, true);
				//noinspection ResultOfMethodCallIgnored
				f.delete();
				it.remove();
			}
		}
		for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				File f = getFile(l, false);
				//noinspection ResultOfMethodCallIgnored
				f.delete();
				it.remove();
			}
		}
	}

	/**
	 * Gets the older backup event UTC time
	 * @return the older backup event UTC time
	 */
	public long getMinDateUTCInMs()
	{
		synchronized (this) {
			return fileTimeStamps.size()>0?fileTimeStamps.get(0):Long.MIN_VALUE;
		}
	}

	/**
	 * Gets the younger backup event UTC time
	 * @return the younger backup event UTC time
	 */
	public long getMaxDateUTCInMS() throws DatabaseException {
		synchronized (this) {
			if (fileTimeStamps.size()>0 && fileReferenceTimeStamps.size()>0)
			{
				long ts=fileTimeStamps.get(fileTimeStamps.size()-1);
				return extractLastBackupEventUTC(getFile(ts, fileReferenceTimeStamps.get(fileReferenceTimeStamps.size()-1)==ts));
			}
			return Long.MAX_VALUE;
		}
	}
	private final byte[] recordBuffer=new byte[1<<24-1];
	/**
	 * Restore the database to the nearest given date UTC
	 * @param dateUTCInMs the UTC time in milliseconds
	 * @return true if the given time corresponds to an available backup. False is chosen if the given time is too old to find a corresponding historical into the backups. In this previous case, it is the nearest backup that is chosen.
	 */
	public boolean restoreDatabaseToDateUTC(long dateUTCInMs) throws DatabaseException {
		synchronized (this) {
			int oldVersion=databaseWrapper.getCurrentDatabaseVersion(dbPackage);

			int newVersion=oldVersion+1;
			while(databaseWrapper.doesVersionExists(dbPackage, newVersion)) {
				++newVersion;
				if (newVersion < 0)
					throw new DatabaseException("No more database version available");
			}
			long startFileReference=Long.MIN_VALUE;
			for (int i=fileReferenceTimeStamps.size()-1;i>=0;i--)
			{
				if (fileReferenceTimeStamps.get(i)<=dateUTCInMs)
				{
					startFileReference=fileReferenceTimeStamps.get(i);
					break;
				}
			}
			if (startFileReference==Long.MIN_VALUE)
			{
				for (Class<? extends Table<?>> c : classes) {
					databaseWrapper.getTableInstance(c, newVersion);
				}
				databaseWrapper.deleteDatabase(databaseConfiguration, oldVersion);
				createBackupReference();
			}
			else {
				try {
					File currentFile=getFile(startFileReference, true);
					LinkedList<Long> listIncrements=new LinkedList<>();
					for (int i=0;i<fileTimeStamps.size();i++)
					{
						Long l=fileTimeStamps.get(i);
						if (l>startFileReference)
						{
							if (fileReferenceTimeStamps.contains(l))
								break;
							listIncrements.add(l);
						}
					}

					checkTablesHeader(currentFile);
					List<Class<? extends Table<?>>> classes=extractClassesList(currentFile);
					final ArrayList<Table<?>> tables=new ArrayList<>();
					for (Class<? extends Table<?>> c : classes) {
						Table<?> t = databaseWrapper.getTableInstance(c, newVersion);
						tables.add(t);
					}

					boolean reference=true;
					while (currentFile!=null)
					{
						try(RandomFileInputStream in=new RandomFileInputStream(currentFile))
						{
							positionForDataRead(in, reference);
							reference=false;
							while (in.available()>0) {

								int nextTransaction=in.readInt();
								if (in.skip(9)!=9)
									throw new IOException();
								final long dataTransactionStartPosition=in.currentPosition();
								databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
									@Override
									public Void run() throws Exception {
										byte eventTypeCode=in.readByte();
										DatabaseEventType eventType=DatabaseEventType.getEnum(eventTypeCode);
										if (eventType==null)
											throw new IOException();

										int tableIndex=in.readUnsignedShort();
										if (tableIndex>=tables.size())
											throw new IOException();
										Table<?> table=tables.get(tableIndex);
										switch(eventType)
										{
											case ADD:
											{
												int s=in.readUnsignedShortInt();
												in.readFully(recordBuffer, 0, s);
												DatabaseRecord dr=table.getNewRecordInstance();
												table.deserializePrimaryKeys(dr, recordBuffer, 0, s);
												s=in.readUnsignedShortInt();
												if (s>0) {
													in.readFully(recordBuffer, 0, s);
													table.deserializeFields(dr, recordBuffer, 0, s, false, true, false);
												}
												s=in.readUnsignedShortInt();
												if (s>0) {
													in.readFully(recordBuffer, 0, s);
													table.deserializeFields(dr, recordBuffer, 0, s, false, false, true);
												}
												table.addRecord(dr);
											}
												break;
											case REMOVE:
											{

											}
											break;
											case REMOVE_WITH_CASCADE:
											{

											}
											break;
											case UPDATE:
											{

											}
											break;

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
									public void initOrReset() throws Exception {
										in.seek(dataTransactionStartPosition);
									}
								});
								if (nextTransaction<0)
									break;

							}

						}

						if (listIncrements.size()>0)
							currentFile=getFile(listIncrements.removeFirst(), false);
						else
							currentFile=null;

					}
					databaseWrapper.validateNewDatabaseVersionAndDeleteOldVersion(databaseConfiguration, oldVersion, newVersion);
					createBackupReference();

				} catch (Exception e) {
					databaseWrapper.deleteDatabase(databaseConfiguration, newVersion);
					throw DatabaseException.getDatabaseException(e);
				}
			}

		}
	}
	private void positionForDataRead(RandomInputStream in, boolean reference) throws DatabaseException {
		try {
			if (reference) {
				in.seek(LIST_CLASSES_POSITION);
				int dataPosition = in.readInt();
				in.seek(dataPosition);
				reference = false;
			} else {
				in.seek(LIST_CLASSES_POSITION);
			}
		}
		catch (Exception e) {
			//TODO remove version
			throw DatabaseException.getDatabaseException(e);
		}
	}


	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param record the record to restore (only primary keys are used)
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false).
	 */
	public <R extends DatabaseRecord> Reference<R> restoreRecordToDateUTC(long dateUTC, R record, boolean restoreWithCascade) throws DatabaseException {
		Table<R> table=databaseWrapper.getTableInstanceFromRecord(record);
		return restoreRecordToDateUTC(dateUTC,table, Table.getFields(table.getPrimaryKeysFieldAccessors(), record), restoreWithCascade);
	}

	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param table the concerned table
	 * @param primaryKeys the primary keys of the record to restore
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param <R> the record type
	 * @param <T> the table type
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false).
	 */
	public <R extends DatabaseRecord, T extends Table<R>> Reference<R> restoreRecordToDateUTC(long dateUTC, T table, Map<String, Object> primaryKeys, boolean restoreWithCascade)
	{
		synchronized (this) {
			//TODO complete
		}
	}

	void createIfNecessaryNewBackupReference() throws DatabaseException {
		if (doesCreateNewBackupReference())
		{
			createBackupReference();

		}

	}

	Transaction startTransaction() throws DatabaseException {
		synchronized (this) {
			createIfNecessaryNewBackupReference();
			if (!isReady())
				return null;
			long last=getMaxDateUTCInMS();
			AtomicLong fileTimeStamp=new AtomicLong();
			RandomFileOutputStream rfos = getFileForBackupIncrementOrCreateIt(fileTimeStamp);
			return new Transaction(fileTimeStamp.get(), last, rfos);
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

	class Transaction
	{
		long lastTransactionUTC;
		int transactionsNumber=0;
		RandomFileOutputStream out;
		private boolean closed=false;
		private long transactionUTC;
		private int nextTransactionReference;
		private boolean containsRemoveWithCascade=false;
		private final long fileTimeStamp;

		Transaction(long fileTimeStamp, long lastTransactionUTC, RandomFileOutputStream out) throws DatabaseException {
			this.fileTimeStamp=fileTimeStamp;
			this.lastTransactionUTC = lastTransactionUTC;
			this.out=out;
			transactionUTC=System.currentTimeMillis();
			if (transactionUTC==lastTransactionUTC)
				++transactionUTC;
			if (transactionUTC<lastTransactionUTC)
				throw new InternalError();
			nextTransactionReference=saveTransactionHeader(out, transactionUTC);
			try(FileOutputStream fos=new FileOutputStream(computeDatabaseReference); DataOutputStream dos=new DataOutputStream(fos))
			{
				dos.writeLong(out.currentPosition());
				dos.writeLong(fileTimeStamp);
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}


		final void cancelTransaction() throws DatabaseException
		{
			if (closed)
				return;
			try {
				out.close();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
			if (transactionsNumber>0)
				deleteDatabaseFilesFromReferenceToLastFile(lastTransactionUTC+1);
			if (!computeDatabaseReference.delete())
				throw new DatabaseException("Impossible to delete file : "+computeDatabaseReference);
			closed=true;

		}

		final void validateTransaction() throws DatabaseException
		{
			if (closed)
				return;
			saveTransactionQueue(out,nextTransactionReference, containsRemoveWithCascade, transactionUTC);

			if (!fileTimeStamps.contains(fileTimeStamp)) {
				fileTimeStamps.add(fileTimeStamp);
			}

			try {
				out.close();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
			if (!computeDatabaseReference.delete())
				throw new DatabaseException("Impossible to delete file : "+computeDatabaseReference);
			closed=true;
		}

		final void backupRecordEvent(RandomOutputStream out, TableEvent<?> _de) throws DatabaseException {
			if (closed)
				return;
			++transactionsNumber;

			if (BackupRestoreManager.this.backupRecordEvent(out, _de.getTable(databaseWrapper), _de.getNewDatabaseRecord(), _de.getType()))
				containsRemoveWithCascade=true;
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
