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
import com.distrimind.util.io.*;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import javax.swing.*;
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
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public class BackupRestoreManager {

	private final static int LAST_BACKUP_UTC_POSITION=0;
	//private final static int RECORDS_INDEX_POSITION=LAST_BACKUP_UTC_POSITION+8;
	private final static int LIST_CLASSES_POSITION=LAST_BACKUP_UTC_POSITION+8;


	private ArrayList<Long> fileReferenceTimeStamps;
	private ArrayList<Long> fileTimeStamps;
	private final File backupDirectory;
	private final BackupConfiguration backupConfiguration;
	private final DatabaseConfiguration databaseConfiguration;
	private final List<Class<? extends Table<?>>> classes;
	private static final Pattern fileReferencePattern = Pattern.compile("^backup-ood-([1-9][0-9]*)\\.dreference$");
	private static final Pattern fileIncrementPattern = Pattern.compile("^backup-ood-([1-9][0-9]*)\\.dincrement");

	private final File computeDatabaseReference;
	private DatabaseWrapper databaseWrapper;
	private final boolean passive;
	private final Package dbPackage;
	private final boolean generateRestoreProgressBar;
	private volatile long lastCurrentRestorationFileUsed=Long.MIN_VALUE;
	private volatile BackupFileListener backupFileListener=null;
	//private volatile long currentBackupReferenceUTC=Long.MAX_VALUE;


	File getLastFile()
	{
		if (fileTimeStamps.size()==0)
			return null;
		long l=fileTimeStamps.get(fileTimeStamps.size()-1);
		return getFile(l, fileReferenceTimeStamps.contains(l));
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
		this.dbPackage=databaseConfiguration.getPackage();
		System.out.println(backupDirectory);
		FileTools.checkFolderRecursive(backupDirectory);
		this.backupDirectory=backupDirectory;
		this.backupConfiguration=backupConfiguration;
		classes=databaseConfiguration.getSortedTableClasses(databaseWrapper);

		this.computeDatabaseReference=new File(this.backupDirectory, "computeDatabaseNewReference.query");
		if (this.computeDatabaseReference.exists() && this.computeDatabaseReference.isDirectory())
			throw new IllegalArgumentException();
		if (this.backupConfiguration.backupProgressMonitorParameters==null) {
			ProgressMonitorParameters p=new ProgressMonitorParameters(String.format(DatabaseMessages.BACKUP_DATABASE.toString(), databaseConfiguration.getPackage().toString()), null, 0, 100);
			p.setMillisToDecideToPopup(1000);
			p.setMillisToPopup(1000);
			this.backupConfiguration.setBackupProgressMonitorParameters(p);
		}
		generateRestoreProgressBar= this.backupConfiguration.restoreProgressMonitorParameters == null;
		scanFiles();
		if (checkTablesHeader(getFileForBackupReference()))
			createIfNecessaryNewBackupReference();
	}

	private void generateProgressBarParameterForRestoration(long timeUTC)
	{
		if (generateRestoreProgressBar)
		{
			ProgressMonitorParameters p=new ProgressMonitorParameters(String.format(DatabaseMessages.RESTORE_DATABASE.toString(), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz").format(new Date(timeUTC)),databaseConfiguration.getPackage().toString()), null, 0, 100);
			p.setMillisToDecideToPopup(1000);
			p.setMillisToPopup(1000);
			this.backupConfiguration.setRestoreProgressMonitorParameters(p);

		}
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
		return new File(backupDirectory, "backup-ood-"+timeStamp+(backupReference?".dreference":".dincrement"));
	}

	public List<File> getFinalFilesFromGivenTime(long utc)
	{
		synchronized (this) {
			ArrayList<File> res = new ArrayList<>(fileTimeStamps.size());
			int s = fileTimeStamps.size();

			if (s > 0) {
				long ts = fileTimeStamps.get(s - 1);
				if (/*ts>=currentBackupReferenceUTC || */!isPartFull(ts, getFile(ts, fileReferenceTimeStamps.contains(ts))))
					--s;
			}
			for (int i = 0; i < s; i++) {
				long ts = fileTimeStamps.get(i);
				/*if (ts>=currentBackupReferenceUTC)
					break;*/
				if (ts >= utc)
					res.add(getFile(ts, fileReferenceTimeStamps.contains(ts)));

			}
			return res;
		}
	}

	public List<File> getFinalFiles()
	{
		synchronized (this) {
			ArrayList<File> res = new ArrayList<>(fileTimeStamps.size());
			int s = fileTimeStamps.size();

			if (s > 0) {
				long ts = fileTimeStamps.get(s - 1);

				if (/*ts>=currentBackupReferenceUTC || */!isPartFull(ts, getFile(ts, fileReferenceTimeStamps.contains(ts))))
					--s;
			}
			for (int i = 0; i < s; i++) {
				long ts = fileTimeStamps.get(i);
				/*if (ts>=currentBackupReferenceUTC)
					break;*/

				res.add(getFile(ts, fileReferenceTimeStamps.contains(ts)));
			}
			return res;
		}
	}


	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private boolean isPartFull(long timeStamp, File file)
	{
		if (timeStamp==lastCurrentRestorationFileUsed)
			return true;
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

	private BufferedRandomOutputStream getFileForBackupIncrementOrCreateIt(AtomicLong fileTimeStamp/*, AtomicReference<RecordsIndex> recordsIndex*/) throws DatabaseException {
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
			final int maxBufferSize=backupConfiguration.getMaxStreamBufferSizeForTransaction();
			final int maxBuffersNumber=backupConfiguration.getMaxStreamBufferNumberForTransaction();

			if (res==null) {
				fileTimeStamp.set(System.currentTimeMillis());
				res = initNewFileForBackupIncrement(fileTimeStamp.get());

				BufferedRandomOutputStream out=new BufferedRandomOutputStream(new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber);
				saveHeader(out, fileTimeStamp.get(), false/*, recordsIndex*/);
				return out;
			}
			else {

				BufferedRandomOutputStream out=new BufferedRandomOutputStream(new RandomFileOutputStream(res, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber);
				//recordsIndex.set(new RecordsIndex(out.getUnbufferedRandomInputStream()));
				positionateFileForNewEvent(out);
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
	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isReady()
	{
		synchronized (this) {
			return fileTimeStamps.size() > 0 && !computeDatabaseReference.exists();
		}
	}

	private void backupRecordEvent(RandomOutputStream out, Table<?> table, DatabaseRecord oldRecord, DatabaseRecord newRecord, DatabaseEventType eventType/*, RecordsIndex index*/) throws DatabaseException {
		try {
			int start=(int)out.currentPosition();
			out.writeByte(eventType.getByte());

			@SuppressWarnings("SuspiciousMethodCalls")
			int tableIndex=classes.indexOf(table.getClass());
			if (tableIndex<0)
				throw new IOException();
			out.writeUnsignedShort(tableIndex);
			byte[] pks=table.serializeFieldsWithUnknownType(newRecord==null?oldRecord:newRecord, true, false, false);
			out.writeUnsignedShortInt(pks.length);
			out.write(pks);

			switch(eventType)
			{
				case ADD:case UPDATE:
				{
					if (!table.isPrimaryKeysAndForeignKeysSame() && table.getForeignKeysFieldAccessors().size()>0) {
						out.writeBoolean(true);
						byte[] fks=table.serializeFieldsWithUnknownType(newRecord, false, true, false);
						out.writeUnsignedShortInt(fks.length);
						out.write(fks);
					}
					else
						out.writeBoolean(false);

					byte[] nonkeys=table.serializeFieldsWithUnknownType(newRecord, false,false, true);
					if (nonkeys==null || nonkeys.length==0)
					{
						out.writeBoolean(false);
					}
					else
					{
						out.writeBoolean(true);
						out.writeInt(nonkeys.length);
						out.write(nonkeys);
					}

					break;
				}
				case REMOVE:case REMOVE_WITH_CASCADE:
				{
					break;
				}
				default:
					throw new IllegalAccessError();
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

	private void saveHeader(RandomOutputStream out, long backupUTC, boolean referenceFile/*, AtomicReference<RecordsIndex> recordsIndex*/) throws DatabaseException {

		try {
			out.writeLong(backupUTC);
			//recordsIndex.set(new RecordsIndex(backupConfiguration.getMaxIndexSize(), out));

			if (referenceFile)
				saveTablesHeader(out);
			else
				out.writeInt(-1);
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	/*private class RecordsIndex
	{
		private final byte bitsNumber;

		private static final int MAX_SIZE=1<<18;
		private final int[] cache;
		private final boolean[] toFlush;
		private boolean globalFlush;
		private final AbstractMessageDigest messageDigest;
		private final int bytesNumber;
		private final int lastMask;
		RecordsIndex(int maxSize, RandomOutputStream out) throws DatabaseException {
			this(getNumBits(maxSize));
			resetIndex(out);

		}
		RecordsIndex(RandomInputStream in) throws DatabaseException {
			this(readBitsNumber(in));
			Arrays.fill(cache, -2);
			Arrays.fill(toFlush, false);
			globalFlush=false;
		}



		private static byte readBitsNumber(RandomInputStream in) throws DatabaseException {
			try {
				in.seek(RECORDS_INDEX_POSITION);
				return in.readByte();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}


		}

		private RecordsIndex(byte bitsNumber) throws DatabaseException {
			try {
				this.bitsNumber=bitsNumber;
				if (this.bitsNumber<8)
					throw new IOException();
				int size=(1<<this.bitsNumber)*2;
				if (size>MAX_SIZE*4)
					throw new IOException();
				this.cache=new int[size];
				this.toFlush=new boolean[size/2];
				messageDigest= MessageDigestType.SHA2_256.getMessageDigestInstance();
				bytesNumber=bitsNumber/8;
				int shift=bitsNumber-(bytesNumber*8);
				if (shift==0)
					lastMask=0;
				else
					lastMask=(1<<shift)-1;

			} catch (IOException | NoSuchAlgorithmException | NoSuchProviderException e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}


		static byte getNumBits(int maxSize) throws DatabaseException {
			if (MAX_SIZE<maxSize)
				throw new DatabaseException("", new IllegalArgumentException());
			if (maxSize<2048)
				maxSize=2048;
			return (byte)(Math.floor(Math.log10(maxSize/8)/Math.log10(2)));
		}
		static int getListClassPosition(RandomInputStream in) throws DatabaseException {
			try {
				in.seek(RECORDS_INDEX_POSITION);
				byte bitsNumber=in.readByte();
				return RECORDS_INDEX_POSITION+1+(1<<bitsNumber)*8;
			} catch (IOException e) {
				throw  DatabaseException.getDatabaseException(e);
			}
		}
		void resetIndex(RandomOutputStream out) throws DatabaseException {
			try {
				out.seek(RECORDS_INDEX_POSITION);
				out.writeByte(bitsNumber);
				Arrays.fill(cache, -1);
				Arrays.fill(toFlush, true);
				globalFlush=true;


			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		int hashPK(byte[] primaryKey, int off, int len)
		{
			messageDigest.reset();
			messageDigest.update(primaryKey,off, len);
			byte[] d=messageDigest.digest();
			int res=d[0];
			int shift=0;
			for (int i=1;i<bytesNumber;i++)
			{
				res+=((int)d[i])<<(shift+=8);
			}
			if (lastMask!=0)
			{
				res+=((d[bytesNumber] & lastMask)<<(shift+8));
			}
			return res;
		}

		private int getStreamPosition(int i)
		{
			return RECORDS_INDEX_POSITION+1+i*4;
		}

		private void refresh(RandomInputStream in, int i) throws DatabaseException {
			try {
				in.seek(getStreamPosition(i));
				cache[i]=in.readInt();
				cache[i+1]=in.readInt();
				if (cache[i]<-1)
					throw new IOException();
				if (cache[i+1]<-1)
					throw new IOException();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		void writeRecord(RandomOutputStream out, int position, byte[] primaryKey, int off, int len) throws DatabaseException {
			try {
				if (position<=0)
					throw new IOException();
				int index=hashPK(primaryKey, off, len);
				toFlush[index]=true;
				index*=2;
				globalFlush=true;

				if (cache[index]==-2)
				{
					refresh(out.getUnbufferedRandomInputStream(), index);
				}

				if (cache[index]==-1)
				{
					cache[index]=position;
					cache[index+1]=position;
					out.seek(getStreamPosition(index*4));
					out.writeInt(position);
					out.writeInt(position);
				}
				else if (cache[index]>position) {

					cache[index] = position;
					out.seek(getStreamPosition(index*4));
					out.writeInt(position);

				}
				else if (cache[index]<position) {
					cache[index + 1] = position;
					out.seek(getStreamPosition((index+1)*4));
					out.writeInt(position);
				}

			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		void flush(RandomOutputStream out) throws DatabaseException {
			if (globalFlush) {
				for (int i = 0; i < toFlush.length; i++) {
					if (toFlush[i]) {
						try {
							int index = i * 2;
							out.seek(getStreamPosition(index));
							out.writeInt(cache[index]);
							out.writeInt(cache[index + 1]);
						} catch (IOException e) {
							throw DatabaseException.getDatabaseException(e);
						}
						toFlush[i] = false;
					}
				}
				globalFlush = false;
			}
		}


		private Position getResearchInterval(RandomOutputStream out, byte[] primaryKey, int off, int len) throws DatabaseException {
			int index=hashPK(primaryKey, off, len)*2;
			try {
				if (cache[index]==-2) {
					refresh(out.getUnbufferedRandomInputStream(), getStreamPosition(index));
				}
				return new Position(cache[index], cache[index+1]);
			} catch (DatabaseException | IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		int getLastPosition(RandomOutputStream out, Table<?> table, byte[] primaryKey, int off, int len) throws DatabaseException {
			Position interval=getResearchInterval(out, primaryKey, off, len);
			if (interval.start==-1)
				return -1;
			try {
				RandomInputStream ris=out.getRandomInputStream();
				do {

					ris.seek(interval.end+1);
					int tableIndex=ris.readUnsignedShort();
					boolean goToNext=false;
					if (tableIndex<classes.size() && classes.get(tableIndex).equals(table.getClass()))
					{
						int s=ris.readUnsignedShortInt();
						if (s==len)
						{
							for (int i=0;i<len && !goToNext; i++)
							{
								if (primaryKey[i]!=ris.readByte())
									goToNext=true;
							}
							if (!goToNext)
							{
								return interval.end;
							}
						}
					}
					else
						goToNext=true;
					if (goToNext)
					{
						ris.seek(interval.end-4);
						interval.end=ris.readInt();
						while (interval.end==-1)
						{
							ris.seek(ris.currentPosition()-20);
							if (ris.readInt()!=-1) {
								ris.seek(ris.currentPosition() - 8);
								interval.end=ris.readInt();
							}
							else
								break;
						}
					}


				} while (interval.end!=-1 && interval.end>=interval.start);
				return -1;
			}
			catch(IOException e)
			{
				throw DatabaseException.getDatabaseException(e);
			}

		}

		int getFirstPosition(RandomOutputStream out, Table<?> table, byte[] primaryKey, int off, int len) throws DatabaseException {
			Position interval=getResearchInterval(out, primaryKey, off, len);
			if (interval.start==-1)
				return -1;
			try {
				RandomInputStream ris=out.getRandomInputStream();
				do {

					ris.seek(interval.start+1);
					int tableIndex=ris.readUnsignedShort();
					boolean goToNext=false;
					if (tableIndex<classes.size() && classes.get(tableIndex).equals(table.getClass()))
					{
						int s=ris.readUnsignedShortInt();
						if (s==len)
						{
							for (int i=0;i<len && !goToNext; i++)
							{
								if (primaryKey[i]!=ris.readByte())
									goToNext=true;
							}
							if (!goToNext)
							{
								return interval.start;
							}
						}
					}
					else
						goToNext=true;
					if (goToNext)
					{
						ris.seek(interval.end-4);
						interval.end=ris.readInt();
						while (interval.end==-1)
						{
							ris.seek(ris.currentPosition()-20);
							if (ris.readInt()!=-1) {
								ris.seek(ris.currentPosition() - 8);
								interval.end=ris.readInt();
							}
							else
								break;
						}
					}


				} while (interval.end!=-1 && interval.end>=interval.start);
				return -1;
			}
			catch(IOException e)
			{
				throw DatabaseException.getDatabaseException(e);
			}

		}

	}

	private static class Position
	{
		int start;
		int end;

		public Position(int start, int end) throws DatabaseException {
			if (start<-1)
				throw DatabaseException.getDatabaseException(new IOException());
			if (end<-1)
				throw DatabaseException.getDatabaseException(new IOException());
			if ((start==-1) != (end==-1))
				throw DatabaseException.getDatabaseException(new IOException());

			this.start = start;
			this.end = end;
		}
	}*/

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

	private long lastBackupEventUTC=Long.MIN_VALUE;

	private void cleanCache()
	{
		currentFileReference=null;
		currentClassesList=null;
		lastBackupEventUTC=Long.MIN_VALUE;
	}


	private List<Class<? extends Table<?>>> extractClassesList(File file) throws DatabaseException {
		if (currentClassesList==null || currentFileReference!=file)
		{

			try(RandomFileInputStream rfis=new RandomFileInputStream(file)) {
				lastBackupEventUTC=rfis.readLong();

				rfis.seek(LIST_CLASSES_POSITION/*RecordsIndex.getListClassPosition(rfis)*/+4);

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
		if (file==currentFileReference && lastBackupEventUTC!=Long.MIN_VALUE)
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
			if (nextTransactionReference<=0)
				throw new InternalError();
			//next transaction
			out.writeInt(-1);
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
	private void saveTransactionQueue(RandomOutputStream out, int nextTransactionReference, long transactionUTC/*, RecordsIndex index*/) throws DatabaseException {
		try {
			out.writeByte(-1);
			int nextTransaction=(int)out.currentPosition();
			//backup previous transaction reference
			out.writeInt(nextTransactionReference);
			out.seek(LAST_BACKUP_UTC_POSITION);
			//last transaction utc
			out.writeLong(transactionUTC);
			//next transaction of previous transaction
			out.seek(nextTransactionReference);
			out.writeInt(nextTransaction);

			//index.flush(out);
		}
		catch(IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

	}

	private void createEmptyBackupReference() throws DatabaseException {
		boolean notify=false;
		try {
			synchronized (this) {
				int oldLength = 0;
				long oldLastFile;
				if (fileTimeStamps.size() == 0)
					oldLastFile = Long.MAX_VALUE;
				else {
					oldLastFile = fileTimeStamps.get(fileTimeStamps.size() - 1);
					File f = getFile(oldLastFile, fileReferenceTimeStamps.contains(oldLastFile));
					oldLength = (int) f.length();
				}
				try {
					long backupTime = System.currentTimeMillis();
					while (backupTime == oldLastFile) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						backupTime = System.currentTimeMillis();
					}

					int maxBufferSize = backupConfiguration.getMaxStreamBufferSizeForBackupRestoration();
					int maxBuffersNumber = backupConfiguration.getMaxStreamBufferNumberForBackupRestoration();


					File file = initNewFileForBackupReference(backupTime);
					try (RandomOutputStream out = new BufferedRandomOutputStream(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber)) {
						saveHeader(out, backupTime, true/*, index*/);
					/*int nextTransactionReference = saveTransactionHeader(out, backupTime);
					saveTransactionQueue(out, nextTransactionReference, backupTime);*/
					}
				} catch (IOException e) {
					deleteDatabaseFilesFromReferenceToLastFile(oldLastFile, oldLength);
					throw DatabaseException.getDatabaseException(e);
				}
				notify=true;
			}
		}
		finally
		{
			if (notify && backupFileListener!=null)
				backupFileListener.fileListChanged();

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
			synchronized (this) {
				int oldLength = 0;
				long oldLastFile;
				if (fileTimeStamps.size() == 0)
					oldLastFile = Long.MAX_VALUE;
				else {
					oldLastFile = fileTimeStamps.get(fileTimeStamps.size() - 1);
					File file = getFile(oldLastFile, fileReferenceTimeStamps.contains(oldLastFile));
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
											boolean reference = fileReferenceTimeStamps.contains(l);
											try (RandomFileOutputStream rfos = new RandomFileOutputStream(getFile(l, reference), RandomFileOutputStream.AccessMode.READ_AND_WRITE)) {
												rfos.setLength(fileRef);
											}
										} else if (fileRef < l) {
											boolean reference = fileReferenceTimeStamps.contains(l);
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

						File file = initNewFileForBackupReference(currentBackupTime.get());
						final AtomicReference<RandomOutputStream> rout = new AtomicReference<RandomOutputStream>(new BufferedRandomOutputStream(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber));
						try {
							//final AtomicReference<RecordsIndex> index=new AtomicReference<>(null);
							saveHeader(rout.get(), currentBackupTime.get(), true/*, index*/);
							final AtomicInteger nextTransactionReference = new AtomicInteger(saveTransactionHeader(rout.get(), currentBackupTime.get()));
							final ProgressMonitor progressMonitor = backupConfiguration.getProgressMonitorForBackup();
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


							for (Class<? extends Table<?>> c : classes) {
								final Table<?> table = databaseWrapper.getTableInstance(c);


								globalNumberOfSavedRecords.set(databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
									long originalPosition = rout.get().currentPosition();
									long numberOfSavedRecords=globalNumberOfSavedRecords.get();
									long startPosition=0;


									@Override
									public Long run() throws Exception {

										table.getPaginedRecordsWithUnknownType(startPosition==0?-1:startPosition, startPosition==0?-1:Long.MAX_VALUE, new Filter<DatabaseRecord>() {
											RandomOutputStream out = rout.get();


											@Override
											public boolean nextRecord(DatabaseRecord _record) throws DatabaseException {
												backupRecordEvent(out, table, null, _record, DatabaseEventType.ADD/*, index.get()*/);
												try {
													originalPosition = out.currentPosition();
													++startPosition;
													if (progressMonitor != null) {

														++numberOfSavedRecords;
														progressMonitor.setProgress((int) ((numberOfSavedRecords * 1000) / totalRecords));
													}

													if (out.currentPosition() >= backupConfiguration.getMaxBackupFileSizeInBytes()) {
														saveTransactionQueue(rout.get(), nextTransactionReference.get(), currentBackupTime.get()/*, index.get()*/);
														out.close();

														//index.set(null);

														long curTime = System.currentTimeMillis();
														while (curTime == currentBackupTime.get()) {
															try {
																Thread.sleep(1);
															} catch (InterruptedException e) {
																e.printStackTrace();
															}
															curTime = System.currentTimeMillis();
														}
														currentBackupTime.set(curTime);
														File file = initNewFileForBackupIncrement(curTime);
														rout.set(out = new BufferedRandomOutputStream(new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE), maxBufferSize, maxBuffersNumber));
														saveHeader(out, curTime, false/*, index*/);
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
							saveTransactionQueue(rout.get(), nextTransactionReference.get(), currentBackupTime.get()/*, index.get()*/);
						} finally {
							rout.get().close();
						}
						//scanFiles();
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
		}
		finally {
			databaseWrapper.unlockRead();
			lastBackupEventUTC=Long.MIN_VALUE;
			//currentBackupReferenceUTC=Long.MAX_VALUE;
			if (notify && backupFileListener!=null && globalNumberOfSavedRecords.get()>0)
				backupFileListener.fileListChanged();

		}
	}

	/**
	 * Clean old backups
	 *
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
					int istart=fileTimeStamps.indexOf(l)+1;
					int iend=fileTimeStamps.indexOf(fileReferenceTimeStamps.get(i+1));
					if (iend<0)
						throw new IllegalAccessError();
					if (iend<istart)
						throw new IllegalAccessError();
					long lastFile=fileTimeStamps.get(iend-1);
					long limit=extractLastBackupEventUTC(getFile(lastFile, istart==iend));
					if ( limit>= limitUTC) {
						continue;
					}

					concretLimitUTC = lastFile;
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
		return !passive && (!isReady() || fileReferenceTimeStamps.size()==0 || fileReferenceTimeStamps.get(fileReferenceTimeStamps.size()-1)+backupConfiguration.getBackupReferenceDurationInMs()<System.currentTimeMillis());
	}

	boolean isExternalBackupManager()
	{
		return passive;
	}

	private void deleteDatabaseFilesFromReferenceToLastFile(long firstFileReference, int oldLength) throws DatabaseException {
		if (firstFileReference==Long.MAX_VALUE)
			return;
		for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l > firstFileReference || (l==firstFileReference && oldLength<=0)) {
				boolean reference=fileReferenceTimeStamps.contains(l);
				File f = getFile(l, reference);

				if (!f.delete())
					throw new IllegalStateException();
				if (reference)
					fileReferenceTimeStamps.remove(l);
				it.remove();
			}
		}
		if (oldLength>0)
		{
			if (fileTimeStamps.size()==0)
				throw new DatabaseException("Reference not found");
			long l=fileTimeStamps.get(fileTimeStamps.size()-1);
			if (l!=firstFileReference)
				throw new DatabaseException("Reference not found");
			boolean isReference=fileReferenceTimeStamps.contains(l);
			File file=getFile(l, isReference);
			try(RandomFileOutputStream out=new RandomFileOutputStream(file, RandomFileOutputStream.AccessMode.READ_AND_WRITE))
			{
				out.setLength(oldLength);
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
		cleanCache();
		//scanFiles();
	}
	private void deleteDatabaseFilesFromReferenceToFirstFile(long fileReference)
	{

		for (Iterator<Long> it = fileTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				File f = getFile(l, fileReferenceTimeStamps.contains(l));

				if (!f.delete()) {
					System.err.println("Impossible to delete file : " + f);
					if (f.exists())
						System.err.println("The file already exists");
					else
						System.err.println("The file does not exists");
				}
				it.remove();
			}
		}
		for (Iterator<Long> it = fileReferenceTimeStamps.iterator(); it.hasNext(); ) {
			Long l = it.next();
			if (l <= fileReference) {
				it.remove();
			}
		}
		cleanCache();
		//scanFiles();
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
				return extractLastBackupEventUTC(getFile(ts, fileReferenceTimeStamps.get(fileReferenceTimeStamps.size()-1).equals(ts)));
			}
			return Long.MIN_VALUE;
		}
	}
	private final byte[] recordBuffer=new byte[1<<24-1];
	/**
	 * Restore the database to the nearest given date UTC
	 * @param dateUTCInMs the UTC time in milliseconds
	 *
	 * @return true if the given time corresponds to an available backup. False is chosen if the given time is too old to find a corresponding historical into the backups. In this previous case, it is the nearest backup that is chosen.
	 */
	@SuppressWarnings("UnusedReturnValue")
	public boolean restoreDatabaseToDateUTC(long dateUTCInMs) throws DatabaseException {
		return restoreDatabaseToDateUTC(dateUTCInMs, true);
	}
	/**
	 * Restore the database to the nearest given date UTC
	 * @param dateUTCInMs the UTC time in milliseconds
	 * @param chooseNearestBackupIfNoBackupMatch if set to true, and when no backup was found at the given date/time, than choose the older backup
	 * @return true if the given time corresponds to an available backup. False is chosen if the given time is too old to find a corresponding historical into the backups. In this previous case, and if the param <code>chooseNearestBackupIfNoBackupMatch</code>is set to true, than it is the nearest backup that is chosen. Else no restoration is done.
	 */
	public boolean restoreDatabaseToDateUTC(long dateUTCInMs, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
		boolean notify=false;
		try
		{
			databaseWrapper.lockWrite();
			int oldVersion;
			int newVersion;
			File currentFile;
			ProgressMonitor pg;
			long s;
			ArrayList<Table<?>> tbls;
			boolean reference=true;
			LinkedList<Long> listIncrements;
			boolean newVersionLoaded=false;
			boolean res = true;


			synchronized (this) {
				if (fileReferenceTimeStamps.size() == 0)
					return false;
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
							if (fileReferenceTimeStamps.contains(l))
								break;
							lastCurrentRestorationFileUsed = l;

							listIncrements.add(l);
						}
					}

					if (!checkTablesHeader(currentFile))
						throw new DatabaseException("The database backup is incompatible with current database tables");

					databaseWrapper.loadDatabase(databaseConfiguration, true, newVersion);
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
					if (isExternalBackupManager()) {
						BackupRestoreManager internalManager = databaseWrapper.getBackupRestoreManager(this.databaseConfiguration.getPackage());
						if (internalManager != null)
							internalManager.createEmptyBackupReference();
					}
					else
						createEmptyBackupReference();

				} catch (Exception e) {
					lastCurrentRestorationFileUsed=Long.MIN_VALUE;
					if (newVersionLoaded)
						databaseWrapper.deleteDatabase(databaseConfiguration, newVersion);
					throw DatabaseException.getDatabaseException(e);
				}
			}
			final long totalSize = s;
			long progressPosition = 0;
			final ProgressMonitor progressMonitor=pg;
			final ArrayList<Table<?>> tables=tbls;
			final int maxBufferSize = backupConfiguration.getMaxStreamBufferSizeForBackupRestoration();
			final int maxBuffersNumber = backupConfiguration.getMaxStreamBufferNumberForBackupRestoration();

			try{
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
								int count=0;
								@Override
								public Long run() throws Exception {
									++count;
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
										int s = in.readUnsignedShortInt();
										if (s==0)
											throw new IOException();
										in.readFully(recordBuffer, 0, s);
										switch (eventType) {
											case ADD:
											{
												assert eventTypeCode==2;
												HashMap<String, Object> hm=new HashMap<>();
												table.deserializeFields(hm, recordBuffer, 0, s, true, false, false);

												if (in.readBoolean()) {
													s = in.readUnsignedShortInt();
													in.readFully(recordBuffer, 0, s);
													table.deserializeFields(hm, recordBuffer, 0, s, false, true, false);
												}

												if (in.readBoolean()) {
													s = in.readInt();
													if (s<0)
														throw new IOException();
													if (s>0) {
														in.readFully(recordBuffer, 0, s);

														table.deserializeFields(hm, recordBuffer, 0, s, false, false, true);
													}
												}
												table.addRecord(hm);
											}
											break;
											case UPDATE: {
												DatabaseRecord dr = table.getNewRecordInstance(false);
												table.deserializeFields(dr, recordBuffer, 0, s, true, false, false);

												if (in.readBoolean()) {
													s = in.readUnsignedShortInt();
													in.readFully(recordBuffer, 0, s);
													table.deserializeFields(dr, recordBuffer, 0, s, false, true, false);
												}

												if (in.readBoolean()) {
													s = in.readInt();
													if (s<0)
														throw new IOException();
													if (s>0) {
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
										if (in.readInt() != startRecord)
											throw new IOException();
										if (progressMonitor != null && totalSize != 0) {
											progressPosition += in.currentPosition() - startRecord;
											progressMonitor.setProgress((int) ((progressPosition * 1000) / totalSize));
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

				//createBackupReference();
				notify=true;
				return res;

			} catch (Exception e) {
				databaseWrapper.deleteDatabase(databaseConfiguration, newVersion);
				throw DatabaseException.getDatabaseException(e);
			}
			finally {
				lastCurrentRestorationFileUsed=Long.MIN_VALUE;
				lastBackupEventUTC=Long.MIN_VALUE;
				if (notify && backupFileListener!=null)
					backupFileListener.fileListChanged();
			}
		}
		finally {
			databaseWrapper.unlockWrite();
		}
	}
	private void positionForDataRead(RandomInputStream in, boolean reference) throws DatabaseException {
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

	/*/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param record the record to restore (only primary keys are used)
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false).
	 */
	/*public <R extends DatabaseRecord> Reference<R> restoreRecordToDateUTC(long dateUTC, R record, boolean restoreWithCascade) throws DatabaseException {
		return restoreRecordToDateUTC(dateUTC,record, restoreWithCascade, true);
	}*/

/*	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param record the record to restore (only primary keys are used)
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param chooseNearestBackupIfNoBackupMatch if set to true, and when no backup was found at the given date/time, than choose the older backup
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false). It can also occurs if no date correspond to the given date, and if chooseNearestBackupIfNoBackupMatch is equals to false.
	 */
	/*public <R extends DatabaseRecord> Reference<R> restoreRecordToDateUTC(long dateUTC, R record, boolean restoreWithCascade, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
		Table<R> table=databaseWrapper.getTableInstanceFromRecord(record);
		return restoreRecordToDateUTC(dateUTC,restoreWithCascade, chooseNearestBackupIfNoBackupMatch, table, Table.getFields(table.getPrimaryKeysFieldAccessors(), record));
	}
	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param table the concerned table
	 * @param primaryKeys the primary keys of the record to restore.
	 *                Must be formatted as follow : {"field1", value1,"field2", value2, etc.}
	 * @param <R> the record type
	 * @param <T> the table type
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false).
	 */
	/*public <R extends DatabaseRecord, T extends Table<R>> Reference<R> restoreRecordToDateUTC(long dateUTC, boolean restoreWithCascade, T table, Object ... primaryKeys) throws DatabaseException {
		return restoreRecordToDateUTC(dateUTC, restoreWithCascade, true, table, primaryKeys);
	}
	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param chooseNearestBackupIfNoBackupMatch if set to true, and when no backup was found at the given date/time, than choose the older backup
	 * @param table the concerned table
	 * @param primaryKeys the primary keys of the record to restore.
	 *                Must be formatted as follow : {"field1", value1,"field2", value2, etc.}
	 * @param <R> the record type
	 * @param <T> the table type
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false). It can also occurs if no date correspond to the given date, and if chooseNearestBackupIfNoBackupMatch is equals to false.
	 */
	/*public <R extends DatabaseRecord, T extends Table<R>> Reference<R> restoreRecordToDateUTC(long dateUTC, boolean restoreWithCascade, boolean chooseNearestBackupIfNoBackupMatch, T table, Object ... primaryKeys) throws DatabaseException {
		return restoreRecordToDateUTC(dateUTC, restoreWithCascade, chooseNearestBackupIfNoBackupMatch,  table, table.transformToMapField(primaryKeys));
	}


	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param table the concerned table
	 * @param primaryKeys the primary keys of the record to restore
	 * @param <R> the record type
	 * @param <T> the table type
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false).
	 */
	/*public <R extends DatabaseRecord, T extends Table<R>> Reference<R> restoreRecordToDateUTC(long dateUTC, boolean restoreWithCascade, T table, Map<String, Object> primaryKeys)
	{
		return restoreRecordToDateUTC(dateUTC, restoreWithCascade, true, table, primaryKeys);
	}
	/**
	 * Restore the given record to the given date
	 *
	 * @param dateUTC the reference date to use for the restoration
	 * @param restoreWithCascade if true, all foreign key pointing to this record, or pointed by this record will be restored. If this boolean is set to false, this record will not be restored if it is in relation with other records that have been altered.
	 * @param chooseNearestBackupIfNoBackupMatch if set to true, and when no backup was found at the given date/time, than choose the older backup
	 * @param table the concerned table
	 * @param primaryKeys the primary keys of the record to restore
	 * @param <R> the record type
	 * @param <T> the table type
	 * @return the reference of record that have been restored. This reference can contain a null pointer if the new version is a null record. Returns null if the restored has not been applied. It can occurs of the record have foreign keys (pointing to or pointed by) that does not exists or that changed, and that are not enabled to be restored (restoreWithCascade=false). It can also occurs if no date correspond to the given date, and if chooseNearestBackupIfNoBackupMatch is equals to false.
	 */
	/*public <R extends DatabaseRecord, T extends Table<R>> Reference<R> restoreRecordToDateUTC(long dateUTC, boolean restoreWithCascade, boolean chooseNearestBackupIfNoBackupMatch, T table, Map<String, Object> primaryKeys) throws DatabaseException {
		synchronized (this) {
			Long foundFile=null;
			int filePosition=-1;
			if (fileReferenceTimeStamps.size()==0)
				return null;
			for (Long l : fileTimeStamps)
			{
				if (l>dateUTC)
					break;
				else {
					foundFile = l;
					++filePosition;
				}
			}
			if (foundFile==null)
			{
				if (!chooseNearestBackupIfNoBackupMatch)
					return null;
				foundFile=fileTimeStamps.get(0);
				filePosition=0;
				dateUTC=foundFile;
			}
			Long fileReference=null;
			for (int i=filePosition; i>=0;i--)
			{
				long t=fileTimeStamps.get(i);
				if (fileReferenceTimeStamps.contains(t)) {
					fileReference = t;
					break;
				}
			}
			if (fileReference==null)
				throw new IllegalStateException();
			if (!checkTablesHeader(getFile(fileReference, true)))
				throw new DatabaseException("The database backup is incompatible with current database tables");
			try(RandomFileInputStream fis=new RandomFileInputStream(getFile(foundFile, foundFile.equals(fileReference))))
			{

			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}


		}
	}*/

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
			int oldLength = 0;
			long oldLastFile;
			if (fileTimeStamps.size() == 0)
				oldLastFile = Long.MAX_VALUE;
			else {
				oldLastFile = fileTimeStamps.get(fileTimeStamps.size() - 1);
				File file = getFile(oldLastFile, fileReferenceTimeStamps.contains(oldLastFile));
				oldLength = (int) file.length();
			}

			long last = getMaxDateUTCInMS();
			AtomicLong fileTimeStamp = new AtomicLong();
			//AtomicReference<RecordsIndex> index=new AtomicReference<>();

			RandomOutputStream rfos = getFileForBackupIncrementOrCreateIt(fileTimeStamp/*, index*/);
			return new Transaction(fileTimeStamp.get(), last, rfos, /*index.get(), */oldLastFile, oldLength);
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
		RandomOutputStream out;
		private boolean closed=false;
		private long transactionUTC;
		private final int nextTransactionReference;
		private final long fileTimeStamp;
		//private final RecordsIndex index;
		private final int oldLength;
		private final long oldLastFile;



		Transaction(long fileTimeStamp, long lastTransactionUTC, RandomOutputStream out, /*RecordsIndex index, */long oldLastFile, int oldLength) throws DatabaseException {
			//this.index=index;
			this.fileTimeStamp=fileTimeStamp;
			this.lastTransactionUTC = lastTransactionUTC;
			this.out=out;
			this.oldLastFile=oldLastFile;
			this.oldLength=oldLength;
			transactionUTC=System.currentTimeMillis();
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

		final long getBackupPosition() throws DatabaseException {
			try {
				return out.currentPosition();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		final void cancelTransaction(long backupPosition) throws DatabaseException
		{
			try {
				out.setLength(backupPosition);
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		final void cancelTransaction() throws DatabaseException
		{
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

		final void validateTransaction() throws DatabaseException
		{
			synchronized (BackupRestoreManager.this) {


				if (closed)
					return;
				saveTransactionQueue(out, nextTransactionReference, transactionUTC/*, index*/);

				try {

			/*if (!fileTimeStamps.contains(fileTimeStamp)) {
				fileTimeStamps.add(fileTimeStamp);
			}*/
					out.close();
				} catch (IOException e) {
					throw DatabaseException.getDatabaseException(e);
				}
				BackupRestoreManager.this.lastBackupEventUTC = Long.MIN_VALUE;
				if (!computeDatabaseReference.delete())
					throw new DatabaseException("Impossible to delete file : " + computeDatabaseReference);

				closed = true;
			}
			if (backupFileListener!=null && fileTimeStamp!=oldLastFile)
				backupFileListener.fileListChanged();
		}

		final void backupRecordEvent(Table<?> table, TableEvent<?> _de) throws DatabaseException {

			if (closed)
				return;
			++transactionsNumber;

			BackupRestoreManager.this.backupRecordEvent(out, table, _de.getOldDatabaseRecord(), _de.getNewDatabaseRecord(), _de.getType()/*, index*/);
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

	public interface BackupFileListener
	{
		void fileListChanged();
	}

	public BackupFileListener getBackupFileListener() {
		return backupFileListener;
	}

	public void setBackupFileListener(BackupFileListener backupFileListener) {
		this.backupFileListener = backupFileListener;
	}
}
