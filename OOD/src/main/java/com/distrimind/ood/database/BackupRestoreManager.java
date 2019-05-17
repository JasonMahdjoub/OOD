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
import com.distrimind.util.io.RandomFileOutputStream;
import com.distrimind.util.io.RandomOutputStream;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public class BackupRestoreManager {
	private ArrayList<Long> fileReferenceTimeStamps;
	private ArrayList<Long> fileTimeStamps;
	private final File backupDirectory;
	private final BackupConfiguration backupConfiguration;
	private static final Pattern fileReferencePattern = Pattern.compile("^backup-ood-([1-9][0-9])*\\.dreference$");
	private static final Pattern fileIncrementPattern = Pattern.compile("^backup-ood-([1-9][0-9])*\\.dincrement");

	private final File computeDatabaseReference;
	private DatabaseWrapper databaseWrapper;

	BackupRestoreManager(DatabaseWrapper databaseWrapper, File backupDirectory, DatabaseConfiguration databaseConfiguration) throws DatabaseException {
		if (backupDirectory==null)
			throw new NullPointerException();
		if (backupDirectory.exists() && backupDirectory.isFile())
			throw new IllegalArgumentException();
		if (databaseConfiguration==null)
			throw new NullPointerException();
		if (databaseWrapper==null)
			throw new NullPointerException();
		this.databaseWrapper=databaseWrapper;
		FileTools.checkFolderRecursive(backupDirectory);
		this.backupDirectory=backupDirectory;
		this.backupConfiguration=databaseConfiguration.getBackupConfiguration();
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
		File file=getFile(dateUTC, false);
		try {
			if (!file.createNewFile())
				throw new DatabaseException("Impossible to create file : "+file);
			//TODO complete position of last remove with cascade for every table

			return file;
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	private File getFileForBackupIncrement() throws DatabaseException {
		if (fileTimeStamps.size()>0)
		{
			Long timeStamp=fileTimeStamps.get(fileTimeStamps.size()-1);
			File file=getFile(timeStamp, false);
			if (!isPartFull(timeStamp, file))
				return file;
		}

		return initNewFileForBackupIncrement(System.currentTimeMillis());

	}


	public boolean isReady()
	{
		return fileTimeStamps.size()>0 && !computeDatabaseReference.exists();
	}

	public long createBackupReference(File backupReferenceLocation) throws DatabaseException
	{
		//TODO complete creation of a new nativeBackup reference
	}

	public int cleanOldBackups() throws DatabaseException
	{
		//TODO clean old backups
	}

	public void activateBackupReferenceCreation() throws DatabaseException {
		try {
			if (!computeDatabaseReference.createNewFile())
				throw new DatabaseException("Impossible to create file "+computeDatabaseReference);
			createIfNecessaryNewBackupReference();
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	boolean doesCreateNewBackupReference()
	{
		return !isReady();
	}

	private int deleteDatabaseFilesFromReferenceToLastFile(long fileReference)
	{
		//TODO complete
	}
	private int deleteDatabaseFilesFromReferenceToFirstFile(long fileReference)
	{
		//TODO complete
	}

	public long getMinDateUTC()
	{

	}

	public long getMaxDateUTC()
	{

	}

	public void restoreDatabaseToDate(Date date)
	{
		//TODO complete
	}

	public void restoreRecordToDate(Date date, DatabaseRecord record)
	{

	}

	boolean createIfNecessaryNewBackupReference() throws DatabaseException {
		if (doesCreateNewBackupReference())
		{
			long reference = createBackupReference(backupDirectory);
			try {

				cleanOldBackups();
				if (!computeDatabaseReference.delete())
					throw new DatabaseException("Impossible to delete file "+computeDatabaseReference);
			}
			catch(DatabaseException e)
			{
				deleteDatabaseFilesFromReferenceToLastFile(reference);
				throw e;
			}

			return true;

		}
		else
		{
			return false;
		}
	}

	void runTransaction(Transaction transaction) throws DatabaseException {
		createIfNecessaryNewBackupReference();
		if (!isReady())
			return;
		File file=getFileForBackupIncrement();
		try(RandomFileOutputStream rfos=new RandomFileOutputStream(file))
		{
			transaction.run(rfos);
		} catch (IOException e) {
			activateBackupReferenceCreation();
			throw DatabaseException.getDatabaseException(e);
		}

	}

	interface Transaction
	{
		void run(RandomOutputStream lastBackupStream) throws DatabaseException;
	}

}
