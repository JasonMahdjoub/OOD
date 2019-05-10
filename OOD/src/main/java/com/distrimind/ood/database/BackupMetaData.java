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
import com.distrimind.util.FileTools;
import com.distrimind.util.io.RandomFileOutputStream;
import com.distrimind.util.io.RandomOutputStream;

import javax.swing.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public class BackupMetaData {
	private ArrayList<Long> fileTimeStamps;
	private final File backupDirectory;
	private final BackupConfiguration backupConfiguration;
	private static final Pattern filePattern=Pattern.compile("^backup-ood-([1-9][0-9])*\\.data$");
	private final File computeDatabaseReference;

	BackupMetaData(File backupDirectory, BackupConfiguration backupConfiguration)
	{
		if (backupDirectory==null)
			throw new NullPointerException();
		if (backupDirectory.exists() && backupDirectory.isFile())
			throw new IllegalArgumentException();
		if (backupConfiguration==null)
			throw new NullPointerException();
		FileTools.checkFolderRecursive(backupDirectory);
		this.backupDirectory=backupDirectory;
		this.backupConfiguration=backupConfiguration;
		this.computeDatabaseReference=new File(this.backupDirectory, "computeDatabaseNewReference.query");
		if (this.computeDatabaseReference.exists() && this.computeDatabaseReference.isDirectory())
			throw new IllegalArgumentException();
		scanFiles();

	}

	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	private void scanFiles()
	{
		fileTimeStamps=new ArrayList<>();
		File []files=this.backupDirectory.listFiles();
		if (files==null)
			return;
		for (File f : files)
		{
			if (f.isDirectory())
				continue;
			Matcher m=filePattern.matcher(f.getName());
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
		}
		Collections.sort(fileTimeStamps);
	}

	public File getBackupDirectory() {
		return backupDirectory;
	}

	private File getFile(long timeStamp)
	{
		return new File(backupDirectory, "backup-ood-"+timeStamp+".data");
	}

	private boolean isPartFull(long timeStamp, File file)
	{
		if (file.length()>=backupConfiguration.getMaxBackupFileSizeInBytes())
			return true;
		return timeStamp < System.currentTimeMillis() - backupConfiguration.getMaxBackupFileAgeInMs();
	}

	private File initNewFile(long dateUTC) throws DatabaseException {
		File file=getFile(dateUTC);
		try {
			file.createNewFile();
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
			File file=getFile(timeStamp);
			if (!isPartFull(timeStamp, file))
				return file;
		}

		return initNewFile(System.currentTimeMillis());

	}


	public boolean isReady()
	{
		return fileTimeStamps.size()>0 && !computeDatabaseReference.exists();
	}

	public boolean createIfNecessaryNewBackupReference(ProgressMonitor progressMonitor)
	{
		if (!isReady())
		{
			//TODO complete creation of a new backup reference
		}
		else
		{
			progressMonitor.setProgress(progressMonitor.getMaximum());
			return false;
		}
	}

	public void runTransaction(Transaction transaction) throws DatabaseException {
		if (!isReady())
			return;
		File file=getFileForBackupIncrement();
		try(RandomFileOutputStream rfos=new RandomFileOutputStream(file))
		{
			transaction.run(rfos);
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
		finally {
			try {
				computeDatabaseReference.createNewFile();
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}


	}

	public interface Transaction
	{
		void run(RandomOutputStream lastBackupStream) throws DatabaseException;
	}

}
