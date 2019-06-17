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

import com.distrimind.util.progress_monitors.ProgressMonitorFactory;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import javax.swing.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0.0
 */
public class BackupConfiguration {
	/**
	 * A complete nativeBackup will be done with this regular interval. If will be understood as a nativeBackup reference.
	 */
	private long backupReferenceDurationInMs;
	/**
	 * A nativeBackup reference will be removed with this duration is reached
	 */
	private long maxBackupDurationInMs;

	/**
	 * Max nativeBackup file size in bytes.
	 * When this size is reached, then a new file is created
	 */
	private int maxBackupFileSizeInBytes;

	/**
	 * Max nativeBackup file age in milliseconds.
	 * When this age is reached, then a new file is created
	 */
	private long maxBackupFileAgeInMs;

/*	/**
	 * Max backup's index size in bytes
	 */
	//private int maxIndexSize=16384;

	/**
	 * The progress monitor's parameter
	 */
	private ProgressMonitorParameters progressMonitorParameters;

	public BackupConfiguration(long backupReferenceDurationInMs, long maxBackupDurationInMs,
							   int maxBackupFileSizeInBytes, long maxBackupFileAgeInMs/*, int maxIndexSize*/,
							   ProgressMonitorParameters progressMonitorParameters) {
		if (backupReferenceDurationInMs<0)
			throw new IllegalArgumentException();
		if (maxBackupDurationInMs<0)
			throw new IllegalArgumentException();
		if (maxBackupFileSizeInBytes<0)
			throw new IllegalArgumentException();
		if (maxBackupFileAgeInMs<0)
			throw new IllegalArgumentException();
		if (backupReferenceDurationInMs>maxBackupDurationInMs)
			throw new IllegalArgumentException();
		this.backupReferenceDurationInMs = backupReferenceDurationInMs;
		this.maxBackupDurationInMs = maxBackupDurationInMs;
		this.maxBackupFileSizeInBytes = maxBackupFileSizeInBytes;
		this.maxBackupFileAgeInMs=maxBackupFileAgeInMs;
		this.progressMonitorParameters = progressMonitorParameters;
		//this.maxIndexSize=maxIndexSize;
	}

	public long getBackupReferenceDurationInMs() {
		return backupReferenceDurationInMs;
	}

	public long getMaxBackupDurationInMs() {
		return maxBackupDurationInMs;
	}

	public int getMaxBackupFileSizeInBytes() {
		return maxBackupFileSizeInBytes;
	}

	public long getMaxBackupFileAgeInMs() {
		return maxBackupFileAgeInMs;
	}

	public ProgressMonitor getProgressMonitor()
	{
		ProgressMonitorFactory progressMonitorFactory;
		if (progressMonitorParameters ==null)
			return null;
		else
			progressMonitorFactory=ProgressMonitorFactory.getDefaultProgressMonitorFactory();
		return progressMonitorFactory.getProgressMonitor(progressMonitorParameters);
	}

	public void setProgressMonitorParameters(ProgressMonitorParameters progressMonitorParameters) {
		this.progressMonitorParameters = progressMonitorParameters;
	}

	/*public int getMaxIndexSize() {
		return maxIndexSize;
	}*/

	private static final int maxStreamBufferSizeForBackupRestoration=2097152;

	int getMaxStreamBufferSizeForBackupRestoration()
	{
		return Math.min(maxStreamBufferSizeForBackupRestoration, getMaxBackupFileSizeInBytes());
	}
	int getMaxStreamBufferNumberForBackupRestoration()
	{
		return maxStreamBufferSizeForBackupRestoration>getMaxBackupFileSizeInBytes()?1:2;
	}
	int getMaxStreamBufferSizeForTransaction()
	{
		return 8192;
	}
	int getMaxStreamBufferNumberForTransaction()
	{
		return 3;
	}

}
