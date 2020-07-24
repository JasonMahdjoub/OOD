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

import com.distrimind.ood.i18n.DatabaseMessages;
import com.distrimind.util.progress_monitors.ProgressMonitorDM;
import com.distrimind.util.progress_monitors.ProgressMonitorFactory;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;
import com.distrimind.util.properties.MultiFormatProperties;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DatabaseConfigurationParameters extends MultiFormatProperties {

	public enum SynchronizationType
	{
		NO_SYNCHRONIZATION,
		DECENTRALIZED_SYNCHRONIZATION,
		DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE
	}

	private Package dbPackage;
	private BackupConfiguration backupConfiguration;
	private SynchronizationType synchronizationType;

	/**
	 * The progress monitor's parameter for database upgrade
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseUpgrade=null;

	/**
	 * The progress monitor's parameter for database initialisation
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseInitialisation=null;
	@SuppressWarnings("unused")
	private DatabaseConfigurationParameters()
	{
		super(null);
	}
	public DatabaseConfigurationParameters(Package _package, SynchronizationType synchronizationType) {
		this(_package, synchronizationType, null);
	}
	public DatabaseConfigurationParameters(Package _package, SynchronizationType synchronizationType, BackupConfiguration backupConfiguration) {
		super(null);
		if (_package == null)
			throw new NullPointerException("_package");
		if (synchronizationType == null)
			throw new NullPointerException("_package");
		this.dbPackage=_package;
		this.synchronizationType=synchronizationType;
		this.backupConfiguration=backupConfiguration;
	}

	/**
	 * @return The progress monitor's parameter for database upgrade
	 */
	public ProgressMonitorParameters getProgressMonitorParametersForDatabaseUpgrade() {
		return progressMonitorParametersForDatabaseUpgrade;
	}

	/**
	 * Set the progress monitor's parameter for database upgrade
	 * @param progressMonitorParametersForDatabaseUpgrade the progress monitor parameter
	 */
	public void setProgressMonitorParametersForDatabaseUpgrade(ProgressMonitorParameters progressMonitorParametersForDatabaseUpgrade) {
		this.progressMonitorParametersForDatabaseUpgrade = progressMonitorParametersForDatabaseUpgrade;
	}

	public ProgressMonitorDM getProgressMonitorForDatabaseUpgrade()
	{
		if (this.progressMonitorParametersForDatabaseUpgrade==null)
		{
			progressMonitorParametersForDatabaseUpgrade=new ProgressMonitorParameters(String.format(DatabaseMessages.CONVERT_DATABASE.toString(), getPackage().toString()), null, 0, 100);
			progressMonitorParametersForDatabaseUpgrade.setMillisToDecideToPopup(1000);
			progressMonitorParametersForDatabaseUpgrade.setMillisToPopup(1000);
		}
		return ProgressMonitorFactory.getDefaultProgressMonitorFactory().getProgressMonitor(progressMonitorParametersForDatabaseUpgrade);
	}

	/**
	 * @return The progress monitor's parameter for database initialisation
	 */
	public ProgressMonitorParameters getProgressMonitorParametersForDatabaseInitialisation() {
		return progressMonitorParametersForDatabaseInitialisation;
	}

	/**
	 * Set the progress monitor's parameter for database initialisation
	 * @param progressMonitorParametersForDatabaseInitialisation the progress monitor parameter
	 */
	public void setProgressMonitorParametersForDatabaseInitialisation(ProgressMonitorParameters progressMonitorParametersForDatabaseInitialisation) {
		this.progressMonitorParametersForDatabaseInitialisation = progressMonitorParametersForDatabaseInitialisation;
	}

	public ProgressMonitorDM getProgressMonitorForDatabaseInitialisation()
	{
		if (this.progressMonitorParametersForDatabaseInitialisation==null)
		{
			progressMonitorParametersForDatabaseInitialisation=new ProgressMonitorParameters(String.format(DatabaseMessages.INIT_DATABASE.toString(), getPackage().toString()), null, 0, 100);
			progressMonitorParametersForDatabaseInitialisation.setMillisToDecideToPopup(1000);
			progressMonitorParametersForDatabaseInitialisation.setMillisToPopup(1000);
		}
		return ProgressMonitorFactory.getDefaultProgressMonitorFactory().getProgressMonitor(progressMonitorParametersForDatabaseInitialisation);
	}



	public boolean isSynchronizedWithCentralBackupDatabase() {
		return backupConfiguration!=null && synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE;
	}

	public boolean isDecentralized()
	{
		return synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION ||
				synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE;
	}

	public SynchronizationType getSynchronizationType() {
		return synchronizationType;
	}

	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	public Package getPackage() {
		return dbPackage;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o instanceof DatabaseConfigurationParameters) {
			DatabaseConfigurationParameters dt = (DatabaseConfigurationParameters) o;
			return dt.dbPackage.equals(dbPackage);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return dbPackage.hashCode();
	}

}
