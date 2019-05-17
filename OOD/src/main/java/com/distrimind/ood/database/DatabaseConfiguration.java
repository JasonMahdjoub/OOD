
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
package com.distrimind.ood.database;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.distrimind.ood.i18n.DatabaseMessages;
import com.distrimind.util.ListClasses;
import com.distrimind.util.progress_monitors.ProgressMonitorFactory;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;

import javax.swing.*;

/**
 * Describe a database configuration. A database is defined by its package, and
 * its classes. All classes must be stored into the given package. Others are
 * not taken into account.
 * 
 * @author Jason Mahdjoub
 * @version 2.0.0
 * @since OOD 2.0
 * @see DatabaseLifeCycles
 */
public class DatabaseConfiguration {
	private int version;
	private final Set<Class<? extends Table<?>>> classes;
	private final Package dbPackage;
	private DatabaseConfiguration oldDatabaseTables;
	private DatabaseLifeCycles databaseLifeCycles;
	private BackupConfiguration backupConfiguration;
	/**
	 * The progress monitor's parameter for database upgrade
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseUpgrade;

	/**
	 * The progress monitor's parameter for database initialisation
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseInitialisation;

	public DatabaseConfiguration(int version, Package _package) {
		this(version, _package, ListClasses.getClasses(_package), null, null);
	}
	public DatabaseConfiguration(int version, Package _package, DatabaseLifeCycles callable,
								 DatabaseConfiguration oldVersionOfDatabaseTables)
	{
		this(version, _package, callable, oldVersionOfDatabaseTables, null);
	}
	public DatabaseConfiguration(int version, Package _package, DatabaseLifeCycles callable,
			DatabaseConfiguration oldVersionOfDatabaseTables, BackupConfiguration backupConfiguration) {
		this(version, _package, ListClasses.getClasses(_package), callable, oldVersionOfDatabaseTables, backupConfiguration);
	}

	public DatabaseConfiguration(int version, Package _package, Collection<Class<?>> _classes) {
		this(version, _package, _classes, null, null);
	}
	public DatabaseConfiguration(int version, Package _package, Collection<Class<?>> _classes, DatabaseLifeCycles callable,
								 DatabaseConfiguration oldVersionOfDatabaseTables)
	{
		this(version, _package, _classes, callable, oldVersionOfDatabaseTables, null);
	}

	public int getVersion() {
		return version;
	}

	/**
	 * @return The progress monitor's parameter for database upgrade
	 */
	public ProgressMonitorParameters getProgressMonitorParametersForDatabaseUpgrade() {
		return progressMonitorParametersForDatabaseUpgrade;
	}

	/**
	 * Set the progress monitor's parameter for database upgrade
	 */
	public void setProgressMonitorParametersForDatabaseUpgrade(ProgressMonitorParameters progressMonitorParametersForDatabaseUpgrade) {
		this.progressMonitorParametersForDatabaseUpgrade = progressMonitorParametersForDatabaseUpgrade;
	}

	public ProgressMonitor getProgressMonitorForDatabaseUpgrade()
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
	 */
	public void setProgressMonitorParametersForDatabaseInitialisation(ProgressMonitorParameters progressMonitorParametersForDatabaseInitialisation) {
		this.progressMonitorParametersForDatabaseInitialisation = progressMonitorParametersForDatabaseInitialisation;
	}

	public ProgressMonitor getProgressMonitorForDatabaseInitialisation()
	{
		if (this.progressMonitorParametersForDatabaseInitialisation==null)
		{
			progressMonitorParametersForDatabaseInitialisation=new ProgressMonitorParameters(String.format(DatabaseMessages.INIT_DATABASE.toString(), getPackage().toString()), null, 0, 100);
			progressMonitorParametersForDatabaseInitialisation.setMillisToDecideToPopup(1000);
			progressMonitorParametersForDatabaseInitialisation.setMillisToPopup(1000);
		}
		return ProgressMonitorFactory.getDefaultProgressMonitorFactory().getProgressMonitor(progressMonitorParametersForDatabaseInitialisation);
	}

	@SuppressWarnings("unchecked")
	public DatabaseConfiguration(int version, Package _package, Collection<Class<?>> _classes, DatabaseLifeCycles callable,
			DatabaseConfiguration oldVersionOfDatabaseTables, BackupConfiguration backupConfiguration) {
		if (_classes == null)
			throw new NullPointerException("_classes");
		if (_package == null)
			throw new NullPointerException("_package");
		if (version<0)
			throw new IllegalArgumentException();
		this.version=version;
		classes = new HashSet<>();
		dbPackage = _package;
		if (oldVersionOfDatabaseTables != null && oldVersionOfDatabaseTables.getPackage().equals(_package))
			throw new IllegalArgumentException("The old database version cannot have the same package");
		this.databaseLifeCycles = callable;
		this.oldDatabaseTables = oldVersionOfDatabaseTables;
		for (Class<?> c : _classes) {
			if (c != null && Table.class.isAssignableFrom(c) && c.getPackage().equals(_package)
					&& !Modifier.isAbstract(c.getModifiers()))
				classes.add((Class<? extends Table<?>>) c);
		}
		this.backupConfiguration=backupConfiguration;
	}

	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	public Set<Class<? extends Table<?>>> getTableClasses() {
		return classes;
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
		if (o instanceof DatabaseConfiguration) {
			DatabaseConfiguration dt = (DatabaseConfiguration) o;
			return dt.dbPackage.equals(dbPackage);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return dbPackage.hashCode();
	}

	public DatabaseLifeCycles getDatabaseLifeCycles() {
		return databaseLifeCycles;
	}

	public void setDatabaseLifeCycles(DatabaseLifeCycles _databaseLifeCycles) {
		databaseLifeCycles = _databaseLifeCycles;
	}

	public DatabaseConfiguration getOldVersionOfDatabaseConfiguration() {
		return oldDatabaseTables;
	}

	public void setOldVersionOfDatabaseTables(DatabaseConfiguration _oldDatabaseTables) {
		oldDatabaseTables = _oldDatabaseTables;
	}
}
