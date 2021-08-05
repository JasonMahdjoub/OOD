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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.i18n.DatabaseMessages;
import com.distrimind.util.ListClasses;
import com.distrimind.util.progress_monitors.ProgressMonitorDM;
import com.distrimind.util.progress_monitors.ProgressMonitorFactory;
import com.distrimind.util.progress_monitors.ProgressMonitorParameters;
import com.distrimind.util.properties.MultiFormatProperties;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
@SuppressWarnings("FieldMayBeFinal")
public class DatabaseSchema extends MultiFormatProperties {



	private Package dbPackage;


	private Set<Class<? extends Table<?>>> classes;
	private transient ArrayList<Class<? extends Table<?>>> sortedClasses=null;
	private DatabaseSchema oldSchema;

	/**
	 * The progress monitor's parameter for database upgrade
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseUpgrade=null;

	/**
	 * The progress monitor's parameter for database initialisation
	 */
	private ProgressMonitorParameters progressMonitorParametersForDatabaseInitialisation=null;
	@SuppressWarnings("unused")
	public DatabaseSchema(Package _package) {
		this(_package, (DatabaseSchema) null);
	}
	public DatabaseSchema(Package _package, Set<Class<?>> classes)  {
		this(_package, classes, null);
	}
	public DatabaseSchema(Package _package, DatabaseSchema oldSchema)  {
		this(_package, ListClasses.getClasses(_package), oldSchema);
	}
	public DatabaseSchema(Package _package, Set<Class<?>> classes, DatabaseSchema oldSchema)  {
		super(null);
		if (_package == null)
			throw new NullPointerException("_package");
		if (classes==null)
			throw new NullPointerException();
		if (classes.contains(null))
			throw new NullPointerException();
		if (classes.size()>Short.MAX_VALUE*2+1)
			throw new IllegalArgumentException("Tables number cannot be greater than "+(Short.MAX_VALUE*2+1)+". Here, "+classes.size()+" tables given.");
		if (oldSchema != null && oldSchema.getPackage().equals(getPackage()))
			throw new IllegalArgumentException("The old database version cannot have the same package");
		this.dbPackage=_package;

		this.classes=new HashSet<>();
		this.oldSchema=oldSchema;
		for (Class<?> c : classes) {
			if (c != null && Table.class.isAssignableFrom(c) && c.getPackage().equals(dbPackage)
					&& !Modifier.isAbstract(c.getModifiers()))
				//noinspection unchecked
				this.classes.add((Class<? extends Table<?>>) c);
		}
		if (this.classes.size()==0)
			throw new IllegalArgumentException();
	}

	public Set<Class<? extends Table<?>>> getTableClasses() {
		return classes;
	}
	void setDatabaseWrapper(DatabaseWrapper wrapper) throws DatabaseException {
		ArrayList<Table<?>> tables=new ArrayList<>(this.classes.size());
		for (Class<? extends Table<?>> c : this.classes)
		{
			tables.add(wrapper.getTableInstance(c));
		}
		Collections.sort(tables);
		sortedClasses=new ArrayList<>(tables.size());
		for (Table<?> t : tables)
			//noinspection unchecked
			sortedClasses.add((Class<? extends Table<?>>)t.getClass());
	}
	public ArrayList<Class<? extends Table<?>>> getSortedTableClasses() throws DatabaseException {
		if (sortedClasses==null)
			throw new DatabaseException("This configuration was not loaded");
		return sortedClasses;


	}

	public DatabaseSchema getOldSchema() {
		return oldSchema;
	}

	void setOldSchema(DatabaseSchema oldSchema) {
		this.oldSchema = oldSchema;
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






	public Package getPackage() {
		return dbPackage;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o instanceof DatabaseSchema) {
			DatabaseSchema dt = (DatabaseSchema) o;
			return dt.dbPackage.equals(dbPackage);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return dbPackage.hashCode();
	}

	@Override
	public String toString() {
		return "DatabaseSchema{" +
				"dbPackage=" + dbPackage +
				", oldSchema=" + oldSchema +
				'}';
	}
}
