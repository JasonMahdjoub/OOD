
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
package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.ListClasses;

import java.lang.reflect.Modifier;
import java.util.*;

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

	private final DatabaseConfigurationParameters databaseConfigurationParameters;
	private final Set<Class<? extends Table<?>>> classes;
	private List<Class<? extends Table<?>>> sortedClasses=null;

	private DatabaseConfiguration oldDatabaseTables;
	private DatabaseLifeCycles databaseLifeCycles;

	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters) {
		this(databaseConfigurationParameters, ListClasses.getClasses(databaseConfigurationParameters.getPackage()), null, null);
	}
	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters, DatabaseLifeCycles callable)
	{
		this(databaseConfigurationParameters, callable, null);
	}
	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters, DatabaseLifeCycles callable,
			DatabaseConfiguration oldVersionOfDatabaseTables) {
		this(databaseConfigurationParameters, ListClasses.getClasses(databaseConfigurationParameters.getPackage()), callable, oldVersionOfDatabaseTables);
	}

	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters, Collection<Class<?>> _classes) {
		this(databaseConfigurationParameters, _classes, null);
	}
	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters, Collection<Class<?>> _classes, DatabaseLifeCycles callable)
	{
		this(databaseConfigurationParameters, _classes, callable, null);
	}



	@SuppressWarnings("unchecked")
	public DatabaseConfiguration(DatabaseConfigurationParameters databaseConfigurationParameters, Collection<Class<?>> _classes, DatabaseLifeCycles callable,
			DatabaseConfiguration oldVersionOfDatabaseTables) {
		if (_classes == null)
			throw new NullPointerException("_classes");
		if (databaseConfigurationParameters == null)
			throw new NullPointerException("_package");
		if (_classes.size()>Short.MAX_VALUE*2+1)
			throw new IllegalArgumentException("Tables number cannot be greater than "+(Short.MAX_VALUE*2+1)+". Here, "+_classes.size()+" tables given.");

		this.databaseConfigurationParameters=databaseConfigurationParameters;
		classes = new HashSet<>();
		if (oldVersionOfDatabaseTables != null && oldVersionOfDatabaseTables.getDatabaseConfigurationParameters().getPackage().equals(databaseConfigurationParameters.getPackage()))
			throw new IllegalArgumentException("The old database version cannot have the same package");
		this.databaseLifeCycles = callable;
		this.oldDatabaseTables = oldVersionOfDatabaseTables;
		for (Class<?> c : _classes) {
			if (c != null && Table.class.isAssignableFrom(c) && c.getPackage().equals(databaseConfigurationParameters.getPackage())
					&& !Modifier.isAbstract(c.getModifiers()))
				classes.add((Class<? extends Table<?>>) c);
		}
	}

	public DatabaseConfigurationParameters getDatabaseConfigurationParameters() {
		return databaseConfigurationParameters;
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
	public List<Class<? extends Table<?>>> getSortedTableClasses() throws DatabaseException {
		if (sortedClasses==null)
			throw new DatabaseException("This configuration was not loaded");
		return sortedClasses;


	}


	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o instanceof DatabaseConfiguration) {
			DatabaseConfiguration dt = (DatabaseConfiguration) o;
			return dt.databaseConfigurationParameters.equals(databaseConfigurationParameters);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return databaseConfigurationParameters.hashCode();
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
