
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

/**
 * The database record class is a generic abstract class destinated to represent
 * a database record with each of its fields. The user must inherit this class
 * to declare a type of record. He must declare it into the correspondent
 * table/class as a public class. Moreover, he must name the child class
 * 'Record'. Every field which must be included into the database must have an
 * annotation ({@link com.distrimind.ood.database.annotations.Field},
 * {@link com.distrimind.ood.database.annotations.PrimaryKey},
 * {@link com.distrimind.ood.database.annotations.AutoPrimaryKey},
 * {@link com.distrimind.ood.database.annotations.RandomPrimaryKey},
 * {@link com.distrimind.ood.database.annotations.NotNull},
 * {@link com.distrimind.ood.database.annotations.Unique},
 * {@link com.distrimind.ood.database.annotations.ForeignKey}). If no annotation
 * is given, the corresponding field will not be added into the database.
 * 
 * Note that the native types are always NotNull. Fields which have the
 * annotation {@link com.distrimind.ood.database.annotations.AutoPrimaryKey}
 * must be 'int' or 'short' values. Fields which have the annotation
 * {@link com.distrimind.ood.database.annotations.RandomPrimaryKey} must be
 * 'long' values. Fields which have the annotation
 * {@link com.distrimind.ood.database.annotations.ForeignKey} must be
 * DatabaseRecord instances.
 *
 * Never alter a field directly throw a function of the child class. Do it with
 * the function
 * {@link com.distrimind.ood.database.Table#updateRecord(DatabaseRecord, java.util.Map)}.
 * Moreover, the constructor of the child class must be protected with no
 * parameter (if this constraint is not respected, an exception will be
 * generated). So it is not possible to instantiate directly this class. To do
 * that, you must use the function
 * {@link com.distrimind.ood.database.Table#addRecord(java.util.Map)}, or the
 * function
 * {@link com.distrimind.ood.database.Table#addRecords(java.util.Map...)}.
 * 
 * The inner database fields do not need to be initialized. However, fields
 * which have no annotation (which are not included into the database) are not
 * concerned.
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 */
public abstract class DatabaseRecord {
	transient boolean __createdIntoDatabase = false;

	public DatabaseRecord() {

	}

}
