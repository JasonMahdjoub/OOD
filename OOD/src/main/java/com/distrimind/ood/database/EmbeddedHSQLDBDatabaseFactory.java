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

import java.io.File;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * 
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 2.0.0
 */
public class EmbeddedHSQLDBDatabaseFactory extends DatabaseFactory {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private File file_name;
	private HSQLDBConcurrencyControl concurrencyControl;
	private int cache_rows;
	private int cache_size;
	private int result_max_memory_rows;
	private int cache_free_count;
	private short constructorNb;
	private boolean lockFile;
	private boolean alwaysDeconectAfterOnTransaction;

	protected EmbeddedHSQLDBDatabaseFactory() {
		constructorNb = 0;
	}
	public EmbeddedHSQLDBDatabaseFactory(File _file_name) {
		this(_file_name, false);
	}

	public EmbeddedHSQLDBDatabaseFactory(File _file_name, boolean alwaysDeconectAfterOnTransaction) {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.exists() && !_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is not a directory !");
		file_name = _file_name;
		constructorNb = 1;
		this.alwaysDeconectAfterOnTransaction=alwaysDeconectAfterOnTransaction;
	}

	public EmbeddedHSQLDBDatabaseFactory(File _file_name, boolean alwaysDeconectAfterOnTransaction, HSQLDBConcurrencyControl concurrencyControl, int _cache_rows,
			int _cache_size, int _result_max_memory_rows, int _cache_free_count, boolean lockFile) {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.exists() && !_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is not a directory !");
		file_name = _file_name;
		this.concurrencyControl = concurrencyControl;
		cache_rows = _cache_rows;
		cache_size = _cache_size;
		result_max_memory_rows = _result_max_memory_rows;
		cache_free_count = _cache_free_count;
		constructorNb = 2;
		this.alwaysDeconectAfterOnTransaction=alwaysDeconectAfterOnTransaction;
		this.lockFile=lockFile;
	}

	@Override
	protected DatabaseWrapper newWrapperInstance() throws DatabaseException {
		if (constructorNb == 1)
			return new EmbeddedHSQLDBWrapper(file_name, alwaysDeconectAfterOnTransaction);
		else if (constructorNb == 2)
			return new EmbeddedHSQLDBWrapper(file_name, alwaysDeconectAfterOnTransaction, concurrencyControl, cache_rows, cache_size,
					result_max_memory_rows, cache_free_count, lockFile);
		else
			throw new InternalError("Invalid database factory configuration");
	}

}
