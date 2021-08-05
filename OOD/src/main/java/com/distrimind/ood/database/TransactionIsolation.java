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

import java.sql.Connection;

/**
 * This class represent a SqlJet database.
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 1.8
 */
public enum TransactionIsolation {
	/**
	 * transactions are not supported.
	 */
	TRANSACTION_NONE(Connection.TRANSACTION_NONE),
	/**
	 * dirty reads, non-repeatable reads and phantom reads can occur. This level
	 * allows a row changed by one transaction to be read by another transaction
	 * before any changes in that row have been committed (a "dirty read"). If any
	 * of the changes are rolled back, the second transaction will have retrieved an
	 * invalid row.
	 */
	TRANSACTION_READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
	/**
	 * dirty reads are prevented; non-repeatable reads and phantom reads can occur.
	 * This level only prohibits a transaction from reading a row with uncommitted
	 * changes in it.
	 */
	TRANSACTION_READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
	/**
	 * dirty reads and non-repeatable reads are prevented; phantom reads can occur.
	 * This level prohibits a transaction from reading a row with uncommitted
	 * changes in it, and it also prohibits the situation where one transaction
	 * reads a row, a second transaction alters the row, and the first transaction
	 * rereads the row, getting different values the second time (a "non-repeatable
	 * read").
	 */
	TRANSACTION_REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
	/**
	 * A constant indicating that dirty reads, non-repeatable reads and phantom
	 * reads are prevented. This level includes the prohibitions in
	 * <code>TRANSACTION_REPEATABLE_READ</code> and further prohibits the situation
	 * where one transaction reads all rows that satisfy a <code>WHERE</code>
	 * condition, a second transaction inserts a row that satisfies that
	 * <code>WHERE</code> condition, and the first transaction rereads for the same
	 * condition, retrieving the additional "phantom" row in the second read.
	 */
	TRANSACTION_SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

	private final int code;

	TransactionIsolation(int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}

	@SuppressWarnings("ConstantConditions")
	public TransactionIsolation getNext() {
		int num = ordinal() + 1;
		TransactionIsolation[] res = TransactionIsolation.values();
		if (res.length >= num)
			return null;
		else
			return res[num];
	}

	public TransactionIsolation getPrevious() {
		int num = ordinal() - 1;
		TransactionIsolation[] res = TransactionIsolation.values();
		if (num < 0)
			return null;
		else
			return res[num];
	}

	public static TransactionIsolation getFromCode(int code) {
		for (TransactionIsolation ti : TransactionIsolation.values())
			if (ti.getCode() == code)
				return ti;
		return null;
	}
}
