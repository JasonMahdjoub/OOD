/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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
package com.distrimind.ood.database.decentralizeddatabasev2;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.decentralizeddatabase.TablePointing;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.crypto.ASymmetricPublicKey;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0
 */

public final class TablePointingV2 extends Table<TablePointingV2.Record> {
	protected TablePointingV2() throws DatabaseException {
		super();
	}

	public static class Record extends DatabaseRecord {
		@PrimaryKey
		public ASymmetricPublicKey id;

		@ForeignKey
		public TablePointedV2.Record table2;

		public Record()
		{

		}

		public Record(TablePointing.Record r)
		{
			this.id=r.id;
			if (r.table2==null)
				this.table2=null;
			else
				this.table2=new TablePointedV2.Record(r.table2);
		}

		@Override
		public String toString() {
			return "TablePointing[" + id + ", " + table2 + "]";
		}

		@SuppressWarnings("MethodDoesntCallSuperMethod")
		@Override
		public Record clone() {
			Record r = new Record();
			r.id = this.id;
			r.table2 = this.table2.clone();
			return r;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof Record)
				return ((Record) o).id.equals(id);
			return false;
		}

	}
}
