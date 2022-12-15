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
package com.distrimind.ood.database;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.distrimind.ood.StorageList;
import com.distrimind.ood.database.exceptions.DatabaseException;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.sql.SQLException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
@RunWith(AndroidJUnit4.class)
public class AndroidHSQLDBTestDatabase extends AbstractAndroidTestDatabase<EmbeddedHSQLDBWrapper> {
    private final InFileEmbeddedHSQLDatabaseFactory factoryA, factoryB;
    public AndroidHSQLDBTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
        factoryA=new InFileEmbeddedHSQLDatabaseFactory(new File("/data/data/"+ StorageList.class.getPackage().getName()+".test/androidDBTestA"),false);
        factoryB=new InFileEmbeddedHSQLDatabaseFactory(new File("/data/data/"+StorageList.class.getPackage().getName()+".test/androidDBTestB"),false);
    }

    @Override
    @Test
    public void allTests() throws DatabaseException, NoSuchProviderException, NoSuchAlgorithmException, IOException, SQLException {
        super.allTests();
    }

    @Override
    InFileEmbeddedHSQLDatabaseFactory getDatabaseFactoryA() {
        return factoryA;
    }

    @Override
    InFileEmbeddedHSQLDatabaseFactory getDatabaseFactoryB() {
        return factoryB;
    }


}
