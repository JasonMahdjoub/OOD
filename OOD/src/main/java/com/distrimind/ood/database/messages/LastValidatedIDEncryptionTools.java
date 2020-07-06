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
package com.distrimind.ood.database.messages;

import com.distrimind.util.Bits;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.EncryptionSignatureHashDecoder;
import com.distrimind.util.crypto.EncryptionSignatureHashEncoder;
import com.distrimind.util.io.RandomByteArrayOutputStream;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class LastValidatedIDEncryptionTools {
	public static final int MAX_ENCRYPTED_ID_SIZE=(int)EncryptionSignatureHashEncoder.getMaximumOutputLengthWhateverParameters(8);
	public static byte[] encryptID(long id, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream();RandomByteArrayOutputStream out2=new RandomByteArrayOutputStream()) {
			out.writeLong(id);
			out.flush();
			new EncryptionSignatureHashEncoder()
					.withEncryptionProfileProvider(random, encryptionProfileProvider)
					.withRandomInputStream(out.getRandomInputStream())
					.encode(out2);
			out2.flush();
			return out2.getBytes();
		}
	}

	public static long decryptID(EncryptionProfileProvider encryptionProfileProvider, byte[] encryptedData) throws IOException {
		byte []data=new byte[8];
		new EncryptionSignatureHashDecoder()
				.withEncryptionProfileProvider(encryptionProfileProvider)
				.decodeAndCheckHashAndSignaturesIfNecessary(encryptedData, 0, encryptedData.length, data, 0, data.length);
		return Bits.getLong(data, 0);
	}
}
