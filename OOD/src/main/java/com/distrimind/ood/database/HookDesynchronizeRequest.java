package com.distrimind.ood.database;

import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 5.10.0
 */
public class HookDesynchronizeRequest extends AbstractHookRequest {
	Set<String> packagesToUnsynchronize;

	@SuppressWarnings("unused")
	HookDesynchronizeRequest()
	{
		super();
	}

	HookDesynchronizeRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
							 Set<String> packagesToUnsynchronize, Set<DecentralizedValue> peersInCloud) {
		super(_hostSource, _hostDestination, peersInCloud);
		if (packagesToUnsynchronize==null)
			throw new NullPointerException();
		if (packagesToUnsynchronize.size()==0)
			throw new IllegalArgumentException();
		this.packagesToUnsynchronize=packagesToUnsynchronize;
	}

	@Override
	public int getInternalSerializedSizeWithoutEncryption() {
		return super.getInternalSerializedSizeWithoutEncryption()+ SerializationTools.getInternalSize(packagesToUnsynchronize, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
	}

	@Override
	public void writeExternalWithoutEncryption(SecuredObjectOutputStream out) throws IOException {
		out.writeCollection(packagesToUnsynchronize, false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
		super.writeExternalWithoutEncryption(out);
	}

	@Override
	public void readExternalWithoutEncryption(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		try {
			packagesToUnsynchronize=in.readCollection(false,MAX_PACKAGE_ENCODING_SIZE_IN_BYTES, false, String.class);
			if (packagesToUnsynchronize.size()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
		}
		catch (ClassCastException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, e);
		}
		super.readExternalWithoutEncryption(in);
	}

	public Set<String> getPackagesToUnsynchronize() {
		return packagesToUnsynchronize;
	}


}
