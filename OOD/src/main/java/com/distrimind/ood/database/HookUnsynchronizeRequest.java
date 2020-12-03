package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 5.10.0
 */
public class HookUnsynchronizeRequest extends AbstractHookRequest {
	Set<String> packagesToUnsynchronize;

	@SuppressWarnings("unused")
	HookUnsynchronizeRequest()
	{
		super();
	}

	HookUnsynchronizeRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
							 Set<String> packagesToUnsynchronize, Set<DecentralizedValue> peersInCloud) {
		super(_hostSource, _hostDestination, peersInCloud);
		if (packagesToUnsynchronize==null)
			throw new NullPointerException();
		if (packagesToUnsynchronize.size()==0)
			throw new IllegalArgumentException();
		this.packagesToUnsynchronize=packagesToUnsynchronize;
	}

	@Override
	public int getInternalSerializedSize() {
		return super.getInternalSerializedSize()+ SerializationTools.getInternalSize(packagesToUnsynchronize, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
	}

	@Override
	public void writeExternalWithoutSignature(SecuredObjectOutputStream out) throws IOException {
		out.writeCollection(packagesToUnsynchronize, false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
		super.writeExternalWithoutSignature(out);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		try {
			//noinspection unchecked
			packagesToUnsynchronize=in.readCollection(false,MAX_PACKAGE_ENCODING_SIZE_IN_BYTES, false);
			if (packagesToUnsynchronize.size()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
		}
		catch (ClassCastException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, e);
		}
		super.readExternal(in);
	}

	public Set<String> getPackagesToUnsynchronize() {
		return packagesToUnsynchronize;
	}


	@Override
	public void messageSent() throws DatabaseException {
		if (this.getHostSource().equals(getDatabaseWrapper().getSynchronizer().getLocalHostID()))
		{
			getDatabaseWrapper().getSynchronizer().receivedHookUnsynchronizeRequest(this);
		}
	}
}
