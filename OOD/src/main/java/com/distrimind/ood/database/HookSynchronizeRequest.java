package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 5.10.0
 */
public class HookSynchronizeRequest extends AbstractHookRequest {
	Map<String, Boolean> packagesToSynchronize;

	@SuppressWarnings("unused")
	HookSynchronizeRequest()
	{
		super();
	}

	HookSynchronizeRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
						   Map<String, Boolean> packagesToSynchronize, Set<DecentralizedValue> peersInCloud) {
		super(_hostSource, _hostDestination, peersInCloud);
		if (packagesToSynchronize==null)
			throw new NullPointerException();
		if (packagesToSynchronize.size()==0)
			throw new IllegalArgumentException();
		this.packagesToSynchronize=packagesToSynchronize;
	}

	@Override
	public int getInternalSerializedSize() {
		return super.getInternalSerializedSize()+ SerializationTools.getInternalSize(packagesToSynchronize, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
	}

	@Override
	public void writeExternalWithoutSignature(SecuredObjectOutputStream out) throws IOException {
		out.writeMap(packagesToSynchronize, false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
		super.writeExternalWithoutSignature(out);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		try {
			//noinspection unchecked
			packagesToSynchronize=(Map<String, Boolean>)in.readMap(false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
			if (packagesToSynchronize.size()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
		}
		catch (ClassCastException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, e);
		}
		super.readExternal(in);
	}

	public Map<String, Boolean> getPackagesToSynchronize(DecentralizedValue localHostID) throws DatabaseException {
		if (localHostID==null)
			throw new DatabaseException("The local host id was not set");
		if (localHostID.equals(getHostSource()))
			return packagesToSynchronize;
		else
		{
			Map<String, Boolean> m=new HashMap<>();
			packagesToSynchronize.forEach((k, v) -> m.put(k, !v));
			return m;
		}
	}


}
