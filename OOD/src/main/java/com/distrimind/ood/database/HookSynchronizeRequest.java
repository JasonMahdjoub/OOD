package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 5.10.0
 */
public class HookSynchronizeRequest extends AbstractHookRequest {
	Map<String, Boolean> packagesToSynchronize;
	//private HookSynchronizeRequest backRequest;

	@SuppressWarnings("unused")
	HookSynchronizeRequest()
	{
		super();
	}
	/*HookSynchronizeRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
						   Map<String, Boolean> packagesToSynchronize, Set<DecentralizedValue> peersInCloud, HookSynchronizeRequest backRequest) {
		this(_hostSource, _hostDestination, packagesToSynchronize, peersInCloud);
		if (backRequest==null)
			throw new NullPointerException();
		this.backRequest=backRequest;
	}*/
	HookSynchronizeRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
						   Map<String, Boolean> packagesToSynchronize, Set<DecentralizedValue> peersInCloud) {
		super(_hostSource, _hostDestination, peersInCloud);
		if (packagesToSynchronize==null)
			throw new NullPointerException();
		if (packagesToSynchronize.size()==0)
			throw new IllegalArgumentException();
		this.packagesToSynchronize=packagesToSynchronize;
		this.concernedPeers=new HashSet<>(concernedPeers);
		this.concernedPeers.add(_hostSource);
		//this.backRequest=null;
	}

	@Override
	public int getInternalSerializedSizeWithoutSignatures() {
		return super.getInternalSerializedSizeWithoutSignatures()
				+ SerializationTools.getInternalSize(packagesToSynchronize, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES);
				//+SerializationTools.getInternalSize(backRequest);
	}

	@Override
	public void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException {
		out.writeMap(packagesToSynchronize, false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES, false, false);
		//out.writeObject(backRequest, true);
		super.writeExternalWithoutSignatures(out);
	}

	/*public HookSynchronizeRequest getBackRequest() {
		return backRequest;
	}*/

	@Override
	public void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		try {
			packagesToSynchronize=in.readMap(false, MAX_PACKAGE_ENCODING_SIZE_IN_BYTES, false, false, String.class, Boolean.class);
			if (packagesToSynchronize.size()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
			/*backRequest=in.readObject(true);
			if (backRequest!=null)
			{
				if (!backRequest.getHostSource().equals(getHostDestination()))
					throw new MessageExternalizationException(Integrity.FAIL);
				if (!backRequest.getHostDestination().equals(getHostSource()))
					throw new MessageExternalizationException(Integrity.FAIL);
			}*/
		}
		catch (ClassCastException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, e);
		}
		super.readExternalWithoutSignatures(in);
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

	@Override
	public String toString() {
		return "HookSynchronizeRequest{" +
				"concernedPeers=" + concernedPeers +
				", packagesToSynchronize=" + packagesToSynchronize +
				'}';
	}
}
