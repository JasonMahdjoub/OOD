package com.distrimind.ood.database;

import android.util.Log;

import java.io.IOException;
import java.io.OutputStream;

public class RedirectLogErrorStream extends OutputStream {
    private String mCache;
    private String tag;

    RedirectLogErrorStream(String tag) {
        this.tag = tag;
    }

    @Override
    public void write(int b) throws IOException {
        if(mCache == null) mCache = "";

        if(((char) b) == '\n'){
            Log.e(tag, mCache);
            mCache = "";
        }else{
            mCache += (char) b;
        }
    }
}