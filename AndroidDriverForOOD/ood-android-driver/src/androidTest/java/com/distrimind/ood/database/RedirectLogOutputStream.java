package com.distrimind.ood.database;

import android.util.Log;

import java.io.IOException;
import java.io.OutputStream;

public class RedirectLogOutputStream extends OutputStream {
    private String mCache;
    private String tag;

    RedirectLogOutputStream(String tag) {
        this.tag = tag;
    }

    @Override
    public void write(int b) throws IOException {
        if(mCache == null) mCache = "";

        if(((char) b) == '\n'){
            Log.i(tag, mCache);
            mCache = "";
        }else{
            mCache += (char) b;
        }
    }
}