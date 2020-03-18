package com.distrimind.ood.database.android;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import com.distrimind.util.harddrive.AndroidHardDriveDetect;

import androidx.annotation.Nullable;

class OODOpenHelper extends SQLiteOpenHelper {
    public OODOpenHelper(@Nullable String name) {
        super((Context)AndroidHardDriveDetect.context, name, null, 1, null);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}
