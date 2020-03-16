package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

public class AndroidSQLiteDatabaseFactory extends DatabaseFactory<AndroidSQLiteDatabaseWrapper> {

    private boolean useExternalCard;
    private String databaseName;
    private String packageName;
    public AndroidSQLiteDatabaseFactory(Package packageName, String databaseName, boolean useExternalCard) {
        this(packageName.getName(), databaseName, useExternalCard);
    }

    public AndroidSQLiteDatabaseFactory(String packageName, String databaseName, boolean useExternalCard) {
        if (packageName==null)
            throw new NullPointerException();
        if (databaseName==null)
            throw new NullPointerException();
        this.useExternalCard = useExternalCard;
        this.databaseName = databaseName;
        this.packageName = packageName;
    }

    @Override
    protected AndroidSQLiteDatabaseWrapper newWrapperInstance() throws DatabaseException {
        return new AndroidSQLiteDatabaseWrapper(packageName, databaseName, useExternalCard);
    }

    public boolean isUseExternalCard() {
        return useExternalCard;
    }

    public void setUseExternalCard(boolean useExternalCard) {
        this.useExternalCard = useExternalCard;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        if (databaseName==null)
            throw new NullPointerException();
        this.databaseName = databaseName;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        if (packageName==null)
            throw new NullPointerException();
        this.packageName = packageName;
    }
    public void setPackageName(Package packageName) {
        if (packageName==null)
            throw new NullPointerException();
        this.packageName = packageName.getName();
    }
}
