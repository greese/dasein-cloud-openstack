package org.dasein.cloud.openstack.nova.os.ext.hp.db;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.RelationalDatabaseCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 05/08/2014
 * Time: 12:31
 */
public class HPRDBMSCapabilities extends AbstractCapabilities<NovaOpenStack> implements RelationalDatabaseCapabilities {
    public HPRDBMSCapabilities(@Nonnull NovaOpenStack cloud) {super(cloud);}

    @Nonnull
    @Override
    public String getProviderTermForDatabase(Locale locale) {
        return "database";
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot(Locale locale) {
        return "snapshot";
    }

    @Override
    public boolean isSupportsFirewallRules() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsHighAvailability() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsLowAvailability() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSupportsMaintenanceWindows() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsAlterDatabase() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsSnapshots() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull String getProviderTermForBackup( Locale locale ) {
        return "backup"; // Should really throw an unsupported exception but core signature doesn't allow!
    }

    @Override
    public boolean isSupportsDatabaseBackups() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsScheduledDatabaseBackups() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsDemandBackups() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsRestoreBackup() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSupportsDeleteBackup() throws CloudException, InternalException {
        return false;
    }
}
