package org.dasein.cloud.openstack.nova.os.ext.hp.cdn;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.CDNCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 05/08/2014
 * Time: 11:31
 */
public class HPCDNCapabilities extends AbstractCapabilities<NovaOpenStack> implements CDNCapabilities {
    public HPCDNCapabilities(@Nonnull NovaOpenStack cloud) { super(cloud); }

    @Override
    public boolean canCreateCDN() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean canDeleteCDN() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean canModifyCDN() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public String getProviderTermForDistribution(@Nonnull Locale locale) {
        return "container";
    }
}
