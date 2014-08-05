package org.dasein.cloud.openstack.nova.os.ext.rackspace.cdn;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.CDNCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 05/08/2014
 * Time: 11:45
 */
public class RackspaceCDNCapabilities extends AbstractCapabilities<NovaOpenStack> implements CDNCapabilities {
    public RackspaceCDNCapabilities(@Nonnull NovaOpenStack provider) { super(provider);}

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
        return "distribution";
    }
}
