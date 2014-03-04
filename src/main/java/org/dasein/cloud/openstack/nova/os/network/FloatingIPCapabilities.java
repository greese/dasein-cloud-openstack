package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.network.IPAddressCapabilities;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Describes the capabilities of Openstack with respect to Dasein ip address operations.
 * <p>Created by Danielle Mayne: 3/04/14 12:05 PM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class FloatingIPCapabilities extends AbstractCapabilities<NovaOpenStack> implements IPAddressCapabilities{
    public FloatingIPCapabilities(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "floating IP";
    }

    @Nonnull
    @Override
    public Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return getVersions().contains(version);
    }

    @Override
    public boolean canBeAssigned(@Nonnull VmState vmState) throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return getVersions().contains(version);
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return getVersions().contains(version);
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return getVersions();
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return false;
    }

    static private volatile List<IPVersion> versions;

    private Collection<IPVersion> getVersions() {
        if( versions == null ) {
            ArrayList<IPVersion> tmp = new ArrayList<IPVersion>();

            tmp.add(IPVersion.IPV4);
            //tmp.add(IPVersion.IPV6);     TODO: when there's API support for IPv6
            versions = Collections.unmodifiableList(tmp);
        }
        return versions;
    }
}
