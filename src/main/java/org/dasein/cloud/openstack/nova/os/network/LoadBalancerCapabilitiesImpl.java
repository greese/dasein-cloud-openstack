package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.*;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;

/**
 * Created by mariapavlova on 22/04/2015.
 */
public class LoadBalancerCapabilitiesImpl extends AbstractCapabilities<NovaOpenStack> implements LoadBalancerCapabilities {
    @Nonnull @Override public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return null;
    }
    public LoadBalancerCapabilitiesImpl(@Nonnull NovaOpenStack cloud) {
        super(cloud);
    }

    @Override public int getMaxPublicPorts() throws CloudException, InternalException {
        return 0;
    }

    @Nonnull @Override public String getProviderTermForLoadBalancer(@Nonnull Locale locale) {
        return null;
    }

    @Nullable @Override public VisibleScope getLoadBalancerVisibleScope() {
        return null;
    }

    @Override public boolean healthCheckRequiresLoadBalancer() throws CloudException, InternalException {
        return false;
    }

    @Override public Requirement healthCheckRequiresName() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Requirement identifyEndpointsOnCreateRequirement() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Requirement identifyListenersOnCreateRequirement() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Requirement identifyVlanOnCreateRequirement() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Requirement identifyHealthCheckOnCreateRequirement() throws CloudException, InternalException {
        return null;
    }

    @Override public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return false;
    }

    @Override public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    @Nonnull @Override public Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Iterable<LbEndpointType> listSupportedEndpointTypes() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Iterable<LbPersistence> listSupportedPersistenceOptions() throws CloudException, InternalException {
        return null;
    }

    @Nonnull @Override public Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        return null;
    }

    @Override public boolean supportsAddingEndpoints() throws CloudException, InternalException {
        return false;
    }

    @Override public boolean supportsMonitoring() throws CloudException, InternalException {
        return false;
    }

    @Override public boolean supportsMultipleTrafficTypes() throws CloudException, InternalException {
        return false;
    }
}
