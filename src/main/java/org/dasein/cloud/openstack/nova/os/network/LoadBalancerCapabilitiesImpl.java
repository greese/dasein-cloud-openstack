package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.*;
import org.dasein.cloud.network.*;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

/**
 * Created by mariapavlova on 22/04/2015.
 */
public class LoadBalancerCapabilitiesImpl extends AbstractCapabilities<NovaOpenStack> implements LoadBalancerCapabilities {
    public LoadBalancerCapabilitiesImpl(@Nonnull NovaOpenStack cloud) {
        super(cloud);
    }

    @Override
    public @Nonnull LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 1;
    }

    @Override
    public @Nonnull String getProviderTermForLoadBalancer(@Nonnull Locale locale) {
        return "pool";
    }

    @Override
    public @Nullable VisibleScope getLoadBalancerVisibleScope() {
        return null;
    }

    @Override
    public boolean healthCheckRequiresLoadBalancer() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull Requirement healthCheckRequiresName() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyEndpointsOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyListenersOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public @Nonnull Requirement identifyVlanOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public @Nonnull Requirement identifyHealthCheckOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        return Arrays.asList(LbAlgorithm.LEAST_CONN, LbAlgorithm.ROUND_ROBIN, LbAlgorithm.SOURCE);
    }

    @Override
    public @Nonnull Iterable<LbEndpointType> listSupportedEndpointTypes() throws CloudException, InternalException {
        return Arrays.asList(LbEndpointType.IP);
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Arrays.asList(IPVersion.IPV4, IPVersion.IPV6);
    }

    @Override
    public @Nonnull Iterable<LbPersistence> listSupportedPersistenceOptions() throws CloudException, InternalException {
        return Arrays.asList(LbPersistence.COOKIE, LbPersistence.SUBNET, LbPersistence.NONE);
    }

    @Override
    public @Nonnull Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        // CAVEAT: HTTPS is excluded for now since there's no way in Neutron API to manage SSL certificates.
        // LBaaS is supposed to address that through Barbican, but as of time of write this was not available
        // or configured in our Juno stack.
        return Arrays.asList(LbProtocol.HTTP, LbProtocol.RAW_TCP);
    }

    @Override
    public boolean supportsAddingEndpoints() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsMonitoring() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsMultipleTrafficTypes() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull NamingConstraints getLoadBalancerNamingConstraints(){
        return NamingConstraints.getAlphaNumeric(1, 100);
    }

    @Override
    public boolean supportsSslCertificateStore(){
        return false;
    }
}
