package org.dasein.cloud.openstack.nova.os.network;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.network.AbstractLoadBalancerSupport;
import org.dasein.cloud.network.LoadBalancer;
import org.dasein.cloud.network.LoadBalancerCapabilities;
import org.dasein.cloud.network.LoadBalancerCreateOptions;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.APITrace;

import javax.annotation.Nonnull;

/**
 * Created by mariapavlova on 23/04/2015.
 */
public class LoadBalancerSupportImpl extends AbstractLoadBalancerSupport<NovaOpenStack> {

    static private final Logger logger = Logger.getLogger(LoadBalancerSupportImpl.class);

    private volatile transient LoadBalancerCapabilitiesImpl capabilities;

    LoadBalancerSupportImpl(NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull @Override public LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if (capabilities == null) {
            capabilities = new LoadBalancerCapabilitiesImpl(getProvider());
        }
        return capabilities;
    }

    @Override public boolean isSubscribed() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull String createLoadBalancer(@Nonnull LoadBalancerCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.createLoadBalancer");
        return null;
    }

    @Override
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listLoadBalancers");
        return null;
    }

    @Override
    public void removeLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.remove");

    }

    @Override public LoadBalancer getLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        return super.getLoadBalancer(loadBalancerId);
    }
}