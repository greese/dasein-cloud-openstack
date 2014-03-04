package org.dasein.cloud.openstack.nova.os.ext.rackspace.lb;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.LbAlgorithm;
import org.dasein.cloud.network.LbEndpointType;
import org.dasein.cloud.network.LbPersistence;
import org.dasein.cloud.network.LbProtocol;
import org.dasein.cloud.network.LoadBalancerAddressType;
import org.dasein.cloud.network.LoadBalancerCapabilities;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Rackspace openstack with respect to Dasein load balancer operations.
 * <p>Created by Danielle Mayne: 3/04/14 11:40 AM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class RackspaceLBCapabilities extends AbstractCapabilities<NovaOpenStack> implements LoadBalancerCapabilities {
    static public final String RESOURCE = "/loadbalancers";
    static public final String SERVICE  = "rax:load-balancer";

    public RackspaceLBCapabilities(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 1;
    }

    @Nonnull
    @Override
    public String getProviderTermForLoadBalancer(@Nonnull Locale locale) {
        return "load balancer";
    }

    @Override
    public boolean healthCheckRequiresLoadBalancer() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public Requirement identifyEndpointsOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Nonnull
    @Override
    public Requirement identifyListenersOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    static private transient Collection<LbAlgorithm> supportedAlgorithms;
    @Nonnull
    @Override
    public Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        if( supportedAlgorithms == null ) {
            ArrayList<LbAlgorithm> algorithms = new ArrayList<LbAlgorithm>();

            algorithms.add(LbAlgorithm.ROUND_ROBIN);
            algorithms.add(LbAlgorithm.LEAST_CONN);
            supportedAlgorithms = Collections.unmodifiableList(algorithms);
        }
        return supportedAlgorithms;
    }

    @Nonnull
    @Override
    public Iterable<LbEndpointType> listSupportedEndpointTypes() throws CloudException, InternalException {
        ArrayList<LbEndpointType> types = new ArrayList<LbEndpointType>();

        types.add(LbEndpointType.IP);
        types.add(LbEndpointType.VM);
        return types;
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Nonnull
    @Override
    public Iterable<LbPersistence> listSupportedPersistenceOptions() throws CloudException, InternalException {
        return Collections.singletonList(LbPersistence.NONE);
    }

    @Nonnull
    @Override
    public Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        Cache<LbProtocol> cache = Cache.getInstance(getProvider(), "lbProtocols", LbProtocol.class, CacheLevel.REGION_ACCOUNT);
        Iterable<LbProtocol> protocols = cache.get(getContext());

        if( protocols != null ) {
            return protocols;
        }
        NovaMethod method = new NovaMethod(getProvider());
        JSONObject ob = method.getResource(SERVICE, RESOURCE, "protocols", false);

        if( ob == null || !ob.has("protocols") || ob.isNull("protocols")) {
            return Collections.singletonList(LbProtocol.RAW_TCP);
        }
        ArrayList<LbProtocol> list = new ArrayList<LbProtocol>();

        list.add(LbProtocol.RAW_TCP);
        try {
            JSONArray matches = ob.getJSONArray("protocols");

            for( int i=0; i<matches.length(); i++ ) {
                JSONObject p = matches.getJSONObject(i);
                String name = (p.has("name") && !p.isNull("name")) ? p.getString("name") : null;

                if( name != null ) {
                    if( name.equalsIgnoreCase("http") ) {
                        list.add(LbProtocol.HTTP);
                    }
                    else if( name.equalsIgnoreCase("https") ) {
                        list.add(LbProtocol.HTTPS);
                    }
                }
            }
        }
        catch( JSONException e ) {
            throw new CloudException("Unable to parse protocols from Rackspace: " + e.getMessage());
        }
        cache.put(getContext(), list);
        return list;
    }

    @Override
    public boolean supportsAddingEndpoints() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsMonitoring() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsMultipleTrafficTypes() throws CloudException, InternalException {
        return false;
    }
}
