package org.dasein.cloud.openstack.nova.os.network;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.network.AbstractVLANSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.Networkable;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.APITrace;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

/**
 * Implements Quantum network support for OpenStack clouds with Quantum networking.
 * <p>Created by George Reese: 2/15/13 11:40 PM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class Quantum extends AbstractVLANSupport {
    static private final Logger logger = NovaOpenStack.getLogger(Quantum.class, "std");

    public Quantum(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nonnull String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.createVlan");
        try {
            HashMap<String,Object> wrapper = new HashMap<String,Object>();
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> md = new HashMap<String, Object>();
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());

            md.put("org.dasein.description", description);
            md.put("org.dasein.domain", domainName);
            if( dnsServers.length > 0 ) {
                for(int i=0; i<dnsServers.length; i++ ) {
                    md.put("org.dasein.dns." + (i+1), dnsServers[i]);
                }
            }
            if( ntpServers.length > 0 ) {
                for(int i=0; i<ntpServers.length; i++ ) {
                    md.put("org.dasein.ntp." + (i+1), ntpServers[i]);
                }
            }
            json.put("metadata", md);
            json.put("label", name);
            json.put("cidr", cidr);
            wrapper.put("network", json);
            JSONObject result = method.postServers("/os-networksv2", null, new JSONObject(wrapper), false);

            if( result != null && result.has("network") ) {
                try {
                    JSONObject ob = result.getJSONObject("network");
                    VLAN vlan = toVLAN(result.getJSONObject("network"));

                    if( vlan == null ) {
                        throw new CloudException("No matching network was generated from " + ob.toString());
                    }
                    return vlan;
                }
                catch( JSONException e ) {
                    logger.error("Unable to understand create response: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
            logger.error("No VLAN was created by the create attempt, and no error was returned");
            throw new CloudException("No VLAN was created");

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public int getMaxNetworkInterfaceCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "network interface";
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "subnet";
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return "network";
    }

    @Override
    public VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.getVlan");
        try {
            if( vlanId.equals("00000000-0000-0000-0000-000000000000") || vlanId.equals("11111111-1111-1111-1111-111111111111") ) {
                return super.getVlan(vlanId);
            }
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());
            JSONObject ob = method.getServers("/os-networksv2", vlanId, false);

            try {
                if( ob != null && ob.has("network") ) {
                    VLAN v = toVLAN(ob.getJSONObject("network"));

                    if( v != null ) {
                        return v;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for networks in " + ob.toString());
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.isSubscribed");
        try {
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());
            JSONObject ob = method.getServers("/os-networksv2", null, false);

            return (ob != null && ob.has("networks"));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull Iterable<Networkable> listResources(@Nonnull String inVlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listResources");
        try {
            ArrayList<Networkable> list = new ArrayList<Networkable>();
            ComputeServices services = getProvider().getComputeServices();

            if( services != null ) {
                VirtualMachineSupport vmSupport = services.getVirtualMachineSupport();

                if( vmSupport != null ) {
                    for( VirtualMachine vm : vmSupport.listVirtualMachines() ) {
                        if( inVlanId.equals(vm.getProviderVlanId()) ) {
                            list.add(vm);
                        }
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        ArrayList<IPVersion> versions = new ArrayList<IPVersion>();

        versions.add(IPVersion.IPV4);
        versions.add(IPVersion.IPV6);
        return versions;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlanStatus");
        try {
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());
            JSONObject ob = method.getServers("/os-networksv2", null, false);
            ArrayList<ResourceStatus> networks = new ArrayList<ResourceStatus>();

            try {
                if( ob != null && ob.has("networks") ) {
                    JSONArray list = ob.getJSONArray("networks");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject net = list.getJSONObject(i);
                        ResourceStatus status = toStatus(net);

                        if( status != null ) {
                            networks.add(status);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for networks in " + ob.toString());
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlans");
        try {
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());
            JSONObject ob = method.getServers("/os-networksv2", null, false);
            ArrayList<VLAN> networks = new ArrayList<VLAN>();

            try {
                if( ob != null && ob.has("networks") ) {
                    JSONArray list = ob.getJSONArray("networks");

                    for( int i=0; i<list.length(); i++ ) {
                        VLAN v = toVLAN(list.getJSONObject(i));

                        if( v != null ) {
                            if( v.getProviderVlanId().equals("00000000-0000-0000-0000-000000000000") || v.getProviderVlanId().equals("11111111-1111-1111-1111-111111111111") ) {
                                continue;
                            }
                            networks.add(v);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to identify expected values in JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for networks in " + ob.toString());
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removeVlan");
        try {
            NovaMethod method = new NovaMethod((NovaOpenStack)getProvider());

            method.deleteServers("/os-networksv2", vlanId);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull VLANState toState(@Nonnull String s) {
        if( s.equalsIgnoreCase("active") ) {
            return VLANState.AVAILABLE;
        }
        else if( s.equalsIgnoreCase("build") ) {
            return VLANState.PENDING;
        }
        return VLANState.PENDING;
    }

    private @Nullable ResourceStatus toStatus(@Nonnull JSONObject network) throws CloudException, InternalException {
        try {
            String id = (network.has("id") ? network.getString("id") : null);

            if( id == null ) {
                return null;
            }
            VLANState s = (network.has("status") ? toState(network.getString("status")) : VLANState.AVAILABLE);

            return new ResourceStatus(id, s);
        }
        catch( JSONException e ) {
            throw new CloudException("Invalid JSON from cloud: " + e.getMessage());
        }
    }

    private @Nullable VLAN toVLAN(@Nonnull JSONObject network) throws CloudException, InternalException {
        try {
            VLAN v = new VLAN();

            v.setProviderOwnerId(getContext().getAccountNumber());
            v.setCurrentState(VLANState.AVAILABLE);
            v.setProviderRegionId(getContext().getRegionId());
            if( network.has("id") ) {
                v.setProviderVlanId(network.getString("id"));
            }
            if( network.has("name") ) {
                v.setName(network.getString("name"));
            }
            else if( network.has("label") ) {
                v.setName(network.getString("label"));
            }
            if( network.has("cidr") ) {
                v.setCidr(network.getString("cidr"));
            }
            if( network.has("status") ) {
                v.setCurrentState(toState(network.getString("status")));
            }
            if( network.has("metadata") ) {
                JSONObject md = network.getJSONObject("metadata");
                String[] names = JSONObject.getNames(md);

                if( names != null && names.length > 0 ) {
                    for( String n : names ) {
                        String value = md.getString(n);

                        if( value != null ) {
                            v.setTag(n, value);
                            if( n.equals("org.dasein.description") && v.getDescription() == null ) {
                                v.setDescription(value);
                            }
                            else if( n.equals("org.dasein.domain") && v.getDomainName() == null ) {
                                v.setDomainName(value);
                            }
                            else if( n.startsWith("org.dasein.dns.") && !n.equals("org.dasein.dsn.") && v.getDnsServers().length < 1 ) {
                                ArrayList<String> dns = new ArrayList<String>();

                                try {
                                    int idx = Integer.parseInt(n.substring("org.dasein.dns.".length() + 1));

                                    dns.ensureCapacity(idx);
                                    dns.set(idx-1, value);
                                }
                                catch( NumberFormatException ignore ) {
                                    // ignore
                                }
                                ArrayList<String> real = new ArrayList<String>();

                                for( String item : dns ) {
                                    if( item != null ) {
                                        real.add(item);
                                    }
                                }
                                v.setDnsServers(real.toArray(new String[real.size()]));
                            }
                            else if( n.startsWith("org.dasein.ntp.") && !n.equals("org.dasein.ntp.") && v.getNtpServers().length < 1 ) {
                                ArrayList<String> ntp = new ArrayList<String>();

                                try {
                                    int idx = Integer.parseInt(n.substring("org.dasein.ntp.".length() + 1));

                                    ntp.ensureCapacity(idx);
                                    ntp.set(idx-1, value);
                                }
                                catch( NumberFormatException ignore ) {
                                    // ignore
                                }
                                ArrayList<String> real = new ArrayList<String>();

                                for( String item : ntp ) {
                                    if( item != null ) {
                                        real.add(item);
                                    }
                                }
                                v.setNtpServers(real.toArray(new String[real.size()]));
                            }
                        }
                    }
                }
            }
            if( v.getProviderVlanId() == null ) {
                return null;
            }
            if( v.getCidr() == null ) {
                v.setCidr("0.0.0.0/0");
            }
            if( v.getName() == null ) {
                v.setName(v.getCidr());
                if( v.getName() == null ) {
                    v.setName(v.getProviderVlanId());
                }
            }
            if( v.getDescription() == null ) {
                v.setDescription(v.getName());
            }
            v.setSupportedTraffic(new IPVersion[] { IPVersion.IPV4, IPVersion.IPV6 });
            return v;
        }
        catch( JSONException e ) {
            throw new CloudException("Invalid JSON from cloud: " + e.getMessage());
        }
    }
}
