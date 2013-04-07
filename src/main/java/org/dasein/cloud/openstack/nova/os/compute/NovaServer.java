/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.openstack.nova.os.compute;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VMScalingOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements services supporting interaction with cloud virtual machines.
 * @author George Reese (george.reese@imaginary.com)
 * @version 2012.09 addressed issue with alternate security group lookup in some OpenStack environments (issue #1)
 * @version 2013.02 implemented setting kernel/ramdisk image IDs (see issue #40 in dasein-cloud-core)
 * @version 2013.02 updated with support for Dasein Cloud 2013.02 model
 * @version 2013.02 added support for fetching shell keys (issue #4)
 * @since unknown
 */
public class NovaServer implements VirtualMachineSupport {
    private NovaOpenStack provider;

    static public final String SERVICE = "compute";

    NovaServer(NovaOpenStack provider) { this.provider = provider; }

    @Override
    public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Vertical scaloing is not currently supported");
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nullable String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cloning is not currently supported");
    }

    @Override
    public VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
        return null;
    }

    @Override
    public void disableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public void enableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return "";
    }

    @Override
    public int getCostFactor(@Nonnull VmState state) throws InternalException, CloudException {
        return 100;
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".getProduct(" + productId + ")");
        }
        try {
            for( VirtualMachineProduct product : listProducts(Architecture.I64) ) {
                if( product.getProviderProductId().equals(productId) ) {
                    return product;
                }
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".getProduct()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".getVirtualMachine(" + vmId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/servers", vmId, true);

            if( ob == null ) {
                return null;
            }
            Iterable<IpAddress> ipv4, ipv6;

            IpAddressSupport support = provider.getNetworkServices().getIpAddressSupport();

            if( support != null ) {
                ipv4 = support.listIpPool(IPVersion.IPV4, false);
                ipv6 = support.listIpPool(IPVersion.IPV6, false);
            }
            else {
                ipv4 = ipv6 = Collections.emptyList();
            }
            try {
                if( ob.has("server") ) {
                    JSONObject server = ob.getJSONObject("server");
                    VirtualMachine vm = toVirtualMachine(server, ipv4, ipv6);
                        
                    if( vm != null ) {
                        return vm;
                    }
                }
            }
            catch( JSONException e ) {
                std.error("getVirtualMachine(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers");
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".getVirtualMachine()");
            }
        }
    }

    @Override
    public @Nullable VmStatistics getVMStatistics(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.OPTIONAL);
    }

    @Override
    @Deprecated
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        if( provider.getIdentityServices().getShellKeySupport() == null ) {
            return Requirement.NONE;
        }
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.testContext() != null);
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions options) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".launch(" + options + ")");
        }
        try {
            MachineImage targetImage = provider.getComputeServices().getImageSupport().getImage(options.getMachineImageId());

            if( targetImage == null ) {
                throw new CloudException("No such machine image: " + options.getMachineImageId());
            }
            HashMap<String,Object> wrapper = new HashMap<String,Object>();
            HashMap<String,Object> json = new HashMap<String,Object>();
            NovaMethod method = new NovaMethod(provider);

            json.put("name", options.getHostName());
            if( options.getUserData() != null ) {
                try {
                    json.put("user_data", Base64.encodeBase64String(options.getUserData().getBytes("utf-8")));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
            }
            if( provider.getMinorVersion() == 0 && provider.getMajorVersion() == 1 ) {
                json.put("imageId", String.valueOf(options.getMachineImageId()));
                json.put("flavorId", options.getStandardProductId());
            }
            else {
                if( provider.getProviderName().equals("HP") ) {
                    json.put("imageRef", options.getMachineImageId());
                }
                else {
                    json.put("imageRef", provider.getComputeServices().getImageSupport().getImageRef(options.getMachineImageId()));
                }
                json.put("flavorRef", getFlavorRef(options.getStandardProductId()));
            }
            if( options.getBootstrapKey() != null ) {
                json.put("key_name", options.getBootstrapKey());
            }
            if( options.getFirewallIds().length > 0 ) {
                ArrayList<HashMap<String,Object>> firewalls = new ArrayList<HashMap<String,Object>>();

                for( String id : options.getFirewallIds() ) {
                    Firewall firewall = provider.getNetworkServices().getFirewallSupport().getFirewall(id);

                    if( firewall != null ) {
                        HashMap<String,Object> fw = new HashMap<String, Object>();

                        fw.put("name", firewall.getName());
                        firewalls.add(fw);
                    }
                }
                json.put("security_groups", firewalls);
            }
            if( !targetImage.getPlatform().equals(Platform.UNKNOWN) ) {
                options.getMetaData().put("dsnPlatform", targetImage.getPlatform().name());
            }
            options.getMetaData().put("dsnDescription", options.getDescription());
            json.put("metadata", options.getMetaData());
            wrapper.put("server", json);
            JSONObject result = method.postServers("/servers", null, new JSONObject(wrapper), true);

            if( result.has("server") ) {
                try {
                    Collection<IpAddress> ips = Collections.emptyList();

                    JSONObject server = result.getJSONObject("server");
                    VirtualMachine vm = toVirtualMachine(server, ips, ips);

                    if( vm != null ) {
                        return vm;
                    }
                }
                catch( JSONException e ) {
                    logger.error("launch(): Unable to understand launch response: " + e.getMessage());
                    if( logger.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
            }
            logger.error("launch(): No server was created by the launch attempt, and no error was returned");
            throw new CloudException("No virtual machine was launched");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".launch()");
            }
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String... firewallIds) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewallIds, new Tag[0]);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        VMLaunchOptions options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description);

        if( inVlanId == null && dataCenterId != null ) {
            options.inDataCenter(dataCenterId);
        }
        else if( inVlanId != null && dataCenterId != null ) { // TODO: when vlans are supported in OpenStack proper...
            options.inVlan(null, dataCenterId, inVlanId);
        }
        if( withKeypairId != null ) {
            options.withBoostrapKey(withKeypairId);
        }
        if( withAnalytics ) {
            options.withExtendedAnalytics();
        }
        if( firewallIds != null ) {
            options.behindFirewalls(firewallIds);
        }
        if( tags != null && tags.length > 0 ) {
            HashMap<String,Object> md = new HashMap<String, Object>();

            for( Tag t : tags ) {
                md.put(t.getKey(), t.getValue());
            }
            options.withMetaData(md);
        }
        return launch(options);
    }

    private @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId, @Nonnull JSONObject server) throws InternalException, CloudException {
        try {
            if( server.has("security_groups") ) {
                NetworkServices services = provider.getNetworkServices();
                Collection<Firewall> firewalls = null;

                if( services != null ) {
                    FirewallSupport support = services.getFirewallSupport();

                    if( support != null ) {
                        firewalls = support.list();
                    }
                }
                if( firewalls == null ) {
                    firewalls = Collections.emptyList();
                }
                JSONArray groups = server.getJSONArray("security_groups");
                ArrayList<String> results = new ArrayList<String>();

                for( int i=0; i<groups.length(); i++ ) {
                    JSONObject group = groups.getJSONObject(i);
                    String id = group.has("id") ? group.getString("id") : null;
                    String name = group.has("name") ? group.getString("name") : null;

                    if( id != null || name != null  ) {
                        for( Firewall fw : firewalls ) {
                            if( id != null ) {
                                if( id.equals(fw.getProviderFirewallId()) ) {
                                    results.add(id);
                                }
                            }
                            else if( name.equals(fw.getName()) ) {
                                results.add(fw.getProviderFirewallId());
                            }
                        }
                    }
                }
                return results;
            }
            else {
                NovaMethod method = new NovaMethod(provider);
                JSONObject ob = method.getServers("/servers", vmId + "/os-security-groups", true);

                if( ob == null ) {
                    throw new CloudException("No such server: " + vmId);
                }
                ArrayList<String> results = new ArrayList<String>();

                if( ob.has("security_groups") ) {
                    JSONArray groups = ob.getJSONArray("security_groups");

                    for( int i=0; i<groups.length(); i++ ) {
                        JSONObject group = groups.getJSONObject(i);

                        if( group.has("id") ) {
                            results.add(group.getString("id"));
                        }
                    }
                }
                return results;
            }
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        NovaMethod method = new NovaMethod(provider);
        JSONObject ob = method.getServers("/servers", vmId, true);

        if( ob == null ) {
            return Collections.emptyList();
        }
        try {
            if( ob.has("server") ) {
                JSONObject server = ob.getJSONObject("server");

                return listFirewalls(vmId, server);
            }
            throw new CloudException("No such server: " + vmId);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers");
        }
    }

    public @Nullable String getFlavorRef(@Nonnull String flavorId) throws InternalException, CloudException {
       Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".getFlavorRef(" + flavorId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/flavors", null, true);
                
            try {
                if( ob.has("flavors") ) {
                    JSONArray list = ob.getJSONArray("flavors");
                        
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject p = list.getJSONObject(i);
                        
                        if( p.getString("id").equals(flavorId) ) {
                            JSONArray links = p.getJSONArray("links");
                            String def = null;
                            
                            for( int j=0; j<links.length(); j++ ) {
                                JSONObject link = links.getJSONObject(j);
                                
                                if( link.getString("rel").equals("self") ) {
                                    return link.getString("href");
                                }
                                else {
                                    if( def != null ) {
                                        def = link.optString("href");
                                    }
                                }
                            }
                            return def;
                        }
                    }
                }
            }
            catch( JSONException e ) {
                std.error("listProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors: " + e.getMessage());
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".listProducts()");
            }
        }
    }
    
    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".listProducts()");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context has been established for this request");
            }
            if( architecture.equals(Architecture.I32) ) {
                return Collections.emptyList();
            }
            if( std.isDebugEnabled() ) {
                std.debug("listProducts(): Cache for " + ctx.getRegionId() + " is empty, fetching values from cloud");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/flavors", null, true);
                
            ArrayList<VirtualMachineProduct> products = new ArrayList<VirtualMachineProduct>();
            
            try {
                if( ob != null && ob.has("flavors") ) {
                    JSONArray list = ob.getJSONArray("flavors");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject p = list.getJSONObject(i);
                        VirtualMachineProduct product = toProduct(p);
                        
                        if( product != null ) {
                            products.add(product);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                std.error("listProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors: " + e.getMessage());
            }
            return products;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".listProducts()");
            }
        }
    }

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return Collections.singletonList(Architecture.I64);
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".listVirtualMachines()");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/servers", null, true);
            ArrayList<ResourceStatus> servers = new ArrayList<ResourceStatus>();

            try {
                if( ob != null && ob.has("servers") ) {
                    JSONArray list = ob.getJSONArray("servers");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject server = list.getJSONObject(i);
                        ResourceStatus vm = toStatus(server);

                        if( vm != null ) {
                            servers.add(vm);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                std.error("listVirtualMachines(): Unable to identify expected values in JSON: " + e.getMessage());                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers in " + ob.toString());
            }
            return servers;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".listVirtualMachines()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".listVirtualMachines()");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/servers", null, true);
            ArrayList<VirtualMachine> servers = new ArrayList<VirtualMachine>();

            Iterable<IpAddress> ipv4, ipv6;

            IpAddressSupport support = provider.getNetworkServices().getIpAddressSupport();

            if( support != null ) {
                ipv4 = support.listIpPool(IPVersion.IPV4, false);
                ipv6 = support.listIpPool(IPVersion.IPV6, false);
            }
            else {
                ipv4 = ipv6 = Collections.emptyList();
            }
            try {
                if( ob != null && ob.has("servers") ) {
                    JSONArray list = ob.getJSONArray("servers");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject server = list.getJSONObject(i);
                        VirtualMachine vm = toVirtualMachine(server, ipv4, ipv6);
                        
                        if( vm != null ) {
                            servers.add(vm);
                        }
                        
                    }
                }
            }
            catch( JSONException e ) {
                std.error("listVirtualMachines(): Unable to identify expected values in JSON: " + e.getMessage());                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers in " + ob.toString());
            }
            return servers;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".listVirtualMachines()");
            }
        }
    }

    @Override
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".pause(" + vmId + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsPauseUnpause(vm) ) {
                throw new OperationNotSupportedException("Pause/unpause is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("pause", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".pause()");
            }
        }
    }

    @Override
    public void resume(@Nonnull String vmId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".pause(" + vmId + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsSuspendResume(vm) ) {
                throw new OperationNotSupportedException("Suspend/resume is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("resume", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".pause()");
            }
        }
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".start(" + vmId + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsStartStop(vm) ) {
                throw new OperationNotSupportedException("Start/stop is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("os-start", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".start()");
            }
        }
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        stop(vmId, false);
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + NovaServer.class.getName() + ".stop(" + vmId + "," + force + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsStartStop(vm) ) {
                throw new OperationNotSupportedException("Start/stop is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("os-stop", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".stop()");
            }
        }
    }

    @Override
    public void suspend(@Nonnull String vmId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".suspend(" + vmId + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsSuspendResume(vm) ) {
                throw new OperationNotSupportedException("Suspend/resume is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("suspend", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".suspend()");
            }
        }
    }

    @Override
    public void unpause(@Nonnull String vmId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".unpause(" + vmId + ")");
        }
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            if( !supportsPauseUnpause(vm) ) {
                throw new OperationNotSupportedException("Pause/unpause is not supported in " + provider.getCloudName());
            }
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("unpause", null);

            NovaMethod method = new NovaMethod(provider);

            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".unpause()");
            }
        }
    }

    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaServer.class.getName() + ".reboot(" + vmId + ")");
        }
        try {
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> action = new HashMap<String,Object>();
            
            action.put("type", "HARD");
            json.put("reboot", action);

            NovaMethod method = new NovaMethod(provider);
            
            method.postServers("/servers", vmId, new JSONObject(json), true);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaServer.class.getName() + ".reboot()");
            }            
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return (!provider.isHP() && (!provider.isRackspace() || !provider.getCloudName().contains("Rackspace")));
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return (!provider.isHP() && (!provider.isRackspace() || !provider.getCloudName().contains("Rackspace")));
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return (!provider.isHP() && (!provider.isRackspace() || !provider.getCloudName().contains("Rackspace")));
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".terminate(" + vmId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/servers", vmId);
                    return;
                }
                catch( NovaException e ) {
                    if( e.getHttpCode() != HttpServletResponse.SC_CONFLICT ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { /* ignore */ }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".terminate()");
            }
        }
    }
    
    private @Nullable VirtualMachineProduct toProduct(@Nullable JSONObject json) throws JSONException, InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".toProduct(" + json + ")");
        }
        try {
            if( json == null ) {
                return null;
            }
            VirtualMachineProduct product = new VirtualMachineProduct();
            
            if( json.has("id") ) {
                product.setProviderProductId(json.getString("id"));
            }
            if( json.has("name") ) {
                product.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                product.setDescription(json.getString("description"));
            }
            if( json.has("ram") ) {
                product.setRamSize(new Storage<Megabyte>(json.getInt("ram"), Storage.MEGABYTE));
            }
            if( json.has("disk") ) {
                product.setRootVolumeSize(new Storage<Gigabyte>(json.getInt("disk"), Storage.GIGABYTE));
            }
            product.setCpuCount(1);
            if( product.getProviderProductId() == null ) {
                return null;
            }
            if( product.getName() == null ) {
                product.setName(product.getProviderProductId());
            }
            if( product.getDescription() == null ) {
                product.setDescription(product.getName());
            }
            return product;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("enter - " + NovaServer.class.getName() + ".toProduct()");
            }            
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject server) throws JSONException, InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + NovaServer.class.getName() + ".toStatus(" + server + ")");
        }
        try {
            if( server == null ) {
                return null;
            }
            String serverId = null;

            if( server.has("id") ) {
                serverId = server.getString("id");
            }
            if( serverId == null ) {
                return null;
            }

            VmState state = VmState.PENDING;

            if( server.has("status") ) {
                String s = server.getString("status").toLowerCase();

                if( s.equals("active") ) {
                    state = VmState.RUNNING;
                }
                else if( s.equals("build") ) {
                    state = VmState.PENDING;
                }
                else if( s.equals("deleted") ) {
                    state = VmState.TERMINATED;
                }
                else if( s.equals("suspended") ) {
                    state = VmState.SUSPENDED;
                }
                else if( s.equalsIgnoreCase("paused") ) {
                    state = VmState.PAUSED;
                }
                else if( s.equalsIgnoreCase("stopped") || s.equalsIgnoreCase("shutoff")) {
                    state = VmState.STOPPED;
                }
                else if( s.equalsIgnoreCase("stopping") ) {
                    state = VmState.STOPPING;
                }
                else if( s.equalsIgnoreCase("pausing") ) {
                    state = VmState.PAUSING;
                }
                else if( s.equalsIgnoreCase("suspending") ) {
                    state = VmState.SUSPENDING;
                }
                else if( s.equals("error") ) {
                    return null;
                }
                else if( s.equals("reboot") || s.equals("hard_reboot") ) {
                    state = VmState.REBOOTING;
                }
                else {
                    std.warn("toVirtualMachine(): Unknown server state: " + s);
                    state = VmState.PENDING;
                }
            }
            return new ResourceStatus(serverId, state);
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".toStatus()");
            }
        }
    }

    private @Nullable VirtualMachine toVirtualMachine(@Nullable JSONObject server, Iterable<IpAddress> ipv4, Iterable<IpAddress> ipv6) throws JSONException, InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaServer.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaServer.class.getName() + ".toVirtualMachine(" + server + ")");
        }
        try {
            if( server == null ) {
                return null;
            }
            VirtualMachine vm = new VirtualMachine();
            
            vm.setCurrentState(VmState.RUNNING);
            vm.setArchitecture(Architecture.I64);
            vm.setClonable(false);
            vm.setCreationTimestamp(-1L);
            vm.setImagable(false);
            vm.setLastBootTimestamp(-1L);
            vm.setLastPauseTimestamp(-1L);
            vm.setPausable(false);
            vm.setPersistent(true);
            vm.setPlatform(Platform.UNKNOWN);
            vm.setRebootable(true);
            vm.setProviderOwnerId(provider.getTenantId());
            if( server.has("id") ) {
                vm.setProviderVirtualMachineId(server.getString("id"));
            }
            if( server.has("name") ) {
                vm.setName(server.getString("name"));
            }
            if( server.has("description") ) {
                vm.setDescription(server.getString("description"));
            }
            if( server.has("kernel_id") ) {
                vm.setProviderKernelImageId(server.getString("kernel_id"));
            }
            if( server.has("ramdisk_id") ) {
                vm.setProviderRamdiskImageId(server.getString("ramdisk_id"));
            }
            if( vm.getDescription() == null ) {
                HashMap<String,String> map = new HashMap<String,String>();
                
                if( server.has("metadata") ) {
                    JSONObject md = server.getJSONObject("metadata");
                    
                    if( md.has("dsnDescription") ) {
                        vm.setDescription(md.getString("dsnDescription"));
                    }
                    else if( md.has("Server Label") ) {
                        vm.setDescription(md.getString("Server Label"));
                    }
                    if( md.has("dsnPlatform") ) {
                        try {
                            vm.setPlatform(Platform.valueOf(md.getString("dsnPlatform")));
                        }
                        catch( Throwable ignore ) {
                            // ignore
                        }
                    }
                    String[] keys = JSONObject.getNames(md);
                    
                    if( keys != null ) {
                        for( String key : keys ) {
                            String value = md.getString(key);
                            
                            if( value != null ) {
                                map.put(key, value);
                            }
                        }
                    }
                }
                if( vm.getDescription() == null ) {
                    if( vm.getName() == null ) {
                        vm.setName(vm.getProviderVirtualMachineId());
                    }
                    vm.setDescription(vm.getName());
                }
                if( server.has("hostId") ) {
                    map.put("host", server.getString("hostId"));
                }
                vm.setTags(map);
            }
            if( server.has("image") ) {
                JSONObject img = server.getJSONObject("image");
                
                if( img.has("id") ) {
                    vm.setProviderMachineImageId(img.getString("id"));
                }
            }
            if( server.has("flavor") ) {
                JSONObject f = server.getJSONObject("flavor");
                
                if( f.has("id") ) {
                    vm.setProductId(f.getString("id"));
                }
            }
            else if( server.has("flavorId") ) {
                vm.setProductId(server.getString("flavorId"));
            }
            if( server.has("adminPass") ) {
                vm.setRootPassword("adminPass");
            }
            if( server.has("key_name") ) {
                vm.setProviderShellKeyIds(server.getString("key_name"));
            }
            if( server.has("status") ) {
                String s = server.getString("status").toLowerCase();
                
                if( s.startsWith("active") ) {
                    vm.setCurrentState(VmState.RUNNING);
                }
                else if( s.startsWith("build") ) {
                    vm.setCurrentState(VmState.PENDING);
                }
                else if( s.startsWith("deleted") ) {
                    vm.setCurrentState(VmState.TERMINATED);
                }
                else if( s.startsWith("suspended") ) {
                    vm.setCurrentState(VmState.SUSPENDED);
                }
                else if( s.startsWith("paused") ) {
                    vm.setCurrentState(VmState.PAUSED);
                }
                else if( s.startsWith("stopped") || s.startsWith("shutoff")) {
                    vm.setCurrentState(VmState.STOPPED);
                }
                else if( s.startsWith("stopping") ) {
                    vm.setCurrentState(VmState.STOPPING);
                }
                else if( s.startsWith("pausing") ) {
                    vm.setCurrentState(VmState.PAUSING);
                }
                else if( s.startsWith("suspending") ) {
                    vm.setCurrentState(VmState.SUSPENDING);
                }
                else if( s.startsWith("error") ) {
                    return null;
                }
                else if( s.startsWith("reboot") || s.startsWith("hard_reboot") ) {
                    vm.setCurrentState(VmState.REBOOTING);
                }
                else {
                    std.warn("toVirtualMachine(): Unknown server state: " + s);
                    vm.setCurrentState(VmState.PENDING);
                }
            }
            if( server.has("created") ) {
                vm.setCreationTimestamp(provider.parseTimestamp(server.getString("created")));
            }
            if( server.has("addresses") ) {
                JSONObject addrs = server.getJSONObject("addresses");
                String[] names = JSONObject.getNames(addrs);

                if( names != null && names.length > 0 ) {
                    ArrayList<RawAddress> pub = new ArrayList<RawAddress>();
                    ArrayList<RawAddress> priv = new ArrayList<RawAddress>();

                    for( String name : names ) {
                        JSONArray arr = addrs.getJSONArray(name);

                        for( int i=0; i<arr.length(); i++ ) {
                            RawAddress addr = null;

                            if( (provider.getMinorVersion() == 0 && provider.getMajorVersion() == 1 ) ) {
                                addr = new RawAddress(arr.getString(i).trim());
                            }
                            else {
                                JSONObject a = arr.getJSONObject(i);

                                if( a.has("version") && a.getInt("version") == 4 && a.has("addr") ) {
                                    addr = new RawAddress(a.getString("addr"));
                                }
                            }
                            if( addr != null ) {
                                if( isPublicIpAddress(addr) ) {
                                	pub.add(addr);
                                }
                                else {
                                    priv.add(addr);
                                }
                            }
                        }
                    }
                    vm.setPublicAddresses(pub.toArray(new RawAddress[pub.size()]));
                    vm.setPrivateAddresses(priv.toArray(new RawAddress[priv.size()]));
                }
                RawAddress[] raw = vm.getPublicAddresses();

                if( raw != null ) {
                    for( RawAddress addr : vm.getPublicAddresses() ) {
                        if( addr.getVersion().equals(IPVersion.IPV4) ) {
                            for( IpAddress a : ipv4 ) {
                                if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                    vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                    break;
                                }
                            }
                        }
                        else if( addr.getVersion().equals(IPVersion.IPV6) ) {
                            for( IpAddress a : ipv6 ) {
                                if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                    vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                    break;
                                }
                            }
                        }
                    }
                }
                if( vm.getProviderAssignedIpAddressId() == null ) {
                    raw = vm.getPrivateAddresses();
                    if( raw != null ) {
                        for( RawAddress addr :raw ) {
                            if( addr.getVersion().equals(IPVersion.IPV4) ) {
                                for( IpAddress a : ipv4 ) {
                                    if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                        vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                        break;
                                    }
                                }
                            }
                            else if( addr.getVersion().equals(IPVersion.IPV6) ) {
                                for( IpAddress a : ipv6 ) {
                                    if( a.getRawAddress().getIpAddress().equals(addr.getIpAddress()) ) {
                                        vm.setProviderAssignedIpAddressId(a.getProviderIpAddressId());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if( vm.getProviderAssignedIpAddressId() == null ) {
                for( IpAddress addr : ipv4 ) {
                    String serverId = addr.getServerId();

                    if( serverId != null && serverId.equals(vm.getProviderVirtualMachineId()) ) {
                        vm.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
                        break;
                    }
                }
                if( vm.getProviderAssignedIpAddressId() == null ) {
                    for( IpAddress addr : ipv6 ) {
                        String serverId = addr.getServerId();

                        if( serverId != null && addr.getServerId().equals(vm.getProviderVirtualMachineId()) ) {
                            vm.setProviderAssignedIpAddressId(addr.getProviderIpAddressId());
                            break;
                        }
                    }
                }
            }
            vm.setProviderRegionId(provider.getContext().getRegionId());
            vm.setProviderDataCenterId(vm.getProviderRegionId() + "-a");
            vm.setTerminationTimestamp(-1L);
            if( vm.getProviderVirtualMachineId() == null ) {
                return null;
            }
            vm.setImagable(vm.getCurrentState().equals(VmState.RUNNING));
            vm.setRebootable(vm.getCurrentState().equals(VmState.RUNNING));
            if( vm.getPlatform().equals(Platform.UNKNOWN) ) {
                Platform p = Platform.guess(vm.getName() + " " + vm.getDescription());
                
                if( p.equals(Platform.UNKNOWN) ) {
                    MachineImage img = provider.getComputeServices().getImageSupport().getImage(vm.getProviderMachineImageId());
                    
                    if( img != null ) {
                        p = img.getPlatform();
                    }
                }
                vm.setPlatform(p);
            }
            Iterable<String> fwIds = listFirewalls(vm.getProviderVirtualMachineId(), server);
            int count = 0;

            //noinspection UnusedDeclaration
            for( String id : fwIds ) {
                count++;
            }
            String[] ids = new String[count];
            int i = 0;

            for( String id : fwIds ) {
                ids[i++] = id;
            }
            vm.setProviderFirewallIds(ids);
            return vm;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaServer.class.getName() + ".toVirtualMachine()");
            }
        }
    }

	private boolean isPublicIpAddress(RawAddress addr) {
		 if( addr.getVersion().equals(IPVersion.IPV4) ) {
	            if( addr.getIpAddress().startsWith("10.") || addr.getIpAddress().startsWith("192.168") || addr.getIpAddress().startsWith("169.254") ) {
	                return false;
	            }
	            else if( addr.getIpAddress().startsWith("172.") ) {
	                String[] parts = addr.getIpAddress().split("\\.");

	                if( parts.length != 4 ) {
	                    return true;
	                }
	                int x = Integer.parseInt(parts[1]);

	                if( x >= 16 && x <= 31 ) {
	                    return false;
	                }
	            }
	      }
	      else {
	          if( addr.getIpAddress().startsWith("fd") || addr.getIpAddress().startsWith("fc00:")) {
	              return false;
	          }
	      }
	      return true;
	}

}
