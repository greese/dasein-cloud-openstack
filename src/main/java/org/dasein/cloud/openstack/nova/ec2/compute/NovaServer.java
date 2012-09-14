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

package org.dasein.cloud.openstack.nova.ec2.compute;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.codec.binary.Base64;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.ec2.NovaEC2;
import org.dasein.cloud.openstack.nova.ec2.NovaException;
import org.dasein.cloud.openstack.nova.ec2.NovaMethod;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class NovaServer implements VirtualMachineSupport {

	static public final String DESCRIBE_INSTANCES    = "DescribeInstances";
	static public final String GET_CONSOLE_OUTPUT    = "GetConsoleOutput";
	static public final String GET_METRIC_STATISTICS = "GetMetricStatistics";
	static public final String GET_PASSWORD_DATA     = "GetPasswordData";
	static public final String MONITOR_INSTANCES     = "MonitorInstances";
    static public final String REBOOT_INSTANCES      = "RebootInstances";
	static public final String RUN_INSTANCES         = "RunInstances";
    static public final String START_INSTANCES       = "StartInstances";
    static public final String STOP_INSTANCES        = "StopInstances";
	static public final String TERMINATE_INSTANCES   = "TerminateInstances";
	static public final String UNMONITOR_INSTANCES   = "UnmonitorInstances";
	
    static private List<VirtualMachineProduct> sixtyFours;
    static private List<VirtualMachineProduct> thirtyTwos;

    static {
        InputStream input = NovaServer.class.getResourceAsStream("/nova/server-products.xml");
        
        if( input != null ) {
            try {
                Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(input);
                ArrayList<VirtualMachineProduct> x32s = new ArrayList<VirtualMachineProduct>();
                ArrayList<VirtualMachineProduct> x64s = new ArrayList<VirtualMachineProduct>();
                NodeList products = doc.getElementsByTagName("product");
                VirtualMachineProduct product;
                
                for( int i=0; i<products.getLength(); i++ ) {
                    Node prd = products.item(i);
                    
                    boolean x32 = prd.getAttributes().getNamedItem("x32").getNodeValue().trim().equalsIgnoreCase("true");
                    boolean x64 = prd.getAttributes().getNamedItem("x64").getNodeValue().trim().equalsIgnoreCase("true");
                    
                    product = new VirtualMachineProduct();
                    product.setProviderProductId(prd.getAttributes().getNamedItem("productId").getNodeValue().trim());
                    product.setRootVolumeSize(new Storage<Gigabyte>(Integer.parseInt(prd.getAttributes().getNamedItem("diskSizeInGb").getNodeValue().trim()), Storage.GIGABYTE));
                    product.setRamSize(new Storage<Megabyte>(Integer.parseInt(prd.getAttributes().getNamedItem("ramInMb").getNodeValue().trim()), Storage.MEGABYTE));
                    product.setCpuCount(Integer.parseInt(prd.getAttributes().getNamedItem("cpuCount").getNodeValue().trim()));
                    
                    NodeList attrs = prd.getChildNodes();
                    
                    for( int j=0; j<attrs.getLength(); j++ ) {
                        Node attr = attrs.item(j);
                        
                        if( attr.getNodeName().equals("name") ) {
                            product.setName(attr.getFirstChild().getNodeValue().trim());
                        }
                        else if( attr.getNodeName().equals("description") ) {
                            product.setDescription(attr.getFirstChild().getNodeValue().trim());
                        }
                    }
                    if( x32 ) {
                        x32s.add(product);
                    }
                    if( x64 ) {
                        x64s.add(product);
                    }
                }
                thirtyTwos = Collections.unmodifiableList(x32s);
                sixtyFours = Collections.unmodifiableList(x64s);
            }
            catch( Throwable t ) {
                t.printStackTrace();
            }
        }
        else {
            ArrayList<VirtualMachineProduct> sizes = new ArrayList<VirtualMachineProduct>();
            VirtualMachineProduct product = new VirtualMachineProduct();
            
            product = new VirtualMachineProduct();
            product.setProviderProductId("m1.small");
            product.setName("Small Instance (m1.small)");
            product.setDescription("Small Instance (m1.small)");
            product.setCpuCount(1);
            product.setRootVolumeSize(new Storage<Gigabyte>(160, Storage.GIGABYTE));
            product.setRamSize(new Storage<Megabyte>(1700, Storage.MEGABYTE));
            sizes.add(product);        
    
            product = new VirtualMachineProduct();
            product.setProviderProductId("c1.medium");
            product.setName("High-CPU Medium Instance (c1.medium)");
            product.setDescription("High-CPU Medium Instance (c1.medium)");
            product.setCpuCount(5);
            product.setRootVolumeSize(new Storage<Gigabyte>(350, Storage.GIGABYTE));
            product.setRamSize(new Storage<Megabyte>(1700, Storage.MEGABYTE));
            sizes.add(product);   
    
            product = new VirtualMachineProduct();
            product.setProviderProductId("m1.large");
            product.setName("Large Instance (m1.large)");
            product.setDescription("Large Instance (m1.large)");
            product.setCpuCount(4);
            product.setRootVolumeSize(new Storage<Gigabyte>(850, Storage.GIGABYTE));
            product.setRamSize(new Storage<Megabyte>(7500, Storage.MEGABYTE));
            sizes.add(product); 
            
            product = new VirtualMachineProduct();
            product.setProviderProductId("m1.xlarge");
            product.setName("Extra Large Instance (m1.xlarge)");
            product.setDescription("Extra Large Instance (m1.xlarge)");
            product.setCpuCount(8);
            product.setRootVolumeSize(new Storage<Gigabyte>(1690, Storage.GIGABYTE));
            product.setRamSize(new Storage<Megabyte>(15000, Storage.MEGABYTE));
            sizes.add(product); 
            
            product = new VirtualMachineProduct();
            product.setProviderProductId("c1.xlarge");
            product.setName("High-CPU Extra Large Instance (c1.xlarge)");
            product.setDescription("High-CPU Extra Large Instance (c1.xlarge)");
            product.setCpuCount(20);
            product.setRootVolumeSize(new Storage<Gigabyte>(1690, Storage.GIGABYTE));
            product.setRamSize(new Storage<Megabyte>(7000, Storage.MEGABYTE));
            sizes.add(product); 
            
            thirtyTwos = Collections.unmodifiableList(sizes);
            sixtyFours = Collections.unmodifiableList(sizes);
        }
    }
    
	private NovaEC2 provider = null;
	
	NovaServer(NovaEC2 provider) {
		this.provider = provider;
	}

	@Override
	public void start(@Nonnull String instanceId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), START_INSTANCES);
        NovaMethod method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( NovaException e ) {
            NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
            throw new CloudException(e);
        }
	}
	
    public VirtualMachine clone(String serverId, String intoDcId, String name, String description, boolean powerOn, String ... firewallIds) throws InternalException, CloudException {
        /*
        final ArrayList<Volume> oldVolumes = new ArrayList<Volume>();
        final ArrayList<Volume> newVolumes = new ArrayList<Volume>();
        final String id = serverId;
        final String zoneId = inZoneId;

        for( Volume volume : provider.getVolumeServices().list() ) {
            String svr = volume.getServerId();
            
            if( svr == null || !svr.equals(serverId)) {
                continue;
            }
            oldVolumes.add(volume);
        }
        Callable<ServerImage> imageTask = new Callable<ServerImage>() {
            @Override
            public ServerImage call() throws Exception {
                provider.getImageServices().create(id);
            }
            
        };
        Callable<Boolean> snapshotTask = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                for( Volume volume : oldVolumes ) {
                    String snapshotId = provider.getSnapshotServices().create(volume.getProviderVolumeId(), "Clone of " + volume.getName());
                    String volumeId = provider.getVolumeServices().create(snapshotId, volume.getSizeInGigabytes(), zoneId);
                    
                    newVolumes.add(provider.getVolumeServices().getVolume(volumeId));
                }
                return true;
            }
        };
        */
        throw new OperationNotSupportedException("AWS instances cannot be cloned.");
    }


    @Override
    public void enableAnalytics(String instanceId) throws InternalException, CloudException {
        // NO-OP
    }
    
	private Architecture getArchitecture(String size) {
		if( size.equals("m1.small") || size.equals("c1.medium") ) {
			return Architecture.I32;
		}
		else {
			return Architecture.I64;
		}
	}
    
	@Override
	public String getConsoleOutput(String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), GET_CONSOLE_OUTPUT);
		long timestamp = -1L;
		String output = null;
		NovaMethod method;
        NodeList blocks;
		Document doc;
        
		parameters.put("InstanceId", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return null;
        	}
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("timestamp");
        for( int i=0; i<blocks.getLength(); i++ ) {
            String ts = blocks.item(i).getFirstChild().getNodeValue();
        	
            timestamp = provider.parseTime(ts);
        	if( timestamp > -1L ) {
        		break;
        	}
        }
        blocks = doc.getElementsByTagName("output");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	Node item = blocks.item(i);
        	
        	if( item.hasChildNodes() ) {
        	    output = item.getFirstChild().getNodeValue().trim();
        	    if( output != null ) {
        	        break;
        	    }
        	}
        }
        try {
			return new String(Base64.decodeBase64(output.getBytes("utf-8")), "utf-8");
		} 
        catch( UnsupportedEncodingException e ) {
        	NovaEC2.getLogger(NovaServer.class, "std").error(e);
        	e.printStackTrace();
        	throw new InternalException(e);
		}
	}

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
	public Iterable<String> listFirewalls(String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_INSTANCES);
		ArrayList<String> firewalls = new ArrayList<String>();
		NovaMethod method;
        NodeList blocks;
		Document doc;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return firewalls;
        	}
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("groupSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList items = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<items.getLength(); j++ ) {
            	Node item = items.item(j);
            	
            	if( item.getNodeName().equals("item") ) {
            		NodeList ids = item.getChildNodes();
            		
            		for( int k=0; k<ids.getLength(); k++ ) {
            			Node id = ids.item(k);
            			
            			if( id.hasChildNodes() ) {
            			    firewalls.add(id.getFirstChild().getNodeValue().trim());
            			}
            		}
            	}
            }
        }
        return firewalls;
	}
	
	@Override
	public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
		return "instance";
	}

	@Override
	public @Nullable VirtualMachine getVirtualMachine(@Nonnull String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_INSTANCES);
		NovaMethod method;
        NodeList blocks;
		Document doc;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidInstanceID") ) {
        		return null;
        	}
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		VirtualMachine server = toVirtualMachine(instance);
            		
            		if( server.getProviderVirtualMachineId().equals(instanceId) ) {
            			return server;
            		}
            	}
            }
        }
        return null;
	}
	
	@Override
	public VirtualMachineProduct getProduct(String sizeId) {
        System.out.println("Looking for: " + sizeId + " among " + get64s());
        for( VirtualMachineProduct product : get64s() ) {
            if( product.getProviderProductId().equals(sizeId) ) {
                return product;
            }
        }
        for( VirtualMachineProduct product : get32s() ) {
            if( product.getProviderProductId().equals(sizeId) ) {
                return product;
            }
        }
        return null;
	}
	
	private VmState getServerState(String state) {
        if( state.equals("pending") ) {
            return VmState.PENDING;
        }
        else if( state.equals("running") ) {
            return VmState.RUNNING;
        }
        else if( state.equals("terminating") || state.equals("stopping") ) {
            return VmState.STOPPING;
        }
        else if( state.equals("stopped") ) {
            return VmState.PAUSED;
        }
        else if( state.equals("shutting-down") ) {
            return VmState.STOPPING;
        }
        else if( state.equals("terminated") ) {
            return VmState.TERMINATED;
        }
        else if( state.equals("rebooting") ) {
            return VmState.REBOOTING;
        }
        NovaEC2.getLogger(NovaServer.class, "std").warn("Unknown server state: " + state);
        return VmState.PENDING;
	}

	@Override
	public VmStatistics getVMStatistics(String instanceId, long startTimestamp, long endTimestamp) throws InternalException, CloudException {
	    return new VmStatistics();
	}

    @Override
    public Iterable<VmStatistics> getVMStatisticsForPeriod(String instanceId, long startTimestamp, long endTimestamp) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
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
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_INSTANCES);
        NovaMethod method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        
        try {
            method.invoke();
            return true;
        }
        catch( NovaException e ) {
            if( e.getStatus() == HttpServletResponse.SC_UNAUTHORIZED || e.getStatus() == HttpServletResponse.SC_FORBIDDEN ) {
                return false;
            }
            String code = e.getCode();
            
            if( code != null && code.equals("SignatureDoesNotMatch") ) {
                return false;
            }
            NovaEC2.getLogger(NovaServer.class, "std").warn(e.getSummary());
            throw new CloudException(e);
        }
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public VirtualMachine launch(VMLaunchOptions options) throws CloudException, InternalException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), RUN_INSTANCES);
        NovaMethod method;
        NodeList blocks;
        Document doc;

        parameters.put("ImageId", options.getMachineImageId());
        parameters.put("MinCount", "1");
        parameters.put("MaxCount", "1");
        parameters.put("InstanceType", options.getStandardProductId());
        if( options.getFirewallIds().length > 0 ) {
            int i = 1;

            for( String id : options.getFirewallIds() ) {
                parameters.put("SecurityGroup." + (i++), id);
            }
        }
        if( options.getDataCenterId() != null ) {
            parameters.put("Placement.AvailabilityZone", options.getDataCenterId());
        }
        if( options.getBootstrapKey() != null ) {
            parameters.put("KeyName", options.getBootstrapKey());
        }
        if( options.getVlanId() != null ) {
            parameters.put("SubnetId", options.getVlanId());
        }
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( NovaException e ) {
            String code = e.getCode();

            if( code != null && code.equals("InsufficientInstanceCapacity") ) {
                return null;
            }
            NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        VirtualMachine server = null;
        for( int i=0; i<blocks.getLength(); i++ ) {
            NodeList instances = blocks.item(i).getChildNodes();

            for( int j=0; j<instances.getLength(); j++ ) {
                Node instance = instances.item(j);

                if( instance.getNodeName().equals("item") ) {
                    server = toVirtualMachine(instance);
                    if( server != null ) {
                        break;
                    }
                }
            }
        }
        if( server != null && options.getBootstrapKey() != null ) {
            try {
                final String sid = server.getProviderVirtualMachineId();

                Callable<String> pwMethod = new Callable<String>() {
                    public String call() throws CloudException {
                        try {
                            Map<String,String> params = provider.getStandardParameters(provider.getContext(), GET_PASSWORD_DATA);
                            NovaMethod m;

                            params.put("InstanceId", sid);
                            m = new NovaMethod(provider, provider.getEc2Url(), params);

                            Document doc = m.invoke();
                            NodeList blocks = doc.getElementsByTagName("passwordData");

                            if( blocks.getLength() > 0 ) {
                                Node pw = blocks.item(0);

                                if( pw.hasChildNodes() ) {
                                    String password = pw.getFirstChild().getNodeValue();

                                    provider.release();
                                    return password;
                                }
                                return null;
                            }
                            return null;
                        }
                        catch( Throwable t ) {
                            throw new CloudException("Unable to retrieve password for " + sid + ", Let's hope it's Unix: " + t.getMessage());
                        }
                    }
                };

                provider.hold();
                try {
                    String password = pwMethod.call();

                    if( password == null ) {
                        server.setRootPassword(null);
                        server.setPasswordCallback(pwMethod);
                    }
                    else {
                        server.setRootPassword(password);
                    }
                    server.setPlatform(Platform.WINDOWS);
                }
                catch( CloudException e ) {
                    NovaEC2.getLogger(NovaServer.class, "std").warn(e.getMessage());
                }
            }
            catch( Throwable t ) {
                NovaEC2.getLogger(NovaServer.class, "std").warn("Unable to retrieve password for " + server.getProviderVirtualMachineId() + ", Let's hope it's Unix: " + t.getMessage());
            }
        }
        Map<String,Object> meta = options.getMetaData();
        Tag[] toCreate;
        int i = 0;

        if( meta.isEmpty() ) {
            toCreate = new Tag[2];
        }
        else {
            int count = 0;

            for( Map.Entry<String,Object> entry : meta.entrySet() ) {
                if( entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description") ) {
                    continue;
                }
                count++;
            }
            toCreate = new Tag[count + 2];
            for( Map.Entry<String,Object> entry : meta.entrySet() ) {
                if( entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description") ) {
                    continue;
                }
                toCreate[i++] = new Tag(entry.getKey(), entry.getValue().toString());
            }
        }
        Tag t = new Tag();

        t.setKey("Name");
        t.setValue(options.getFriendlyName());
        toCreate[i++] = t;
        t = new Tag();
        t.setKey("Description");
        t.setValue(options.getDescription());
        toCreate[i] = t;
        provider.createTags(server.getProviderVirtualMachineId(), toCreate);
        while( server.getProviderDataCenterId().equals("unknown zone") ) {
            try { Thread.sleep(1000L); }
            catch( InterruptedException e ) { }
            try {
                server = getVirtualMachine(server.getProviderVirtualMachineId());
                if( server == null ) {
                    return null;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
        }
        return server;
    }

    private List<VirtualMachineProduct> get64s() {
        return sixtyFours;
    }
    
    
    private List<VirtualMachineProduct> get32s() {
        return thirtyTwos;
    }
    
	@Override
	public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
		if( architecture == null ) {
		    return Collections.emptyList();
		}
		switch( architecture ) {
		case I32: return get32s();
		case I64: return get64s();
		default: return Collections.emptyList();
		}
	}

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private String guess(String privateDnsAddress) {
	    String dnsAddress = privateDnsAddress;
	    String[] parts = dnsAddress.split("\\.");
	    
	    if( parts != null & parts.length > 1 ) {
	        dnsAddress = parts[0];
	    }
	    if( dnsAddress.startsWith("ip-") ) {
	        dnsAddress = dnsAddress.replace('-', '.');
            return dnsAddress.substring(3);
	    }
	    return null;
	}
	
	@Override
    public VirtualMachine launch(String imageId, VirtualMachineProduct size, String inZoneId, String name, String description, String keypair, String inVlanId, boolean withMonitoring, boolean asImageSandbox, String ... protectedByFirewalls) throws CloudException, InternalException {
	    return launch(imageId, size, inZoneId, name, description, keypair, inVlanId, withMonitoring, asImageSandbox, protectedByFirewalls, new Tag[0]);
	}

	@Override
	public @Nonnull VirtualMachine launch(@Nonnull String imageId, @Nonnull VirtualMachineProduct size, @Nullable String inZoneId, @Nonnull String name, @Nonnull String description, String keypair, String inVlanId, boolean withMonitoring, boolean asImageSandbox, String[] firewallIds, Tag ... tags) throws CloudException, InternalException {
        VMLaunchOptions options = VMLaunchOptions.getInstance(size.getProviderProductId(), imageId, name, description);

        if( inVlanId == null && inZoneId != null ) {
            options.inDataCenter(inZoneId);
        }
        else if( inVlanId != null && inZoneId != null ) {
            options.inVlan(null, inZoneId, inVlanId);
        }
        if( keypair != null ) {
            options.withBoostrapKey(keypair);
        }
        if( withMonitoring ) {
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

	@Override
	public Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_INSTANCES);
		NovaMethod method = new NovaMethod(provider, provider.getEc2Url(), parameters);
		ArrayList<VirtualMachine> list = new ArrayList<VirtualMachine>();
        NodeList blocks;
		Document doc;
        
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("instancesSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList instances = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<instances.getLength(); j++ ) {
            	Node instance = instances.item(j);
            	
            	if( instance.getNodeName().equals("item") ) {
            		list.add(toVirtualMachine(instance));
            	}
            }
        }
        return list;
	}

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

	@Override
	public void stop(@Nonnull String instanceId) throws InternalException, CloudException {
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), STOP_INSTANCES);
        NovaMethod method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            method.invoke();
        }
        catch( NovaException e ) {
            NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
            throw new CloudException(e);
        }	    
	}

	@Override
	public void reboot(String instanceId) throws CloudException, InternalException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), REBOOT_INSTANCES);
		NovaMethod method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
	}

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Pause/resume is not supported by the EC2 api");
    }

    private String resolve(String dnsName) {
        if( dnsName != null && dnsName.length() > 0 ) {
            InetAddress[] addresses;
            
            try {
                addresses = InetAddress.getAllByName(dnsName);
            }
            catch( UnknownHostException e ) {
                addresses = null;
            }
            if( addresses != null && addresses.length > 0 ) {
                dnsName = addresses[0].getHostAddress();
            }
            else {
                dnsName = dnsName.split("\\.")[0];
                dnsName = dnsName.replaceAll("-", "\\.");
                dnsName = dnsName.substring(4);
            }
        }        
        return dnsName;
	}

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public void pause(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Pause/unpause is not supported by the EC2 API");
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Suspend/resume is not supported by the EC2 API");
    }

    @Override
	public void terminate(String instanceId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), TERMINATE_INSTANCES);
		NovaMethod method;
        
        parameters.put("InstanceId.1", instanceId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaServer.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
	}

    @Override
    public void unpause(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Pause/Unpause is not supported by the EC2 API");
    }

    private VirtualMachine toVirtualMachine(Node instance) throws CloudException {
		NodeList attrs = instance.getChildNodes();
		VirtualMachine server = new VirtualMachine();

		server.setPersistent(false);
		server.setProviderOwnerId(provider.getContext().getAccountNumber());
		server.setCurrentState(VmState.PENDING);
		server.setName(null);
		server.setDescription(null);
        server.setArchitecture(Architecture.I64);
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("instanceId") ) {
				String value = attr.getFirstChild().getNodeValue().trim();
				
				server.setProviderVirtualMachineId(value);
			}
			else if( name.equals("imageId") ) {
				String value = attr.getFirstChild().getNodeValue().trim();
				
				server.setProviderMachineImageId(value);
			}
			else if( name.equals("instanceState") ) {
				NodeList details = attr.getChildNodes();
				
				for( int j=0; j<details.getLength(); j++ ) {
					Node detail = details.item(j);
					
					name = detail.getNodeName();
					if( name.equals("name") ) {
						String value = detail.getFirstChild().getNodeValue().trim();
						
						server.setCurrentState(getServerState(value));
					}
				}
			}
			else if( name.equals("privateDnsName") ) {
				if( attr.hasChildNodes() ) {
					String value = attr.getFirstChild().getNodeValue();
				
					server.setPrivateDnsAddress(value);
                    if( server.getPrivateIpAddresses() == null || server.getPrivateIpAddresses().length < 1 ) {
					    value = guess(value);
					    if( value != null ) {
					        server.setPrivateIpAddresses(new String[] { value });
					    }
					    else {
					        server.setPrivateIpAddresses(new String[0]);
					    }
					}
				}
			}
			else if( name.equals("dnsName") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPublicDnsAddress(value);
					if( server.getPublicIpAddresses() == null || server.getPublicIpAddresses().length < 1 ) {
					    server.setPublicIpAddresses(new String[] { resolve(value) });
					}
				}
			}
            else if( name.equals("privateIpAddress") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPrivateIpAddresses(new String[] { value });
                }
            }
            else if( name.equals("ipAddress") ) {
                if( attr.hasChildNodes() ) {
                    String value = attr.getFirstChild().getNodeValue();

                    server.setPublicIpAddresses(new String[] { value });
                }
            }
            else if( name.equals("rootDeviceType") ) {
                if( attr.hasChildNodes() ) {
                    server.setPersistent(attr.getFirstChild().getNodeValue().equalsIgnoreCase("ebs"));
                }                
            }
            else if( name.equals("tagSet") ) {
                if( attr.hasChildNodes() ) {
                    NodeList tags = attr.getChildNodes();
                    
                    for( int j=0; j<tags.getLength(); j++ ) {
                        Node tag = tags.item(j);
                        
                        if( tag.getNodeName().equals("item") && tag.hasChildNodes() ) {
                            NodeList parts = tag.getChildNodes();
                            String key = null, value = null;
                            
                            for( int k=0; k<parts.getLength(); k++ ) {
                                Node part = parts.item(k);
                                
                                if( part.getNodeName().equalsIgnoreCase("key") ) {
                                    if( part.hasChildNodes() ) {
                                        key = part.getFirstChild().getNodeValue().trim();
                                    }
                                }
                                else if( part.getNodeName().equalsIgnoreCase("value") ) {
                                    if( part.hasChildNodes() ) {
                                        value = part.getFirstChild().getNodeValue().trim();
                                    }
                                }
                            }
                            if( key != null ) {
                                if( key.equalsIgnoreCase("name") ) {
                                    server.setName(value);
                                }
                                else if( key.equalsIgnoreCase("description") ) {
                                    server.setDescription(value);
                                }
                                else {
                                    server.addTag(key, value);
                                }
                            }
                        }
                    }
                }
            }
			else if( name.equals("instanceType") && attr.hasChildNodes()) {
				String value = attr.getFirstChild().getNodeValue().trim();

				server.setProductId(value);
			}
			else if( name.equals("launchTime") ) {
				String value = attr.getFirstChild().getNodeValue().trim();

				server.setLastBootTimestamp(provider.parseTime(value));
				server.setCreationTimestamp(server.getLastBootTimestamp());
			}
			else if( name.equals("platform") ) {
			    if( attr.hasChildNodes() ) {
			        server.setPlatform(Platform.guess(attr.getFirstChild().getNodeValue()));
			    }
			}
			else if( name.equals("placement") ) {
				NodeList details = attr.getChildNodes();
				
				for( int j=0; j<details.getLength(); j++ ) {
					Node detail = details.item(j);
					
					name = detail.getNodeName();
					if( name.equals("availabilityZone") ) {
					    if( detail.hasChildNodes() ) {
					        String value = detail.getFirstChild().getNodeValue().trim();
						
					        server.setProviderDataCenterId(value);
					    }
					}
				}
			}
		}
		if( server.getPlatform() == null ) {
		    server.setPlatform(Platform.UNKNOWN);
		}
        server.setProviderRegionId(provider.getContext().getRegionId());
        if( server.getName() == null ) {
            server.setName(server.getProviderVirtualMachineId());
        }
        if( server.getDescription() == null ) {
            server.setDescription(server.getName());
        }
		return server;
	}
	
	@Override
	public void disableAnalytics(String instanceId) throws InternalException, CloudException {
	    // NO-OP
	}
}
