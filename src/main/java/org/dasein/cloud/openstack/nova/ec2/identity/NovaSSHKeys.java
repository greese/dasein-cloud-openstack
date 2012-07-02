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

package org.dasein.cloud.openstack.nova.ec2.identity;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.openstack.nova.ec2.NovaEC2;
import org.dasein.cloud.openstack.nova.ec2.NovaException;
import org.dasein.cloud.openstack.nova.ec2.NovaMethod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;

public class NovaSSHKeys implements ShellKeySupport {
    static public final String CREATE_KEY_PAIR    = "CreateKeyPair";
    static public final String IMPORT_KEY_PAIR    = "ImportKeyPair";
    static public final String DELETE_KEY_PAIR    = "DeleteKeyPair";
	static public final String DESCRIBE_KEY_PAIRS = "DescribeKeyPairs";
	
	private NovaEC2 provider = null;
	
	public NovaSSHKeys(NovaEC2 provider) {
		this.provider =  provider;
	}
	
	@Override
	public SSHKeypair createKeypair(String name) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this call.");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request.");
        }
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), CREATE_KEY_PAIR);
		NovaMethod method;
		NodeList blocks;
		Document doc;

		parameters.put("KeyName", name);
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaSSHKeys.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        String material = null, fingerprint = null;
        blocks = doc.getElementsByTagName("CreateKeyPairResponse");
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node attr = blocks.item(i);

            if( attr.getNodeName().equalsIgnoreCase("keyMaterial") ) {
                if( attr.hasChildNodes() ) {
                    material = attr.getFirstChild().getNodeValue();
                }
            }
            else if( attr.getNodeName().equalsIgnoreCase("keyFingerprint") ) {
                if( attr.hasChildNodes() ) {
                    fingerprint = attr.getFirstChild().getNodeValue();
                }
            }
        }
        if( fingerprint == null || material == null ) {
            throw new CloudException("Invalid response to attempt to create the keypair");
        }
        SSHKeypair key = new SSHKeypair();

        try {
            key.setPrivateKey(material.getBytes("utf-8"));
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
        key.setFingerprint(fingerprint);
        key.setName(name);
        key.setProviderKeypairId(name);
        key.setProviderOwnerId(ctx.getAccountNumber());
        key.setProviderRegionId(regionId);
        return key;
	}

	@Override
	public void deleteKeypair(String name) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DELETE_KEY_PAIR);
		NovaMethod method;
		NodeList blocks;
		Document doc;

		parameters.put("KeyName", name);
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidKeyPair") ) {
        		return;
        	}
        	NovaEC2.getLogger(NovaSSHKeys.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
        	if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
        		throw new CloudException("Deletion of keypair denied.");
        	}
        }
	}

	@Override
	public String getFingerprint(String name) throws InternalException,	CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_KEY_PAIRS);
		NovaMethod method;
        NodeList blocks;
		Document doc;

		parameters.put("KeyName.1", name);
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidKeyPair") ) {
        		return null;
        	}
        	NovaEC2.getLogger(NovaSSHKeys.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("keyFingerprint");
        if( blocks.getLength() > 0 ) {
        	return blocks.item(0).getFirstChild().getNodeValue().trim();
        }
        throw new CloudException("Unable to identify key fingerprint.");
	}

    @Override
    public Requirement getKeyImportSupport() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public SSHKeypair getKeypair(String name) throws InternalException,	CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this call.");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request.");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_KEY_PAIRS);
        NovaMethod method;
        NodeList blocks;
        Document doc;

        parameters.put("KeyName.1", name);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( NovaException e ) {
            String code = e.getCode();

            if( code != null && code.startsWith("InvalidKeyPair") ) {
                return null;
            }
            NovaEC2.getLogger(NovaSSHKeys.class, "std").error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("item");
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            NodeList attrs = item.getChildNodes();
            String fingerprint = null;
            String keyName = null;

            for( int j=0; j<attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);

                if( attr.getNodeName().equalsIgnoreCase("keyFingerprint") && attr.hasChildNodes() ) {
                    fingerprint = attr.getFirstChild().getNodeValue().trim();
                }
                else if( attr.getNodeName().equalsIgnoreCase("keyName") && attr.hasChildNodes() ) {
                    keyName = attr.getFirstChild().getNodeValue().trim();
                }
            }
            if( keyName != null && keyName.equals(name) && fingerprint != null ) {
                SSHKeypair kp = new SSHKeypair();

                kp.setFingerprint(fingerprint);
                kp.setName(keyName);
                kp.setPrivateKey(null);
                kp.setPublicKey(null);
                kp.setProviderKeypairId(keyName);
                kp.setProviderOwnerId(ctx.getAccountNumber());
                kp.setProviderRegionId(regionId);
                return kp;
            }
        }
        return null;
    }

	@Override
	public String getProviderTermForKeypair(Locale locale) {
		return "keypair";
	}

    @Override
    public @Nonnull SSHKeypair importKeypair(@Nonnull String name, @Nonnull String material) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this call.");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request.");
        }
        Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), IMPORT_KEY_PAIR);
        NovaMethod method;
        NodeList blocks;
        Document doc;

        parameters.put("KeyName", name);
        parameters.put("PublicKeyMaterial", material);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( NovaException e ) {
            throw new CloudException(e);
        }
        String fingerprint = null;

        blocks = doc.getElementsByTagName("ImportKeyPairResponse");
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);
            NodeList attrs = item.getChildNodes();

            for( int j=0; j<attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);

                if( attr.getNodeName().equalsIgnoreCase("keyFingerPrint")) {
                    fingerprint = attr.getFirstChild().getNodeValue();

                }
            }
        }
        if( fingerprint == null ) {
            throw new CloudException("Invalid response to attempt to create the keypair");
        }
        SSHKeypair key = new SSHKeypair();

        try {
            key.setPrivateKey(material.getBytes("utf-8"));
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
        key.setFingerprint(fingerprint);
        key.setName(name);
        key.setProviderKeypairId(name);
        key.setProviderOwnerId(ctx.getAccountNumber());
        key.setProviderRegionId(regionId);
        return key;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

	public Collection<SSHKeypair> list() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was established for this call.");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request.");
        }
	    Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_KEY_PAIRS);
	    ArrayList<SSHKeypair> keypairs = new ArrayList<SSHKeypair>();
        NovaMethod method;
        NodeList blocks;
        Document doc;

        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
            doc = method.invoke();
        }
        catch( NovaException e ) {
            String code = e.getCode();
            
            if( code != null && code.startsWith("InvalidKeyPair") ) {
                return null;
            }
            NovaEC2.getLogger(NovaSSHKeys.class, "std").error(e.getSummary());
            throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("item");
        for( int i=0; i<blocks.getLength(); i++ ) {
            Node item = blocks.item(i);

            if( item.hasChildNodes() ) {
                NodeList attrs = item.getChildNodes();
                String fingerprint = null;

                String keyName = null;
                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);

                    if( attr.getNodeName().equalsIgnoreCase("keyName") && attr.hasChildNodes() ) {
                        keyName = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equalsIgnoreCase("keyFingerprint") && attr.hasChildNodes() ) {
                        fingerprint = attr.getFirstChild().getNodeValue().trim();
                    }
                }
                if( keyName != null && fingerprint != null ) {
                    SSHKeypair keypair = new SSHKeypair();

                    keypair.setName(keyName);
                    keypair.setProviderKeypairId(keyName);
                    keypair.setFingerprint(fingerprint);
                    keypair.setProviderOwnerId(ctx.getAccountNumber());
                    keypair.setProviderRegionId(regionId);
                    keypairs.add(keypair);
                }
            }
        }
        return keypairs;
	}

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
}
