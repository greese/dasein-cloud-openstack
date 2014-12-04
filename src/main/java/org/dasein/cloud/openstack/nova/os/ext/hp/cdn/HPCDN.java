/**
 * Copyright (C) 2009-2014 Dell, Inc.
 * See annotations for authorship information
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

package org.dasein.cloud.openstack.nova.os.ext.hp.cdn;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.CDNSupport;
import org.dasein.cloud.platform.Distribution;
import org.dasein.cloud.util.APITrace;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Supports the HP CDN terminology. If this becomes standard Nova, great. If not, oh well.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 * @version 2013.02 updated for 2013.02 model
 */
public class HPCDN implements CDNSupport {
    static private final Logger logger = NovaOpenStack.getLogger(HPCDN.class, "std");

    static public final String SERVICE  = "hpext:cdn";
    static public final String RESOURCE = null;

    private NovaOpenStack provider;

    public HPCDN(NovaOpenStack cloud) {
        provider = cloud;
    }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return provider.getAuthenticationContext().getTenantId();
    }

    @Override
    public @Nonnull String create(@Nonnull String origin, @Nonnull String name, boolean active, @CheckForNull String... aliases) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.create");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.putHPCDN(origin);
            return origin;
        }
        finally {
            APITrace.end();
        }        
    }

    @Override
    public void delete(@Nonnull String distributionId) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.delete");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteHPCDN(distributionId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable Distribution getDistribution(@Nonnull String distributionId) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.getDistribution");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new InternalException("No context exists for this request");
            }
            return toDistribution(ctx, distributionId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForDistribution(@Nonnull Locale locale) {
        return "container";
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.isSubscribed");
        try {
            return (provider.getProviderName().equals("HP") && provider.getAuthenticationContext().getServiceUrl(SERVICE) != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<Distribution> list() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.list");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            String response = method.getHPCDN(null);
            ArrayList<Distribution> distributions = new ArrayList<Distribution>();
            
            try {
                if( response != null ) {
                    BufferedReader reader = new BufferedReader(new StringReader(response));
                    String container;
                    
                    while( (container = reader.readLine()) != null ) {
                        Distribution d = toDistribution(ctx, container);

                        if( d != null ) {
                            distributions.add(d);
                        }
                    }
                }
            }
            catch( IOException e ) {
                logger.error("list(): I/O error parsing response: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidResponse", "I/O error parsing " + response);
            }
            return distributions;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listDistributionStatus() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.listDistributionStatus");
        try {
            NovaMethod method = new NovaMethod(provider);
            String response = method.getHPCDN(null);
            ArrayList<ResourceStatus> distributions = new ArrayList<ResourceStatus>();

            try {
                if( response != null ) {
                    BufferedReader reader = new BufferedReader(new StringReader(response));
                    String container;

                    while( (container = reader.readLine()) != null ) {
                        ResourceStatus d = toStatus(container);

                        if( d != null ) {
                            distributions.add(d);
                        }
                    }
                }
            }
            catch( IOException e ) {
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidResponse", "I/O error parsing " + response);
            }
            return distributions;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void update(@Nonnull String distributionId, @Nonnull String name, boolean active, @CheckForNull String... aliases) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.update");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            HashMap<String,String> headers = new HashMap<String, String>();
            NovaMethod method = new NovaMethod(provider);

            headers.put("X-CDN-Enabled", active ? "True" : "False");
            method.postHPCDN(distributionId, headers);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable Distribution toDistribution(@Nonnull ProviderContext ctx, @Nullable String container) throws CloudException, InternalException {
        if( container == null ) {
            return null;
        }
        NovaMethod method = new NovaMethod(provider);
        Map<String,String> headers = method.headResource(SERVICE, RESOURCE, container);

        if( headers == null ) {
            return null;
        }
        String enabled = null, uriString = null;
        for( String key : headers.keySet() ) {
            if( key.equalsIgnoreCase("X-CDN-Enabled") ) {
                enabled = headers.get(key);
            }
            else if( key.equalsIgnoreCase("X-CDN-URI") ) {
                uriString = headers.get(key);
                
            }
        }
        if( uriString == null ) {
            return null;
        }
        String dns;

        try {
            URI uri = new URI(uriString);
            
            dns = uri.getHost();
            if( uri.getPort() > 0 ) {
                if( dns.startsWith("https:") && uri.getPort() != 443 ) {
                    dns = dns + ":" + uri.getPort();
                }
                if( dns.startsWith("http:") && uri.getPort() != 80 ) {
                    dns = dns + ":" + uri.getPort();
                }
            }
        }
        catch( URISyntaxException e ) {
            throw new CloudException(e);
        }

        Distribution distribution = new Distribution();


        distribution.setName(container);
        distribution.setActive(enabled != null && enabled.equalsIgnoreCase("true"));
        distribution.setAliases(new String[0]);
        distribution.setDeployed(enabled != null && enabled.equalsIgnoreCase("true"));
        distribution.setDnsName(dns);
        distribution.setLocation(uriString);
        distribution.setLogDirectory(null);
        distribution.setProviderDistributionId(container);
        distribution.setProviderOwnerId(getTenantId());
        return distribution;
    }

    private @Nullable ResourceStatus toStatus(@Nullable String container) throws CloudException, InternalException {
        if( container == null ) {
            return null;
        }
        NovaMethod method = new NovaMethod(provider);
        Map<String,String> headers = method.headResource(SERVICE, RESOURCE, container);

        if( headers == null ) {
            return null;
        }

        String enabled = null, uriString = null;
        for( String key : headers.keySet() ) {
            if( key.equalsIgnoreCase("X-CDN-Enabled") ) {
                enabled = headers.get(key);
            }
            else if( key.equalsIgnoreCase("X-CDN-URI") ) {
                uriString = headers.get(key);

            }
        }
        if( uriString == null ) {
            return null;
        }
        return new ResourceStatus(container, enabled != null && enabled.equalsIgnoreCase("true"));
    }
}
