/**
 * Copyright (C) 2009-2012 Enstratius, Inc.
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

package org.dasein.cloud.openstack.nova.os.ext.rackspace.cdn;

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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Implements CDN support for the Rackspace Cloud.
 * @since unknown
 * @version 2013.02 updated for 2013.02 model
 */
public class RackspaceCDN implements CDNSupport {
    static public final String SERVICE  = "rax:object-cdn";
    static public final String RESOURCE = null;

    private NovaOpenStack provider;
    
    public RackspaceCDN(NovaOpenStack provider) { this.provider = provider; }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return provider.getAuthenticationContext().getTenantId();
    }

    @Override
    public @Nonnull String create(@Nonnull String origin, @Nonnull String name, boolean active, @Nullable String... aliases) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.create");
        try {
            HashMap<String,String> customHeaders = new HashMap<String,String>();

            customHeaders.put("X-Log-Retention", "True");
            customHeaders.put("X-CDN-Enabled", "True");
            NovaMethod method = new NovaMethod(provider);

            method.putResourceHeaders(SERVICE, RESOURCE, origin, customHeaders);
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
            HashMap<String,String> customHeaders = new HashMap<String,String>();

            customHeaders.put("X-Log-Retention", "True");
            customHeaders.put("X-CDN-Enabled", "False");

            NovaMethod method = new NovaMethod(provider);

            method.putResourceHeaders(SERVICE, RESOURCE, distributionId, customHeaders);
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
            NovaMethod method = new NovaMethod(provider);
            Map<String,String> metaData = method.headResource(SERVICE, RESOURCE, distributionId);

            if( metaData == null ) {
                return null;
            }
            Distribution distribution = new Distribution();

            distribution.setAliases(new String[0]);

            String dnsName = null, cdnUri = null;
            boolean enabled = false;

            for( String key : metaData.keySet() ) {
                if( key.equalsIgnoreCase("X-CDN-Enabled") ) {
                    String value = metaData.get(key);

                    enabled = (value != null && value.equalsIgnoreCase("true"));
                }
                else if( key.equalsIgnoreCase("X-CDN-SSL-URI") ) {
                    dnsName = metaData.get(key);
                }
                else if( key.equalsIgnoreCase("X-CDN-URI") ) {
                    cdnUri = metaData.get(key);
                }
            }
            distribution.setActive(enabled);
            distribution.setDeployed(enabled);

            String prefix = "http://";

            if( dnsName == null ) {
                dnsName = cdnUri;
                if( dnsName == null ) {
                    return null;
                }
                if( dnsName.startsWith("http://") ) {
                    dnsName = dnsName.substring("http://".length());
                }
            }
            else {
                if( dnsName.startsWith("https://") ) {
                    dnsName = dnsName.substring("https://".length());
                    prefix = "https://";
                }
            }
            distribution.setDnsName(dnsName);
            distribution.setLocation(prefix + distribution.getDnsName() + "/" + distributionId);
            distribution.setLogDirectory(null);
            distribution.setLogName(null);
            distribution.setName(distributionId);
            distribution.setProviderDistributionId(distributionId);
            distribution.setProviderOwnerId(getTenantId());
            return distribution;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForDistribution(@Nonnull Locale locale) {
        return "distribution";
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.isSubscribed");
        try {
            return (provider.getProviderName().equals("Rackspace") && provider.getAuthenticationContext().getServiceUrl(SERVICE) != null);
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
                throw new InternalException("No context exists for this request");
            }
            ArrayList<Distribution> distributions = new ArrayList<Distribution>();
            NovaMethod method = new NovaMethod(provider);
            String[] list = method.getItemList(SERVICE, RESOURCE, false);

            if( list != null ) {
                for( String container : list ) {
                    Distribution d = toDistribution(ctx, container);

                    if( d != null ) {
                        distributions.add(d);
                    }
                }
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
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new InternalException("No context exists for this request");
            }
            ArrayList<ResourceStatus> distributions = new ArrayList<ResourceStatus>();
            NovaMethod method = new NovaMethod(provider);
            String[] list = method.getItemList(SERVICE, RESOURCE, false);

            if( list != null ) {
                for( String container : list ) {
                    ResourceStatus d = toStatus(container);

                    if( d != null ) {
                        distributions.add(d);
                    }
                }
            }
            return distributions;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void update(@Nonnull String distributionId, @Nonnull String name, boolean active, @Nullable String... aliases) throws InternalException, CloudException {
        APITrace.begin(provider, "CDN.update");
        try {
            HashMap<String,String> customHeaders = new HashMap<String,String>();

            customHeaders.put("X-Log-Retention", "True");
            customHeaders.put("X-CDN-Enabled", active ? "True" : "False");

            NovaMethod method = new NovaMethod(provider);

            method.putResourceHeaders(SERVICE, RESOURCE, distributionId, customHeaders);
        }
        finally {
            APITrace.end();
        }
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
                if( uriString == null ) {
                    uriString = headers.get(key);
                }
            }
            else if( key.equalsIgnoreCase("X-CDN-SSL-URI") ) {
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
                if( uriString == null ) {
                    uriString = headers.get(key);
                }
            }
            else if( key.equalsIgnoreCase("X-CDN-SSL-URI") ) {
                uriString = headers.get(key);
            }
        }
        if( uriString == null ) {
            return null;
        }
        return new ResourceStatus(container, enabled != null && enabled.equalsIgnoreCase("true"));
    }
}
