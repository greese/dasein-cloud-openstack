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

package org.dasein.cloud.openstack.nova.os;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NovaLocationServices implements DataCenterServices {
    private NovaOpenStack provider;
    
    public NovaLocationServices(NovaOpenStack provider) { this.provider = provider; }

    private transient volatile NovaLocationCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new NovaLocationCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getDataCenter");
        try {
            ProviderContext ctx = provider.getContext();
            
            if( ctx == null ) {
                throw new CloudException("No context exists for this request");
            }
            String regionId = ctx.getRegionId();
            
            if( regionId == null ) {
                throw new CloudException("No region is known for zones request");
            }
            for( DataCenter dc : listDataCenters(regionId) ) {
                if( dc.getProviderDataCenterId().equals(providerDataCenterId) ) {
                    return dc;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "data center";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "region";
    }

    @Override
    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getRegion");
        try {
            for( Region region : listRegions() ) {
                if( region.getProviderRegionId().equals(providerRegionId) ) {
                    return region;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable String getDCNameFromHostAggregate() {
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject aggregates = method.getResource("compute", "/os-aggregates", null, false);
            if( aggregates == null || !aggregates.has("aggregates") ) {
                return null;
            }
            JSONArray objs = aggregates.getJSONArray("aggregates");
            for( int i=0; i < objs.length(); i++ ) {
                JSONObject aggregate = objs.getJSONObject(i);
                if( !aggregate.has("hosts") ) {
                    continue;
                }
                JSONArray hosts = aggregate.getJSONArray("hosts");
                if( hosts.length() == 0 ) {
                    continue;
                }
                if( !aggregate.has("metadata") ) {
                    continue;
                }

                JSONObject metadata = aggregate.getJSONObject("metadata");
                if( metadata.has("availability_zone") ) {
                    return metadata.getString("availability_zone");
                }
            }
        }
        catch( Exception ex ) {
            //The user likely has too few permissions to request aggregates
        }
        return null;
    }

    private @Nonnull Collection<String> getDCNamesFromHosts() {
        try {
            Map<String, String> names = new HashMap<String, String>();
            NovaMethod method = new NovaMethod(provider);
            JSONObject hosts = method.getResource("compute", "/os-hosts", null, false);
            if( hosts == null || !hosts.has("hosts") ) {
                return null;
            }
            JSONArray objs = hosts.getJSONArray("hosts");
            for( int i=0; i < objs.length(); i++ ) {
                JSONObject host = objs.getJSONObject(i);
                String az = host.getString("zone");
                if( az == null || "internal".equalsIgnoreCase(az) ) {
                    continue;
                }
                names.put(az, az);
            }
            return names.values();
        }
        catch( Exception ex ) {
            //The user likely has too few permissions to request aggregates
        }
        return Collections.emptyList();
    }

    private DataCenter constructDataCenter(String name, String providerRegionId) {
        DataCenter dc = new DataCenter();
        dc.setActive(true);
        dc.setAvailable(true);
        dc.setName(name);
        dc.setProviderDataCenterId(name);
        dc.setRegionId(providerRegionId);
        return dc;
    }

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            Cache<DataCenter> cache = Cache.getInstance(provider, "datacenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(1, TimePeriod.MINUTE));
            Iterable<DataCenter> cached = cache.get(provider.getContext());
            List<DataCenter> dataCenters = new ArrayList<DataCenter>();
            if( cached != null && cached.iterator().hasNext() ) {
                Iterator<DataCenter> it = cached.iterator();
                while( it.hasNext() ) {
                    dataCenters.add(it.next());
                }
                return dataCenters;
            }

            Region region = getRegion(providerRegionId);
            if( region == null ) {
                throw new CloudException("No such region: " + providerRegionId);
            }
            // look up the zone in the host aggregates
            String hostAggregateZone = getDCNameFromHostAggregate();
            if( hostAggregateZone != null ) {
                dataCenters.add(constructDataCenter(hostAggregateZone, providerRegionId));
                cache.put(provider.getContext(), dataCenters);
                return dataCenters;
            }
            // if host aggregates is not configured, let's look in the hosts
            for( String hostZone : getDCNamesFromHosts() ) {
                dataCenters.add(constructDataCenter(hostZone, providerRegionId));
            }
            if( !dataCenters.isEmpty() ) {
                cache.put(provider.getContext(), dataCenters);
                return dataCenters;
            }
            // if failed to get from the hosts, fallback to "<region>-a"
            dataCenters.add(constructDataCenter(providerRegionId+"-a", providerRegionId));
            cache.put(provider.getContext(), dataCenters);
            return dataCenters;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listRegions");
        try {
            AuthenticationContext ctx = provider.getAuthenticationContext();
            
            return ctx.listRegions();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Nonnull
    @Override
    public Collection<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }
}
