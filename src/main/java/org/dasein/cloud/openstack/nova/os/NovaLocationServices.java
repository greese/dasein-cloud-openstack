/**
 * Copyright (C) 2009-2015 Dell, Inc.
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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;
import org.dasein.cloud.openstack.nova.os.compute.CinderVolume;
import org.dasein.cloud.openstack.nova.os.compute.NovaServer;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.util.Cache;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NovaLocationServices extends AbstractDataCenterServices<NovaOpenStack> {

    public NovaLocationServices(NovaOpenStack provider) { super(provider); }

    private transient volatile NovaLocationCapabilities capabilities;

    @Override
    public @Nonnull DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new NovaLocationCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "DC.getDataCenter");
        try {
            ProviderContext ctx = getProvider().getContext();

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
    public @Nullable Region getRegion(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "DC.getRegion");
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

    private @Nonnull Collection<String> getCinderAvailabilityZones() {
        try {
            Map<String, String> names = new HashMap<String, String>();
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject zones = method.getResource(CinderVolume.SERVICE, "/os-availability-zone", null, false);
            if( zones == null || !zones.has("availabilityZoneInfo") ) {
                return null;
            }
            JSONArray objs = zones.getJSONArray("availabilityZoneInfo");
            for( int i = 0; i < objs.length(); i++ ) {
                JSONObject zoneInfo = objs.getJSONObject(i);
                String zoneName = zoneInfo.getString("zoneName");
                JSONObject zoneState = zoneInfo.getJSONObject("zoneState");
                boolean available = false;
                if( zoneState != null && zoneState.has("available") ) {
                    available = zoneState.getBoolean("available");
                }
                if( zoneName == null || zoneName.isEmpty() || "internal".equalsIgnoreCase(zoneName) || !available ) {
                    continue;
                }
                names.put(zoneName, zoneName);
            }
            return names.values();
        }
        catch( Exception ex ) {
            //The user likely has too few permissions to request hosts
        }
        return Collections.emptyList();
    }

    private @Nonnull Collection<String> getComputeAvailabilityZones() {
        try {
            Map<String, String> names = new HashMap<String, String>();
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject hosts = method.getResource(NovaServer.SERVICE, "/os-hosts", null, false);
            if( hosts == null || !hosts.has("hosts") ) {
                return null;
            }
            JSONArray objs = hosts.getJSONArray("hosts");
            for( int i = 0; i < objs.length(); i++ ) {
                JSONObject host = objs.getJSONObject(i);
                String az = host.getString("zone");
                if( az == null || az.isEmpty() || "internal".equalsIgnoreCase(az) ) {
                    continue;
                }
                names.put(az, az);
            }
            return names.values();
        }
        catch( Exception ex ) {
            //The user likely has too few permissions to request hosts
        }
        return Collections.emptyList();
    }

    private DataCenter constructDataCenter(String name, String providerRegionId) {
        DataCenter dc = new DataCenter(name, name, providerRegionId, true, true);
        dc.setCompute(true);
        dc.setStorage(false);
        return dc;
    }

    @Override
    public Iterable<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "DC.listDataCenters");
        try {
            Cache<DataCenter> cache = Cache.getInstance(getProvider(), "datacenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(10, TimePeriod.MINUTE));
            Iterable<DataCenter> cached = cache.get(getProvider().getContext());
            List<DataCenter> dataCenters = new ArrayList<DataCenter>();
            if( cached != null && cached.iterator().hasNext() ) {
                return cached;
            }

            Region region = getRegion(providerRegionId);
            if( region == null ) {
                throw new CloudException("No such region: " + providerRegionId);
            }
            Collection<String> cinderZones = getCinderAvailabilityZones();
            // let's look in the hosts
            for( String hostZone : getComputeAvailabilityZones() ) {
                DataCenter dc = constructDataCenter(hostZone, providerRegionId);
                for( String cinderZone : cinderZones ) {
                    if( dc.getProviderDataCenterId().equalsIgnoreCase(cinderZone) ) {
                        dc.setStorage(true);
                        break;
                    }
                }
                dataCenters.add(dc);
            }

            if( !dataCenters.isEmpty() ) {
                cache.put(getProvider().getContext(), dataCenters);
                return dataCenters;
            }
            // if failed to get from the hosts, fall back to "<region>-a"
            DataCenter fakeDataCenter = constructDataCenter(providerRegionId+"-a", providerRegionId);
            fakeDataCenter.setStorage(true);
            dataCenters.add(fakeDataCenter);
            cache.put(getProvider().getContext(), dataCenters);
            return dataCenters;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "DC.listRegions");
        try {
            AuthenticationContext ctx = getProvider().getAuthenticationContext();

            return ctx.listRegions();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nullable ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }
}