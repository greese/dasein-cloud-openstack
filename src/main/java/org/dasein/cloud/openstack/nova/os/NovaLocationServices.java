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

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;
import org.dasein.cloud.util.APITrace;

import javax.annotation.Nonnull;

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

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            Region region = getRegion(providerRegionId);

            if( region == null ) {
                throw new CloudException("No such region: " + providerRegionId);
            }
            DataCenter dc = new DataCenter();

            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName(region.getProviderRegionId() + "-a");
            dc.setProviderDataCenterId(region.getProviderRegionId() + "-a");
            dc.setRegionId(providerRegionId);
            return Collections.singletonList(dc);
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
    public @Nonnull Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Collection<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }
}