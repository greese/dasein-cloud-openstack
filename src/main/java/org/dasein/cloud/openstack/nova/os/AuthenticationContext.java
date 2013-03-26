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

package org.dasein.cloud.openstack.nova.os;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.dasein.cloud.dc.Jurisdiction;
import org.dasein.cloud.dc.Region;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AuthenticationContext { 
    private String                         authToken;
    private Map<String,Map<String,String>> endpoints;
    private String                         myRegion;
    private String                         storageToken;
    private String                         tenantId;

    public AuthenticationContext(@Nonnull String regionId, @Nonnull String token, @Nonnull String tenantId, @Nonnull Map<String,Map<String,String>> services, @Nullable String storageToken) {
        myRegion = regionId;
        authToken = token;
        endpoints = services;
        this.tenantId = tenantId;
        this.storageToken = storageToken;
    }

    public @Nonnull String getAuthToken() {
        return authToken;
    }
    
    public @Nullable String getComputeUrl() {
        Map<String,String> map = endpoints.get("compute");
        
        if( map == null ) {
            return null;
        }
        return map.get(myRegion);
    }
    
    public String getStorageToken() {
        if( storageToken == null ) {
            return getAuthToken();
        }
        return storageToken;
    }
        
    public @Nullable String getStorageUrl() {
        return getServiceUrl("object-store");
    }

    public @Nonnull String getTenantId() {
        return tenantId;
    }

    @SuppressWarnings("unused")
    public @Nonnull String getMyRegion() {
        return myRegion;
    }

    public @Nullable String getServiceUrl(String service) {
        Map<String,String> map = endpoints.get(service);

        if( map == null ) {
            return null;
        }
        String endpoint = null;

        for( String key : map.keySet() ) {
            if( myRegion == null ) {
                myRegion = key;
            }
            if( key == null ) {
                endpoint = map.get(null);
            }
            else if( key.equals(myRegion) ) {
                return map.get(myRegion);
            }
            else if( myRegion.endsWith(key) ) {
                endpoint = map.get(key);
            }
        }
        return endpoint;
    }
    
    public @Nonnull Collection<Region> listRegions() {
        Map<String,String> map = endpoints.get("compute");
        
        if( map == null ) {
            map = endpoints.get("object-store");
        }
        if( map == null ) {
            return Collections.emptyList();
        }
        ArrayList<Region> regions = new ArrayList<Region>();
        
        for( String regionId : map.keySet() ) {
            Region region = new Region();
        
            region.setActive(true);
            region.setAvailable(true);
            region.setJurisdiction(Jurisdiction.US.name());
            region.setName(regionId);
            region.setProviderRegionId(regionId);
            regions.add(region);
        }
        return regions;
    }
}
