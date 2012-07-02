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

package org.dasein.cloud.openstack.nova.os;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;

public class NovaLocationServices implements DataCenterServices {
    private NovaOpenStack provider;
    
    public NovaLocationServices(NovaOpenStack provider) { this.provider = provider; }
    
    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaLocationServices.class, "std");
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaLocationServices.class.getName() + ".getDataCenter(" + providerDataCenterId + ")");
        }
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
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaLocationServices.class.getName() + ".getDataCenter()");
            }            
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
        Logger std = NovaOpenStack.getLogger(NovaLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaLocationServices.class.getName() + ".getRegion(" + providerRegionId + ")");
        }
        try {
            for( Region region : listRegions() ) {
                if( region.getProviderRegionId().equals(providerRegionId) ) {
                    return region;
                }
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaLocationServices.class.getName() + ".getRegion()");
            }            
        }
    }

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaLocationServices.class.getName() + ".listDataCenters(" + providerRegionId + ")");
        }
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
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaLocationServices.class.getName() + ".listDataCenters()");
            }            
        }
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + NovaLocationServices.class.getName() + ".listRegions()");
        }
        try {
            AuthenticationContext ctx = provider.getAuthenticationContext();
            
            return ctx.listRegions();
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaLocationServices.class.getName() + ".listRegions()");
            }            
        }
    }
}
