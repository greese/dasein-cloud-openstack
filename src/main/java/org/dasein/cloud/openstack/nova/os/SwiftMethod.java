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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SwiftMethod extends AbstractMethod {
    public SwiftMethod(NovaOpenStack provider) { super(provider); }
        
    public void delete(@Nonnull String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();
        
        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            delete(context.getAuthToken(), endpoint, "/" + bucket);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                delete(bucket);
            }
            else {
                throw ex;
            }
        }
    }
    
    public void delete(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            delete(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                delete(bucket, object);
            }
            else {
                throw ex;
            }
        }
    }
    
    public @Nonnull List<String> get(@Nullable String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            String response = getString(context.getAuthToken(), endpoint, bucket == null ? "/" : "/" + bucket);

            ArrayList<String> entries = new ArrayList<String>();

            if( response != null ) {
                response = response.trim();
                if( response.length() > 0 ) {
                    String[] lines = response.split("\n");

                    if( lines.length < 1 ) {
                        entries.add(response);
                    }
                    else {
                        for( String line : lines ) {
                            entries.add(line.trim());
                        }
                    }

                }
            }
            return entries;
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return get(bucket);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable InputStream get(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            return getStream(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return get(bucket, object);
            }
            else {
                throw ex;
            }
        }
    }
    
    @SuppressWarnings("unused")
    public @Nullable Map<String,String> head(@Nonnull String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            return head(context.getAuthToken(), endpoint, "/" + bucket);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return head(bucket);
            }
            else {
                throw ex;
            }
        }
    }
    
    public @Nullable Map<String,String> head(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            return head(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return head(bucket, object);
            }
            else {
                throw ex;
            }
        }
    }
    
    public void put(@Nonnull String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            putString(context.getAuthToken(), endpoint, "/" + bucket, null);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                put(bucket);
            }
            else {
                throw ex;
            }
        }
    }
    
    public void put(@Nonnull String bucket, @Nonnull String object, @Nullable String md5Hash, @Nonnull InputStream payload) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        try {
            putStream(context.getAuthToken(), endpoint, "/" + bucket + "/" + object, md5Hash, payload);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                put(bucket, object, md5Hash, payload);
            }
            else {
                throw ex;
            }
        }
    }
}
