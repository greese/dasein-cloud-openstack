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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

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
        delete(context.getAuthToken(), endpoint, "/" + bucket);
    }
    
    public void delete(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        delete(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
    }
    
    public @Nonnull List<String> get(@Nullable String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
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

    public @Nullable InputStream get(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        return getStream(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
    }
    
    @SuppressWarnings("unused")
    public @Nullable Map<String,String> head(@Nonnull String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        return head(context.getAuthToken(), endpoint, "/" + bucket);
    }
    
    public @Nullable Map<String,String> head(@Nonnull String bucket, @Nonnull String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        return head(context.getAuthToken(), endpoint, "/" + bucket + "/" + object);
    }
    
    public void put(@Nonnull String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        putString(context.getAuthToken(), endpoint, "/" + bucket, null);
    }
    
    public void put(@Nonnull String bucket, @Nonnull String object, @Nullable String md5Hash, @Nonnull InputStream payload) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getStorageUrl();

        if( endpoint == null ) {
            throw new CloudException("No storage endpoint exists for " + context.getMyRegion());
        }
        putStream(context.getAuthToken(), endpoint, "/" + bucket + "/" + object, md5Hash, payload);
    }
}
