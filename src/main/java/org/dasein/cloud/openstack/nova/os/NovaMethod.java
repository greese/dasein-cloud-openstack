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

import org.apache.http.HttpStatus;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.ext.hp.cdn.HPCDN;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class NovaMethod extends AbstractMethod {
    public NovaMethod(NovaOpenStack provider) { super(provider); }
    
    public void deleteServers(@Nonnull final String resource, @Nonnull final String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getComputeUrl();
        
        if( endpoint == null ) {
            throw new CloudException("No compute endpoint exists");
        }
        try {
            delete(context.getAuthToken(), endpoint, resource + "/" + resourceId);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                deleteServers(resource, resourceId);
            }
            else {
                throw ex;
            }
        }
    }

    public void deleteNetworks(@Nonnull final String resource, @Nonnull final String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getNetworkUrl();

        if( endpoint == null ) {
            throw new CloudException("No network endpoint exists");
        }
        if (resource != null && (!endpoint.endsWith("/") && !resource.startsWith("/"))) {
            endpoint = endpoint+"/";
        }
        try {
            delete(context.getAuthToken(), endpoint, resource + "/" + resourceId);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                deleteNetworks(resource, resourceId);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject getPorts(@Nonnull final String resource, @Nonnull final String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getComputeUrl();

        if( endpoint == null ) {
            throw new CloudException("No compute URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri += "/" + resourceId;
        }

        try {
            String response = getString(context.getAuthToken(), endpoint, resourceUri);

            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return getPorts(resource, resourceId);
            }
            else {
                throw ex;
            }
        }
    }
    
    public @Nullable JSONObject getServers(@Nonnull final String resource, @Nullable final String resourceId, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getComputeUrl();
        
        if( endpoint == null ) {
            throw new CloudException("No compute URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource; // make a copy in case we need to retry with the original resource
        if( resourceId != null ) {
            resourceUri += "/" + resourceId;
        }
        else if( suffix ) {
            resourceUri += "/detail";
        }
        try {
            String response = getString(context.getAuthToken(), endpoint, resourceUri);

            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return getServers(resource, resourceId, suffix);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject getNetworks(@Nonnull final String resource, @Nullable final String resourceId, final boolean suffix) throws CloudException, InternalException {
        return getNetworks(resource, resourceId, suffix, null);
    }

    public @Nullable JSONObject getNetworks(@Nonnull final String resource, @Nullable final String resourceId, final boolean suffix, final String query) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getNetworkUrl();

        if( endpoint == null ) {
            throw new CloudException("No network URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource; // make a copy in case we need to retry with the original resource
        if( resourceId != null ) {
            resourceUri += "/" + resourceId;
        }
        else if( suffix ) {
            resourceUri += "/detail";
        }
        if( query != null ) {
            resourceUri += query;
        }
        if (resourceUri != null && (!endpoint.endsWith("/") && !resourceUri.startsWith("/"))) {
            endpoint = endpoint+"/";
        }
        try {
            String response = getString(context.getAuthToken(), endpoint, resourceUri);

            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return getNetworks(resource, resourceId, suffix, query);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable String postServersForString(@Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri += "/" + (suffix ? (resourceId + "/action") : resourceId);
        }
        String computeEndpoint = context.getComputeUrl();

        if( computeEndpoint == null ) {
            throw new CloudException("No compute endpoint exists");
        }
        try {
            return postString(context.getAuthToken(), computeEndpoint, resourceUri, body.toString());
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return postServersForString(resource, resourceId, body, suffix);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject postServers(@Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri += "/" + (suffix ? (resourceId + "/action") : resourceId);
        }
        String computeEndpoint = context.getComputeUrl();

        if( computeEndpoint == null ) {
            throw new CloudException("No compute endpoint exists");
        }
        try {
            String response = postString(context.getAuthToken(), computeEndpoint, resourceUri, body.toString());
        
            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return postServers(resource, resourceId, body, suffix);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject postNetworks(@Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, @Nullable final String action) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri = resource + "/" + (action != null ? (resourceId + "/" + action) : resourceId);
        }
        String endpoint = context.getNetworkUrl();

        if( endpoint == null ) {
            throw new CloudException("No network endpoint exists");
        }

        if (resourceUri != null && (!endpoint.endsWith("/") && !resourceUri.startsWith("/"))) {
            endpoint = endpoint+"/";
        }
        try {
            String response = postString(context.getAuthToken(), endpoint, resourceUri, body.toString());

            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return postNetworks(resource, resourceId, body, action);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject putNetworks(@Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, final String action) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri = resource + "/" + (action != null ? (resourceId + "/" + action) : resourceId);
        }
        String endpoint = context.getNetworkUrl();

        if( endpoint == null ) {
            throw new CloudException("No network endpoint exists");
        }

        if (resourceUri != null && (!endpoint.endsWith("/") && !resourceUri.startsWith("/"))) {
            endpoint = endpoint+"/";
        }
        try {
            String response = putString(context.getAuthToken(), endpoint, resourceUri, body.toString());

            if( response == null ) {
                return null;
            }
            try {
                return new JSONObject(response);
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return putNetworks(resource, resourceId, body, action);
            }
            else {
                throw ex;
            }
        }
    }

    public @Nullable JSONObject postNetworks(@Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, final boolean suffix) throws CloudException, InternalException {
        return postNetworks(resource, resourceId, body, suffix ? "action" : null);
    }

    public @Nullable String getHPCDN(@Nullable final String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(HPCDN.SERVICE);

        if( endpoint == null ) {
            throw new CloudException("No CDN URL has been established in " + context.getMyRegion());
        }
        try {
            return getString(context.getAuthToken(), endpoint, resourceId == null ? "" : ("/" + resourceId));
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return getHPCDN(resourceId);
            }
            else {
                throw ex;
            }
        }
    }
    
    public void putHPCDN(final String container) throws CloudException, InternalException {
        Map<String,String> headers = new HashMap<String, String>();
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(HPCDN.SERVICE);

        if( endpoint == null ) {
            throw new CloudException("No CDN URL has been established in " + context.getMyRegion());
        }
        if( container == null ) {
            throw new InternalException("No container was specified");
        }
        headers.put("X-TTL", "86400");
        try {
            putHeaders(context.getAuthToken(), endpoint, "/" + container, headers);
        
            headers = headResource(HPCDN.SERVICE, HPCDN.RESOURCE, container);
            if( headers == null ) {
                throw new CloudException("No container enabled");
            }
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                putHPCDN(container);
            }
            else {
                throw ex;
            }
        }
    }

    public void postHPCDN(@Nonnull final String container, @Nonnull final Map<String,String> headers) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(HPCDN.SERVICE);

        if( endpoint == null ) {
            throw new CloudException("No CDN URL has been established in " + context.getMyRegion());
        }
        if( container == null ) {
            throw new InternalException("No container was specified");
        }
        try {
            postHeaders(context.getAuthToken(), endpoint, "/" + container, headers);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                postHPCDN(container, headers);
            }
            else {
                throw ex;
            }
        }
    }
    
    public void deleteHPCDN(@Nonnull final String container) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(HPCDN.SERVICE);

        if( endpoint == null ) {
            throw new CloudException("No CDN URL has been established in " + context.getMyRegion());
        }
        try {
            delete(context.getAuthToken(), endpoint, "/" + container);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                deleteHPCDN(container);
            }
            else {
                throw ex;
            }
        }
    }
}

