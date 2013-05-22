/**
 * Copyright (C) 2009-2013 Dell, Inc
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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.openstack.nova.os.ext.hp.db.HPRDBMS;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class AbstractMethod {
    protected NovaOpenStack provider;

    
    public AbstractMethod(NovaOpenStack provider) { this.provider = provider; }

    public synchronized @Nullable AuthenticationContext authenticate() throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".authenticate()");
        }
        try {
            ProviderContext ctx = provider.getContext();
            
            if( ctx == null ) {
                throw new CloudException("Unable to authenticate due to lack of context");
            }
            String endpoint = ctx.getEndpoint();
            
            if( endpoint == null ) {
                throw new CloudException("No authentication endpoint");
            }
            AuthenticationContext auth;
            
            if( endpoint.startsWith("ks:") ) {
                endpoint = endpoint.substring(3);
                auth = authenticateKeystone(endpoint);
            }
            else if( endpoint.startsWith("st:") ) {
                endpoint = endpoint.substring(3);
                auth = authenticateStandard(endpoint);
            }
            else {
                if( endpoint.endsWith("1.0") || endpoint.endsWith("1.0/") || endpoint.endsWith("1.1") || endpoint.endsWith("1.1/")) {
                    auth = authenticateStandard(endpoint);
                    if (auth == null) {
                    	auth = authenticateSwift(endpoint);
                    }
                    if( auth == null ) {
                        auth = authenticateKeystone(endpoint);
                    }
                }
                else {
                    auth = authenticateKeystone(endpoint);
                    if( auth == null ) {
                        auth = authenticateStandard(endpoint);
                    }
                    if (auth == null) {
                    	auth = authenticateSwift(endpoint);
                    }
                }
            }
            return auth;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticate()");
            }
        }
    }
    
    private @Nullable AuthenticationContext authenticateKeystone(@Nonnull String endpoint) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".authenticateKeystone(" + endpoint + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("KEYSTONE --------------------------------------------------------> " + endpoint);
            wire.debug("");
        }
        try {
            if( std.isInfoEnabled() ) {
                std.info("authenticateKeystone(): Attempting keystone authentication...");
            }
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> credentials = new HashMap<String,Object>();

            if( provider.getProviderName().equals("HP") ) {
                if( std.isInfoEnabled() ) {
                    std.info("HP authentication");
                }
                try {
                    credentials.put("accessKey", new String(provider.getContext().getAccessPublic(), "utf-8"));
                    credentials.put("secretKey", new String(provider.getContext().getAccessPrivate(), "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                json.put("apiAccessKeyCredentials", credentials);
            }
            else if( provider.getProviderName().equals("Rackspace") ) {
                if( std.isInfoEnabled() ) {
                    std.info("Rackspace authentication");
                }
                try {
                    credentials.put("username", new String(provider.getContext().getAccessPublic(), "utf-8"));
                    credentials.put("apiKey", new String(provider.getContext().getAccessPrivate(), "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                json.put("RAX-KSKEY:apiKeyCredentials", credentials);
            }
            else {
                if( std.isInfoEnabled() ) {
                    std.info("Standard authentication");
                }
                try {
                    credentials.put("username", new String(provider.getContext().getAccessPublic(), "utf-8"));
                    credentials.put("password", new String(provider.getContext().getAccessPrivate(), "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                json.put("passwordCredentials", credentials);
            }
            if( std.isDebugEnabled() ) {
                std.debug("authenticateKeystone(): tenantId=" + provider.getContext().getAccountNumber());
            }
            if( !provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
                String acct = provider.getContext().getAccountNumber();

                if( provider.getCloudProvider().equals(OpenStackProvider.HP) ) {
                    json.put("tenantId", acct);
                }
                else {
                    // a hack
                    if( acct.length() == 32 ) {
                        json.put("tenantId", acct);
                    }
                    else {
                        json.put("tenantName", acct);
                    }
                }
            }
            HashMap<String,Object> jsonAuth = new HashMap<String,Object>();
            
            jsonAuth.put("auth", json);

            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + "/tokens");
            
            post.addRequestHeader("Content-Type", "application/json");
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            String payload = (new JSONObject(jsonAuth)).toString();
            wire.debug(payload);
            wire.debug("");
            try {
                post.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticateKeystone(): UTF-8 is not supported locally: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("authenticateKeystone(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("authenticateKeystone(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpStatus.SC_OK ) {
                if( code == 401 ) {
                    std.warn("authenticateKeystone(): Authentication failed");
                    return null;
                }
                std.error("authenticateKeystone(): Expected OK, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("authenticateKeystone(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + "/tokens";
                }
                std.error("authenticateKeystone(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                String response;
                    
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("authenticateKeystone(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);                    
                }
                if( response != null && !response.trim().equals("") ) {
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response);
                    }
                    wire.debug("");

                    if( std.isInfoEnabled() ) {
                        std.info("authenticateKeystone(): Keystone authentication successful");
                    }
                    String id, tenantId;
                    JSONArray catalog;
                    JSONObject token;

                    try {
                        JSONObject rj = new JSONObject(response);
                        JSONObject auth = rj.getJSONObject("access");
                        
                        token = auth.getJSONObject("token");
                        catalog = auth.getJSONArray("serviceCatalog");
                        id = (token.has("id") ? token.getString("id") : null);
                        tenantId = (token.has("tenantId") ? token.getString("tenantId") : null);
                    }
                    catch( JSONException e ) {
                        std.error("authenticateKeystone(): Invalid response from server: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( tenantId == null ) {
                        tenantId = provider.getContext().getAccountNumber();
                    }
                    if( id != null ) {
                        HashMap<String,Map<String,String>> services = new HashMap<String,Map<String,String>>();
                        HashMap<String,Map<String,String>> bestVersion = new HashMap<String,Map<String,String>>();
                        String myRegionId = provider.getContext().getRegionId();

                        if( std.isDebugEnabled() ) {
                            std.debug("authenticateKeystone(): myRegionId=" + myRegionId);
                        }
                        if( std.isInfoEnabled() ) {
                            std.info("authenticateKeystone(): Processing service catalog...");
                        }
                        for( int i=0; i<catalog.length(); i++ ) {
                            try {
                                JSONObject service = catalog.getJSONObject(i);
                                /*
                                System.out.println("---------------------------------------------------");
                                System.out.println("Service=");
                                System.out.println(service.toString());
                                System.out.println("---------------------------------------------------");
                                */
                                String type = service.getString("type");
                                JSONArray endpoints = service.getJSONArray("endpoints");

                                if( std.isDebugEnabled() ) {
                                    std.debug("authenticateKeystone(): type=" + type);
                                }
                                for( int j=0; j<endpoints.length(); j++ ) {
                                    JSONObject test = endpoints.getJSONObject(j);
                                    String url = test.getString("publicURL");

                                    if( std.isDebugEnabled() ) {
                                        std.debug("authenticateKeystone(): endpoint[" + j + "]=" + url);
                                    }
                                    if( url != null ) {
                                        String version = test.optString("versionId");

                                        if( version == null || version.equals("") ) {
                                            std.debug("No versionId parameter... Parsing URL " + url + " for best guess.  (vSadTrombone)");
                                            Pattern p = Pattern.compile("/v(.+?)/|/v(.+?)$");
                                            Matcher m = p.matcher(url);
                                            if (m.find()) {
                                                version = m.group(1);
                                                if( version == null ) {
                                                    version = m.group(2);
                                                }
                                            } else {
                                                version = "1.0";
                                            }
                                        }
                                        if( std.isDebugEnabled() ) {
                                            std.debug("authenticateKeystone(): version[" + j + "]=" + version);
                                        }
                                        if( NovaOpenStack.isSupported(version) ) {
                                            String regionId = (test.has("region") ? test.getString("region") : null);
                                            
                                            if( std.isDebugEnabled() ) {
                                                std.debug("authenticateKeystone(): region[" + j + "]=" + regionId);                                                
                                            }
                                            Map<String,String> map = services.get(type);
                                            Map<String,String> verMap = bestVersion.get(type);

                                            if( map == null ) {
                                                map = new HashMap<String,String>();
                                                verMap = new HashMap<String,String>();
                                                if( std.isInfoEnabled() ) {
                                                    std.info("authenticateKeystone(): Putting ("+type+","+map+") into services.");
                                                }
                                                services.put(type, map);
                                                bestVersion.put(type, verMap);
                                            }
                                            if( regionId == null & version.equals("1.0") && !provider.getProviderName().equals("Rackspace") ) {
                                                std.warn("authenticateKeystone(): No region defined, making one up based on the URL: " + url);
                                                regionId = toRegion(url);
                                                std.warn("authenticateKeystone(): Fabricated region is: " + regionId);
                                            }
                                            else if( regionId == null && (type.equals("compute") || type.equals("object-store")) ) {
                                                std.warn("authenticateKeystone(): No region defined for Rackspace, assuming it is pre-OpenStack and skipping");
                                                continue;
                                            }
                                            if( std.isDebugEnabled() ) {
                                                std.debug("authenticateKeystone(): finalRegionId=" + regionId);
                                            }
                                            if( std.isInfoEnabled() ) {
                                                std.info("authenticateKeystone(): Comparing " + version + " against " + verMap.get(type));
                                            }
                                            if (verMap.get(type) == null || compareVersions(version, verMap.get(type)) >= 0 ) {
                                                if( std.isInfoEnabled() ) {
                                                    std.info("authenticateKeystone(): Putting ("+regionId+","+url+") into the "+type+" map.");
                                                }
            	                                verMap.put(type, version);
            	                                map.put(regionId, url);
                                            } 
                                            else {
                                                std.warn("authenticateKeystone(): Skipping lower version url "+url+" for " + type+ " map.");
                                            }

                                            if( myRegionId == null ) {
                                                myRegionId = regionId;
                                                if( std.isInfoEnabled() ) {
                                                    std.info("authenticateKeystone(): myRegionId now " + myRegionId);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            catch( JSONException e ) {
                                std.error("authenticateKeystone(): Failed to read JSON from server: " + e.getMessage());
                                e.printStackTrace();
                                throw new CloudException(e);
                            }
                        }
                        if( std.isDebugEnabled() ) {
                            std.debug("services=" + services);
                        }
                        // TODO: remove this when HP has the DBaaS in the service catalog
                        if( provider.getProviderName().equals("HP") && provider.getContext().getAccountNumber().equals("66565797737008") ) {
                            HashMap<String,String> endpoints = new HashMap<String, String>();

                            endpoints.put("region-a.geo-1", "https://region-a.geo-1.dbaas-mysql.hpcloudsvc.com:8779/v1.0/66565797737008");
                            services.put(HPRDBMS.SERVICE, endpoints);
                        }
                        return new AuthenticationContext(myRegionId, id, tenantId, services, null);
                    }
                }
            }
            throw new CloudException("No authentication tokens were provided");
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticateKeystone()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("KEYSTONE --------------------------------------------------------> " + endpoint);
            }            
        }
    }
    
    private @Nullable AuthenticationContext authenticateStandard(@Nonnull String endpointUrls) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".authenticateStandard(" + endpointUrls + ")");
        }
        try {
            String[] endpoints;
            
            if( endpointUrls.indexOf(',') > 0 ) {
                endpoints = endpointUrls.split(",");
            }
            else {
                endpoints = new String[] { endpointUrls };
            }
            HashMap<String,Map<String,String>> services = new HashMap<String,Map<String,String>>();
            String authToken = null, myRegion = provider.getContext().getRegionId();
            String tenantId = provider.getContext().getAccountNumber();

            for( String endpoint : endpoints ) {
                if( wire.isDebugEnabled() ) {
                    wire.debug("STANDARD --------------------------------------------------------> " + endpoint);
                    wire.debug("");
                }
                try {
                    ProviderContext ctx = provider.getContext();
                    HttpClient client = getClient();
                    GetMethod get = new GetMethod(endpoint);
                    
                    try {
                        get.addRequestHeader("Content-Type", "application/json");
                        get.addRequestHeader("X-Auth-User", new String(ctx.getAccessPublic(), "utf-8"));
                        get.addRequestHeader("X-Auth-Key", new String(ctx.getAccessPrivate(), "utf-8"));
                        get.addRequestHeader("X-Auth-Project-Id", ctx.getAccountNumber());
                    }
                    catch( UnsupportedEncodingException e ) {
                        std.error("authenticate(): Unsupported encoding when building request headers: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new InternalException(e);
                    }
                    get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
                    if( wire.isDebugEnabled() ) {
                        wire.debug("GET " + get.getPath());
                        for( Header header : get.getRequestHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    int code;
                    
                    try {
                        code = client.executeMethod(get);
                    }
                    catch( IOException e ) {
                        std.error("authenticate(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);
                    }
                    if( std.isDebugEnabled() ) {
                        std.debug("authenticate(): HTTP Status " + code);
                    }
                    Header[] headers = get.getResponseHeaders();
                    
                    if( wire.isDebugEnabled() ) {
                        wire.debug(get.getStatusLine().toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                    }
                    if( code != HttpStatus.SC_NO_CONTENT ) {
                        if( code == HttpStatus.SC_FORBIDDEN || code == HttpStatus.SC_UNAUTHORIZED ) {
                            return null;
                        }
                        std.error("authenticateStandard(): Expected NO CONTENT for an authentication request, got " + code);
                        String response;
                        
                        try {
                            response = get.getResponseBodyAsString();
                        }
                        catch( IOException e ) {
                            std.error("authenticateStandard(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                            if( std.isTraceEnabled() ) {
                                e.printStackTrace();
                            }
                            throw new CloudException(e);                    
                        }
                        if( code == HttpStatus.SC_INTERNAL_SERVER_ERROR && response.contains("<faultstring>") ) {
                            return null;
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        NovaException.ExceptionItems items = NovaException.parseException(code, response);
                        
                        if( items.type.equals(CloudErrorType.AUTHENTICATION) ) {
                            return null;
                        }
                        std.error("authenticateStandard(): [" +  code + " : " + items.message + "] " + items.details);
                        throw new NovaException(items);
                    }
                    else {
                        String cdnUrl = null, computeUrl = null, objectUrl = null;
                        String thisRegion = toRegion(endpoint);
                        
                        if( myRegion == null ) {
                            myRegion = thisRegion;
                        }
                        for( Header h : headers ) {
                            if( h.getName().equalsIgnoreCase("x-auth-token") && myRegion.equals(thisRegion) ) {
                                authToken = h.getValue().trim();
                            }
                            else if( h.getName().equalsIgnoreCase("x-server-management-url") ) {
                                String url = h.getValue().trim();
                                
                                if( url.endsWith("/") ) {
                                    url = url.substring(0,url.length()-1);
                                }
                                if( endpoint.endsWith("v1.0") ) {
                                    url = url + "/v1.0";
                                }
                                computeUrl = url;
                            }
                            else if( h.getName().equalsIgnoreCase("x-storage-url") ) {
                                objectUrl = h.getValue().trim();
                            }
                            else if( h.getName().equalsIgnoreCase("x-cdn-management-url") ) {
                                cdnUrl = h.getValue().trim();
                            }
                        }
                        if( computeUrl != null ) {
                            Map<String,String> map = services.get("compute");
                            
                            if( map == null ) {
                                map = new HashMap<String,String>();
                                map.put(thisRegion, computeUrl);
                            }
                            services.put("compute", map);
                        }
                        if( objectUrl != null ) {
                            Map<String,String> map = services.get("object-store");
                            
                            if( map == null ) {
                                map = new HashMap<String,String>();
                                map.put(thisRegion, objectUrl);                     
                            }
                            services.put("object-store", map);
                        }
                        if( cdnUrl != null ) {
                            Map<String,String> map = services.get("cdn");

                            if( map == null ) {
                                map = new HashMap<String,String>();
                                map.put(thisRegion, cdnUrl);
                                
                            }
                            services.put("cdn", map);
                        }
                    }
                }
               finally {
                   if( wire.isDebugEnabled() ) {
                       wire.debug("");
                       wire.debug("STANDARD --------------------------------------------------------> " + endpoint);
                   }                    
               }
            }
            if( authToken == null ) {
                std.warn("authenticateStandard(): No authentication token in response");
                throw new CloudException("No authentication token in cloud response");
            }
            return new AuthenticationContext(myRegion, authToken, tenantId, services, null);
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticateStandard()");
            }           
        }
    }
    
    
    private @Nullable AuthenticationContext authenticateSwift(@Nonnull String endpoint) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".authenticate()");
        }
        String tenantId = provider.getContext().getAccountNumber();
        String authToken = null, storageToken = null, myRegion = provider.getContext().getRegionId();
        String thisRegion = toRegion(endpoint);
        
        if( myRegion == null ) {
            myRegion = thisRegion;
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(endpoint);
            
            try {
                ProviderContext ctx = provider.getContext();
                String account;
            
                if( ctx.getAccessPublic().length < 1 ) {
                    account = ctx.getAccountNumber();
                }
                else {
                    String pk = new String(ctx.getAccessPublic(), "utf-8");
                    
                    if( pk.equals("-----") ) {
                        account = ctx.getAccountNumber();                        
                    }
                    else {
                        account = ctx.getAccountNumber() + ":" + new String(ctx.getAccessPublic(), "utf-8");
                    }
                }
                get.addRequestHeader("Content-Type", "application/json");
                get.addRequestHeader("X-Auth-User", account);
                get.addRequestHeader("X-Auth-Key", new String(ctx.getAccessPrivate(), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticate(): Unsupported encoding when building request headers: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("authenticate(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("authenticate(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
            }
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK ) {
                if( code == HttpStatus.SC_FORBIDDEN || code == HttpStatus.SC_UNAUTHORIZED ) {
                    return null;
                }
                std.error("authenticate(): Expected NO CONTENT for an authentication request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("authenticate(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items.type.equals(CloudErrorType.AUTHENTICATION) ) {
                    return null;
                }
                std.error("authenticate(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                HashMap<String,Map<String,String>> services = new HashMap<String,Map<String,String>>();
                
                for( Header h : headers ) {
                    if( h.getName().equalsIgnoreCase("x-auth-token") ) {
                    	authToken = h.getValue().trim();
                    }
                    else if( h.getName().equalsIgnoreCase("x-server-management-url") ) {
                        Map<String,String> map = services.get("compute");
                        
                        if( map == null ) {
                            map = new HashMap<String,String>();
                            map.put(thisRegion, h.getValue().trim());
                        }
                        services.put("compute", map);
                    }
                    else if( h.getName().equalsIgnoreCase("x-storage-url") ) {
                        Map<String,String> map = services.get("object-store");
                        
                        if( map == null ) {
                            map = new HashMap<String,String>();
                            map.put(thisRegion, h.getValue().trim());
                        }
                        services.put("object-store", map);
                    }
                    else if( h.getName().equalsIgnoreCase("x-cdn-management-url") ) {
                        Map<String,String> map = services.get("cdn");

                        if( map == null ) {
                            map = new HashMap<String,String>();
                            map.put(thisRegion, h.getValue().trim());
                        }
                        services.put("cdn", map);
                    }
                    else if( h.getName().equalsIgnoreCase("x-storage-token") ) {
                    	storageToken = h.getValue().trim();
                    }
                }
                if( authToken == null ) {
                    std.warn("authenticate(): No authentication token in response");
                    throw new CloudException("No authentication token in cloud response");
                }
                return new AuthenticationContext(thisRegion, authToken, tenantId, services, storageToken);
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticate()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint);
            }            
        }
    }

    public void deleteResource(@Nonnull String service, @Nonnull String resource, @Nonnull String resourceId, String suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
        }
        if( suffix == null ) {
            resource = resource + "/" + resourceId;
        }
        else {
            resource = resource + "/" + resourceId + "/" + suffix;
        }
        delete(context.getAuthToken(), endpoint, resource);
    }
    
    protected void delete(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".delete(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            DeleteMethod delete = new DeleteMethod(endpoint + resource);
            
            delete.addRequestHeader("Content-Type", "application/json");
            delete.addRequestHeader("X-Auth-Token", authToken);
            delete.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("DELETE " + delete.getPath());
                for( Header header : delete.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(delete);
            }
            catch( IOException e ) {
                std.error("delete(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("delete(): HTTP Status " + code);
            }
            Header[] headers = delete.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_ACCEPTED ) {
                std.error("delete(): Expected NO CONTENT for DELETE request, got " + code);
                String response;
                
                try {
                    response = delete.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("delete(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("delete(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                wire.debug("");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".delete()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    public @Nullable JSONArray getList(@Nonnull String service, @Nonnull String resource, boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        if( suffix ) {
            resource = resource + "/detail";
        }
        String response = getString(context.getAuthToken(), endpoint, resource);

        if( response == null ) {
            return null;
        }
        try {
            return new JSONArray(response);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
        }
    }

    public @Nullable String[] getItemList(@Nonnull String service, @Nonnull String resource, boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        if( suffix ) {
            resource = resource + "/detail";
        }
        String response = getString(context.getAuthToken(), endpoint, resource);

        if( response == null ) {
            return null;
        }
        if( response.length() < 1 ) {
            return new String[0];
        }
        String[] items = response.split("\n");

        if( items == null || items.length < 1 ) {
            return new String[] { response.trim() };
        }
        for( int i=0; i< items.length; i++ ) {
            items[i] = items[i].trim();
        }
        return items;
    }
    
    public @Nullable JSONObject getResource(@Nonnull String service, @Nonnull String resource, @Nullable String resourceId, boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        if( resourceId != null ) {
            if( resourceId.startsWith("?") ) {
                resource = resource + resourceId;

            }
            else {
                resource = resource + "/" + resourceId;
            }
        }
        else if( suffix ) {
            resource = resource + "/detail";
        }
        String response = getString(context.getAuthToken(), endpoint, resource);

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
    
    protected @Nullable String getString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".getString(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(resource == null ? endpoint : endpoint + resource);
            
            get.addRequestHeader("Content-Type", "application/json");
            get.addRequestHeader("X-Auth-Token", authToken);
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("getString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("getString(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpStatus.SC_NOT_FOUND ) {
                return null;
            }
            if( code == HttpStatus.SC_BAD_REQUEST ) {
                String response;

                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                try {
                    JSONObject err = (new JSONObject(response)).getJSONObject("badRequest");
                    String msg = err.getString("message");

                    if( msg.contains("id should be integer") ) {
                        return null;
                    }
                }
                catch( JSONException e ) {
                    // ignore
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);

                if( items == null ) {
                    return null;
                }
                if( provider.getMajorVersion() == 1 && provider.getMinorVersion() == 0 && items.message != null && (items.message.contains("not found") || items.message.contains("unknown")) ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK && code != HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getString(): Expected OK for GET request, got " + code);
                String response;

                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                if( provider.getMajorVersion() == 1 && provider.getMinorVersion() == 0 && items.message != null && (items.message.contains("not found") || items.message.contains("unknown")) ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                return response;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".getString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable InputStream getStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".getStream(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(endpoint + resource);
            
            get.addRequestHeader("Content-Type", "application/json");
            get.addRequestHeader("X-Auth-Token", authToken);
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("getStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("getStream(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpStatus.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpStatus.SC_OK && code != HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getStream(): Expected OK for GET request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("getStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                InputStream input;
                
                try {
                    input = get.getResponseBodyAsStream();
                }
                catch( IOException e ) {
                    std.error("getStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug("---> Binary Data <---");
                }
                wire.debug("");
                return input;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".getStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }
    
    protected @Nonnull HttpClient getClient() {
        ProviderContext ctx = provider.getContext();
        HttpClient client = new HttpClient();

        if( ctx != null ) {
            Properties p = ctx.getCustomProperties();
            
            if( p != null ) {
                String proxyHost = p.getProperty("proxyHost");
                String proxyPort = p.getProperty("proxyPort");

                if( proxyHost != null ) {
                    int port = 0;

                    if( proxyPort != null && proxyPort.length() > 0 ) {
                        port = Integer.parseInt(proxyPort);
                    }
                    client.getHostConfiguration().setProxy(proxyHost, port);
                }
            }
        }
        return client;
    }

    public @Nullable Map<String,String> headResource(@Nonnull String service, @Nullable String resource, @Nullable String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        if( resource == null && resourceId == null ) {
            resource = "/";
        }
        else if( resource == null ) {
            resource = "/" + resourceId;
        }
        else if( resourceId != null ) {
            resource = resource + "/" + resourceId;
        }
        return head(context.getAuthToken(), endpoint, resource);
    }

    protected @Nullable Map<String,String> head(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".head(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HeadMethod head = new HeadMethod(endpoint + resource);
            
            head.addRequestHeader("X-Auth-Token", authToken);
            head.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("HEAD " + head.getPath());
                for( Header header : head.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(head);
            }
            catch( IOException e ) {
                std.error("head(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("head(): HTTP Status " + code);
            }
            Header[] headers = head.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(head.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK ) {
                if( code == HttpStatus.SC_NOT_FOUND ) {
                    return null;
                }
                std.error("head(): Expected NO CONTENT or OK for HEAD request, got " + code);
                String response;
                
                try {
                    response = head.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("head(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("head(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            HashMap<String,String> map = new HashMap<String,String>();
            
            for( Header h : headers ) {
                map.put(h.getName().trim(), h.getValue().trim());
            }
            return map;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".head()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    public void postResourceHeaders(String service, String resource, String resourceId, Map<String,String> headers) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " has been established in " + context.getMyRegion());
        }
        if( resourceId == null ) {
            throw new InternalException("No container was specified");
        }
        postHeaders(context.getAuthToken(), endpoint, resource + "/" + resourceId, headers);
    }
    
    @SuppressWarnings("unused")
    protected @Nullable String postHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postString(" + authToken + "," + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/json");
            post.addRequestHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addRequestHeader(entry.getKey(), val);
                }
            }
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postString(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".postString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    public @Nullable JSONObject postString(@Nonnull String service, @Nonnull String resource, @Nullable String resourceId, @Nonnull String extra, @Nonnull JSONObject body) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);
        
        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
        }
        String response = postString(context.getAuthToken(), endpoint, resource + "/" + resourceId + "/" + extra, body.toString());

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
    
    public @Nullable JSONObject postString(@Nonnull String service, @Nonnull String resource, @Nullable String resourceId, @Nonnull JSONObject body, boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        if( resourceId != null ) {
            resource = resource + "/" + (suffix ? (resourceId + "/action") : resourceId);
        }
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
        }
        String response = postString(context.getAuthToken(), endpoint, resource, body.toString());

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
    
    protected @Nullable String postString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull String payload) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postString(" + authToken + "," + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/json");
            post.addRequestHeader("X-Auth-Token", authToken);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    post.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("postString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postString(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            String location = null;
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getName().equalsIgnoreCase("Location") ) {
                        location = (h.getValue() == null ? null : h.getValue().trim());
                    }
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpStatus.SC_OK && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_CREATED ) {
                std.error("postString(): Expected OK, ACCEPTED, or NO CONTENT for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                if( code != HttpStatus.SC_NO_CONTENT ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                    else if( code == HttpStatus.SC_ACCEPTED ) {
                        return "{\"location\" : \"" + location + "\"}";
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".postString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    @SuppressWarnings("unused")
    protected @Nullable String postStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull String md5Hash, @Nonnull InputStream stream) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postStream(" + authToken + "," + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/octet-stream");
            post.addRequestHeader("X-Auth-Token", authToken);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            wire.debug("---> BINARY DATA <---");
            wire.debug("");
            post.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postStream(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : post.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("postStream(): Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                wire.debug("");
                if( code == HttpStatus.SC_ACCEPTED ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaOpenStack.class.getName() + ".postStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    public void putResourceHeaders(@Nonnull String service, @Nullable String resource, @Nullable String resourceId, @Nonnull Map<String,String> headers) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " has been established in " + context.getMyRegion());
        }
        if( resource == null && resourceId == null ) {
            resource = "/";
        }
        else if( resource == null ) {
            resource = "/" + resourceId;
        }
        else if( resourceId != null ) {
            resource = resource + "/" + resourceId;
        }
        putHeaders(context.getAuthToken(), endpoint, resource, headers);
    }

    @SuppressWarnings("unused")
    protected @Nonnull String putHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putHeaders(" + authToken + "," + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addRequestHeader(entry.getKey(), val);
                }
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String putString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putString(" + authToken + "," + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    put.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("putString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String putStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nonnull InputStream stream) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putStream(" + authToken + "," + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/octet-stream");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addRequestHeader("ETag", md5Hash);
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                wire.debug("---> BINARY DATA <---");
                wire.debug("");                
            }

            put.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putStream(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : put.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("putStream(): Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, response);
                
                if( items == null ) {
                    items = new NovaException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaOpenStack.class.getName() + ".putStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    private @Nonnull String toRegion(@Nonnull String endpoint) {
        Logger logger = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + AbstractMethod.class.getName() + ".toRegion(" + endpoint + ")");
        }
        try {
            if( logger.isInfoEnabled() ) {
                logger.info("Looking up official region for " + endpoint);
            }
            String host;
            
            try {
                URI uri = new URI(endpoint);
            
                host = uri.getHost();
            }
            catch( URISyntaxException e ) {
                host = endpoint;
            }
            String[] parts = host.split("\\.");
            String regionId;
            
            if( parts.length < 3 ) {
                regionId = host;
            }
            else if( parts.length == 3 ) {
                regionId = parts[0];
            }
            else {
                regionId = parts[0] + "." + parts[1];
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("regionId=" + regionId);
            }
            return regionId;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + AbstractMethod.class.getName() + ".toRegion()");
            }
        }
    }

    private @Nonnegative int compareVersions(@Nullable String ver1, @Nullable String ver2) throws InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + AbstractMethod.class.getName() + ".compareVersions("+ver1+","+ver2+")");
        }
		int result = 0;
		
		if( ver1 == null && ver2 == null ) {
			return 0;
        }
        else if( ver1 == null ) {
            ver1 = "1.0";
        }
        else if( ver2 == null ) {
            ver2 = "1.0";
        }
		//Assumes only x.x granularity.   Anything more will require rewrite of this component.
		try {
            String majorStr1 = ver1.contains(".") ? ver1.substring(0,ver1.indexOf(".")) : ver1;
            String minorStr1 = ver1.contains(".") ? ver1.substring(ver1.indexOf(".")+1) : "0";
            String majorStr2 = ver2.contains(".") ? ver2.substring(0,ver2.indexOf(".")) : ver2;
            String minorStr2 = ver2.contains(".") ? ver2.substring(ver2.indexOf(".")+1) : "0";
            int major1 = Integer.parseInt(majorStr1);
            int minor1 = Integer.parseInt(minorStr1);
            int major2 = Integer.parseInt(majorStr2);
            int minor2 = Integer.parseInt(minorStr2);
            result = major1 - major2;
            if( result == 0 ) {
                result = minor1 - minor2;
            }
		} catch (Exception e) {
			// Something really stupid showed up in the version string...
            throw new InternalException(e);
		}
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - "+AbstractMethod.class.getName() + ".compareVersions("+ver1+","+ver2+") = "+result);
            }
        }
		
		return result;
	}
}
