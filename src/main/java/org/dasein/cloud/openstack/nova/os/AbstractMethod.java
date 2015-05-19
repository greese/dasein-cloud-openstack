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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ContextRequirements;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.openstack.nova.os.ext.hp.db.HPRDBMS;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
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

        HttpClient client = null;

        try {
            String accessPublic = null;
            String accessPrivate = null;
            String account = provider.getContext().getAccountNumber();
            try {
                List<ContextRequirements.Field> fields = provider.getContextRequirements().getConfigurableValues();
                for(ContextRequirements.Field f : fields ) {
                    if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                        byte[][] keyPair = (byte[][])provider.getContext().getConfigurationValue(f);
                        accessPublic = new String(keyPair[0], "utf-8");
                        accessPrivate = new String(keyPair[1], "utf-8");
                    }
                }
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }

            if( std.isInfoEnabled() ) {
                std.info("authenticateKeystone(): Attempting keystone authentication...");
            }
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> credentials = new HashMap<String,Object>();

            if( provider.getCloudProvider().equals(OpenStackProvider.HP) ) {
                if( std.isInfoEnabled() ) {
                    std.info("HP authentication");
                }
                credentials.put("accessKey", accessPublic);
                credentials.put("secretKey", accessPrivate);
                json.put("apiAccessKeyCredentials", credentials);
            }
            else if( provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
                if( std.isInfoEnabled() ) {
                    std.info("Rackspace authentication");
                }
                credentials.put("username", accessPublic);
                credentials.put("apiKey", accessPrivate);
                json.put("RAX-KSKEY:apiKeyCredentials", credentials);
            }
            else {
                if( std.isInfoEnabled() ) {
                    std.info("Standard authentication");
                }
                credentials.put("username", accessPublic);
                credentials.put("password", accessPrivate);
                json.put("passwordCredentials", credentials);
            }
            if( std.isDebugEnabled() ) {
                std.debug("authenticateKeystone(): tenantId=" + account);
            }
            if( !provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
                String acct = account;

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

            client = getClient();
            HttpPost post = new HttpPost(endpoint + "/tokens");
            
            post.addHeader("Content-Type", "application/json");
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }


            String payload = (new JSONObject(jsonAuth)).toString();

            try {
                //noinspection deprecation
                post.setEntity(new StringEntity(payload == null ? "" : payload, "application/json", "UTF-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            /*try { wire.debug(EntityUtils.toString(post.getEntity())); }
            catch( IOException ignore ) { }*/

            wire.debug("");

            HttpResponse response;

            try {
                APITrace.trace(provider, "POST authenticateKeystone");
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_OK ) {
                if( code == 401 || code == 405 ) {
                    std.warn("authenticateKeystone(): Authentication failed");
                    return null;
                }
                std.error("authenticateKeystone(): Expected OK, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
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
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                if( data != null && !data.trim().equals("") ) {
                    if( std.isInfoEnabled() ) {
                        std.info("authenticateKeystone(): Keystone authentication successful");
                    }
                    String id, tenantId;
                    JSONArray catalog;
                    JSONObject token;

                    try {
                        JSONObject rj = new JSONObject(data);
                        JSONObject auth = rj.getJSONObject("access");
                        
                        token = auth.getJSONObject("token");
                        catalog = auth.getJSONArray("serviceCatalog");
                        id = (token.has("id") ? token.getString("id") : null);
                        tenantId = ((token.has("tenantId") && !token.isNull("tenantId")) ? token.getString("tenantId") : null);
                        if( tenantId == null && token.has("tenant") && !token.isNull("tenant") ) {
                            JSONObject t = token.getJSONObject("tenant");

                            if( t.has("id") && !t.isNull("id") ) {
                                tenantId = t.getString("id");
                            }
                        }
                    }
                    catch( JSONException e ) {
                        std.error("authenticateKeystone(): Invalid response from server: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( tenantId == null ) {
                        tenantId = account;
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
                                                if (version == null)
                                                    version = m.group(2);
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
                                            if( regionId == null & version.equals("1.0") && !provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
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
                        if( provider.getCloudProvider().equals(OpenStackProvider.HP) && provider.getContext().getAccountNumber().equals("66565797737008") ) {
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
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
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
            String accessPublic = null;
            String accessPrivate = null;
            String account = provider.getContext().getAccountNumber();
            try {
                List<ContextRequirements.Field> fields = provider.getContextRequirements().getConfigurableValues();
                for(ContextRequirements.Field f : fields ) {
                    if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                        byte[][] keyPair = (byte[][])provider.getContext().getConfigurationValue(f);
                        accessPublic = new String(keyPair[0], "utf-8");
                        accessPrivate = new String(keyPair[1], "utf-8");
                    }
                }
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            String[] endpoints;
            
            if( endpointUrls.indexOf(',') > 0 ) {
                endpoints = endpointUrls.split(",");
            }
            else {
                endpoints = new String[] { endpointUrls };
            }
            HashMap<String,Map<String,String>> services = new HashMap<String,Map<String,String>>();
            String authToken = null, myRegion = provider.getContext().getRegionId();
            String tenantId = account;

            for( String endpoint : endpoints ) {
                if( wire.isDebugEnabled() ) {
                    wire.debug("STANDARD --------------------------------------------------------> " + endpoint);
                    wire.debug("");
                }
                
                HttpClient client = null;

                try {
                    ProviderContext ctx = provider.getContext();
                    client = getClient();
                    HttpGet get = new HttpGet(endpoint);

                    get.addHeader("Content-Type", "application/json");
                    get.addHeader("X-Auth-User", accessPublic);
                    get.addHeader("X-Auth-Key", accessPrivate);
                    get.addHeader("X-Auth-Project-Id", account);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(get.getRequestLine().toString());
                        for( Header header : get.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    HttpResponse response;

                    try {
                        APITrace.trace(provider, "GET authenticateStandard");
                        response = client.execute(get);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response.getStatusLine().toString());
                            for( Header header : response.getAllHeaders() ) {
                                wire.debug(header.getName() + ": " + header.getValue());
                            }
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        std.error("I/O error from server communications: " + e.getMessage());
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                    int code = response.getStatusLine().getStatusCode();

                    std.debug("HTTP STATUS: " + code);
                    if( code != HttpStatus.SC_NO_CONTENT ) {
                        if( code == HttpStatus.SC_FORBIDDEN || code == HttpStatus.SC_UNAUTHORIZED ) {
                            return null;
                        }
                        std.error("authenticateStandard(): Expected NO CONTENT for an authentication request, got " + code);
                        String data = null;

                        try {
                            HttpEntity entity = response.getEntity();

                            if( entity != null ) {
                                data = EntityUtils.toString(entity);
                                if( wire.isDebugEnabled() ) {
                                    wire.debug(data);
                                    wire.debug("");
                                }
                            }
                        }
                        catch( IOException e ) {
                            std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                            e.printStackTrace();
                            throw new CloudException(e);
                        }
                        if( code == HttpStatus.SC_INTERNAL_SERVER_ERROR && data.contains("<faultstring>") ) {
                            return null;
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        NovaException.ExceptionItems items = NovaException.parseException(code, data);
                        
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
                        for( Header h : response.getAllHeaders() ) {
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
                   if (client != null) {
                       client.getConnectionManager().shutdown();
                   }
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
        String accessPublic = null;
        String accessPrivate = null;
        String accountNum = provider.getContext().getAccountNumber();
        try {
            List<ContextRequirements.Field> fields = provider.getContextRequirements().getConfigurableValues();
            for(ContextRequirements.Field f : fields ) {
                if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                    byte[][] keyPair = (byte[][])provider.getContext().getConfigurationValue(f);
                    accessPublic = new String(keyPair[0], "utf-8");
                    accessPrivate = new String(keyPair[1], "utf-8");
                }
            }
        }
        catch( UnsupportedEncodingException e ) {
            std.error("authenticateKeystone(): Unable to read access credentials: " + e.getMessage());
            e.printStackTrace();
            throw new InternalException(e);
        }
        String tenantId = accountNum;
        String authToken = null, storageToken = null;
        String thisRegion = toRegion(endpoint);

        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint);
            wire.debug("");
        }
        
        HttpClient client = null;
        try {
            client = getClient();
            HttpGet get = new HttpGet(endpoint);

            ProviderContext ctx = provider.getContext();
            String account;

            if( accessPublic.length() < 1 ) {
                account = accountNum;
            }
            else {
                String pk = accessPublic;

                if( pk.equals("-----") ) {
                    account = accountNum;
                }
                else {
                    account = accountNum + ":" + accessPublic;
                }
            }
            get.addHeader("Content-Type", "application/json");
            get.addHeader("X-Auth-User", account);
            get.addHeader("X-Auth-Key", accessPrivate);
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "GET authenticateSwift");
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK ) {
                if( code == HttpStatus.SC_FORBIDDEN || code == HttpStatus.SC_UNAUTHORIZED ) {
                    return null;
                }
                std.error("authenticate(): Expected NO CONTENT for an authentication request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
                if( items.type.equals(CloudErrorType.AUTHENTICATION) ) {
                    return null;
                }
                std.error("authenticate(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                HashMap<String,Map<String,String>> services = new HashMap<String,Map<String,String>>();
                
                for( Header h : response.getAllHeaders() ) {
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
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticate()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint);
            }            
        }
    }

    public void deleteResource(@Nonnull final String service, @Nonnull final String resource, @Nonnull final String resourceId, @Nullable final String suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
        }
        String resourceUri = resource + "/" + resourceId;
        if( suffix != null ) {
            resourceUri = resource + "/" + resourceId + "/" + suffix;
        }
        try {
            delete(context.getAuthToken(), endpoint, resourceUri);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                deleteResource(service, resource, resourceId, suffix);
            }
            else {
                throw ex;
            }
        }
    }
    
    protected void delete(@Nonnull final String authToken, @Nonnull final String endpoint, @Nonnull final String resource) throws CloudException, InternalException {
        Logger std = NovaOpenStack.getLogger(NovaOpenStack.class, "std");
        Logger wire = NovaOpenStack.getLogger(NovaOpenStack.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".delete(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        HttpClient client = null;
        try {
            client = getClient();
            HttpDelete delete = new HttpDelete(endpoint + resource);
            
            delete.addHeader("Content-Type", "application/json");
            delete.addHeader("X-Auth-Token", authToken);
            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getRequestLine().toString());
                for( Header header : delete.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "DELETE " + toAPIResource(resource));
                response = client.execute(delete);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_OK ) {
                std.error("delete(): Expected NO CONTENT for DELETE request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
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
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".delete()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    public @Nullable String[] getItemList(@Nonnull final String service, @Nonnull final String resource, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource;
        if( suffix ) {
            resourceUri += "/detail";
        }
        try {
            String response = getString(context.getAuthToken(), endpoint, resourceUri);

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
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return getItemList(service, resource, suffix);
            }
            else {
                throw ex;
            }
        }
    }
    
    public @Nullable JSONObject getResource(@Nonnull final String service, @Nonnull final String resource, @Nullable final String resourceId, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource;
        if( resourceId != null ) {
            if( resourceId.startsWith("?") ) {
                resourceUri += resourceId;
            }
            else {
                resourceUri += "/" + resourceId;
            }
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
                return getResource(service, resource, resourceId, suffix);
            }
            else {
                throw ex;
            }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpGet get = new HttpGet(resource == null ? endpoint : endpoint + resource);
            
            get.addHeader("Content-Type", "application/json");
            get.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "GET " + toAPIResource(resource));
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code == HttpStatus.SC_NOT_FOUND ) {
                return null;
            }
            if( code == HttpStatus.SC_BAD_REQUEST ) {
                std.error("Expected OK for GET request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                try {
                    JSONObject err = (new JSONObject(data)).getJSONObject("badRequest");
                    String msg = err.getString("message");

                    if( msg.contains("id should be integer") ) {
                        return null;
                    }
                }
                catch( JSONException e ) {
                    // ignore
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);

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
                std.error("Expected OK for GET request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
                if( items == null ) {
                    return null;
                }
                if (items.code == HttpStatus.SC_UNAUTHORIZED) {
                    std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                    throw new NovaException(items);
                }
                if( provider.getMajorVersion() == 1 && provider.getMinorVersion() == 0 && items.message != null && (items.message.contains("not found") || items.message.contains("unknown")) ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                return data;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
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
            HttpGet get = new HttpGet(endpoint + resource);
            
            get.addHeader("Content-Type", "application/json");
            get.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "GET " + toAPIResource(resource));
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code == HttpStatus.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpStatus.SC_OK && code != HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("Expected OK for GET request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
                if( items == null ) {
                    return null;
                }
                std.error("getStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            else {
                InputStream input = null;
                
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        input = entity.getContent();
                        if( wire.isDebugEnabled() ) {
                            wire.debug(" ---- BINARY DATA ---- ");
                            wire.debug("");
                        }
                    }
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

    protected @Nonnull HttpClient getClient() throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was defined for this request");
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null ) {
            throw new CloudException("No cloud endpoint was defined");
        }
        boolean ssl = endpoint.startsWith("https");
        int targetPort;
        URI uri;

        try {
            uri = new URI(endpoint);
            targetPort = uri.getPort();
            if( targetPort < 1 ) {
                targetPort = (ssl ? 443 : 80);
            }
        }
        catch( URISyntaxException e ) {
            throw new CloudException(e);
        }
        HttpHost targetHost = new HttpHost(uri.getHost(), targetPort, uri.getScheme());
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        DefaultHttpClient client = new DefaultHttpClient(params);

        if( provider.isInsecure() ) {
            try {
                client.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, new SSLSocketFactory(new TrustStrategy() {

                    public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                        return true;
                    }
                }, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)));
            }
            catch( Throwable t ) {
                t.printStackTrace();
            }
        }
        return client;
    }

    public @Nullable Map<String,String> headResource(@Nonnull final String service, @Nullable final String resource, @Nullable final String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " URL has been established in " + context.getMyRegion());
        }
        String resourceUri = resource;
        if( resource == null && resourceId == null ) {
            resourceUri = "/";
        }
        else if( resource == null ) {
            resourceUri = "/" + resourceId;
        }
        else if( resourceId != null ) {
            resourceUri += "/" + resourceId;
        }
        try {
            return head(context.getAuthToken(), endpoint, resourceUri);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return headResource(service, resource, resourceId);
            }
            else {
                throw ex;
            }
        }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpHead head = new HttpHead(endpoint + resource);
            
            head.addHeader("X-Auth-Token", authToken);
            if( wire.isDebugEnabled() ) {
                wire.debug(head.getRequestLine().toString());
                for( Header header : head.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "HEAD " + toAPIResource(resource));
                response = client.execute(head);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK ) {
                if( code == HttpStatus.SC_NOT_FOUND ) {
                    return null;
                }
                std.error("Expected OK for HEAD request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
                if( items == null ) {
                    return null;
                }
                std.error("head(): [" +  code + " : " + items.message + "] " + items.details);
                throw new NovaException(items);
            }
            HashMap<String,String> map = new HashMap<String,String>();
            
            for( Header h : response.getAllHeaders() ) {
                map.put(h.getName().trim(), h.getValue().trim());
            }
            return map;
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".head()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    public void postResourceHeaders(final String service, final String resource, final String resourceId, final Map<String,String> headers) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " has been established in " + context.getMyRegion());
        }
        if( resourceId == null ) {
            throw new InternalException("No container was specified");
        }
        try {
            postHeaders(context.getAuthToken(), endpoint, resource + "/" + resourceId, headers);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                postResourceHeaders(service, resource, resourceId, headers);
            }
            else {
                throw ex;
            }
        }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/json");
            post.addHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addHeader(entry.getKey(), val);
                }
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }

            HttpResponse response;

            try {
                APITrace.trace(provider, "POST " + toAPIResource(resource));
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code == HttpStatus.SC_REQUEST_TOO_LONG || code == HttpStatus.SC_REQUEST_URI_TOO_LONG ) {
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                try {
                    if( data != null ) {
                        JSONObject ob = new JSONObject(data);

                        if( ob.has("overLimit") ) {
                            ob = ob.getJSONObject("overLimit");
                            if( ob.has("retryAfter") ) {
                                int min = ob.getInt("retryAfter");

                                if( min < 1 ) {
                                    throw new CloudException(CloudErrorType.CAPACITY, 413, "Over Limit", ob.has("message") ? ob.getString("message") : "Over Limit");
                                }
                                try { Thread.sleep(CalendarWrapper.MINUTE * min); }
                                catch( InterruptedException ignore ) { }
                                return postHeaders(authToken, endpoint, resource, customHeaders);
                            }
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException(e);
                }
            }

            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
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
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".postString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    public @Nullable JSONObject postString(@Nonnull final String service, @Nonnull final String resource, @Nullable final String resourceId, @Nonnull final String extra, @Nonnull final JSONObject body) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);
        
        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
        }
        try {
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
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                return postString(service, resource, resourceId, extra, body);
            }
            else {
                throw ex;
            }
        }
    }
    
    public @Nullable JSONObject postString(@Nonnull final String service, @Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body, final boolean suffix) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        String resourceUri = resource;
        if( resourceId != null ) {
            resourceUri += "/" + (suffix ? (resourceId + "/action") : resourceId);
        }
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " endpoint exists");
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
                return postString(service, resource, resourceId, body, suffix);
            }
            else {
                throw ex;
            }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/json");
            post.addHeader("X-Auth-Token", authToken);
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            if( payload != null ) {
                try {
                    //noinspection deprecation
                    post.setEntity(new StringEntity(payload == null ? "" : payload, "application/json", "UTF-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
                try { wire.debug(EntityUtils.toString(post.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "POST " + toAPIResource(resource));
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code == HttpStatus.SC_REQUEST_TOO_LONG || code == HttpStatus.SC_REQUEST_URI_TOO_LONG ) {
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                try {
                    if( data != null ) {
                        JSONObject ob = new JSONObject(data);

                        if( ob.has("overLimit") ) {
                            ob = ob.getJSONObject("overLimit");
                            if( ob.has("retryAfter") ) {
                                int min = ob.getInt("retryAfter");

                                if( min < 1 ) {
                                    throw new CloudException(CloudErrorType.CAPACITY, 413, "Over Limit", ob.has("message") ? ob.getString("message") : "Over Limit");
                                }
                                try { Thread.sleep(CalendarWrapper.MINUTE * min); }
                                catch( InterruptedException ignore ) { }
                                return postString(authToken, endpoint, resource, payload);
                            }
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException(e);
                }
            }
            if( code != HttpStatus.SC_OK && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_CREATED ) {
                std.error("postString(): Expected OK, ACCEPTED, or NO CONTENT for POST request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);

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
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                    else if( code == HttpStatus.SC_ACCEPTED ) {
                        Header[] headers = response.getAllHeaders();

                        for( Header h : headers ) {
                            if( h.getName().equalsIgnoreCase("Location") ) {
                                return "{\"location\" : \"" + h.getValue().trim() + "\"}";
                            }
                        }
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/octet-stream");
            post.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            post.setEntity(new InputStreamEntity(stream, -1));
            wire.debug(" ---- BINARY DATA ---- ");
            wire.debug("");

            HttpResponse response;

            try {
                APITrace.trace(provider, "POST " + toAPIResource(resource));
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            String responseHash = null;
            
            for( Header h : post.getAllHeaders() ) {
                if( h.getName().equalsIgnoreCase("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("postStream(): Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);

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
                wire.debug("");
                if( code == HttpStatus.SC_ACCEPTED ) {
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaOpenStack.class.getName() + ".postStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    public void putResourceHeaders(@Nonnull final String service, @Nullable final String resource, @Nullable final String resourceId, @Nonnull final Map<String,String> headers) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        String endpoint = context.getServiceUrl(service);

        if( endpoint == null ) {
            throw new CloudException("No " + service + " has been established in " + context.getMyRegion());
        }
        String resourceUri = resource;
        if( resource == null && resourceId == null ) {
            resourceUri = "/";
        }
        else if( resource == null ) {
            resourceUri = "/" + resourceId;
        }
        else if( resourceId != null ) {
            resourceUri += "/" + resourceId;
        }
        try {
            putHeaders(context.getAuthToken(), endpoint, resourceUri, headers);
        }
        catch (NovaException ex) {
            if (ex.getHttpCode() == HttpStatus.SC_UNAUTHORIZED) {
                Cache<AuthenticationContext> cache = Cache.getInstance(provider, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
                cache.clear();
                putResourceHeaders(service, resource, resourceId, headers);
            }
            else {
                throw ex;
            }
        }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addHeader(entry.getKey(), val);
                }
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "PUT " + toAPIResource(resource));
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
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
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    public @Nullable JSONObject putString(@Nonnull final String service, @Nonnull final String resource, @Nullable final String resourceId, @Nonnull final JSONObject body , final String suffix) throws CloudException, InternalException {
    	AuthenticationContext context = provider.getAuthenticationContext();
        String resourceUri = resource;
    	if( resourceId != null ) {
    		resourceUri += "/" + (suffix != null ? (resourceId + "/" + suffix) : resourceId);
    	}
    	String endpoint = context.getServiceUrl(service);
    	if( endpoint == null ) {
    		throw new CloudException("No " + service + " endpoint exists");
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
    			return putString(service, resource, resourceId, body, suffix);
    		}
    		else {
    			throw ex;
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            if( payload != null ) {
                try {
                    //noinspection deprecation
                    put.setEntity(new StringEntity(payload == null ? "" : payload, "application/json", "UTF-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
                try { wire.debug(EntityUtils.toString(put.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                APITrace.trace(provider, "PUT " + toAPIResource(resource));
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);
                
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
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
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
        HttpClient client = null;
        try {
            client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/octet-stream");
            put.addHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addHeader("ETag", md5Hash);
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            put.setEntity(new InputStreamEntity(stream, -1, ContentType.APPLICATION_OCTET_STREAM));
            wire.debug(" ---- BINARY DATA ---- ");
            wire.debug("");

            HttpResponse response;

            try {
                APITrace.trace(provider, "PUT " + toAPIResource(resource));
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            String responseHash = null;
            
            for( Header h : put.getAllHeaders() ) {
                if( h.getName().equalsIgnoreCase("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                std.error("putStream(): Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                String data = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        data = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(data);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                NovaException.ExceptionItems items = NovaException.parseException(code, data);

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
                    String data = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            data = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(data);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        std.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        e.printStackTrace();
                        throw new CloudException(e);
                    }
                    if( data != null && !data.trim().equals("") ) {
                        return data;
                    }
                }
                return null;
            }
        }
        finally {
            if (client != null) {
                client.getConnectionManager().shutdown();
            }
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
            if (result==0)
                result = minor1 - minor2;
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

    private @Nonnull String toAPIResource(@Nonnull String resource) {
        if( resource == null || resource.equals("/") || resource.length() < 2 ) {
            return resource;
        }
        while( resource.startsWith("/") ) {
            if( resource.equals("/") ) {
                return "/";
            }
            resource = resource.substring(1);
        }
        int idx = resource.indexOf("/");

        if( idx > 0 ) {
            return resource.substring(0, idx);
        }
        return resource;
    }
}
