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

package org.dasein.cloud.openstack.nova.ec2;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.openstack.nova.ec2.compute.NovaComputeServices;

public class NovaEC2 extends AbstractCloud {
    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, String context) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        return Logger.getLogger("dasein.cloud.nova." + context + "." + pkg + "." + getLastItem(cls.getName()));
    }

    static public final String P_ACCESS            = "AWSAccessKeyId";
    static public final String P_ACTION            = "Action";
    static public final String P_CFAUTH            = "Authorization";
    static public final String P_DATE              = "x-amz-date";
    static public final String P_SIGNATURE         = "Signature";
    static public final String P_SIGNATURE_METHOD  = "SignatureMethod";
    static public final String P_SIGNATURE_VERSION = "SignatureVersion";
    static public final String P_TIMESTAMP         = "Timestamp";
    static public final String P_VERSION           = "Version";
    
    static public final String EC2_ALGORITHM         = "HmacSHA256";
    static public final String SIGNATURE             = "2";
    static public final String VERSION               = "2009-11-30";
    static public final String ELB_VERSION           = "2009-11-30";
    
    static public String encode(String value, boolean encodePath) throws InternalException {
        String encoded = null;
        
        try {
            encoded = URLEncoder.encode(value, "utf-8").replace("+", "%20").replace("*", "%2A").replace("%7E","~");
            if( encodePath ) {
                encoded = encoded.replace("%2F", "/");
            }
        } 
        catch( UnsupportedEncodingException e ) {
            getLogger(NovaEC2.class, "std").error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        return encoded;     
    }
    
    static public String escapeXml(String nonxml) {
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<nonxml.length(); i++ ) {
            char c = nonxml.charAt(i);
            
            switch( c ) {
                case '&': str.append("&amp;"); break;
                case '>': str.append("&gt;"); break;
                case '<': str.append("&lt;"); break;
                case '"': str.append("&quot;"); break;
                case '[': str.append("&#091;"); break;
                case ']': str.append("&#093;"); break;
                case '!': str.append("&#033;"); break;
                default: str.append(c);
            }
        }
        return str.toString();
    }
    
    public NovaEC2() { }

    private String buildEc2AuthString(String method, String serviceUrl, Map<String, String> parameters) throws InternalException {
        Logger logger = getLogger(NovaEC2.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - buildEc2AuthString(" + method + "," + serviceUrl + "," + parameters);
        }
        try {
            StringBuilder authString = new StringBuilder();
            TreeSet<String> sortedKeys;
            URI endpoint = null;
            String tmp;
            
            authString.append(method);
            authString.append("\n");
            try {
                endpoint = new URI(serviceUrl);
            } 
            catch( URISyntaxException e ) {
                getLogger(NovaEC2.class, "std").error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            authString.append(endpoint.getHost().toLowerCase());
            if( endpoint.getPort() != 80 && endpoint.getPort() != 443 && endpoint.getPort() > 0 ) {
                authString.append(":" + endpoint.getPort());
            }
            authString.append("\n");
            tmp = endpoint.getPath();
            if( tmp == null || tmp.length() == 0) {
                tmp = "/";
            }
            else if( !tmp.endsWith("/") ) {
                tmp = tmp + "/";
            }
            authString.append(encode(tmp, true));
            authString.append("\n");
            sortedKeys = new TreeSet<String>();
            sortedKeys.addAll(parameters.keySet());
            boolean first = true;
            for( String key : sortedKeys ) {
                String value = parameters.get(key);
                
                if( !first ) {
                    authString.append("&");
                }
                else {
                    first = false;
                }
                authString.append(encode(key, false));
                authString.append("=");
                authString.append(encode(value, false));
            }
            return authString.toString();
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - buildEc2AuthString()");
            }
        }
    }
    
    public boolean createTags(String resourceId, Tag ... keyValuePairs) {
        try {
            Map<String,String> parameters = getStandardParameters(getContext(), "CreateTags");
            NovaMethod method;
            
            parameters.put("ResourceId.1", resourceId);
            for( int i=0; i<keyValuePairs.length; i++ ) {
                String key = keyValuePairs[i].getKey();
                String value = keyValuePairs[i].getValue();
                
                parameters.put("Tag." + i + ".Key", key);
                parameters.put("Tag." + i + ".Value", value);
            }
            method = new NovaMethod(this, getEc2Url(), parameters);
            try {
                method.invoke();
            }
            catch( NovaException e ) {
                String code = e.getCode();
                
                if( code != null && code.equals("InvalidInstanceID.NotFound") ) {
                    try { Thread.sleep(5000L); }
                    catch( InterruptedException ignore ) { }
                    parameters = getStandardParameters(getContext(), "CreateTags");
                    parameters.put("ResourceId.1", resourceId);
                    for( int i=0; i<keyValuePairs.length; i++ ) {
                        String key = keyValuePairs[i].getKey();
                        String value = keyValuePairs[i].getValue();
                        
                        parameters.put("Tag." + i + ".Key", key);
                        parameters.put("Tag." + i + ".Value", value);
                    }
                    method = new NovaMethod(this, getEc2Url(), parameters);
                    try {
                        method.invoke();
                        return true;
                    }
                    catch( NovaException ignore ) {
                        // ignore me
                    }
                }
                getLogger(NovaEC2.class, "std").error("EC2 error settings tags for " + resourceId + ": " + e.getSummary());
                return false;
            }   
            return true;
        }
        catch( Throwable ignore ) {
            return false;
        }
    }
    
    private String[] getBootstrapUrls(ProviderContext ctx) {
        String endpoint = ctx.getEndpoint();
        
        if( endpoint == null ) {
            return new String[0];
        }
        if( endpoint.indexOf(",") == -1 ) {
            return new String[] { endpoint };
        }
        String[] endpoints = endpoint.split(",");
        if( endpoints != null && endpoints.length > 1 ) {
            String second = endpoints[1];
            
            if( !second.startsWith("http") ) {
                if( endpoints[0].startsWith("http") ) {
                    // likely a URL with a , in it
                    return new String[] { (endpoint + "/Eucalyptus") };
                }
            }
        }
        for( int i=0; i<endpoints.length; i++ ) {
            if( !endpoints[i].startsWith("http") ) {
                endpoints[i] = ("https://" + endpoints[i] + "/Eucalyptus");        
            }
        }
        return endpoints;
    }
       
    @Override
    public @Nonnull String getCloudName() {
        String name = getContext().getCloudName();
        
        return ((name == null ) ? "Nova" : name);
    }
    
    @Override
    public @Nonnull NovaLocation getDataCenterServices() {
        return new NovaLocation(this);
    }
    
    @Override
    public @Nonnull NovaComputeServices getComputeServices() {
        return new NovaComputeServices(this);
    }
    
    public @Nullable String getEc2Url() throws InternalException, CloudException {
        return getEc2Url(getContext().getRegionId());
    }
    
    public @Nullable String getEc2Url(String regionId) throws InternalException, CloudException {
        String url;
        
        if( regionId == null ) {
            return getBootstrapUrls(getContext())[0];
        }
        url = getContext().getEndpoint();
        if( url == null ) {
            return null;
        }
        if( !url.startsWith("http") ) {
            return "https://" + url;
        }
        else {
            return url;
        }
    }
    
    @Override
    public @Nonnull String getProviderName() {
        String name = getContext().getProviderName();
        
        return ((name == null) ? "OpenStack" : name);
    }
    
    public @Nonnull Map<String,String> getStandardParameters(@Nonnull ProviderContext ctx, @Nonnull String action) throws InternalException {
        HashMap<String,String> parameters = new HashMap<String,String>();
        
        parameters.put(P_ACTION, action);
        parameters.put(P_SIGNATURE_VERSION, SIGNATURE);
        try {
            parameters.put(P_ACCESS, new String(ctx.getAccessPublic(), "utf-8"));// + ":" + ctx.getAccountNumber());
        } 
        catch( UnsupportedEncodingException e ) {
            getLogger(NovaEC2.class, "std").error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        parameters.put(P_SIGNATURE_METHOD, EC2_ALGORITHM);
        parameters.put(P_TIMESTAMP, getTimestamp(System.currentTimeMillis(), false));
        parameters.put(P_VERSION, VERSION);
        return parameters;
    }
    
    public @Nonnull String getTimestamp(@Nonnegative long timestamp, boolean withMillis) {
        SimpleDateFormat fmt;
        
        if( withMillis ) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }
        else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");            
        }
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(new Date(timestamp));
    }
    
    public @Nonnegative long parseTime(@Nullable String time) throws CloudException {
        if( time == null ) {
            return 0L;
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            
        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } 
            catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try {
                    return fmt.parse(time).getTime();
                } 
                catch( ParseException encore ) {
                    throw new CloudException("Could not parse date: " + time);
                }
            }
        }
        return 0L;
    }
    
    private @Nonnull String sign(@Nonnull byte[] key, @Nonnull String authString, @Nonnull String algorithm) throws InternalException {
        Logger logger = getLogger(NovaEC2.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaEC2.class.getName() + ".sign(" + key + "," + authString + "," + algorithm + ")");
        }
        try {
            try {
                Mac mac = Mac.getInstance(algorithm);
                
                mac.init(new SecretKeySpec(key, algorithm));
                return new String(Base64.encodeBase64(mac.doFinal(authString.getBytes("utf-8"))));
            } 
            catch( NoSuchAlgorithmException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            } 
            catch( InvalidKeyException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            } 
            catch( IllegalStateException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            } 
            catch( UnsupportedEncodingException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaEC2.class.getName() + ".sign()");
            }
        }
    }

    public @Nonnull String signEc2(@Nonnull byte[] key, @Nonnull String serviceUrl, @Nonnull Map<String, String> parameters) throws InternalException {
        return sign(key, buildEc2AuthString("GET", serviceUrl, parameters), EC2_ALGORITHM);
    }

    @Override
    public @Nullable String testContext() {
        try {
            getDataCenterServices().listRegions();
            return getContext().getAccountNumber();
        }
        catch( Throwable t ) {
            getLogger(NovaEC2.class, "std").warn("Unable to connect to the cloud \"" + getCloudName() + "\" for " + getContext().getAccountNumber() + ": " + t.getMessage());
            return null;
        }
    }
}
