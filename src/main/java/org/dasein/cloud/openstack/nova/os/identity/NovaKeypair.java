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

package org.dasein.cloud.openstack.nova.os.identity;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;

/**
 * OpenStack Nova SSH keypairs
 * @author George Reese (george.reese@enstratus.com)
 * @since 2011.10
 * @version 2011.10
 * @version 2012.04.1 Added some intelligence around features Rackspace does not support
 */
public class NovaKeypair implements ShellKeySupport {
    private NovaOpenStack provider;

    NovaKeypair(@Nonnull NovaOpenStack cloud) { provider = cloud; }

    @Override
    public @Nonnull SSHKeypair createKeypair(@Nonnull String name) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(NovaKeypair.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + NovaKeypair.class.getName() + ".createKeypair(" + name + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            HashMap<String,Object> wrapper = new HashMap<String,Object>();
            HashMap<String,Object> json = new HashMap<String,Object>();
            NovaMethod method = new NovaMethod(provider);

            json.put("name", name);
            wrapper.put("keypair", json);
            JSONObject result = method.postServers("/os-keypairs", null, new JSONObject(wrapper), false);

            if( result != null && result.has("keypair") ) {
                try {
                    JSONObject ob = result.getJSONObject("keypair");

                    SSHKeypair kp = toKeypair(ctx, ob);
                    
                    if( kp == null ) {
                        throw new CloudException("No matching keypair was generated from " + ob.toString());
                    }
                    return kp;
                }
                catch( JSONException e ) {
                    logger.error("createKeypair(): Unable to understand create response: " + e.getMessage());
                    if( logger.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
            }
            logger.error("createKeypair(): No keypair was created by the create attempt, and no error was returned");
            throw new CloudException("No keypair was created");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + NovaKeypair.class.getName() + ".createKeypair()");
            }
        }
    }

    @Override
    public void deleteKeypair(@Nonnull String keypairId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaKeypair.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + NovaKeypair.class.getName() + ".deleteKeypair(" + keypairId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/os-keypairs", keypairId);
                    return;
                }
                catch( NovaException e ) {
                    if( e.getHttpCode() != HttpServletResponse.SC_CONFLICT ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { /* ignore */ }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + NovaKeypair.class.getName() + ".deleteKeypair()");
            }
        }
    }

    @Override
    public @Nullable String getFingerprint(@Nonnull String keypairId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaKeypair.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + NovaKeypair.class.getName() + ".getFingerprint(" + keypairId + ")");
        }
        try {
            SSHKeypair kp = getKeypair(keypairId);
            
            return (kp == null ? null : kp.getFingerprint());
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaKeypair.class.getName() + ".getFingerprint()");
            }
        }
    }
    
    @Override
    public @Nullable SSHKeypair getKeypair(@Nonnull String keypairId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaKeypair.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + NovaKeypair.class.getName() + ".getKeypair(" + keypairId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/os-keypairs", null, false);

            try {
                if( ob != null && ob.has("keypairs") ) {
                    JSONArray list = ob.getJSONArray("keypairs");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject json = list.getJSONObject(i);

                        try {
                            if( json.has("keypair") ) {
                                JSONObject kp = json.getJSONObject("keypair");

                                SSHKeypair k = toKeypair(ctx, kp);
                                
                                if( k != null && keypairId.equals(k.getProviderKeypairId()) ) {
                                    return k;
                                }
                            }
                        }
                        catch( JSONException e ) {
                            std.error("Invalid JSON from cloud: " + e.getMessage());
                            throw new CloudException("Invalid JSON from cloud: " + e.getMessage());
                        }
                    }
                }
                return null;
            }
            catch( JSONException e ) {
                std.error("list(): Unable to identify expected values in JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for keypair in " + ob.toString());
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaKeypair.class.getName() + ".getKeypair()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForKeypair(@Nonnull Locale locale) {
        return "keypair";
    }

    private boolean verifySupport() throws InternalException, CloudException {
        NovaMethod method = new NovaMethod(provider);

        try {
            method.getServers("/os-keypairs", null, false);
            return true;
        }
        catch( CloudException e ) {
            if( e.getHttpCode() == 404 ) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        if( provider.getMajorVersion() > 1 && provider.getComputeServices().getVirtualMachineSupport().isSubscribed() ) {
            return verifySupport();
        }
        return false;
    }

    @Override
    public @Nonnull Collection<SSHKeypair> list() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(NovaKeypair.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + NovaKeypair.class.getName() + ".list()");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/os-keypairs", null, false);
            ArrayList<SSHKeypair> keypairs = new ArrayList<SSHKeypair>();

            try {
                if( ob != null && ob.has("keypairs") ) {
                    JSONArray list = ob.getJSONArray("keypairs");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject json = list.getJSONObject(i);

                        try {
                            if( json.has("keypair") ) {
                                JSONObject kp = json.getJSONObject("keypair");

                                keypairs.add(toKeypair(ctx, kp));
                            }
                        }
                        catch( JSONException e ) {
                            std.error("Invalid JSON from cloud: " + e.getMessage());
                            throw new CloudException("Invalid JSON from cloud: " + e.getMessage());
                        }
                    }
                }
            }
            catch( JSONException e ) {
                std.error("list(): Unable to identify expected values in JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for keypair in " + ob.toString());
            }
            return keypairs;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + NovaKeypair.class.getName() + ".list()");
            }
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable SSHKeypair toKeypair(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws InternalException {
        if( json == null ) {
            return null;
        }
        try {
            SSHKeypair kp = new SSHKeypair();
            String name = null;

            if( json.has("private_key") ) {
                kp.setPrivateKey(json.getString("private_key").getBytes("utf-8"));
            }
            if( json.has("public_key") ) {
                kp.setPublicKey(json.getString("public_key"));
            }
            if( json.has("fingerprint") ) {
                kp.setFingerprint(json.getString("fingerprint"));
            }
            if( json.has("name") ) {
                name = json.getString("name");
            }
            if( name == null ) {
                return null;
            }
            kp.setName(name);
            kp.setProviderKeypairId(name);
            kp.setProviderOwnerId(ctx.getAccountNumber());
            String regionId = ctx.getRegionId();
            kp.setProviderRegionId(regionId == null ? "" : regionId);
            return kp;
        }
        catch( UnsupportedEncodingException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( JSONException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
    }
}
