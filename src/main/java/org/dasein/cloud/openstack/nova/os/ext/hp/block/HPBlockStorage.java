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

package org.dasein.cloud.openstack.nova.os.ext.hp.block;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

/**
 * Implements block storage according to HP.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 */
public class HPBlockStorage implements VolumeSupport {
    static public final String SERVICE  = "volume";
    static public final String RESOURCE = "/os-volumes";
    
    private NovaOpenStack provider;
    
    public HPBlockStorage(@Nonnull NovaOpenStack provider) { this.provider = provider; }

    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String device) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + HPBlockStorage.class.getName() + ".attach("+ volumeId + "," + toServer + "," + device + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            HashMap<String,Object> attachment = new HashMap<String, Object>();
            HashMap<String,Object> wrapper = new HashMap<String, Object>();
            NovaMethod method = new NovaMethod(provider);

            attachment.put("volumeId", volumeId);
            attachment.put("device", device);
            wrapper.put("volumeAttachment", attachment);
            
            if( method.postString(SERVICE, "/servers", toServer, "os-volume_attachments", new JSONObject(wrapper)) == null ) {
                throw new CloudException("No response from the cloud");
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + HPBlockStorage.class.getName() + ".detach()");
            }
        }
    }

    @Override
    public @Nonnull String create(@Nullable String fromSnapshot, @Nonnegative int sizeInGb, @Nonnull String inZone) throws InternalException, CloudException {
        if( fromSnapshot != null ) {
            return createVolume(VolumeCreateOptions.getInstanceForSnapshot(fromSnapshot, new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), "volume-" + System.currentTimeMillis(), "dsn-auto-volume").inDataCenter(inZone));
        }
        else {
            return createVolume(VolumeCreateOptions.getInstance(new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), "volume-" + System.currentTimeMillis(), "dsn-auto-volume").inDataCenter(inZone));
        }
    }

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + HPBlockStorage.class.getName() + ".createVolume(" + options + ")");
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

            json.put("display_name", options.getName());
            json.put("display_description", options.getDescription());
            json.put("size", options.getVolumeSize().intValue());
            if( options.getSnapshotId() != null ) {
                json.put("snapshot_id", options.getSnapshotId());
            }
            wrapper.put("volume", json);
            JSONObject result = method.postString(SERVICE, RESOURCE, null, new JSONObject(wrapper), true);

            if( result != null && result.has("volume") ) {
                try {
                    Volume volume = toVolume(ctx, result.getJSONObject("volume"));

                    if( volume != null ) {
                        return volume.getProviderVolumeId();
                    }
                }
                catch( JSONException e ) {
                    logger.error("create(): Unable to understand create response: " + e.getMessage());
                    if( logger.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
            }
            logger.error("create(): No volume was created by the create attempt, and no error was returned");
            throw new CloudException("No volume was created");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + HPBlockStorage.class.getName() + ".create()");
            }
        }
    }

    @Override
    public void detach(@Nonnull String volumeId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + HPBlockStorage.class.getName() + ".detach("+ volumeId + ")");
        }
        try {
            Volume volume = getVolume(volumeId);
            
            if( volume == null ) {
                throw new CloudException("No such volume: " + volumeId);
            }
            if( volume.getProviderVirtualMachineId() == null ) {
                throw new CloudException("Volume " + volumeId + " is not attached");
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, "/servers", volume.getProviderVirtualMachineId(), "os-volume_attachments/" + volumeId);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + HPBlockStorage.class.getName() + ".detach()");
            }
        }
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1024, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Override
    public @Nonnull String getProviderTermForVolume(@Nonnull Locale locale) {
        return "volume";
    }

    @Override
    public @Nullable Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("enter - " + HPBlockStorage.class.getName() + ".getVolume(" + volumeId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, RESOURCE, volumeId, true);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("volume") ) {
                    return toVolume(ctx, ob.getJSONObject("volume"));
                }
            }
            catch( JSONException e ) {
                std.error("getVolume(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for volume");
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + HPBlockStorage.class.getName() + ".getVolume()");
            }
        }
    }

    @Override
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        ArrayList<String> list = new ArrayList<String>();

        if( platform.isWindows() ) {
            list.add("xvdf");
            list.add("xvdg");
            list.add("xvdh");
            list.add("xvdi");
            list.add("xvdj");
        }
        else {
            list.add("/dev/sdf");
            list.add("/dev/sdg");
            list.add("/dev/sdh");
            list.add("/dev/sdi");
            list.add("/dev/sdj");
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + HPBlockStorage.class.getName() + ".listVolumes()");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            ArrayList<Volume> volumes = new ArrayList<Volume>();

            JSONObject json = method.getResource(SERVICE, RESOURCE, null, false);

            if( json != null && json.has("volumes") ) {
                try {
                    JSONArray list = json.getJSONArray("volumes");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject v = list.getJSONObject(i);
                        Volume volume = toVolume(ctx, v);
                        
                        if( volume != null ) {
                            volumes.add(volume);
                        }
                    }
                }
                catch( JSONException e ) {
                    std.error("listVolumes(): Unable to identify expected values in JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for volumes in " + json.toString());
                }
            }
            return volumes;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + HPBlockStorage.class.getName() + ".listVolumes()");
            }
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.getAuthenticationContext().getServiceUrl(SERVICE) != null);
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(HPBlockStorage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + HPBlockStorage.class.getName() + ".remove("+ volumeId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, RESOURCE, volumeId, null);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + HPBlockStorage.class.getName() + ".remove()");
            }
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0]; 
    }
    
    private @Nullable Volume toVolume(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String region = ctx.getRegionId();
            String dataCenter = region + "-a";
            
            String name = null, volumeId = null;

            if( json.has("id") ) {
                volumeId = json.getString("id");
            }
            if( volumeId == null ) {
                return null;
            }
            if( json.has("displayName") ) {
                name = json.getString("displayName");
            }
            if( name == null ) {
                name = volumeId;
            }

            long created = 0L;
            
            if( json.has("createdAt") ) {
                created = provider.parseTimestamp(json.getString("createdAt"));
            }
            
            int size = 0;
            
            if( json.has("size") ) {
                size = json.getInt("size");
            }
            String vmId = null, deviceId = null;

            if( json.has("attachments") ) {
                JSONArray servers = json.getJSONArray("attachments");

                for( int i=0; i<servers.length(); i++ ) {
                    JSONObject ob = servers.getJSONObject(i);

                    if( ob.has("serverId") ) {
                        vmId = ob.getString("serverId");
                    }
                    if( ob.has("device") ) {
                        deviceId = ob.getString("device");
                    }
                    if( vmId != null ) {
                        break;
                    }
                }
            }
            String snapshotId = null;
            
            // TODO: identify snapshot

            VolumeState currentState = VolumeState.PENDING;
            
            if( json.has("status") ) {
                String status = json.getString("status");
                
                if( status.equalsIgnoreCase("available") ) {
                    currentState = VolumeState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("creating") ) {
                    currentState = VolumeState.PENDING;
                }
                else {
                    System.out.println("DEBUG OS VOLUME STATE=" + status);
                }
            }
            Volume volume = new Volume();

            volume.setCreationTimestamp(created);
            volume.setCurrentState(currentState);
            volume.setDeviceId(deviceId);
            volume.setName(name);
            volume.setProviderDataCenterId(dataCenter);
            volume.setProviderRegionId(region);
            volume.setProviderSnapshotId(snapshotId);
            volume.setProviderVirtualMachineId(vmId);
            volume.setProviderVolumeId(volumeId);
            volume.setSize(new Storage<Gigabyte>(size, Storage.GIGABYTE));
    
            return volume;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }
}
