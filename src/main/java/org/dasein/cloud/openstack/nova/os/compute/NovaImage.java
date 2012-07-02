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

package org.dasein.cloud.openstack.nova.os.compute;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class NovaImage implements MachineImageSupport {
    private NovaOpenStack provider;
    
    NovaImage(NovaOpenStack provider) { this.provider = provider; }
    
    @Override
    public void downloadImage(@Nonnull String machineImageId, @Nonnull OutputStream toOutput) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Rackspace does not currently support image downloading.");
    }

    public @Nullable String getImageRef(@Nonnull String machineImageId) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".getImageRef(" + machineImageId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", machineImageId, true);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("image") ) {
                    JSONObject image = ob.getJSONObject("image");
                    JSONArray links = image.getJSONArray("links");
                    String def = null;
                    
                    for( int j=0; j<links.length(); j++ ) {
                        JSONObject link = links.getJSONObject(j);
                        
                        if( link.getString("rel").equals("self") ) {
                            return link.getString("href");
                        }
                        else if( def == null ) {
                            def = link.optString("href");
                        }
                    }
                    return def;
                }
                return null;
            }
            catch( JSONException e ) {
                logger.error("getImageRef(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".getImageRef()");
            }
        }
    }
    
    @Override
    public @Nullable MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".getMachineImage(" + machineImageId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", machineImageId, true);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("image") ) {
                    JSONObject server = ob.getJSONObject("image");
                    MachineImage img = toImage(server);
                        
                    if( img != null ) {
                        return img;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("getMachineImage(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
            }
            return null;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".getMachineImage()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "image";
    }

    @Override
    public boolean hasPublicLibrary() {
        return false;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".imageVirtualMachine(" + vmId + "," + name + "," + description + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            HashMap<String,Object> action = new HashMap<String,Object>();
            AsynchronousTask<String> task = new AsynchronousTask<String>();
            
            action.put("name", name);
            task.setStartTime(System.currentTimeMillis());
            
            JSONObject result;
            
            if( provider.isPostCactus() ) {     
                HashMap<String,Object> json = new HashMap<String,Object>();
                HashMap<String,String> metaData = new HashMap<String,String>();

                metaData.put("dsnDescription", description);
                action.put("metadata", metaData);
                json.put("createImage", action);
            
                result = method.postServers("/servers", vmId, new JSONObject(json), true);
            }
            else {
                HashMap<String,Object> json = new HashMap<String,Object>();
                
                action.put("serverId", String.valueOf(vmId));
                json.put("image", action);
                result = method.postServers("/images", null, new JSONObject(json), true);
            }
            if( result.has("image") ) {
                try {
                    JSONObject img = result.getJSONObject("image");
                    MachineImage image = toImage(img);
                    
                    if( image != null ) {
                        task.completeWithResult(image.getProviderMachineImageId());
                        return task;
                    }
                }
                catch( JSONException e ) {
                    logger.error("imageVirtualMachine(): Unable to understand image response: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            else if( result.has("location") ) {
                try {
                    String location = result.getString("location");
                    int idx = location.lastIndexOf('/');
                    
                    if( idx > 0 ) {
                        location = location.substring(idx+1);
                    }
                    task.completeWithResult(location);
                    return task;
                }
                catch( JSONException e ) {
                    logger.error("imageVirtualMachine(): Unable to understand image response: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            logger.error("imageVirtualMachine(): No image was created by the imaging attempt, and no error was returned");
            throw new CloudException("No image was created");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".imageVirtualMachine()");
            }            
        }
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachineToStorage(@Nonnull String vmId, @Nonnull String name, @Nonnull String description, @Nonnull String bucket) throws CloudException, InternalException {
        throw new OperationNotSupportedException("OpenStack does not support overt imaging to object storage.");
    }

    @Override
    public @Nonnull String installImageFromUpload(@Nonnull MachineImageFormat format, @Nonnull InputStream imageStream) throws CloudException, InternalException {
        throw new OperationNotSupportedException("OpenStack does not support the creation of an image from an upload.");
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.testContext() != null);
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".listMachineImages()");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", null, true);
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();
            
            try {
                if( ob != null && ob.has("images") ) {
                    JSONArray list = ob.getJSONArray("images");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject image = list.getJSONObject(i);
                        MachineImage img = toImage(image);
                        
                        if( img != null ) {
                            images.add(img);
                        }
                        
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listMachineImages(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
            }
            return images;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".listMachineImages()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        return listMachineImages();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    @Override
    public @Nonnull String registerMachineImage(@Nonnull String atStorageLocation) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No registering images in object storage");
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".remove(" + machineImageId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/images", machineImageId);
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
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".remove()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();
        
        for( MachineImage img : listMachineImages() ) {
            if( architecture != null ) {
                if( !architecture.equals(img.getArchitecture()) ) {
                    continue;
                }
            }
            if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
                Platform p = img.getPlatform();
                
                if( p.equals(Platform.UNKNOWN) ) {
                    continue;
                }
                else if( platform.isWindows() ) {
                    if( !p.isWindows() ) {
                        continue;
                    }
                }
                else if( platform.equals(Platform.UNIX) ) {
                    if( !p.isUnix() ) {
                        continue;
                    }
                }
                else if( !platform.equals(p) ) {
                    continue;
                }
            }
            if( keyword != null ) {
                if( !img.getName().contains(keyword) ) {
                    if( !img.getDescription().contains(keyword) ) {
                        if( !img.getProviderMachineImageId().contains(keyword) ) {
                            continue;
                        }
                    }
                }
            }
            images.add(img);
        }
        return images;
    }

    @Override
    public void shareMachineImage(@Nonnull String machineImageId, @Nonnull String withAccountId, boolean allow) throws CloudException, InternalException {
        throw new OperationNotSupportedException("OpenStack does not support image sharing");
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public @Nonnull String transfer(@Nonnull CloudProvider fromCloud, @Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Rackspace does not support image transfers");
    }
    
    public @Nullable MachineImage toImage(@Nullable JSONObject json) throws JSONException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".toImage(" + json + ")");
        }
        try {
            if( json == null ) {
                return null;
            }
            MachineImage image = new MachineImage();
            
            image.setArchitecture(Architecture.I64);
            image.setPlatform(Platform.UNKNOWN);
            image.setProviderOwnerId(provider.getContext().getAccountNumber());
            image.setProviderRegionId(provider.getContext().getRegionId());
            image.setTags(new HashMap<String,String>());
            image.setType(MachineImageType.STORAGE);
            image.setSoftware("");
            if( json.has("id") ) {
                image.setProviderMachineImageId(json.getString("id"));
            }
            if( json.has("name") ) {
                image.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                image.setDescription(json.getString("description"));
            }
            if( json.has("metadata") ) {
                JSONObject md = json.getJSONObject("metadata");
                
                if( image.getDescription() == null && md.has("dsnDescription") ) {
                    image.setDescription(md.getString("dsnDescription"));
                }
            }
            if( json.has("status") ) {
                String s = json.getString("status").toLowerCase();
                
                if( s.equals("saving") ) {
                    image.setCurrentState(MachineImageState.PENDING);
                }
                else if( s.equals("active") || s.equals("queued") || s.equals("preparing") ) {
                    image.setCurrentState(MachineImageState.ACTIVE);
                }
                else if( s.equals("deleting") ) {
                    image.setCurrentState(MachineImageState.PENDING);
                }
                else if( s.equals("failed") ) {
                    return null;
                }
                else {
                    logger.warn("toImage(): Unknown image status: " + s);
                    image.setCurrentState(MachineImageState.PENDING);
                }
            }
            if( image.getProviderMachineImageId() == null ) {
                return null;
            }
            if( image.getName() == null ) {
                image.setName(image.getProviderMachineImageId());
            }
            if( image.getDescription() == null ) {
                image.setDescription(image.getName());
            }
            image.setPlatform(Platform.guess(image.getName() + " " + image.getDescription()));
            return image;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".toImage()");
            }            
        }
    }
}
