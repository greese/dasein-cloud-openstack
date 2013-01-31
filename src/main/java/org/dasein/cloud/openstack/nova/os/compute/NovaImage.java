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
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class NovaImage extends AbstractImageSupport {
    static private final Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");

    private NovaOpenStack provider;
    
    NovaImage(NovaOpenStack provider) {
        super(provider);
        this.provider = provider;
    }

    public @Nullable String getImageRef(@Nonnull String machineImageId) throws CloudException, InternalException {
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
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        NovaMethod method = new NovaMethod(provider);
        HashMap<String,Object> action = new HashMap<String,Object>();

        action.put("name", options.getName());
        if( task != null ) {
            task.setStartTime(System.currentTimeMillis());
        }
        String vmId = options.getVirtualMachineId();

        if( vmId != null ) {
            long timeout = (System.currentTimeMillis() + CalendarWrapper.MINUTE*10L);

            while( timeout > System.currentTimeMillis() ) {
                try {
                    VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);

                    if( vm == null ) {
                        throw new CloudException("No such virtual machine: " + vmId);
                    }
                    if( !VmState.PENDING.equals(vm.getCurrentState()) ) {
                        break;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
            }
        }
        JSONObject result;

        if( provider.isPostCactus() ) {
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,String> metaData = new HashMap<String,String>();

            metaData.put("dsnDescription", options.getDescription());
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
        if( result != null && result.has("image") ) {
            try {
                JSONObject img = result.getJSONObject("image");
                MachineImage image = toImage(img);

                if( image != null ) {
                    if( task != null ) {
                        task.completeWithResult(image);
                    }
                    return image;
                }
            }
            catch( JSONException e ) {
                throw new CloudException(e);
            }
        }
        else if( result != null && result.has("location") ) {
            try {
                long timeout = System.currentTimeMillis() + CalendarWrapper.MINUTE * 20L;
                String location = result.getString("location");
                int idx = location.lastIndexOf('/');

                if( idx > 0 ) {
                    location = location.substring(idx+1);
                }

                while( timeout > System.currentTimeMillis() ) {
                    MachineImage image = getImage(location);

                    if( image != null ) {
                        if( task != null ) {
                            task.completeWithResult(image);
                        }
                        return image;
                    }
                    try { Thread.sleep(15000L); }
                    catch( InterruptedException ignore ) { }
                }
            }
            catch( JSONException e ) {
                throw new CloudException(e);
            }
        }
        logger.error("No image was created by the imaging attempt, and no error was returned");
        throw new CloudException("No image was created");
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".getMachineImage(" + providerImageId + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", providerImageId, true);

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
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        switch( cls ) {
            case MACHINE: return "machine image";
            case KERNEL: return "kernel image";
            case RAMDISK: return "ramdisk image";
        }
        return "image";
    }

    @Override
    public boolean hasPublicLibrary() {
        return false;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
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
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".listImageStatus(" + cls + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", null, true);
            ArrayList<ResourceStatus> images = new ArrayList<ResourceStatus>();

            try {
                if( ob != null && ob.has("images") ) {
                    JSONArray list = ob.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject image = list.getJSONObject(i);
                        ResourceStatus img = toStatus(image);

                        if( img != null ) {
                            images.add(img);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
            }
            return images;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".listImageStatus()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".listImages(" + options + ")");
        }
        try {
            ImageClass cls = (options == null ? null : options.getImageClass());
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String account = (options == null ? null : options.getAccountNumber());

            if( account != null && !account.equals(ctx.getAccountNumber()) ) {
                return Collections.emptyList();
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", null, true);
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            try {
                if( ob != null && ob.has("images") ) {
                    JSONArray list = ob.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject image = list.getJSONObject(i);
                        MachineImage img = toImage(image);

                        if( img != null && (cls == null || cls.equals(img.getImageClass())) ) {
                            images.add(img);
                        }

                    }
                }
            }
            catch( JSONException e ) {
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
            }
            return images;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".listImages()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        ArrayList<ImageClass> values = new ArrayList<ImageClass>();

        Collections.addAll(values, ImageClass.values());
        return values;
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageType.VOLUME);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".remove(" + provider + "," + checkState + ")");
        }
        try {
            NovaMethod method = new NovaMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/images", providerImageId);
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
    @Deprecated
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        return searchImages(null, keyword, platform, architecture, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( accountNumber != null && !accountNumber.equals(ctx.getAccountNumber()) ) {
            return Collections.emptyList();
        }

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

                }
            }
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for images: " + e.getMessage());
        }
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false; // need Glance support
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
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
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return false;
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

            image.setImageClass(ImageClass.MACHINE); // TODO: really?
            image.setArchitecture(Architecture.I64);
            image.setPlatform(Platform.UNKNOWN);
            image.setProviderOwnerId(provider.getContext().getAccountNumber());
            image.setProviderRegionId(provider.getContext().getRegionId());
            image.setTags(new HashMap<String,String>());
            image.setType(MachineImageType.VOLUME);
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

    public @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws JSONException {

        if( json == null ) {
            return null;
        }
        MachineImageState state = MachineImageState.PENDING;
        String id = null;

        if( json.has("id") ) {
            id = json.getString("id");
        }
        if( id == null ) {
            return null;
        }
        if( json.has("status") ) {
            String s = json.getString("status").toLowerCase();

            if( s.equals("saving") ) {
                state = MachineImageState.PENDING;
            }
            else if( s.equals("active") || s.equals("queued") || s.equals("preparing") ) {
                state = MachineImageState.ACTIVE;
            }
            else if( s.equals("deleting") ) {
                state = MachineImageState.PENDING;
            }
            else if( s.equals("failed") ) {
                return null;
            }
            else {
                state = MachineImageState.PENDING;
            }
        }
        return new ResourceStatus(id, state);
    }
}
