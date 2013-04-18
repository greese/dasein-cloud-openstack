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
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
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


public class NovaImage implements MachineImageSupport {
    static private final Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");

    private NovaOpenStack provider;
    
    NovaImage(NovaOpenStack provider) { this.provider = provider; }

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

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return provider.getAuthenticationContext().getTenantId();
    }

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not currently supported");
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not currently supported");
    }

    @Override
    public @Nonnull String bundleVirtualMachine(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        throw new OperationNotSupportedException("This operation is not currently supported");
    }

    @Override
    public void bundleVirtualMachineAsync(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name, @Nonnull AsynchronousTask<String> trackingTask) throws CloudException, InternalException {
        throw new OperationNotSupportedException("This operation is not currently supported");
    }

    @Override
    public @Nonnull MachineImage captureImage(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        return captureImage(options, null);
    }

    private MachineImage captureImage(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
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
                MachineImage image = toImage(ctx, img);

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
    public void captureImageAsync(final @Nonnull ImageCreateOptions options, final @Nonnull AsynchronousTask<MachineImage> taskTracker) throws CloudException, InternalException {
        VirtualMachine vm = null;

        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 30L);

        while( timeout > System.currentTimeMillis() ) {
            try {
                //noinspection ConstantConditions
                vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(options.getVirtualMachineId());
                if( vm == null ) {
                    break;
                }
                if( !vm.isPersistent() ) {
                    throw new OperationNotSupportedException("You cannot capture instance-backed virtual machines");
                }
                if( VmState.RUNNING.equals(vm.getCurrentState()) || VmState.STOPPED.equals(vm.getCurrentState()) ) {
                    break;
                }
            }
            catch( Throwable ignore ) {
                // ignore
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + options.getVirtualMachineId());
        }
        Thread t = new Thread() {
            public void run() {
                try {
                    taskTracker.completeWithResult(captureImage(options, taskTracker));
                }
                catch( Throwable t ) {
                    taskTracker.complete(t);
                }
                finally {
                    provider.release();
                }
            }
        };

        provider.hold();
        t.setName("Imaging " + options.getVirtualMachineId() + " as " + options.getName());
        t.setDaemon(true);
        t.start();
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
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            try {
                if( ob.has("image") ) {
                    JSONObject server = ob.getJSONObject("image");
                    MachineImage img = toImage(ctx, server);

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
    @Deprecated
    public @Nullable MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        return getImage(machineImageId);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "image";
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
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
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
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        @SuppressWarnings("ConstantConditions") VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + vmId);
        }
        final AsynchronousTask<MachineImage> task = new AsynchronousTask<MachineImage>();
        final AsynchronousTask<String> oldTask = new AsynchronousTask<String>();

        captureImageAsync(ImageCreateOptions.getInstance(vm,  name, description), task);

        final long timeout = System.currentTimeMillis() + (CalendarWrapper.HOUR * 2);

        Thread t = new Thread() {
            public void run() {
                while( timeout > System.currentTimeMillis() ) {
                    try { Thread.sleep(15000L); }
                    catch( InterruptedException ignore ) { }
                    oldTask.setPercentComplete(task.getPercentComplete());

                    Throwable error = task.getTaskError();
                    MachineImage img = task.getResult();

                    if( error != null ) {
                        oldTask.complete(error);
                        return;
                    }
                    else if( img != null ) {
                        oldTask.completeWithResult(img.getProviderMachineImageId());
                        return;
                    }
                    else if( task.isComplete() ) {
                        oldTask.complete(new CloudException("Task completed without info"));
                        return;
                    }
                }
                oldTask.complete(new CloudException("Image creation task timed out"));
            }
        };

        t.setDaemon(true);
        t.start();

        return oldTask;
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        MachineImage img = getImage(machineImageId);
        String ownerId = (img != null ? img.getProviderOwnerId() : null);

        return (ownerId != null && !ownerId.equals(provider.getTenantId()));
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
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", null, true);
            ArrayList<ResourceStatus> images = new ArrayList<ResourceStatus>();

            try {
                if( ob != null && ob.has("images") ) {
                    JSONArray list = ob.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject image = list.getJSONObject(i);
                        ResourceStatus img = toStatus(ctx, image, cls);

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
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".listImages(" + cls + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getServers("/images", null, true);
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            try {
                if( ob != null && ob.has("images") ) {
                    JSONArray list = ob.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject image = list.getJSONObject(i);
                        MachineImage img = toImage(ctx, image);

                        if( img != null && img.getProviderOwnerId().equals(provider.getTenantId()) && img.getImageClass().equals(cls) ) {
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
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls, @Nonnull String ownedBy) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( !ownedBy.equals(provider.getTenantId()) ) {
            return Collections.emptyList();
        }
        return listImages(cls);
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        return listImages(ImageClass.MACHINE);
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            return Collections.emptyList();
        }
        return listImages(ImageClass.MACHINE, accountId);
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
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Glance integration not yet supported");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        remove(machineImageId, false);
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
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
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
        NovaMethod method = new NovaMethod(provider);
        JSONObject ob = method.getServers("/images", null, true);
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        try {
            if( ob != null && ob.has("images") ) {
                JSONArray list = ob.getJSONArray("images");

                for( int i=0; i<list.length(); i++ ) {
                    JSONObject image = list.getJSONObject(i);
                    MachineImage img = toImage(ctx, image);

                    if( img != null ) {
                        if( accountNumber != null && !accountNumber.equals(img.getProviderOwnerId()) ) {
                            continue;
                        }
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
                        if( imageClasses != null && imageClasses.length > 0 ) {
                            boolean ok = false;

                            for( ImageClass cls : imageClasses ) {
                                if( cls.equals(img.getImageClass()) ) {
                                    ok = true;
                                    break;
                                }
                            }
                            if( !ok ) {
                                continue;
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
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        NovaMethod method = new NovaMethod(provider);
        JSONObject ob = method.getServers("/images", null, true);
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        try {
            if( ob != null && ob.has("images") ) {
                JSONArray list = ob.getJSONArray("images");

                for( int i=0; i<list.length(); i++ ) {
                    JSONObject image = list.getJSONObject(i);
                    MachineImage img = toImage(ctx, image);

                    if( img != null ) {
                        if( getTenantId().equals(img.getProviderOwnerId()) ) {
                            continue;
                        }
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
                        if( imageClasses != null && imageClasses.length > 0 ) {
                            boolean ok = false;

                            for( ImageClass cls : imageClasses ) {
                                if( cls.equals(img.getImageClass()) ) {
                                    ok = true;
                                    break;
                                }
                            }
                            if( !ok ) {
                                continue;
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
    public void shareMachineImage(@Nonnull String machineImageId, @Nullable String withAccountId, boolean allow) throws CloudException, InternalException {
        throw new OperationNotSupportedException("OpenStack does not support image sharing");
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
        return ImageClass.MACHINE.equals(cls);
    }

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    public @Nullable MachineImage toImage(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws CloudException, JSONException {
        Logger logger = NovaOpenStack.getLogger(NovaImage.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaImage.class.getName() + ".toImage(" + json + ")");
        }
        try {
            if( json == null ) {
                return null;
            }
            MachineImage image = new MachineImage();
            String description = (json.has("description") && !json.isNull("description") ? json.getString("description") : null);

            image.setImageClass(ImageClass.MACHINE); // TODO: really?
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
            JSONObject md = (json.has("metadata") ? json.getJSONObject("metadata") : null);
            Architecture architecture = Architecture.I64;
            Platform platform = Platform.UNKNOWN;
            String owner = provider.getCloudProvider().getDefaultImageOwner(provider.getTenantId());

            if( md != null ) {
                if( description == null && md.has("org.dasein.description") ) {
                    description = md.getString("org.dasein.description");
                }
                if( md.has("org.dasein.platform") ) {
                    try {
                        platform = Platform.valueOf(md.getString("org.dasein.platform"));
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                }
                String[] akeys = { "arch", "architecture", "org.openstack__1__architecture", "com.hp__1__architecture" };
                String a = null;

                for( String key : akeys ) {
                    if( md.has(key) && !md.isNull(key) ) {
                        a = md.getString(key);
                        if( a != null ) {
                            break;
                        }
                    }
                }
                if( a != null ) {
                    a = a.toLowerCase();
                    if( a.contains("32") ) {
                        architecture = Architecture.I32;
                    }
                    else if( a.contains("sparc") ) {
                        architecture = Architecture.SPARC;
                    }
                    else if( a.contains("power") ) {
                        architecture = Architecture.POWER;
                    }
                }
                if( md.has("os_type") && !md.isNull("os_type") ) {
                    Platform p = Platform.guess(md.getString("os_type"));

                    if( !p.equals(Platform.UNKNOWN) ) {
                        if( platform.equals(Platform.UNKNOWN) ) {
                            platform = p;
                        }
                        else if( platform.equals(Platform.UNIX) && !p.equals(Platform.UNIX) ) {
                            platform = p;
                        }
                    }
                }
                if( md.has("owner") && !md.isNull("owner")) {
                    owner = md.getString("owner");
                }
                else if( md.has("image_type") && !md.isNull("image_type") && md.getString("image_type").equals("base") ) {
                    owner = "--public--";
                }
            }
            image.setProviderOwnerId(owner);
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
            if( description == null ) {
                image.setDescription(image.getName());
            }
            else {
                image.setDescription(description);
            }
            if( platform.equals(Platform.UNKNOWN) ) {
                image.setPlatform(Platform.guess(image.getName() + " " + image.getDescription()));
            }
            else {
                image.setPlatform(platform);
            }
            image.setArchitecture(architecture);
            return image;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaImage.class.getName() + ".toImage()");
            }            
        }
    }

    public @Nullable ResourceStatus toStatus(@Nonnull ProviderContext ctx, @Nullable JSONObject json, @Nonnull ImageClass cls) throws CloudException, InternalException {

        if( json == null ) {
            return null;
        }
        String owner = provider.getCloudProvider().getDefaultImageOwner(provider.getTenantId());
        MachineImageState state = MachineImageState.PENDING;
        String id = null;

        try {
            if( json.has("id") ) {
                id = json.getString("id");
            }
            if( id == null ) {
                return null;
            }
            if( !ImageClass.MACHINE.equals(cls) ) {
                return null;
            }
            JSONObject md = (json.has("metadata") ? json.getJSONObject("metadata") : null);

            if( md != null && md.has("owner") && !md.isNull("owner")) {
                owner = md.getString("owner");
            }
            else if( md != null && md.has("image_type") && !md.isNull("image_type") && md.getString("image_type").equals("base") ) {
                owner = "--public--";
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
        }
        catch( JSONException e ) {
            throw new InternalException(e);
        }
        if( !owner.equals(provider.getTenantId()) ) {
            return null;
        }
        return new ResourceStatus(id, state);
    }
}
