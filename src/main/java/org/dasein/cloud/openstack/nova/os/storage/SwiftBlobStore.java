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

package org.dasein.cloud.openstack.nova.os.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.NameRules;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.AuthenticationContext;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.SwiftMethod;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SwiftBlobStore extends AbstractBlobStoreSupport {
    static public final int                                       MAX_BUCKETS     = 100;
    static public final int                                       MAX_OBJECTS     = -1;
    static public final Storage<Byte>                             MAX_OBJECT_SIZE = new Storage<org.dasein.util.uom.storage.Byte>(5000000000L, Storage.BYTE);

    private NovaOpenStack provider = null;
    
    SwiftBlobStore(@Nonnull NovaOpenStack provider) { this.provider = provider; }

    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull Blob createBucket(@Nonnull String bucketName, boolean findFreeName) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new InternalException("No region ID was specified for this request");
        }

        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".createBucket(" + bucketName + "," + findFreeName + ")");
        }
        try {
            try {
                if( exists(bucketName) ) {
                    if( !findFreeName ) {
                        throw new CloudException("The bucket " + bucketName + " already exists.");
                    }
                    else {
                        bucketName = findFreeName(bucketName);
                    }

                }
                createBucket(bucketName);
                return getBucket(bucketName);
            }
            catch( CloudException e ) {
                logger.error(e);
                e.printStackTrace();
                throw e;
            }
            catch(InternalException e ) {
                logger.error(e);
                e.printStackTrace();
                throw e;
            }
            catch( RuntimeException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit" + SwiftBlobStore.class.getName() + ".createBucket()");
            }
        }
    }

    private void createBucket(@Nonnull String name) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".createBucket(" + name + ")");
        }
        try {
            try {
                SwiftMethod method = new SwiftMethod(provider);

                method.put(name);
            }
            catch( RuntimeException e ) {
                logger.error("Could not create bucket: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".createBucket()");
            }
        }
    }

    @Override
    public boolean exists(@Nonnull String bucketName) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".exists(" + bucketName + ")");
        }
        try {
            try {
                SwiftMethod method = new SwiftMethod(provider);

                for( String container : method.get(null) ) {
                    if( container.equals(bucketName) ) {
                        return true;
                    }
                }
                return false;
            }
            catch( RuntimeException e ) {
                logger.error("Could not retrieve file info for " + bucketName + ": " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".exists()");
            }
        }
    }

    @Override
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        for( Blob blob : list(null) ) {
            if( blob.isContainer() ) {
                String name = blob.getBucketName();

                if( name != null && name.equals(bucketName) ) {
                    return blob;
                }
            }
        }
        return null;
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        if( bucketName == null ) {
            throw new CloudException("No bucket was specified for this request");
        }
        for( Blob blob : list(bucketName) ) {
            String name = blob.getObjectName();

            if( name != null && name.equals(objectName) ) {
                return blob;
            }
        }
        return null;
    }

    @Override
    public @Nullable Storage<org.dasein.util.uom.storage.Byte> getObjectSize(@Nullable String bucket, @Nullable String object) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".getObjectSize(" + bucket + "," + object + ")");
        }
        try {
            if( bucket == null ) {
                throw new CloudException("Requested object size for object in null bucket");
            }
            if( object == null ) {
                return null;
            }
            SwiftMethod method = new SwiftMethod(provider);

            Map<String,String> metaData = method.head(bucket, object);

            if( metaData == null ) {
                return null;
            }
            long len = getMetaDataLength(metaData);

            if( len < 0L ) {
                return null;
            }
            return new Storage<Byte>(len, Storage.BYTE);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".loadFiles()");
            }
        }
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return MAX_BUCKETS;
    }

    private @Nonnull String findFreeName(@Nonnull String bucket) throws InternalException, CloudException {
        int idx = bucket.lastIndexOf(".");
        String prefix, rawName;

        if( idx == -1 ) {
            prefix = null;
            rawName = bucket;
            bucket = rawName;
        }
        else {
            prefix = bucket.substring(0, idx);
            rawName = bucket.substring(idx+1);
            bucket = prefix + "." + rawName;
        }
        while( exists(bucket) ) {
            idx = rawName.lastIndexOf("-");
            if( idx == -1 ) {
                rawName = rawName + "-1";
            }
            else if( idx == rawName.length()-1 ) {
                rawName = rawName + "1";
            }
            else {
                String postfix = rawName.substring(idx+1);
                int x;

                try {
                    x = Integer.parseInt(postfix) + 1;
                    rawName = rawName.substring(0,idx) + "-" + x;
                }
                catch( NumberFormatException e ) {
                    rawName = rawName + "-1";
                }
            }
            if( prefix == null) {
                bucket = rawName;
            }
            else {
                bucket = prefix + "." + rawName;
            }
        }
        return bucket;
    }

    @Override
    protected void get(@Nullable String bucket, @Nonnull String location, @Nonnull File toFile, @Nullable FileTransfer transfer) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".get(" + bucket + "," + location + "," + toFile + "," + transfer + ")");
        }
        try {
            if( bucket == null ) {
                throw new OperationNotSupportedException("No such object: " + bucket + "/" + location);
            }
            if( toFile.exists() ) {
                if( !toFile.delete() ) {
                    throw new InternalException("File already exists that cannot be overwritten.");
                }
            }
            SwiftMethod method = new SwiftMethod(provider);
            InputStream input;

            input = method.get(bucket, location);
            if( input == null ) {
                throw new CloudException("No such object: " + bucket + "/" + location);
            }
            try {
                copy(input, new FileOutputStream(toFile), transfer);
            }
            catch( IOException e ) {
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".get()");
            }
        }
    }

    @Override
    public Storage<org.dasein.util.uom.storage.Byte> getMaxObjectSize() {
        return MAX_OBJECT_SIZE;
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return MAX_OBJECTS;
    }


    private long getMetaDataLength(@Nonnull Map<String,String> meta) {
        return getMetaDataLong("Content-Length", meta);
    }

    private long getMetaDataLong(@Nonnull String key, @Nonnull Map<String,String> meta) {
        if( !meta.containsKey(key) ) {
            return -1L;
        }
        String val = meta.get(key);

        return (val == null ? -1L : Long.parseLong(val));
    }

    private @Nonnull String getMetaDataString(@Nonnull String key, @Nonnull Map<String,String> meta, @Nonnull String def) {
        if( meta.containsKey(key) ) {
            return def;
        }
        String val = meta.get(key);

        if( val == null ) {
            return def;
        }
        return val;
    }

    @Override
    public @Nonnull String getProviderTermForBucket(@Nonnull Locale locale) {
        return "bucket";
    }

    @Override
    public @Nonnull String getProviderTermForObject(@Nonnull Locale locale) {
        return "object";
    }

    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        AuthenticationContext ctx = provider.getAuthenticationContext();
        String endpoint = ctx.getStorageUrl();

        return (endpoint != null && endpoint.startsWith("http"));
    }

    @Override
    public @Nonnull Collection<Blob> list(final @Nullable String bucket) throws CloudException, InternalException {
        final ProviderContext ctx = provider.getContext();
        PopulatorThread<Blob> populator;

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        final String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified");
        }
        provider.hold();
        populator = new PopulatorThread<Blob>(new JiteratorPopulator<Blob>() {
            public void populate(@Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
                try {
                    list(regionId, bucket, iterator);
                }
                finally {
                    provider.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    private void list(@Nonnull String regionId, @Nullable String bucket, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        if( bucket == null ) {
            loadBuckets(regionId, iterator);
        }
        else {
            loadObjects(regionId, bucket, iterator);
        }
    }

    private void loadBuckets(@Nonnull String regionId, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".loadBuckets(" + regionId + "," + iterator + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            Collection<String> containers;

            try {
                containers = method.get(null);
            }
            catch( RuntimeException e ) {
                logger.error("Could not load buckets: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            for( String container : containers ) {
                iterator.push(Blob.getInstance(regionId, "/" + container, container, -1L));
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".loadBuckets()");
            }
        }
    }

    private void loadObjects(@Nonnull String regionId, @Nonnull String bucketName, @Nonnull Jiterator<Blob> iterator) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".loadFiles(" + bucketName + "," + iterator + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            Collection<String> files;

            try {
                if( bucketName == null ) {
                    files = method.get(null);
                }
                else {
                    files = method.get(bucketName);
                }
            }
            catch( RuntimeException e ) {
                logger.error("Could not list files in " + bucketName + ": " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            for( String info : files ) {
                Map<String,String> metaData = method.head(bucketName, info);

                iterator.push(Blob.getInstance(regionId, "/" + bucketName + "/" + info, bucketName, info, -1L, new Storage<Byte>(getMetaDataLength(metaData), Storage.BYTE)));
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".loadFiles()");
            }
        }
    }

    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Swift does not support bucket sharing");
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nullable String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Swift does not support object sharing");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void move(@Nullable String sourceBucket, @Nullable String object, @Nullable String targetBucket) throws InternalException, CloudException {
        if( sourceBucket == null ) {
            throw new CloudException("No source bucket was specified");
        }
        if( targetBucket == null ) {
            throw new CloudException("No target bucket was specified");
        }
        if( object == null ) {
            throw new CloudException("No source object was specified");
        }
        copy(sourceBucket, object, targetBucket, object);
        removeObject(sourceBucket, object);
    }

    @Override
    protected void put(@Nullable String bucket, @Nonnull String object, @Nonnull File file) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".put(" + bucket + "," + object + "," + file + ")");
        }
        try {
            if( bucket == null ) {
                throw new OperationNotSupportedException("A bucket must be specified for Swift");
            }
            SwiftMethod method = new SwiftMethod(provider);

            try {
                method.put(bucket, object, null, new FileInputStream(file));
            }
            catch( IOException e ) {
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".put()");
            }
        }
    }

    @Override
    protected void put(@Nullable String bucket, @Nonnull String object, @Nonnull String content) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".put(" + bucket + "," + object + "," + content + ")");
        }
        try {
            try {
                File tmp = File.createTempFile(object, ".txt");
                PrintWriter writer;

                try {
                    writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tmp)));
                    writer.print(content);
                    writer.flush();
                    writer.close();
                    put(bucket, object, tmp);
                }
                finally {
                    if( !tmp.delete() ) {
                        logger.warn("Unable to delete temp file: " + tmp);
                    }
                }
            }
            catch( IOException e ) {
                logger.error("Failed to write file: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".put()");
            }
        }
    }

    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".removeBucket(" + bucket + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);

            method.delete(bucket);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".removeBucket()");
            }
        }
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String name) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".removeObject(" + bucket + "," + name + ")");
        }
        try {
            if( bucket == null ) {
                throw new OperationNotSupportedException("Swift does not support root objects");
            }
            SwiftMethod method = new SwiftMethod(provider);

            method.delete(bucket, name);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".removeObject()");
            }
        }
    }

    @Override
    public @Nonnull String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        Blob bucket = createBucket(newName, findFreeName);

        for( Blob file : list(oldName) ) {
            int retries = 10;

            while( true ) {
                retries--;
                try {
                    move(oldName, file.getObjectName(), bucket.getBucketName());
                    break;
                }
                catch( CloudException e ) {
                    if( retries < 1 ) {
                        throw e;
                    }
                }
                try { Thread.sleep(retries * 10000L); }
                catch( InterruptedException ignore ) { }
            }
        }
        boolean ok = true;
        for( Blob file : list(oldName ) ) {
            if( file != null ) {
                ok = false;
            }
        }
        if( ok ) {
            removeBucket(oldName);
        }
        return newName;
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String object, @Nonnull String newName) throws CloudException, InternalException {
        if( bucket == null ) {
            throw new CloudException("No bucket was specified");
        }
        copy(bucket, object, bucket, newName);
        removeObject(bucket, object);
    }

    @Override
    public @Nonnull Blob upload(@Nonnull File source, @Nullable String bucket, @Nonnull String fileName) throws CloudException, InternalException {
        if( bucket == null ) {
            throw new CloudException("No bucket was specified for this request");
        }
        if( !exists(bucket) ) {
            createBucket(bucket, false);
        }
        put(bucket, fileName, source);
        return getObject(bucket, fileName);
    }

    @Override
    public @Nonnull NameRules getBucketNameRules() throws CloudException, InternalException {
        //return NameRules.getInstance(minChars, maxChars, mixedCase, allowNumbers, latin1Only, specialChars);
        return NameRules.getInstance(1, 255, false, true, true, new char[] { '-', '.' });
    }

    @Override
    public @Nonnull NameRules getObjectNameRules() throws CloudException, InternalException {
        return NameRules.getInstance(1, 255, false, true, true, new char[] { '-', '.', ',', '#', '+' });
    }

}
