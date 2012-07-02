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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.encryption.Encryption;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.AuthenticationContext;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.SwiftMethod;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.CloudStoreObject;
import org.dasein.cloud.storage.FileTransfer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SwiftBlobStore extends AbstractBlobStoreSupport {
    static public final String SEPARATOR = ".";
    
    private NovaOpenStack provider = null;
    
    SwiftBlobStore(@Nonnull NovaOpenStack provider) { this.provider = provider; }

    public void clear(@Nonnull String directoryName) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".clear(" + directoryName + ")");
        }
        try {
            for( CloudStoreObject item : listFiles(directoryName) ) {
                if( item.isContainer() ) {
                    clear(item.getDirectory() + "." + item.getName());
                }
                else {
                    removeFile(directoryName, item.getName(), false);
                }
            }
            SwiftMethod method = new SwiftMethod(provider);
            
            method.delete(directoryName);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".clear()");
            }
        }
    }

    public @Nonnull String createDirectory(@Nonnull String abstractDirectoryName, boolean findFreeName) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".createDirectory(" + abstractDirectoryName + "," + findFreeName + ")");
        }
        try {
            try {
                String[] path = abstractDirectoryName.split("\\.");
                
                if( path == null || path.length < 1 ) {
                    path = new String[] { abstractDirectoryName };
                }
                for( int i=0; i<path.length; i++ ) {
                    String root = null;
                    
                    path[i] = verifyName(path[i], true);
                    if( i > 0 ) {
                        StringBuilder str = new StringBuilder();
                        
                        for( int j=0; j<i; j++ ) {
                            if( j > 0 ) {
                                str.append(".");
                            }
                            str.append(path[j]);
                        }
                        root = str.toString();
                    }
                    String p;
                    
                    if( root == null ) {
                        p = path[i];
                    }
                    else {
                        p = root + "." + path[1];
                    }
                    if( !exists(p) ) {
                        createDirectory(root, path[i]);
                    }
                    else if( i == path.length-1 ) {
                        if( !findFreeName ) {
                            throw new CloudException("The directory " + abstractDirectoryName + " already exists.");
                        }
                        else {
                            String tempName = path[i];
                            String suffix = "-";
                            char c = 'a';
                            
                            while( true ) {
                                path[i] = tempName + suffix + c;
                                if( root == null ) {
                                    p = path[i];
                                }
                                else {
                                    p = root + "." + path[1];
                                }
                                if( !exists(p) ) {
                                    break;
                                }
                                if( c == 'z' ) {
                                    suffix = suffix + "a";
                                    c = 'a';
                                }
                                else {
                                    c++;
                                }
                            }
                            createDirectory(root, path[i]);
                        }
                    }
                }
                return join(".", path);
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
                logger.trace("exit" + SwiftBlobStore.class.getName() + ".createDirectory()");
            }
        }
    }
    
    @Override
    public long exists(@Nonnull String abstractDirectoryName, @Nullable String object, boolean multiPart) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".exists(" + abstractDirectoryName + "," + object + "," + multiPart + ")");
        }
        try {
            if( !multiPart ) {
                try {
                    if( object == null ) {
                        SwiftMethod method = new SwiftMethod(provider);
                        
                        for( String container : method.get(null) ) {
                            if( !SEPARATOR.equals(".") ) {
                                container = container.replaceAll(SEPARATOR, ".");
                            }
                            if( container.equals(abstractDirectoryName) ) {
                                return 0;
                            }
                        }
                        return -1L;
                    }
                    else {
                        if( !SEPARATOR.equals(".") ) {
                            abstractDirectoryName = abstractDirectoryName.replaceAll("\\.", SEPARATOR);
                        }
                        if( logger.isTraceEnabled() ) {
                            logger.trace("exists(): Checking existence of " + abstractDirectoryName + "/" + object);
                        }
                        SwiftMethod method = new SwiftMethod(provider);
                        Map<String,String> metaData = method.head(abstractDirectoryName.replaceAll("\\.", SEPARATOR), object);
                        if( logger.isTraceEnabled() ) {
                            logger.trace("exists(): " + metaData);
                        }
                        long len = (metaData == null ? -1L : getMetaDataLength(metaData));
                        
                        if( logger.isTraceEnabled() ) {
                            logger.trace("exists(): " + abstractDirectoryName + "/" + object + "=" + len);
                        }
                        return len;
                    }
                }
                catch( RuntimeException e ) {
                    logger.error("Could not retrieve file info for " + abstractDirectoryName + "." + object + ": " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            else {
                if( exists(abstractDirectoryName, object + ".properties", false) == -1L ) {
                    return -1L;
                }
                Properties properties = new Properties();
                String str;
                
                try {
                    SwiftMethod method = new SwiftMethod(provider);
                    InputStream input = method.get(abstractDirectoryName, object + ".properties");

                    if( input == null ) {
                        throw new CloudException("Object was modified while we were reading it.");
                    }
                    try {
                        properties.load(input);
                    }
                    catch( IOException e ) {
                        logger.error("IO error loading file data for " + abstractDirectoryName + "." + object + ": " + e.getMessage());
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                }
                catch( RuntimeException e ) {
                    logger.error("Could not retrieve file info for " + abstractDirectoryName + "." + object + ": " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                str = properties.getProperty("length");
                if( str == null ) {
                    return 0L;
                }
                else {
                    return Long.parseLong(str);
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".exists()");
            }
        }
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
    public String getSeparator() {
        return SEPARATOR;
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        AuthenticationContext ctx = provider.getAuthenticationContext();
        String endpoint = ctx.getStorageUrl();
        
        return (endpoint != null && endpoint.startsWith("http"));
    }
    
    @Override
    public @Nonnull Iterable<CloudStoreObject> listFiles(@Nullable String parentDirectory) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".listFiles(" + parentDirectory + ")");
        }
        try {
            ArrayList<CloudStoreObject> results = new ArrayList<CloudStoreObject>();
            
            loadDirectories(parentDirectory, results);
            if( parentDirectory != null ) {
                loadFiles(parentDirectory, results);
            }
            return results;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".listFiles()");
            }
        }
    }
    
    private @Nonnull Collection<String> filter(@Nonnull Collection<String> buckets, @Nullable String prefix) {
        ArrayList<String> filtered = new ArrayList<String>();
        
        for( String container : buckets ) {
            if( container == null && prefix == null ) {
                filtered.add(container);
            }
            else if( prefix == null ) {
                if( !container.contains(SEPARATOR) ) {
                    filtered.add(container);
                }
            }
            else if( container != null && container.startsWith(prefix) && !container.equals(prefix) ) {
                container = container.substring(prefix.length() + SEPARATOR.length());
                if( !container.contains(SEPARATOR) ) {
                    filtered.add(container);
                }
            }
        }
        return filtered;
    }
    
    private void loadDirectories(@Nullable String abstractDirectoryName, @Nonnull List<CloudStoreObject> list) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".loadDirectories(" + abstractDirectoryName + "," + list + ")");
        }
        try {
            Collection<String> containers;
            
            try {
                SwiftMethod method = new SwiftMethod(provider);
    
                if( abstractDirectoryName != null ) {
                    containers = filter(method.get(abstractDirectoryName), abstractDirectoryName + SEPARATOR);
                }
                else {
                    containers = filter(method.get(null), null);
                }
            }
            catch( RuntimeException e ) {
                logger.error("Could not load directories in " + abstractDirectoryName + ": " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            for( String container : containers ) {
                String azureDirectoryName;
                
                azureDirectoryName = (abstractDirectoryName == null ? null : abstractDirectoryName);
                if( azureDirectoryName == null ) {
                    if( container.contains(".") ) {  // a sub-directory of another directory
                        continue;
                    }
                }
                else if( container.equals(azureDirectoryName) ) {
                    continue;
                }
                else if( !container.startsWith(azureDirectoryName + SEPARATOR) ) {
                    continue;
                }
                if( azureDirectoryName != null ) {
                    String tmp = container.substring(azureDirectoryName.length() + 1);
                    int idx = tmp.indexOf(SEPARATOR);
                    
                    if( idx > 0 /* yes 0, not -1 */ && idx < (tmp.length()-SEPARATOR.length()) ) { // this is a sub of a sub
                        continue;
                    }
                }
                CloudStoreObject file = new CloudStoreObject();
                String[] parts = container.split("\\.");
                
                if( parts == null || parts.length < 2 ) {
                    file.setName(container);
                    file.setDirectory(null);
                }
                else {
                    StringBuilder dirName = new StringBuilder();
                    
                    file.setName(parts[parts.length-1]);
                    for( int part=0; part<parts.length-1; part++ ) {
                        if( dirName.length() > 0 ) {
                            dirName.append(".");
                        }
                        dirName.append(parts[part]);
                    }
                    file.setDirectory(dirName.toString());
                }
                file.setContainer(true);
                file.setProviderRegionId(provider.getContext().getRegionId());
                file.setSize(0L);
                file.setCreationDate(new Date());
                list.add(file);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".loadDirectories()");
            }
        }
    }
        
    private void loadFiles(@Nullable String abstractDirectoryName, @Nonnull List<CloudStoreObject> list) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".loadFiles(" + abstractDirectoryName + "," + list + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            Collection<String> files;
            
            try {
                if( abstractDirectoryName == null ) {
                    files = method.get(null);
                }
                else {
                    files = method.get(abstractDirectoryName);
                }
            }
            catch( RuntimeException e ) {
                logger.error("Could not list files in " + abstractDirectoryName + ": " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
            for( String info : files ) {
                Map<String,String> metaData = method.head(abstractDirectoryName, info);
                CloudStoreObject file = new CloudStoreObject();
    
                file.setContainer(false);
                file.setDirectory(abstractDirectoryName);
                file.setName(info);
                file.setProviderRegionId(provider.getContext().getRegionId());
                file.setSize(getMetaDataLength(metaData));
                file.setLocation(getMetaDataString("uri", metaData, null));
                file.setCreationDate(new Date(-1L));  // TODO: better meta data?
                list.add(file);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".loadFiles()");
            }            
        }
    }
    
    public void makePublic(@Nonnull String abstractFileName) throws InternalException, CloudException {
        throw new OperationNotSupportedException();         
    }

    public void makePublic(@Nonnull String abstractDirectoryName, @Nonnull String fileName) throws InternalException, CloudException {
        throw new OperationNotSupportedException();         
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    public void moveFile(@Nonnull String fromDirectory, @Nonnull String fileName, @Nullable String targetRegionId, @Nonnull String toDirectory) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".moveFile(" + fromDirectory + "," + fileName + "," + targetRegionId + "," + toDirectory + ")");
        }
        try {
            moveFile(fromDirectory, fileName, toDirectory);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".moveFile()");
            }
        }
    }


    @Override
    public void moveFile(@Nonnull String sourceDirectory, @Nonnull String object, @Nonnull String toDirectory) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".moveFile(" + sourceDirectory + "," + object + "," + toDirectory + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context is configured for this operation.");
            }
            CloudStoreObject directory = new CloudStoreObject();
            CloudStoreObject file = new CloudStoreObject();
            String[] parts = toDirectory.split("\\.");
            String dirPath, dirName;
            
            if( parts == null || parts.length < 2 ) {
                dirPath = null;
                dirName = toDirectory;
            }
            else {
                StringBuilder str = new StringBuilder();
                
                dirName = parts[parts.length-1];
                for( int i = 0; i<parts.length-1; i++ ) {
                    if( i > 0 ) {
                        str.append(".");
                    }
                    str.append(parts[i]);
                }
                dirPath = str.toString();
            }
            file.setContainer(false);
            file.setDirectory(sourceDirectory);
            file.setName(object);
            file.setProviderRegionId(ctx.getRegionId());
            directory.setContainer(true);
            directory.setDirectory(dirPath);
            directory.setName(dirName);
            directory.setProviderRegionId(ctx.getRegionId());
            copy(file, directory, object);
            removeFile(sourceDirectory, object, false); 
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".moveFile()");
            }
        }
    }
    
    public void removeDirectory(@Nonnull String directory) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".removeDirectory(" + directory + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            
            method.delete(directory);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".removeDirectory()");
            }
        }
    }

    public void removeFile(@Nonnull String directory, @Nonnull String name, boolean multipartFile) throws CloudException, InternalException {     
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".removeFile(" + directory + "," + name + "," + multipartFile + ")");
        }
        try {
            if( !multipartFile ) {
                removeFile(directory, name);
            }
            else {
                removeMultipart(directory, name);
            }
        }
        finally {
            logger.debug("exit - removeFile(String, String, boolean)");         
        }
    }

    @Override
    protected void removeFile(@Nonnull String directory, @Nonnull String name) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".removeFile(" + directory + "," + name + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            
            method.delete(directory, name);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".removeFile()");
            }
        }
    }
    
    public @Nonnull String renameDirectory(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".renameDirectory(" + oldName + "," + newName + "," + findFreeName + ")");
        }
        try {
            String nd = createDirectory(newName, findFreeName);
            
            // list all the old files, move them
            for( CloudStoreObject f : listFiles(oldName) ) {
                moveFile(oldName, f.getName(), null, nd);
            }
            // delete the old container/objects
            removeDirectory(oldName);       
            return nd;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".renameDirectory()");
            }
        }
    }

    public void renameFile(@Nonnull String directory, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".renameFile(" + oldName + "," + newName + "," + newName + ")");
        }
        try {
            File tmp = null;
            
            try {
                // download old file
                tmp = File.createTempFile(newName, "tmp");          
                get(directory, oldName, tmp, null);
                        
                // create new name + upload file
                upload(tmp, directory, newName, false, null);
                
                // delete old name
                removeFile(directory, oldName, false);
            } 
            catch( CloudException e ) {
                logger.error(e);
                e.printStackTrace();
                throw e;
            } 
            catch( IOException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new CloudException(e);
            } 
            catch( RuntimeException e ) {
                logger.error(e);
                e.printStackTrace();
                throw new InternalException(e);
            }
            finally {
                if (tmp != null) {
                    if( !tmp.delete() ) {
                        logger.warn("Unable to delete temp file: " + tmp);
                    }
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".renameFile()");
            }            
        }
    }
    
    public void upload(@Nonnull File sourceFile, @Nonnull String directory, @Nonnull String fileName, boolean multipart, @Nullable Encryption encryption) throws CloudException, InternalException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".upload(" + sourceFile + "," + directory + "," + fileName + "," + multipart + "," + encryption + ")");
        }
        try {
            try {
                if( multipart ) {
                    try {
                        uploadMultipartFile(sourceFile, directory, fileName, encryption);
                    }
                    catch( InterruptedException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new CloudException(e.getMessage());
                    }
                }
                else {
                    try {
                        put(directory, fileName, sourceFile);
                    }
                    catch( NoSuchAlgorithmException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new InternalException(e);
                    }
                    catch( IOException e ) {
                        logger.error(e);
                        e.printStackTrace();
                        throw new CloudException(e.getMessage());
                    }
                }        
            }
            finally {
                if( encryption != null ) {
                    encryption.clear();
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("enter - " + SwiftBlobStore.class.getName() + ".upload()");
            }            
        }
    }
    
    private boolean createDirectory(@Nullable String parent, @Nonnull String name) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".createDirectory(" + parent + "," + name + ")");
        }
        try {
            try {
                SwiftMethod method = new SwiftMethod(provider);

                if( parent == null ) {
                    method.put(name);
                }
                else {
                    method.put(parent + SEPARATOR + name);
                }
                return true;
            }
            catch( RuntimeException e ) {
                logger.error("Could not create directory: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".createDirectory()");
            }
        }
    }
    
    @Override
    protected void get(@Nonnull String directory, @Nonnull String location, @Nonnull File toFile, @Nonnull FileTransfer transfer) throws IOException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".get(" + directory + "," + location + "," + toFile + "," + transfer + ")");
        }
        try {
            if( toFile.exists() ) {
                if( !toFile.delete() ) {
                    throw new IOException("File already exists that cannot be overwritten.");
                }
            }
            SwiftMethod method = new SwiftMethod(provider);
            InputStream input;
            
            try {
                input = method.get(directory, location);
            }
            catch( InternalException e ) {
                throw new IOException(e);
            }
            if( input == null ) {
                throw new IOException("No such object: " + directory + "." + location);
            }
            copy(input, new FileOutputStream(toFile), transfer);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".get()");
            }
        }
    }  
    
    @Override
    protected void put(@Nonnull String directory, @Nonnull String fileName, @Nonnull File file) throws NoSuchAlgorithmException, IOException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".put(" + directory + "," + fileName + "," + file + ")");
        }
        try {
            SwiftMethod method = new SwiftMethod(provider);
            
            try {
                method.put(directory, fileName, null, new FileInputStream(file)); // TODO: MD5 hash
            }
            catch( InternalException e ) {
                throw new IOException(e);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".put()");
            }
        }
    }
    
    @Override
    protected void put(@Nonnull String directory, @Nonnull String fileName, @Nonnull String content) throws NoSuchAlgorithmException, IOException, CloudException {
        Logger logger = NovaOpenStack.getLogger(SwiftBlobStore.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SwiftBlobStore.class.getName() + ".put(" + directory + "," + fileName + "," + content + ")");
        }
        try {
            File tmp = File.createTempFile(fileName, ".txt");
            PrintWriter writer;
            
            try {
                writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tmp)));
                writer.print(content);
                writer.flush();
                writer.close();
                put(directory, fileName, tmp);
            }
            finally {
                if( !tmp.delete() ) {
                    logger.warn("Unable to delete temp file: " + tmp);
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SwiftBlobStore.class.getName() + ".put()");
            }
        }
    }

    @Override
    public boolean isPublic(String directory, String file) throws CloudException, InternalException {
        return false;
    }
    
    public long getMaxFileSizeInBytes() {
        return 5000000000L;
    }

    public String getProviderTermForDirectory(Locale locale) {
        return "container";
    }

    public String getProviderTermForFile(Locale locale) {
        return "object";
    }

    
    @Override
    protected String verifyName(String name, boolean container) throws CloudException {
        if( name == null ) {
            return null;
        }
        StringBuilder str = new StringBuilder();
        name = name.toLowerCase().trim();
        if( name.length() > 255 ) {
            String extra = name.substring(255);
            int idx = extra.indexOf(".");
            
            if( idx > -1 ) {
                throw new CloudException("S3 names are limited to 255 characters.");
            }
            name = name.substring(0,255);
        }
        while( name.contains("--") ) {
            name = name.replaceAll("--", "-");         
        }
        while( name.contains("..") ) {
            name = name.replaceAll("\\.\\.", ".");         
        }
        while( name.contains(".-") ) {
            name = name.replaceAll("\\.-", ".");         
        }
        while( name.contains("-.") ) {
            name = name.replaceAll("-\\.", ".");         
        }
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( Character.isLetterOrDigit(c) ) {
                str.append(c);
            }
            else {
                if( i > 0 ) {
                    if( c == '/' ) {
                        c = '.';
                    }
                    else if( c != '.' && c != '-' ) {
                        c = '-';
                    }
                    str.append(c);
                }
            }
        }
        name = str.toString();
        while( name.contains("..") ) {
            name = name.replaceAll("\\.\\.", ".");
        }
        if( name.length() < 1 ) { 
            return "000";
        }
        while( name.charAt(name.length()-1) == '-' || name.charAt(name.length()-1) == '.' ) {
            name = name.substring(0,name.length()-1);
            if( name.length() < 1 ) { 
                return "000";
            }
        }
        if( name.length() < 1 ) { 
            return "000";
        }
        else if( name.length() == 1 ) {
            name = name + "00";
        }
        else if ( name.length() == 2 ) {
            name = name + "0";
        }
        return name;
    }   
}
