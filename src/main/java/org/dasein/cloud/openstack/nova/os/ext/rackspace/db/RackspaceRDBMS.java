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

package org.dasein.cloud.openstack.nova.os.ext.rackspace.db;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Implements Dasein Cloud relational database support for the Rackspace cloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 * @version 2013.02 updated for 2013.02 model
 */
public class RackspaceRDBMS extends AbstractRelationalDatabaseSupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(RackspaceRDBMS.class, "std");

    static public final String RESOURCE  = "/instances";

    static public final String SERVICE  = "rax:database";

    public RackspaceRDBMS(NovaOpenStack provider) {
        super(provider);
    }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return getProvider().getContext().getAccountNumber();
    }

    @Override
    public @Nonnull String createFromScratch(@Nonnull String dataSourceName, @Nonnull DatabaseProduct product, @Nonnull String databaseVersion, @Nonnull String withAdminUser, @Nonnull String withAdminPassword, int hostPort) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.createFromScratch");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            
            Map<String,Object> wrapper = new HashMap<String,Object>();
            Map<String,Object> json = new HashMap<String,Object>();
            NovaMethod method = new NovaMethod(getProvider());

            Map<String,Object> database = new HashMap<String, Object>();

            database.put("name", dataSourceName);
            
            List<Map<String,Object>> dblist= new ArrayList<Map<String, Object>>();
            
            dblist.add(database);
            
            json.put("databases", dblist);

            //get the product id
            String[] parts = product.getProductSize().split(":");
            String id = parts[0];
            json.put("flavorRef", getFlavorRef(id));
            json.put("name", dataSourceName);
            if( withAdminUser != null && withAdminPassword != null ) {
                List<Map<String,Object>> users = new ArrayList<Map<String, Object>>();
                Map<String,Object> entry = new HashMap<String, Object>();
                
                entry.put("name", withAdminUser);
                entry.put("password", withAdminPassword);
                
                List<Map<String,Object>> dbaccess = new ArrayList<Map<String, Object>>();
                Map<String,Object> oneDb = new HashMap<String, Object>();
                
                oneDb.put("name", dataSourceName);
                dbaccess.add(oneDb);
                        
                entry.put("databases", dbaccess);
                users.add(entry);
                json.put("users", users);
            }
            int size = product.getStorageInGigabytes();
            
            if( size < 1 ) {
                size = 5;
            }
            Map<String,Object> volume = new HashMap<String, Object>();
            
            volume.put("size", String.valueOf(size));
            
            json.put("volume", volume);
            
            if( hostPort != 3306 ) {
                json.put("port", hostPort > 0 ? hostPort : 3306);
            }
            wrapper.put("instance", json);
            JSONObject result = method.postString(SERVICE, RESOURCE, null, new JSONObject(wrapper), true);

            if( result != null && result.has("instance") ) {
                try {
                    Database db = toDatabase(ctx, result.getJSONObject("instance"));

                    if( db != null ) {
                        return db.getProviderDatabaseId();
                    }
                }
                catch( JSONException e ) {
                    logger.error("createFromScratch(): Unable to understand create response: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            logger.error("createFromScratch(): No database was created by the create attempt, and no error was returned");
            throw new CloudException("No database was created");

        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile RackspaceRDBMSCapabilities capabilities;

    @Override
    public @Nonnull RelationalDatabaseCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new RackspaceRDBMSCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public @Nullable DatabaseConfiguration getConfiguration(@Nonnull String providerConfigurationId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nullable Database getDatabase(@Nonnull String providerDatabaseId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getDatabase");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject ob = method.getResource(SERVICE, RESOURCE, providerDatabaseId, false);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("instance") ) {
                    return toDatabase(ctx, ob.getJSONObject("instance"));
                }
            }
            catch( JSONException e ) {
                logger.error("getDatabase(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for instance");
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<DatabaseEngine> getDatabaseEngines() throws CloudException, InternalException {
        return Collections.singletonList(DatabaseEngine.MYSQL);
    }

    @Override
    public @Nullable String getDefaultVersion(@Nonnull DatabaseEngine forEngine) throws CloudException, InternalException {
        if( forEngine.equals(DatabaseEngine.MYSQL) ) {
            return "5.5";
        }
        return null;
    }

    @Override
    public Iterable<String> getSupportedVersions(DatabaseEngine forEngine) throws CloudException, InternalException {
        if( forEngine.equals(DatabaseEngine.MYSQL) ) {
            return Collections.singletonList("5.5");
        }
        return Collections.emptyList();
    }

    public @Nullable DatabaseProduct getDatabaseProduct(@Nonnull String flavor) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getDatabaseProduct");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            int idx = flavor.indexOf(":");
            int size = 5;
            
            if( idx > -1 ) {
                size = Integer.parseInt(flavor.substring(idx+1));
                flavor = flavor.substring(0, idx);
            }
            NovaMethod method = new NovaMethod(getProvider());

            JSONObject json = method.getResource(SERVICE, "/flavors", flavor, false);

            if( json != null && json.has("flavor") ) {
                try {
                    return toProduct(ctx, size, json.getJSONObject("flavor"));
                }
                catch( JSONException e ) {
                    logger.error("getDatabaseProduct(): Unable to identify expected values in JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors in " + json.toString());
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    } 
    
    @Override
    public Iterable<DatabaseProduct> getDatabaseProducts(DatabaseEngine forEngine) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getDatabaseProducts");
        try {
            if( DatabaseEngine.MYSQL.equals(forEngine) ) {
                Logger std = NovaOpenStack.getLogger(RackspaceRDBMS.class, "std");

                if( std.isTraceEnabled() ) {
                    std.trace("ENTER: " + RackspaceRDBMS.class.getName() + ".getDatabaseProducts()");
                }
                try {
                    ProviderContext ctx = getProvider().getContext();

                    if( ctx == null ) {
                        std.error("No context exists for this request");
                        throw new InternalException("No context exists for this request");
                    }
                    NovaMethod method = new NovaMethod(getProvider());

                    JSONObject json = method.getResource(SERVICE, "/flavors", null, false);

                    List<DatabaseProduct> products = new ArrayList<DatabaseProduct>();

                    if( json != null && json.has("flavors") ) {
                        try {
                            JSONArray flavors = json.getJSONArray("flavors");

                            for( int i=0; i<flavors.length(); i++ ) {
                                JSONObject flavor = flavors.getJSONObject(i);

                                if( flavor != null ) {
                                    for( int size : new int[] { 2, 5, 10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 150}) { //150 is max size , 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000 } ) {
                                       DatabaseProduct product = toProduct(ctx, size, flavor);

                                        if( product != null ) {
                                            products.add(product);
                                        }
                                    }
                                }
                            }
                        }
                        catch( JSONException e ) {
                            std.error("getDatabaseProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                            e.printStackTrace();
                            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors in " + json.toString());
                        }
                    }
                    return products;
                }
                finally {
                    if( std.isTraceEnabled() ) {
                        std.trace("exit - " + RackspaceRDBMS.class.getName() + ".getDatabaseProducts()");
                    }
                }
            }
            else {
                return Collections.emptyList();
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<DatabaseProduct> listDatabaseProducts(DatabaseEngine databaseEngine) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getDatabaseProducts");
        try {
            if( DatabaseEngine.MYSQL.equals(databaseEngine) ) {
                Logger std = NovaOpenStack.getLogger(RackspaceRDBMS.class, "std");

                if( std.isTraceEnabled() ) {
                    std.trace("ENTER: " + RackspaceRDBMS.class.getName() + ".getDatabaseProducts()");
                }
                try {
                    ProviderContext ctx = getProvider().getContext();

                    if( ctx == null ) {
                        std.error("No context exists for this request");
                        throw new InternalException("No context exists for this request");
                    }
                    NovaMethod method = new NovaMethod(getProvider());

                    JSONObject json = method.getResource(SERVICE, "/flavors", null, false);

                    List<DatabaseProduct> products = new ArrayList<DatabaseProduct>();

                    if( json != null && json.has("flavors") ) {
                        try {
                            JSONArray flavors = json.getJSONArray("flavors");

                            for( int i=0; i<flavors.length(); i++ ) {
                                JSONObject flavor = flavors.getJSONObject(i);

                                if( flavor != null ) {
                                    for( int size : new int[] { 2, 5, 10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 150}) { //150 is max size , 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000 } ) {
                                        DatabaseProduct product = toProduct(ctx, size, flavor);

                                        if( product != null ) {
                                            products.add(product);
                                        }
                                    }
                                }
                            }
                        }
                        catch( JSONException e ) {
                            std.error("getDatabaseProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                            e.printStackTrace();
                            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors in " + json.toString());
                        }
                    }
                    return products;
                }
                finally {
                    if( std.isTraceEnabled() ) {
                        std.trace("exit - " + RackspaceRDBMS.class.getName() + ".getDatabaseProducts()");
                    }
                }
            }
            else {
                return Collections.emptyList();
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable String getFlavorRef(@Nonnull String productId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getFlavorRef");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            int idx = productId.indexOf(":");

            if( idx > -1 ) {
                productId = productId.substring(0, idx);
            }
            NovaMethod method = new NovaMethod(getProvider());

            JSONObject json = method.getResource(SERVICE, "/flavors", productId, false);

            if( json != null && json.has("flavor") ) {
                try {
                    JSONObject flavor = json.getJSONObject("flavor");

                    if( flavor.has("links") ) {
                        JSONArray links = flavor.getJSONArray("links");
                        
                        if( links != null ) {
                            for( int i=0; i<links.length(); i++ ) {
                                JSONObject link = links.getJSONObject(i);
                                
                                if( link.has("rel") ) {
                                    String rel = link.getString("rel");

                                    if( rel != null && rel.equalsIgnoreCase("self") ) {
                                        return link.getString("href");
                                    }
                                }
                            }
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("getFlavorRef(): Unable to identify expected values in JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors in " + json.toString());
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }
            
    @Override
    public String getProviderTermForDatabase(Locale locale) {
        return "database";
    }

    @Override
    public String getProviderTermForSnapshot(Locale locale) {
        return "snapshot";
    }

    @Override
    public DatabaseSnapshot getSnapshot(String providerDbSnapshotId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.isSubscribed");
        try {
            return (getProvider().getAuthenticationContext().getServiceUrl(SERVICE) != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listDatabaseStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.listDatabaseStatus");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(getProvider());
            List<ResourceStatus> databases = new ArrayList<ResourceStatus>();

            JSONObject json = method.getResource(SERVICE, RESOURCE, null, false);

            if( json != null && json.has("instances") ) {
                try {
                    JSONArray list = json.getJSONArray("instances");

                    for( int i=0; i<list.length(); i++ ) {
                        ResourceStatus db = toStatus(list.getJSONObject(i));

                        if( db != null ) {
                            databases.add(db);
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for instances in " + json.toString());
                }
            }
            return databases;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<Database> listDatabases() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.listDatabases");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(getProvider());
            List<Database> databases = new ArrayList<Database>();

            JSONObject json = method.getResource(SERVICE, RESOURCE, null, false);

            if( json != null && json.has("instances") ) {
                try {
                    JSONArray list = json.getJSONArray("instances");

                    for( int i=0; i<list.length(); i++ ) {
                        Database db = toDatabase(ctx, list.getJSONObject(i));

                        if( db != null ) {
                            databases.add(db);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("listDatabases(): Unable to identify expected values in JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for instances in " + json.toString());
                }
            }
            return databases;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeDatabase(String providerDatabaseId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.removeDatabase");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(getProvider());

            method.deleteResource(SERVICE, RESOURCE, providerDatabaseId, null);

            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5L);

            while( timeout > System.currentTimeMillis() ) {
                try {
                    Database db = getDatabase(providerDatabaseId);

                    if( db == null || DatabaseState.DELETED.equals(db.getCurrentState()) ) {
                        return;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                try { Thread.sleep(15000L); }
                catch( Throwable ignore ) { }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void resetConfiguration(String providerConfigurationId, String... parameters) throws CloudException, InternalException {
        // NO-OP since all configurations are at their defaults without configuration support
    }

    @Override
    public void restart(String providerDatabaseId, boolean blockUntilDone) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.restart");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }

            NovaMethod method = new NovaMethod(getProvider());
            Map<String,Object> wrapper = new HashMap<String, Object>();

            wrapper.put("restart", new HashMap<String,Object>());
            method.postString(SERVICE, RESOURCE, "action", new JSONObject(wrapper), false);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable Database toDatabase(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        
        String regionId = ctx.getRegionId();

        try {
            String dbId = (json.has("id") ? json.getString("id") : null);
            
            if( dbId == null ) {
                return null;
            }
            
            String name= (json.has("name") ? json.getString("name") : null);
            
            if( name == null ) {
                name = "RDBMS MySQL #" + dbId;
            }
    
            DatabaseState currentState = DatabaseState.PENDING;
            String status = (json.has("status") ? json.getString("status") : null);

            if( status != null ) {
                if( status.equalsIgnoreCase("BUILD") || status.equalsIgnoreCase("building") ) {
                    currentState = DatabaseState.PENDING;
                }
                else if( status.equalsIgnoreCase("ACTIVE") || status.equalsIgnoreCase("AVAILABLE") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("RESIZE") ) {
                    currentState = DatabaseState.MODIFYING;
                }
                else if( status.equalsIgnoreCase("SHUTDOWN") ) {
                    currentState = DatabaseState.RESTARTING;
                }
                else if( status.equalsIgnoreCase("BLOCKED") ) {
                    currentState = DatabaseState.PENDING;
                }
                else {
                    System.out.println("DEBUG OS Rackspace DB STATE: " + status);
                }
            }
            long created = (json.has("created") ? getProvider().parseTimestamp(json.getString("created")) : -1L);

            String hostname = (json.has("hostname") ? json.getString("hostname") : null);
            String flavor = null;
            
            if( json.has("flavor") ) {
                JSONObject f = json.getJSONObject("flavor");
                
                if( f != null && f.has("id") ) {
                    flavor = f.getString("id");
                }
            }
            int size = 0;
            
            if( json.has("volume") ) {
                JSONObject v = json.getJSONObject("volume");

                if( v != null && v.has("size") ) {
                    size = v.getInt("size");
                }
            }
            int port = (json.has("port") ? json.getInt("port") : 3306);

            Database database = new Database();

            database.setAdminUser(null);
            database.setAllocatedStorageInGb(size);
            database.setCreationTimestamp(created);
            database.setCurrentState(currentState);
            database.setEngine(DatabaseEngine.MYSQL);
            database.setHighAvailability(false);
            database.setHostName(hostname);
            database.setHostPort(port);
            database.setName(name);
            database.setProductSize(flavor + ":" + size);
            database.setProviderDatabaseId(dbId);
            database.setProviderDataCenterId(regionId + "-a");
            database.setProviderOwnerId(getTenantId());
            database.setProviderRegionId(regionId);
            return database;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    private @Nullable DatabaseProduct toProduct(@Nonnull ProviderContext ctx, @Nonnegative int size, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String id = (json.has("id") ? json.getString("id") : null);

            if( id == null ) {
                return null;
            }

            String name = (json.has("name") ? json.getString("name") : null);

            if( name == null ) {
                name = id + " (" + size + " GB)";
            }
            else {
                name = name + " [" + size + " GB]";
            }
            id = id + ":" + size;

            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new InternalException("No region is associated with this request");
            }
            DatabaseProduct product = new DatabaseProduct(id, name);

            if( regionId.equals("LON") ) {
                product.setCurrency("GBP");
            }
            else {
                product.setCurrency("USD");
            }
            product.setEngine(DatabaseEngine.MYSQL);
            product.setHighAvailability(false);
            product.setProviderDataCenterId(regionId + "-1");
            product.setStandardHourlyRate(0.0f);
            product.setStandardIoRate(0.0f);
            product.setStandardStorageRate(0.0f);
            product.setStorageInGigabytes(size);

            return product;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        try {
            String dbId = (json.has("id") ? json.getString("id") : null);

            if( dbId == null ) {
                return null;
            }
            DatabaseState currentState = DatabaseState.PENDING;
            String status = (json.has("status") ? json.getString("status") : null);

            if( status != null ) {
                if( status.equalsIgnoreCase("BUILD") || status.equalsIgnoreCase("building") ) {
                    currentState = DatabaseState.PENDING;
                }
                else if( status.equalsIgnoreCase("ACTIVE") || status.equalsIgnoreCase("AVAILABLE") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("RESIZE") ) {
                    currentState = DatabaseState.MODIFYING;
                }
                else if( status.equalsIgnoreCase("SHUTDOWN") ) {
                    currentState = DatabaseState.RESTARTING;
                }
                else if( status.equalsIgnoreCase("BLOCKED") ) {
                    currentState = DatabaseState.PENDING;
                }
                else {
                    System.out.println("DEBUG OS Rackspace DB STATE: " + status);
                }
            }
            return new ResourceStatus(dbId, currentState);
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }
}