/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.openstack.nova.os.compute;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.Snapshot;
import org.dasein.cloud.compute.SnapshotState;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

/**
 * Implements support for snapshots from the OpenStack Cinder API.
 * <p>Created by George Reese: 10/25/12 9:27 AM</p>
 * @author George Reese
 * @version 2012.09.1 copied from the HP block storage support
 * @since 2012.09.1
 */
public class CinderSnapshot implements SnapshotSupport {
    static private final Logger logger = NovaOpenStack.getLogger(CinderSnapshot.class, "std");

    static public final String SERVICE  = "volume";

    private NovaOpenStack provider;

    public CinderSnapshot(NovaOpenStack provider) { this.provider = provider; }

    private @Nonnull String getResource() {
        return (provider.isHP() ? "/os-snapshots" : "/snapshots");
    }
    
    @Override
    public @Nonnull  String create(@Nonnull String ofVolume, @Nonnull String description) throws InternalException, CloudException {

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CinderSnapshot.class.getName() + ".create(" + ofVolume + "," + description + ")");
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

            json.put("volume_id", ofVolume);
            json.put("display_name", description);
            json.put("display_description", description);
            json.put("force", "True");
            wrapper.put("snapshot", json);
            JSONObject result = method.postString(SERVICE, getResource(), null, new JSONObject(wrapper), true);

            if( result != null && result.has("snapshot") ) {
                try {
                    Snapshot snapshot = toSnapshot(ctx, result.getJSONObject("snapshot"));

                    if( snapshot != null ) {
                        return snapshot.getProviderSnapshotId();
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
            logger.error("create(): No snapshot was created by the create attempt, and no error was returned");
            throw new CloudException("No snapshot was created");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CinderSnapshot.class.getName() + ".create()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Override
    public Snapshot getSnapshot(String snapshotId) throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(CinderSnapshot.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CinderSnapshot.class.getName() + ".getSnapshot(" + snapshotId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject ob = method.getResource(SERVICE, getResource(), snapshotId, true);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("snapshot") ) {
                    return toSnapshot(ctx, ob.getJSONObject("snapshot"));
                }
            }
            catch( JSONException e ) {
                std.error("getSnapshot(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for snapshot");
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CinderSnapshot.class.getName() + ".getSnapshot()");
            }
        }
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String snapshotId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public boolean isPublic(@Nonnull String snapshotId) throws InternalException, CloudException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        return (provider.getAuthenticationContext().getServiceUrl(SERVICE) != null);
    }

    @Override
    public @Nonnull Iterable<Snapshot> listSnapshots() throws InternalException, CloudException {
        Logger std = NovaOpenStack.getLogger(CinderSnapshot.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + CinderSnapshot.class.getName() + ".listSnapshots()");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();

            JSONObject json = method.getResource(SERVICE, getResource(), null, false);

            if( json != null && json.has("snapshots") ) {
                try {
                    JSONArray list = json.getJSONArray("snapshots");

                    for( int i=0; i<list.length(); i++ ) {
                        Snapshot snapshot = toSnapshot(ctx, list.getJSONObject(i));

                        if( snapshot != null ) {
                            snapshots.add(snapshot);
                        }
                    }
                }
                catch( JSONException e ) {
                    std.error("listSnapshots(): Unable to identify expected values in JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for snapshots in " + json.toString());
                }
            }
            return snapshots;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CinderSnapshot.class.getName() + ".listSnapshots()");
            }
        }
    }

    @Override
    public void remove(String snapshotId) throws InternalException, CloudException {
        Logger logger = NovaOpenStack.getLogger(CinderSnapshot.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CinderSnapshot.class.getName() + ".remove("+ snapshotId + ")");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, getResource(), snapshotId, null);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CinderSnapshot.class.getName() + ".remove()");
            }
        }
    }

    @Override
    public void shareSnapshot(@Nonnull String snapshotId, @Nullable String withAccountId, boolean affirmative) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Snapshot sharing not currently supported");
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return false;
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable Snapshot toSnapshot(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        try {
            String snapshotId = (json.has("id") ? json.getString("id") : null);

            if( snapshotId == null ) {
                return null;
            }

            String regionId = ctx.getRegionId();
            String name = (json.has("displayName") ? json.getString("displayName") : null);

            if( name == null ) {
                name = snapshotId;
            }

            String description = (json.has("displayDescription") ? json.getString("displayDescription") : null);

            if( description == null ) {
                description = null;
            }

            String volumeId = (json.has("volumeId") ? json.getString("volumeId") : null);


            SnapshotState currentState = SnapshotState.PENDING;
            String status = (json.has("status") ? json.getString("status") : null);

            if( status != null ) {
                if( status.equalsIgnoreCase("deleted") ) {
                    currentState = SnapshotState.DELETED;
                }
                else if( status.equalsIgnoreCase("available") ) {
                    currentState = SnapshotState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("creating") ) {
                    currentState = SnapshotState.PENDING;
                }
                else {
                    logger.warn("DEBUG: Unknown OpenStack snapshot state: " + status);
                }
            }
            long created = (json.has("createdAt") ? provider.parseTimestamp(json.getString("createdAt")) : -1L);

            int size = (json.has("size") ? json.getInt("size") : 0);

            Snapshot snapshot = new Snapshot();

            snapshot.setCurrentState(currentState);
            snapshot.setDescription(description);
            snapshot.setName(name);
            snapshot.setOwner(ctx.getAccountNumber());
            snapshot.setProviderSnapshotId(snapshotId);
            snapshot.setRegionId(regionId);
            snapshot.setSizeInGb(size);
            snapshot.setSnapshotTimestamp(created);
            snapshot.setVolumeId(volumeId);
            return snapshot;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

}
