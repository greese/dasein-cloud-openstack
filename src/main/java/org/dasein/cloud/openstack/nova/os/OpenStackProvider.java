/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2013 enStratus Networks Inc - ALL RIGHTS RESERVED
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
package org.dasein.cloud.openstack.nova.os;

import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.VirtualMachine;

import javax.annotation.Nonnull;

/**
 * Helps encapsulates minor differences among different OpenStack distributions.
 * <p>Created by George Reese: 3/26/13 5:30 PM</p>
 * @author George Reese
 * @version 2013.04.1
 * @since 2013.04.1
 */
public enum OpenStackProvider {
    DELL, DREAMHOST, HP, IBM, METACLOUD, RACKSPACE, OTHER;

    static public OpenStackProvider getProvider(@Nonnull String name) {
        if( name.equalsIgnoreCase("dell") ) {
            return DELL;
        }
        else if( name.equalsIgnoreCase("dreamhost") ) {
            return DREAMHOST;
        }
        else if( name.equalsIgnoreCase("hp") ) {
            return HP;
        }
        else if( name.equalsIgnoreCase("ibm") ) {
            return IBM;
        }
        else if( name.equalsIgnoreCase("metacloud") ) {
            return METACLOUD;
        }
        else if( name.equalsIgnoreCase("rackspace") ) {
            return RACKSPACE;
        }
        return OTHER;
    }

    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        switch( this ) {
            case HP: case RACKSPACE: case METACLOUD: return false;
            default: return true;
        }
    }

    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        switch( this ) {
            case HP: case RACKSPACE: return false;
            default: return true;
        }
    }

    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        switch( this ) {
            case HP: case RACKSPACE: return false;
            default: return true;
        }
    }

    public @Nonnull String getDefaultImageOwner(@Nonnull String tenantId) {
        switch( this ) {
            case RACKSPACE: return tenantId;
            default: return "--public--";
        }
    }
}

