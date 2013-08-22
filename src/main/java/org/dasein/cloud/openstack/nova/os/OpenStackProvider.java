/**
 * Copyright (C) 2009-2013 Dell, Inc.
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
    DELL, DREAMHOST, GRIZZLY, HP, IBM, METACLOUD, RACKSPACE, OTHER;

    static public OpenStackProvider getProvider(@Nonnull String name) {
        if( name.equalsIgnoreCase("dell") ) {
            return DELL;
        }
        else if( name.equalsIgnoreCase("dreamhost") ) {
            return DREAMHOST;
        }
        else if( name.equalsIgnoreCase("grizzly") ) {
            return GRIZZLY;
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
