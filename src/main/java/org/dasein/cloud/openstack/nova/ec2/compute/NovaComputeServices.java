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

package org.dasein.cloud.openstack.nova.ec2.compute;

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.openstack.nova.ec2.NovaEC2;

import javax.annotation.Nonnull;

public class NovaComputeServices extends AbstractComputeServices {
    private NovaEC2 provider;
    
    public NovaComputeServices(@Nonnull NovaEC2 provider) { this.provider = provider; }
    
    @Override
    public @Nonnull NovaImage getImageSupport() {
        return new NovaImage(provider);
    }
    
    /*
    @Override
    public NovaSnapshot getSnapshotSupport() {
        return new NovaSnapshot(provider);
    }
    */
    
    @Override
    public @Nonnull NovaServer getVirtualMachineSupport() {
        return new NovaServer(provider);
    }
    
    /*
    @Override
    public NovaVolume getVolumeSupport() {
        return new NovaVolume(provider);
    }
    */
}
