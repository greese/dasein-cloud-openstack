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

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.ext.hp.block.HPBlockStorage;
import org.dasein.cloud.openstack.nova.os.ext.hp.block.HPVolumeSnapshot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NovaComputeServices extends AbstractComputeServices {
    private NovaOpenStack provider;
    
    public NovaComputeServices(@Nonnull NovaOpenStack provider) { this.provider = provider; }
    
    @Override
    public @Nonnull NovaImage getImageSupport() {
        return new NovaImage(provider);
    }
    
    @Override
    public @Nonnull NovaServer getVirtualMachineSupport() {
        return new NovaServer(provider);
    }

    @Override
    public @Nullable SnapshotSupport getSnapshotSupport() {
        if( provider.getProviderName().equals("HP") ) {
            return new HPVolumeSnapshot(provider);
        }
        return null;
    }

    @Override
    public @Nullable VolumeSupport getVolumeSupport() {
        if( provider.getProviderName().equals("HP") ) {
            return new HPBlockStorage(provider);
        }
        return null;
    }
}
