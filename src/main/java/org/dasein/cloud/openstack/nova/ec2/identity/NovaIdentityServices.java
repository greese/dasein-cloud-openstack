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

package org.dasein.cloud.openstack.nova.ec2.identity;

import org.dasein.cloud.identity.AbstractIdentityServices;
import org.dasein.cloud.openstack.nova.ec2.NovaEC2;

import javax.annotation.Nonnull;

public class NovaIdentityServices extends AbstractIdentityServices {
    private NovaEC2 cloud;
    
    public NovaIdentityServices(@Nonnull NovaEC2 cloud) { this.cloud = cloud; }
    
    @Override
    public @Nonnull NovaSSHKeys getShellKeySupport() {
        return new NovaSSHKeys(cloud);
    }
}
