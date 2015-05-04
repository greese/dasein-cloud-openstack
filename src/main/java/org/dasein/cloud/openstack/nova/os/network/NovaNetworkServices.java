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

package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.DNSSupport;
import org.dasein.cloud.network.LoadBalancerSupport;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.OpenStackProvider;
import org.dasein.cloud.openstack.nova.os.ext.rackspace.dns.RackspaceCloudDNS;
import org.dasein.cloud.openstack.nova.os.ext.rackspace.lb.RackspaceLoadBalancers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Support for the Dasein Cloud network services in OpenStack Nova.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2011.10
 * @version 2011.10
 * @version 2012.04.1 Added some intelligence around features Rackspace does not support
 */
public class NovaNetworkServices extends AbstractNetworkServices {
    private NovaOpenStack provider;

    public NovaNetworkServices(@Nonnull NovaOpenStack cloud) {
        provider = cloud;
    }

    @Override
    public @Nullable DNSSupport getDnsSupport() {
        if( provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
            return new RackspaceCloudDNS(provider);
        }
        return null;
    }

    @Override
    public @Nullable NovaSecurityGroup getFirewallSupport() {
        if( provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
            return null;
        }
        return new NovaSecurityGroup(provider);
    }

    @Override
    public @Nullable NovaFloatingIP getIpAddressSupport() {
        if( provider.getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
            return null;
        }
        return new NovaFloatingIP(provider);
    }

    @Override
    public @Nullable LoadBalancerSupport getLoadBalancerSupport() {
        return new RackspaceLoadBalancers(provider);

    }

    @Override
    public @Nullable Quantum getVlanSupport() {
        return new Quantum(provider);
    }
}
