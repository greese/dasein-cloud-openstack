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

package org.dasein.cloud.openstack.nova.os.network;

import org.dasein.cloud.*;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.FirewallCapabilities;
import org.dasein.cloud.network.FirewallConstraints;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.RuleTargetType;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Openstack with respect to Dasein ip address operations.
 * <p>Created by Danielle Mayne: 3/04/14 12:05 PM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class SecurityGroupCapabilities extends AbstractCapabilities<NovaOpenStack> implements FirewallCapabilities {
    public SecurityGroupCapabilities(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public FirewallConstraints getFirewallConstraintsForCloud() throws InternalException, CloudException {
        return FirewallConstraints.getInstance();
    }

    @Nonnull
    @Override
    public String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "security group";
    }

    @Nullable
    @Override
    public VisibleScope getFirewallVisibleScope() {
        return VisibleScope.ACCOUNT_DATACENTER;
    }

    @Nonnull
    @Override
    public Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            return Collections.emptyList();
        }
        return Collections.singletonList(RuleTargetType.GLOBAL);
    }

    @Nonnull
    @Override
    public Iterable<Direction> listSupportedDirections(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            return Collections.emptyList();
        }
        return Collections.singletonList(Direction.INGRESS);
    }

    @Nonnull
    @Override
    public Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            return Collections.emptyList();
        }
        return Collections.singletonList(Permission.ALLOW);
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
        if( inVlan ) {
            return Collections.emptyList();
        }
        ArrayList<RuleTargetType> list= new ArrayList<RuleTargetType>();

        list.add(RuleTargetType.CIDR);
        list.add(RuleTargetType.GLOBAL);
        return list;
    }

    @Override
    public boolean requiresRulesOnCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public Requirement requiresVLAN() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        return (!inVlan && Direction.INGRESS.equals(direction) && Permission.ALLOW.equals(permission));
    }

    @Override
    public boolean supportsFirewallCreation(boolean inVlan) throws CloudException, InternalException {
        return !inVlan;
    }

    @Override
    public boolean supportsFirewallDeletion() throws CloudException, InternalException {
        return true;
    }
}
