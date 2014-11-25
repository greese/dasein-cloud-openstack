/**
 * Copyright (C) 2009-2014 Dell, Inc.
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

package org.dasein.cloud.openstack.nova.os.ext.hp.cdn;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.CDNCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 05/08/2014
 * Time: 11:31
 */
public class HPCDNCapabilities extends AbstractCapabilities<NovaOpenStack> implements CDNCapabilities {
    public HPCDNCapabilities(@Nonnull NovaOpenStack cloud) { super(cloud); }

    @Override
    public boolean canCreateCDN() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean canDeleteCDN() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean canModifyCDN() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public String getProviderTermForDistribution(@Nonnull Locale locale) {
        return "container";
    }
}
