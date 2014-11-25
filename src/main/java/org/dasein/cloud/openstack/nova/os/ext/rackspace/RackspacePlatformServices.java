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

package org.dasein.cloud.openstack.nova.os.ext.rackspace;

import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.ext.rackspace.cdn.RackspaceCDN;
import org.dasein.cloud.openstack.nova.os.ext.rackspace.db.RackspaceRDBMS;
import org.dasein.cloud.platform.AbstractPlatformServices;
import org.dasein.cloud.platform.RelationalDatabaseSupport;

/**
 * Implements support for platform services specific to the Rackspace Cloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 */
public class RackspacePlatformServices extends AbstractPlatformServices {
    private NovaOpenStack provider;

    public RackspacePlatformServices(NovaOpenStack provider) {
        this.provider = provider;
    }

    @Override
    public RackspaceCDN getCDNSupport() {
        return new RackspaceCDN(provider);
    }

    @Override
    public RelationalDatabaseSupport getRelationalDatabaseSupport() {
        return new RackspaceRDBMS(provider);
    }
}
