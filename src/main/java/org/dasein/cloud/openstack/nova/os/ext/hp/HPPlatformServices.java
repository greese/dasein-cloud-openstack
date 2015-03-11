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

package org.dasein.cloud.openstack.nova.os.ext.hp;

import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.openstack.nova.os.ext.hp.cdn.HPCDN;
import org.dasein.cloud.openstack.nova.os.ext.hp.db.HPRDBMS;
import org.dasein.cloud.platform.AbstractPlatformServices;
import org.dasein.cloud.platform.CDNSupport;

/**
 * Implements support for platform services specific to the HP Cloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 */
public class HPPlatformServices extends AbstractPlatformServices {
    private NovaOpenStack provider;
    
    public HPPlatformServices(NovaOpenStack provider) {
        this.provider = provider;
    }

    @Override
    public CDNSupport getCDNSupport() {
        return new HPCDN(provider);
    }

    @Override
    public HPRDBMS getRelationalDatabaseSupport() {
        return new HPRDBMS(provider);
    }
}
