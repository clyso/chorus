/*
 * Copyright © 2025 Clyso GmbH
 *
 *  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://www.gnu.org/licenses/agpl-3.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { type I18nMessages, I18nLocale } from '@clyso/clyso-ui-kit';
import { RouteName } from '@/utils/types/router';

type Routes = {
  [k in RouteName]: string;
};

const routes: I18nMessages<Routes> = {
  [I18nLocale.EN]: {
    [RouteName.LOGIN]: 'Login',
    [RouteName.CHORUS_HOME]: 'Home',
    [RouteName.CHORUS_STORAGES]: 'Storages',
    [RouteName.CHORUS_STORAGE_DETAILS]: 'Storage Details',
    [RouteName.CHORUS_REPLICATION]: 'Replication',
    [RouteName.CHORUS_ADD_REPLICATION]: 'Add Replication',
    [RouteName.CHORUS_MONITORING]: 'Monitoring',
  },
  [I18nLocale.DE]: {
    [RouteName.LOGIN]: 'Login',
    [RouteName.CHORUS_HOME]: 'Startseite',
    [RouteName.CHORUS_STORAGES]: 'Speicher',
    [RouteName.CHORUS_STORAGE_DETAILS]: 'Speicherdetails',
    [RouteName.CHORUS_REPLICATION]: 'Replikation',
    [RouteName.CHORUS_ADD_REPLICATION]: 'Replikation hinzufügen',
    [RouteName.CHORUS_MONITORING]: 'Monitoring',
  },
};

export default routes;
