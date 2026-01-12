/*
 * Copyright © 2026 Clyso GmbH
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

import type { NavigationHookAfter } from 'vue-router';
import { ROUTE_TITLE_PROJECT_PREFIX } from '@/utils/constants/router';
import { i18n } from '@/i18n';

const { t } = i18n.global;

const metaHook: NavigationHookAfter = (to) => {
  document.title = `${ROUTE_TITLE_PROJECT_PREFIX} | ${t(String(to.name))}`;
};

export default metaHook;
