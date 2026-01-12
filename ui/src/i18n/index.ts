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

import { createI18n } from 'vue-i18n';
import { I18N_DEFAULT_LOCALE } from '@clyso/clyso-ui-kit';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
import formErrors from '@/i18n/messages/formErrors';
import common from '@/i18n/messages/common';
import routes from '@/i18n/messages/routes';

const messages = GeneralHelper.merge(common, formErrors, routes);

export const i18n = createI18n({
  legacy: false,
  locale: I18N_DEFAULT_LOCALE,
  fallbackLocale: I18N_DEFAULT_LOCALE,
  messages,
});
