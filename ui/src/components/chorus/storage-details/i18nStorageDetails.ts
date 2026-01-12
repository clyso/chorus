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

import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    storageDetailsErrorTitle: 'Storage Details Error',
    storageDetailsErrorText:
      'An error occurred while retrieving storage details.\nPlease try again.',
    storageDetailsErrorAction: 'Retry',
    storagesBreadcrumb: 'Storages',
    storageDetailsBreadcrumb: 'Storage Details',
  },
  [I18nLocale.DE]: {
    storageDetailsErrorTitle: 'Fehler bei den Speicherdetails',
    storageDetailsErrorText:
      'Beim Abrufen der Speicherdetails ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    storageDetailsErrorAction: 'Erneut versuchen',
    storagesBreadcrumb: 'Speicher',
    storageDetailsBreadcrumb: 'Speicherdetails',
  },
};
