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
    breadcrumbDiffReports: 'Diff Reports',
    errorTitle: 'Error',
    errorText:
      'An error occurred while loading the diff report.\nPlease try again.',
    errorAction: 'Retry',
    notFoundTitle: 'Report Not Found',
    notFoundText: 'The requested diff report could not be found.',
    invalidLocationsTitle: 'Invalid Locations',
    invalidLocationsText: 'Diff reports require exactly two locations.',
    invalidLocationsAction: 'Back to Diff Reports',
    notFoundAction: 'Back to Diff Reports',
  },
  [I18nLocale.DE]: {
    breadcrumbDiffReports: 'Diff-Reports',
    errorTitle: 'Fehler',
    errorText:
      'Beim Laden des Diff-Reports ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    errorAction: 'Erneut versuchen',
    notFoundTitle: 'Report nicht gefunden',
    notFoundText: 'Der angeforderte Diff-Report wurde nicht gefunden.',
    invalidLocationsTitle: 'Ungültige Speicherorte',
    invalidLocationsText: 'Diff-Reports erfordern genau zwei Speicherorte.',
    invalidLocationsAction: 'Zurück zu Diff-Reports',
    notFoundAction: 'Zurück zu Diff-Reports',
  },
};
