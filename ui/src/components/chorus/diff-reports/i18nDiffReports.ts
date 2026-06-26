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
    diffReportTitle: 'Diff Reports',
    columnBucket: 'Buckets Pair',
    columnDirection: 'Direction',
    columnProgress: 'Progress',
    columnStatus: 'Status',
    columnConfigs: 'Configs',
    columnVersioned: 'Versioned',
    columnActions: 'Actions',
    columnEtags: 'ETags',
    columnSizes: 'Sizes',
    statusChecking: 'Checking',
    statusConsistent: 'Consistent',
    statusInconsistent: 'Inconsistent',
    configVersioned: 'Versioned',
    configNotVersioned: 'Not versioned',
    configIgnoresEtags: 'Ignores ETags',
    configNotIgnoresEtags: 'Considers ETags',
    configIgnoresSizes: 'Ignores Sizes',
    configNotIgnoresSizes: 'Considers Sizes',
    diffReportFrom: 'the source of comparison',
    diffReportTo: 'comparison destination',
    diffReportStatusDetails: 'Diff Report Status Details',
    cliCheckWarning:
      'This check was created via CLI. For further details or actions, please use the CLI.',
    filterByDirectionPlaceholder: 'Filter by direction',
    filterByBucketPlaceholder: 'Filter by bucket',
    filterByStatusPlaceholder: 'Filter by status',
    filterStatusChecking: 'Checking',
    filterStatusConsistent: 'Consistent',
    filterStatusInconsistent: 'Inconsistent',
    noResultsTitle: 'No Diff Reports yet',
    noResultsText: 'There are no diff reports available at the moment.',
    errorTitle: 'Error',
    errorText:
      'An error occurred while getting the diff report list.\nPlease try one more time.',
    errorAction: 'Retry',
  },
  [I18nLocale.DE]: {
    diffReportTitle: 'Diff-Reports',
    columnBucket: 'Bucket Paare',
    columnDirection: 'Richtung',
    columnProgress: 'Fortschritt',
    columnStatus: 'Status',
    columnConfigs: 'Konfigurationen',
    columnVersioned: 'Versioniert',
    columnEtags: 'ETags',
    columnSizes: 'Dateigrößen',
    columnActions: 'Aktionen',
    statusChecking: 'Prüfung läuft',
    statusConsistent: 'Konsistent',
    statusInconsistent: 'Inkonsistent',
    configVersioned: 'Versioniert',
    configNotVersioned: 'Nicht versioniert',
    configIgnoresEtags: 'Ignoriert ETags',
    configNotIgnoresEtags: 'Ignoriert ETags nicht',
    configIgnoresSizes: 'Ignoriert Dateigrößen',
    configNotIgnoresSizes: 'Ignoriert Dateigrößen nicht',
    diffReportFrom: 'Quelle des Vergleichs',
    diffReportTo: 'Vergleichsziel',
    diffReportStatusDetails: 'Diff-Report Status Details',
    cliCheckWarning:
      'Dieser Check wurde über die CLI erstellt. Für weitere Details oder Aktionen verwenden Sie bitte die CLI.',
    filterByDirectionPlaceholder: 'Nach Richtung filtern',
    filterByBucketPlaceholder: 'Nach Bucket filtern',
    filterByStatusPlaceholder: 'Nach Status filtern',
    filterStatusChecking: 'Prüfung läuft',
    filterStatusConsistent: 'Konsistent',
    filterStatusInconsistent: 'Inkonsistent',
    noResultsTitle: 'Noch keine Diff-Reports',
    noResultsText: 'Derzeit sind keine Diff-Reports verfügbar.',
    errorTitle: 'Fehler',
    errorText:
      'Beim Abrufen der Diff-Report-Liste ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    errorAction: 'Erneut versuchen',
  },
};
