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
    // Breadcrumbs
    breadcrumbDiffReports: 'Diff Reports',
    breadcrumbCreateDiffReport: 'Add Diff Report',
    // Page / tile
    addDiffReportTitle: 'Add Diff Report',
    addDiffReportDescription:
      'Run an on-demand verification check between two storage locations. Select a source and target storage/bucket pair with optional validation criteria.',
    // Wizard
    step1Title: 'Diff Report Source',
    step1Description: 'Select source storage and bucket',
    step2Title: 'Diff Report Destination',
    step2Description: 'Select storage and bucket for comparison',
    step3Title: 'User',
    step3Description: 'Select user',
    step4Title: 'Additional Settings',
    step4Description: 'Select additional settings',
    nextAction: 'Next',
    // Submit button
    actionAddDiffReport: 'Add Diff Report',
    // Error state
    errorTitle: 'Error',
    errorText: 'An error occurred while loading storages.\nPlease try again.',
    errorAction: 'Retry',
    // Not enough storages
    notEnoughStoragesTitle: 'Not Enough Storages',
    notEnoughStoragesText:
      'At least two storages are required to create a diff report.',
  },
  [I18nLocale.DE]: {
    breadcrumbDiffReports: 'Diff-Reports',
    breadcrumbCreateDiffReport: 'Diff-Report hinzufügen',
    addDiffReportTitle: 'Diff-Report hinzufügen',
    addDiffReportDescription:
      'Führen Sie eine On-Demand-Prüfung zwischen zwei Storage-Standorten durch. Wählen Sie ein Quell- und Ziel-Storage-/Bucket-Paar mit optionalen Validierungskriterien.',
    step1Title: 'Diff-Report Quelle',
    step1Description: 'Quell-Storage und -Bucket auswählen',
    step2Title: 'Diff-Report Ziel',
    step2Description: 'Storage und Bucket zum Vergleich auswählen',
    step3Title: 'Benutzer',
    step3Description: 'Benutzer auswählen',
    step4Title: 'Erweiterte Einstellungen',
    step4Description: 'Erweiterte Einstellungen auswählen',
    nextAction: 'Weiter',
    actionAddDiffReport: 'Diff-Report hinzufügen',
    errorTitle: 'Fehler',
    errorText:
      'Beim Laden der Storages ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    errorAction: 'Erneut versuchen',
    notEnoughStoragesTitle: 'Nicht genug Storages',
    notEnoughStoragesText:
      'Für einen Diff-Report werden mindestens zwei Storages benötigt.',
  },
};
