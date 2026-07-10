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
    // From Storage section
    fromStorageTitle: 'Source Storage',
    fromStorageDescription:
      'Select the source storage location where the original data resides.',
    fromStorageRequired: 'Please select a source storage.',
    // Bucket section
    fromBucketTitle: 'Source Bucket',
    fromBucketDescription:
      'Enter the target bucket from the source storage to include in the consistency check.',
    bucketPlaceholder: 'e.g. my-bucket',
    bucketRequired: 'Please enter a bucket name.',
    bucketErrLength: 'Bucket name must be between 3 and 63 characters.',
    bucketErrChars:
      'Bucket name may only contain lowercase letters, numbers, dots, and hyphens.',
    bucketErrStartEnd:
      'Bucket name must start and end with a letter or number.',
    bucketErrAdjacentPeriods: 'Bucket name must not contain adjacent periods.',
    bucketErrIpAddress: 'Bucket name must not be formatted as an IP address.',
    bucketErrPrefixSuffix:
      'Bucket name contains a restricted AWS prefix or suffix.',
    // To Storage section
    toStorageTitle: 'Destination Storage',
    toStorageDescription:
      'Select the destination storage location to compare against the source.',
    toStorageRequired: 'Please select a destination storage.',
    // To Bucket section
    toBucketTitle: 'Destination Bucket',
    toBucketDescription:
      'Enter the bucket from the destination storage to compare against the source bucket.',
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
    fromStorageTitle: 'Quell-Storage',
    fromStorageDescription:
      'Wählen Sie den Quell-Storage, in dem sich die Originaldaten befinden.',
    fromStorageRequired: 'Bitte wählen Sie einen Quell-Storage.',
    fromBucketTitle: 'Quell-Bucket',
    fromBucketDescription:
      'Geben Sie den Ziel-Bucket des Quell-Storages für die Konsistenzprüfung ein.',
    bucketPlaceholder: 'z.B. my-bucket',
    bucketRequired: 'Bitte geben Sie einen Bucket-Namen ein.',
    bucketErrLength:
      'Der Bucket-Name muss zwischen 3 und 63 Zeichen lang sein.',
    bucketErrChars:
      'Der Bucket-Name darf nur Kleinbuchstaben, Zahlen, Punkte und Bindestriche enthalten.',
    bucketErrStartEnd:
      'Der Bucket-Name muss mit einem Buchstaben oder einer Zahl beginnen und enden.',
    bucketErrAdjacentPeriods:
      'Der Bucket-Name darf keine aufeinanderfolgenden Punkte enthalten.',
    bucketErrIpAddress:
      'Der Bucket-Name darf nicht als IP-Adresse formatiert sein.',
    bucketErrPrefixSuffix:
      'Der Bucket-Name enthält ein unzulässiges AWS-Präfix oder -Suffix.',
    toStorageTitle: 'Ziel-Storage',
    toStorageDescription:
      'Wählen Sie den Ziel-Storage, mit dem die Quelle verglichen werden soll.',
    toStorageRequired: 'Bitte wählen Sie einen Ziel-Storage.',
    toBucketTitle: 'Ziel-Bucket',
    toBucketDescription:
      'Geben Sie den Bucket des Ziel-Storages ein, der mit dem Quell-Bucket verglichen werden soll.',
  },
};
