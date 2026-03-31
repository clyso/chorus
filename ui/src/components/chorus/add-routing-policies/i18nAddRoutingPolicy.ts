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
    breadcrumbRoutingPolicies: 'Routing Policies',
    breadcrumbAddRoutingPolicy: 'Add Routing Policy',
    addRoutingPolicyTitle: 'Add Routing Policy',
    addRoutingPolicyHeader:
      'Create a new routing policy to control traffic direction within the proxy.',
    userSelectionTitle: 'User',
    userSelectionDescription: 'Select a user:',
    bucketSelectionTitle: 'Bucket Name',
    bucketSelectionDescription: 'Narrow down a policy to a specific bucket:',
    bucketSelectionAll: 'Apply to all buckets (including future ones)',
    bucketSelectionPlaceholder: 'Enter bucket name',
    bucketSelectionInputLabel: 'Bucket Name',
    bucketNameRequired: 'Please enter a bucket name.',
    bucketNameInvalid:
      'Invalid bucket name. Use 3-63 lowercase letters, numbers, hyphens, or dots.',
    bucketErrLength: 'Bucket name must be between 3 and 63 characters long.',
    bucketErrChars:
      'Bucket name can only contain lowercase letters, numbers, dots, and hyphens.',
    bucketErrStartEnd:
      'Bucket name must begin and end with a letter or number.',
    bucketErrAdjacentPeriods:
      'Bucket name must not contain two adjacent periods.',
    bucketErrIpAddress: 'Bucket name must not be formatted as an IP address.',
    bucketErrPrefixSuffix:
      'Bucket name contains a restricted AWS prefix or suffix.',
    storageSelectionTitle: 'Storage',
    storageSelectionDescription: 'Select a destination storage:',
    storageSelectionRequired: 'Please select a destination storage.',
    storageSelectionBlockedOnlyHint:
      'No storage selected: a blocked-only policy without routing will be created.',
    unknownValidationError: 'Unknown validation error',
    statusSelectionTitle: 'Blocked Status',
    statusSelectionDescription:
      'Determine whether this routing policy should be actively allowed or explicitly blocked.',
    statusSelectionAllowed: 'Allowed',
    statusSelectionBlocked: 'Blocked',
    actionAddRoutingPolicy: 'Add Routing Policy',
    userSelectionRequired: 'Please select a user.',
    addRoutingPolicyConfirmTitle: 'Confirm Routing Policy Creation',
    confirmRoutingPolicyDescription:
      'You are about to create the following routing policy:',
    confirmRoutingPolicyPositive: 'Create Routing Policy',
    confirmRoutingPolicyNegative: 'Cancel',
    summaryColumnUser: 'User',
    summaryColumnStorage: 'Storage',
    summaryNoStoragePlaceholder: 'No storage selected',
    summaryColumnBucket: 'Bucket',
    summaryColumnStatus: 'Blocked Status',
    summaryStatusBlocked: 'Blocked',
    summaryStatusAllowed: 'Allowed',
    addRoutingPolicySuccessTitle: 'Routing Policy created!',
    addRoutingPolicySuccessContent:
      'Routing Policy has been created successfully.',
    addRoutingPolicyErrorTitle: 'Routing Policy not created!',
    addRoutingPolicyErrorContent:
      'An error occurred while creating the routing policy.',
    addErrorAction: 'Retry',
    addRoutingPolicyErrorUnknown:
      'An unknown error occurred while creating the routing policy.',
    addBlockErrorUnknown:
      'An unknown error occurred while creating a blocked policy',
    addRoutingPolicyBlockErrorUnknown:
      'An unknown error occurred while blocking the newly created routing policy. The creation of the routing policy has been rolled back. You may want to try creating the routing policy first and blocking it in a separate step.',
  },
  [I18nLocale.DE]: {
    breadcrumbRoutingPolicies: 'Routing-Richtlinien',
    breadcrumbAddRoutingPolicy: 'Routing-Richtlinie Hinzufügen',
    addRoutingPolicyTitle: 'Routing-Richtlinie Hinzufügen',
    addRoutingPolicyHeader:
      'Neue Routing-Richtlinie erstellen, um den Datenverkehr im Proxy zu steuern.',
    userSelectionTitle: 'Benutzer',
    userSelectionDescription: 'Benutzer für die Routing-Richtlinie auswählen:',
    bucketSelectionTitle: 'Bucket Name',
    bucketSelectionDescription:
      'Richtlinie auf einen speziellen Bucket anwenden:',
    bucketSelectionAll:
      'Auf alle Buckets anwenden (einschließlich zukünftiger)',
    bucketSelectionPlaceholder: 'Bucket Namen eingeben',
    bucketSelectionInputLabel: 'Bucket-Name',
    bucketNameRequired: 'Bitte geben Sie einen Bucket-Namen ein.',
    bucketNameInvalid:
      'Ungültiger Bucket-Name. Erlaubt sind 3-63 Kleinbuchstaben, Zahlen, Bindestriche oder Punkte.',
    bucketErrLength:
      'Der Bucket-Name muss zwischen 3 und 63 Zeichen lang sein.',
    bucketErrChars:
      'Der Bucket-Name darf nur Kleinbuchstaben, Zahlen, Punkte und Bindestriche enthalten.',
    bucketErrStartEnd:
      'Der Bucket-Name muss mit einem Buchstaben oder einer Zahl beginnen und enden.',
    bucketErrAdjacentPeriods:
      'Der Bucket-Name darf keine zwei aufeinanderfolgenden Punkte enthalten.',
    bucketErrIpAddress:
      'Der Bucket-Name darf nicht als IP-Adresse formatiert sein.',
    bucketErrPrefixSuffix:
      'Der Bucket-Name enthält ein unzulässiges AWS-Präfix oder -Suffix.',
    storageSelectionTitle: 'Speicher',
    storageSelectionDescription: 'Wählen Sie einen Zielspeicher:',
    storageSelectionRequired: 'Bitte wählen Sie einen Zielspeicher aus.',
    storageSelectionBlockedOnlyHint:
      'Kein Speicher ausgewählt: es wird nur eine blockierte Richtlinie erstellt.',
    unknownValidationError: 'Unbekannter Fehler beim Validieren der Eingabe',
    statusSelectionTitle: 'Blockiert-Status',
    statusSelectionDescription:
      'Legen Sie fest, ob diese Routing-Policy zugelassen oder explizit blockiert werden soll.',
    statusSelectionAllowed: 'Zugelassen',
    statusSelectionBlocked: 'Blockiert',
    actionAddRoutingPolicy: 'Routing-Richtlinie hinzufügen',
    userSelectionRequired: 'Bitte wählen Sie einen Benutzer aus.',
    addRoutingPolicyConfirmTitle: 'Anlegen der Routing-Richtlinie bestätigen',
    confirmRoutingPolicyDescription:
      'Sie erstellen folgende Routing-Richtlinie:',
    confirmRoutingPolicyPositive: 'Routing-Richtlinie erstellen',
    confirmRoutingPolicyNegative: 'Abbrechen',
    summaryColumnUser: 'Benutzer',
    summaryColumnStorage: 'Speicher',
    summaryNoStoragePlaceholder: 'Kein Speicher gewählt',
    summaryColumnBucket: 'Bucket',
    summaryColumnStatus: 'Blockiert-Status',
    summaryStatusBlocked: 'Blockiert',
    summaryStatusAllowed: 'Zugelassen',
    addRoutingPolicySuccessTitle: 'Routing-Richtlinie erstellt!',
    addRoutingPolicySuccessContent:
      'Routing-Richtlinie wurde erfoldgreich erstellt.',
    addRoutingPolicyErrorTitle: 'Routing-Richtlinie nicht erstellt!',
    addRoutingPolicyErrorContent:
      'Beim Erstellen der Routing-Richtlinie ist ein Fehler aufgetreten.',
    addErrorAction: 'Erneut versuchen',
    addRoutingPolicyErrorUnknown:
      'Beim Erstellen der Routing-Richtlinie ist ein unbekannter Fehler aufgetreten.',
    addBlockErrorUnknown:
      'Beim Erstellen einer Blockierung ist ein unbekannter Fehler aufgetreten.',
    addRoutingPolicyBlockErrorUnknown:
      'Beim Blockieren der neu erstellten Routing-Richtlinie ist ein unbekannter Fehler aufgetreten. Die Erstellung der Routing-Richtlinie wurde rückgängig gemacht. Sie können versuchen, die Routing-Richtlinie zunächst nur zu erstellen und sie in einem zweiten Schritt zu blockieren.',
  },
};
