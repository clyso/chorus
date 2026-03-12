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
    unknownValidationError: 'Unknown validation error',
    statusSelectionTitle: 'Blocked Status',
    statusSelectionDescription:
      'Determine whether this routing policy should be actively allowed or explicitly blocked.',
    statusSelectionAllowed: 'Allowed',
    statusSelectionBlocked: 'Blocked',
  },
  [I18nLocale.DE]: {
    addRoutingPolicyTitle: 'Routing-Richtlinie Hinzufügen',
    addRoutingPolicyHeader:
      'Neue Routing-Richtlinie erstellen, um den Datenverkehr im Proxy zu steuern.',
    userSelectionTitle: 'Benutzer',
    userSelectionDescription: 'Benutzer für die Routing-Richtlinie auswählen:',
    bucketSelectionTitle: 'Bucket Name',
    bucketSelectionDescription:
      'Richtlinie auf einen speziellen Bucket anwenden:',
    bucketSelectionAll: 'Apply to all buckets',
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
    unknownValidationError: 'Unbekannter Fehler beim Validieren der Eingabe',
    statusSelectionTitle: 'Blockiert-Status',
    statusSelectionDescription:
      'Legen Sie fest, ob diese Routing-Policy zugelassen oder explizit blockiert werden soll.',
    statusSelectionAllowed: 'Zugelassen',
    statusSelectionBlocked: 'Blockiert',
  },
};
