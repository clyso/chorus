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
    addReplicationTitle: 'Add Replication',
    step1Title: 'Source Storage',
    step1Description: 'Select storage to replicate from',
    step2Title: 'Destination Storage',
    step2Description: 'Select storage to replicate to',
    step3Title: 'User',
    step3Description: 'Select user',
    step4Title: 'Buckets',
    step4Description: 'Select buckets for replication',
    nextAction: 'Next',
    addReplicationAction: 'Add Replication',
    fromStorageStepTitle: 'Select Source storage for replication:',
    alertTitle: 'Please notice!',
    alertDescription:
      'Currently only Main storage can be selected as Source storage.',
    toStorageStepTitle: 'Select Destination storage for replication:',
    addReplicationError:
      'An error occurred while preparing the Add Replication form. Please try again.',
    userStepTitle: 'Select user:',
    bucketsStepTitle: 'Select buckets for replication:',
    bucketsSelectOption: 'Replicate selected buckets',
    bucketsAllOption: 'Replicate all buckets (including future ones)',
    columnName: 'Bucket',
    columnStatus: 'Status',
    searchBuckets: 'Search buckets',
    showReplicatedBuckets: 'Also show replicated buckets',
    selectedBuckets: '{total} buckets selected',
    statusReplicated: 'Replicated',
    statusAvailable: 'Available for replication',
    initBucketsError:
      'An error occurred while processing the list of buckets. Please try again.',
    noResultsTitle: 'No Buckets found',
    noResultsText:
      'There is no buckets available for replication on {storage} storage for {user} user.',
    filterNoResultsTitle: 'No Buckets found',
    filterNoResultsText: 'There is no buckets matching your search query.',
    filterNoResultsAction: 'Reset search',
    validationErrorTitle: 'Validation Error',
    validationErrorDescription:
      'Please select at least one bucket to create a replication.',
    confirmReplicationTitle: 'Confirm Replication Creation',
    confirmReplicationDescription:
      'You are about to create the following Replication:',
    confirmReplicationPositive: 'Create Replication',
    confirmReplicationNegative: 'Cancel',
    summaryAllBuckets: 'All buckets (including future ones)',
    createReplicationSuccess:
      'Replication(s) were successfully created. You should be able to see them at the top of the list.',
    createReplicationError:
      'An error occurred when creating the replication(s). Please try one more time.',
  },
  [I18nLocale.DE]: {
    addReplicationTitle: 'Replikation hinzufügen',
    step1Title: 'Quellspeicher',
    step1Description:
      'Wählen Sie den Speicher aus, von dem repliziert werden soll',
    step2Title: 'Zielspeicher',
    step2Description:
      'Wählen Sie den Speicher aus, in den repliziert werden soll',
    step3Title: 'Benutzer',
    step3Description: 'Wählen Sie einen Benutzer',
    step4Title: 'Buckets',
    step4Description: 'Wählen Sie Buckets für die Replikation aus',
    nextAction: 'Weiter',
    addReplicationAction: 'Replikation hinzufügen',
    fromStorageStepTitle: 'Quellspeicher für die Replikation auswählen:',
    alertTitle: 'Bitte beachten!',
    alertDescription:
      'Derzeit kann nur der Hauptspeicher als Quellspeicher ausgewählt werden.',
    toStorageStepTitle: 'Zielspeicher für die Replikation auswählen:',
    addReplicationError:
      'Beim Vorbereiten des Replikationsformulars ist ein Fehler aufgetreten. Bitte versuchen Sie es erneut.',
    userStepTitle: 'Benutzer auswählen:',
    bucketsStepTitle: 'Buckets für die Replikation auswählen:',
    bucketsSelectOption: 'Ausgewählte Buckets replizieren',
    bucketsAllOption: 'Alle Buckets replizieren (einschließlich zukünftiger)',
    columnName: 'Bucket',
    columnStatus: 'Status',
    searchBuckets: 'Buckets suchen',
    showReplicatedBuckets: 'Auch replizierte Buckets anzeigen',
    selectedBuckets: '{total} Buckets ausgewählt',
    statusReplicated: 'Repliziert',
    statusAvailable: 'Für Replikation verfügbar',
    initBucketsError:
      'Beim Verarbeiten der Bucket-Liste ist ein Fehler aufgetreten. Bitte versuchen Sie es erneut.',
    noResultsTitle: 'Keine Buckets gefunden',
    noResultsText:
      'Es sind keine Buckets zur Replikation im Speicher {storage} für den Benutzer {user} verfügbar.',
    filterNoResultsTitle: 'Keine Buckets gefunden',
    filterNoResultsText:
      'Es gibt keine Buckets, die Ihrer Suchanfrage entsprechen.',
    filterNoResultsAction: 'Suche zurücksetzen',
    validationErrorTitle: 'Validierungsfehler',
    validationErrorDescription:
      'Bitte wählen Sie mindestens einen Bucket aus, um eine Replikation zu erstellen.',
    confirmReplicationTitle: 'Replikationserstellung bestätigen',
    confirmReplicationDescription:
      'Sie sind dabei, die folgende Replikation zu erstellen:',
    confirmReplicationPositive: 'Replikation erstellen',
    confirmReplicationNegative: 'Abbrechen',
    summaryAllBuckets: 'Alle Buckets (einschließlich zukünftiger)',
    createReplicationSuccess:
      'Die Replikation(en) wurden erfolgreich erstellt. Sie sollten sie oben in der Liste sehen können.',
    createReplicationError:
      'Beim Erstellen der Replikation(en) ist ein Fehler aufgetreten. Bitte versuchen Sie es erneut.',
  },
};
