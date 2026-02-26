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
    replicationsTitle: 'Replication',
    noResultsTitle: 'No Replications yet',
    noResultsText:
      'There is no replications available at the moment.\nTo create a new replication please click the button below.',
    addReplicationAction: 'Add Replication',
    filterNoResultsTitle: 'No Replications found',
    filterNoResultsText:
      'There is no replication matching your filter criteria.',
    filterNoResultsAction: 'Clear Filters',
    errorTitle: 'Error',
    errorText:
      'An error occurred while getting the replication list.\nPlease try one more time.',
    errorAction: 'Retry',
    cancel: 'Cancel',
    replicationFrom: 'the source of replication',
    replicationTo: 'replication destination',
    columnUser: 'User',
    columnBucket: 'Bucket',
    columnCreatedAt: 'Created At',
    columnDirection: 'Direction',
    columnStatus: 'Status',
    columnActions: 'Actions',
    statusPaused: 'Paused',
    statusActive: 'Active',
    initialReplication: 'Initial Replication',
    statusDone: 'Done',
    replicationDetails: 'Replication Details',
    labelObjects: 'Objects',
    labelBytes: 'Bytes',
    liveReplication: 'Live Replication',
    statusBehind: 'events behind',
    statusUptodate: 'Up to date',
    labelEvents: 'Events',
    labelReplicationLatency: 'Replication Latency',
    labelLastProcessed: 'Last Processed At',
    actionViewDetails: 'View Details',
    actionResume: 'Resume',
    actionPause: 'Pause',
    actionDelete: 'Delete',
    actionDeleteUserReplication: 'Delete User Replication',
    pauseSuccessTitle: 'Paused!',
    pauseSuccessContent: 'Replication for the following bucket was paused:',
    pauseErrorTitle: 'Pause failed!',
    pauseErrorContent:
      'An error occurred when pausing replication for the following bucket:',
    pauseErrorAction: 'Retry',
    resumeSuccessTitle: 'Resumed!',
    resumeSuccessContent: 'Replication for the following bucket was resumed:',
    resumeErrorTitle: 'Resume failed!',
    resumeErrorContent:
      'An error occurred when resuming replication for the following bucket:',
    resumeErrorAction: 'Retry',
    deleteSuccessTitle: 'Deleted!',
    deleteSuccessContent: 'Replication for the following bucket was deleted:',
    deleteErrorTitle: 'Deletion failed!',
    deleteErrorContent:
      'An error occurred when deleting replication for the following bucket:',
    deleteErrorAction: 'Retry',
    deleteUserSuccessTitle: 'Deleted!',
    deleteUserSuccessContent: 'User Replication rule was deleted for: ',
    deleteUserSuccessContentBuckets:
      '{total} Bucket Replication rules associated with the User rule were also deleted: ',
    deleteUserErrorTitle: 'Deletion failed!',
    deleteUserErrorContent:
      'An error occurred when deleting User Replication rule for: ',
    deleteUserErrorAction: 'Retry',
    bucketDeletionConfirmTitle: 'Bucket Replication Deletion',
    bucketDeletionConfirmContent:
      'You are about to delete the following replication:',
    bucketDeletionConfirmQuestion: 'Are you sure you want to continue?',
    bucketDeletionConfirmAction: 'Confirm Deletion',
    bucketDeletionConfirmCancel: 'Cancel',
    userDeletionConfirmTitle: 'User Replication Deletion',
    userDeletionConfirmContent:
      'You are about to delete User Replication rule for:',
    userDeletionConfirmContent2:
      ' New buckets will not be automatically replicated after that.',
    userDeletionConfirmQuestion: 'Are you sure you want to continue?',
    userDeletionBucketsConfirmContent:
      'Also remove {total} Bucket Replication rules associated with this user',
    userDeletionConfirmAction: 'Confirm Deletion',
    userDeletionConfirmCancel: 'Cancel',
    actionResumeSelected: 'Resume {total} selected replications',
    actionPauseSelected: 'Pause {total} selected replications',
    actionDeleteSelected: 'Delete {total} selected replications',
    tagSelectedReplications: '{total} replications selected',
    tagClearFilters: 'Filters applied',
    actionSelectedResumeTitle: 'Resume selected',
    actionSelectedResumeContent:
      'You are about to resume the following {total} replications:',
    actionSelectedResumeQuestion: 'Are you sure you want to continue?',
    actionSelectedPauseTitle: 'Pause selected',
    actionSelectedPauseContent:
      'You are about to pause the following {total} replications:',
    actionSelectedPauseQuestion: 'Are you sure you want to continue?',
    actionSelectedDeleteTitle: 'Delete selected',
    actionSelectedDeleteContent:
      'You are about to delete the following {total} replications:',
    actionSelectedDeleteQuestion: 'Are you sure you want to continue?',
    resumeSelectedSuccessContent:
      'The following {total} replications were resumed:',
    resumeSelectedErrorContent:
      'And error occurred while resuming the following {total} replications:',
    pauseSelectedSuccessContent:
      'The following {total} replications were paused:',
    pauseSelectedErrorContent:
      'And error occurred while pausing the following {total} replications:',
    deleteSelectedSuccessContent:
      'The following {total} replications were deleted:',
    deleteSelectedErrorContent:
      'And error occurred while deleting the following {total} replications:',
    filterByUserPlaceholder: 'Filter by User',
    filterByUserNoResult: 'No user matches your search criteria.',
    filterByBucketPlaceholder: 'Search by Bucket',
    filterByToPlaceholder: 'Filter by Direction (To)',
    filterByToNoResult: 'No storage matches your search criteria.',
    filterByStatusPlaceholder: 'Filter by Status',
    filterStatusPaused: 'Paused',
    filterStatusActive: 'Active',
    filterStatusInitialDone: 'Initial Replication: Done',
    filterStatusInitialInProgress: 'Initial Replication: In Progress',
    filterStatusLiveUpToDate: 'Live Replication: Up to date',
    filterStatusLiveBehind: 'Live Replication: Behind',
    filterByCreatedAtStartPlaceholder: 'Filter by',
    filterByCreatedAtEndPlaceholder: 'Created At',
    userReplication: 'User Replication',
    userReplicationDescription:
      'Replicates all current and future buckets for the user.',
    filterTypeBucket: 'Bucket replication',
    filterTypeUser: 'User replication',
    filterByTypePlaceholder: 'Filter by Type',
  },
  [I18nLocale.DE]: {
    replicationsTitle: 'Replikation',
    noResultsTitle: 'Noch keine Replikationen',
    noResultsText:
      'Derzeit sind keine Replikationen verfügbar.\nUm eine neue Replikation zu erstellen, klicken Sie bitte auf die Schaltfläche unten.',
    addReplicationAction: 'Replikation hinzufügen',
    filterNoResultsTitle: 'Keine Replikationen gefunden',
    filterNoResultsText:
      'Es gibt keine Replikationen, die Ihren Filterkriterien entsprechen.',
    filterNoResultsAction: 'Filter zurücksetzen',
    errorTitle: 'Fehler',
    errorText:
      'Beim Abrufen der Replikationsliste ist ein Fehler aufgetreten.\nBitte versuchen Sie es noch einmal.',
    errorAction: 'Erneut versuchen',
    cancel: 'Abbrechen',
    replicationFrom: 'die Quelle der Replikation',
    replicationTo: 'Replikationsziel',
    columnUser: 'Benutzer',
    columnBucket: 'Bucket',
    columnCreatedAt: 'Erstellt am',
    columnDirection: 'Richtung',
    columnStatus: 'Status',
    columnActions: 'Aktionen',
    statusPaused: 'Pausiert',
    statusActive: 'Aktiv',
    initialReplication: 'Erste Replikation',
    statusDone: 'Abgeschlossen',
    replicationDetails: 'Replikationsdetails',
    labelObjects: 'Objekte',
    labelBytes: 'Bytes',
    liveReplication: 'Live-Replikation',
    statusBehind: 'hinterher',
    statusUptodate: 'Aktuell',
    labelEvents: 'Ereignisse',
    labelReplicationLatency: 'Replikationslatenz',
    labelLastProcessed: 'Letzte Verarbeitung',
    actionViewDetails: 'Details ansehen',
    actionResume: 'Fortsetzen',
    actionPause: 'Pausieren',
    actionDelete: 'Löschen',
    actionDeleteUserReplication: 'Benutzer-Replikation löschen',
    pauseSuccessTitle: 'Pausiert!',
    pauseSuccessContent:
      'Die Replikation für den folgenden Bucket wurde pausiert:',
    pauseErrorTitle: 'Pausieren fehlgeschlagen!',
    pauseErrorContent:
      'Beim Pausieren der Replikation für den folgenden Bucket ist ein Fehler aufgetreten:',
    pauseErrorAction: 'Erneut versuchen',
    resumeSuccessTitle: 'Fortgesetzt!',
    resumeSuccessContent:
      'Die Replikation für den folgenden Bucket wurde fortgesetzt:',
    resumeErrorTitle: 'Fortsetzen fehlgeschlagen!',
    resumeErrorContent:
      'Beim Fortsetzen der Replikation für den folgenden Bucket ist ein Fehler aufgetreten:',
    resumeErrorAction: 'Erneut versuchen',
    deleteSuccessTitle: 'Gelöscht!',
    deleteSuccessContent:
      'Die Replikation für den folgenden Bucket wurde gelöscht:',
    deleteErrorTitle: 'Löschen fehlgeschlagen!',
    deleteErrorContent:
      'Beim Löschen der Replikation für den folgenden Bucket ist ein Fehler aufgetreten:',
    deleteErrorAction: 'Erneut versuchen',
    deleteUserSuccessTitle: 'Gelöscht!',
    deleteUserSuccessContent:
      'Die Benutzer-Replikationsregel wurde für gelöscht:',
    deleteUserSuccessContentBuckets:
      '{total} Bucket-Replikationsregeln, die mit der Benutzerregel verknüpft sind, wurden ebenfalls gelöscht:',
    deleteUserErrorTitle: 'Löschen fehlgeschlagen!',
    deleteUserErrorContent:
      'Beim Löschen der Benutzer-Replikationsregel für ist ein Fehler aufgetreten:',
    deleteUserErrorAction: 'Erneut versuchen',
    bucketDeletionConfirmTitle: 'Bestätigung der Bucket-Replikation',
    bucketDeletionConfirmContent:
      'Sie sind dabei, die folgende Replikation zu löschen:',
    bucketDeletionConfirmQuestion: 'Möchten Sie fortfahren?',
    bucketDeletionConfirmAction: 'Löschen bestätigen',
    bucketDeletionConfirmCancel: 'Abbrechen',
    userDeletionConfirmTitle: 'Bestätigung der Benutzer-Replikation',
    userDeletionConfirmContent:
      'Sie sind dabei, die Benutzer-Replikationsregel für zu löschen:',
    userDeletionConfirmContent2:
      'Neue Buckets werden nach diesem Schritt nicht mehr automatisch repliziert.',
    userDeletionConfirmQuestion: 'Möchten Sie fortfahren?',
    userDeletionBucketsConfirmContent:
      'Löschen Sie auch {total} Bucket-Replikationsregeln, die mit diesem Benutzer verknüpft sind',
    userDeletionConfirmAction: 'Löschen bestätigen',
    userDeletionConfirmCancel: 'Abbrechen',
    actionResumeSelected: 'Fortsetzen {total} ausgewählter Replikationen',
    actionPauseSelected: 'Pausieren {total} ausgewählter Replikationen',
    actionDeleteSelected: 'Löschen {total} ausgewählter Replikationen',
    tagSelectedReplications: '{total} Replikationen ausgewählt',
    tagClearFilters: 'Filter angewendet',
    actionSelectedResumeTitle: 'Ausgewählte fortsetzen',
    actionSelectedResumeContent:
      'Sie sind dabei, die folgenden {total} Replikationen fortzusetzen:',
    actionSelectedResumeQuestion: 'Möchten Sie fortfahren?',
    actionSelectedPauseTitle: 'Ausgewählte pausieren',
    actionSelectedPauseContent:
      'Sie sind dabei, die folgenden {total} Replikationen zu pausieren:',
    actionSelectedPauseQuestion: 'Möchten Sie fortfahren?',
    actionSelectedDeleteTitle: 'Ausgewählte löschen',
    actionSelectedDeleteContent:
      'Sie sind dabei, die folgenden {total} Replikationen zu löschen:',
    actionSelectedDeleteQuestion: 'Möchten Sie fortfahren?',
    resumeSelectedSuccessContent:
      'Die folgenden {total} Replikationen wurden fortgesetzt:',
    resumeSelectedErrorContent:
      'Es ist ein Fehler beim Fortsetzen der folgenden {total} Replikationen aufgetreten:',
    pauseSelectedSuccessContent:
      'Die folgenden {total} Replikationen wurden pausiert:',
    pauseSelectedErrorContent:
      'Es ist ein Fehler beim Pausieren der folgenden {total} Replikationen aufgetreten:',
    deleteSelectedSuccessContent:
      'Die folgenden {total} Replikationen wurden gelöscht:',
    deleteSelectedErrorContent:
      'Es ist ein Fehler beim Löschen der folgenden {total} Replikationen aufgetreten:',
    filterByUserPlaceholder: 'Nach Benutzer filtern',
    filterByUserNoResult: 'Kein Benutzer entspricht Ihren Suchkriterien.',
    filterByBucketPlaceholder: 'Nach Bucket suchen',
    filterByToPlaceholder: 'Nach Richtung (Zu) filtern',
    filterByToNoResult: 'Kein Speicher entspricht Ihren Suchkriterien.',
    filterByStatusPlaceholder: 'Nach Status filtern',
    filterStatusPaused: 'Pausiert',
    filterStatusActive: 'Aktiv',
    filterStatusInitialDone: 'Erste Replikation: Abgeschlossen',
    filterStatusInitialInProgress: 'Erste Replikation: In Bearbeitung',
    filterStatusLiveUpToDate: 'Live-Replikation: Aktuell',
    filterStatusLiveBehind: 'Live-Replikation: Hinterher',
    filterByCreatedAtStartPlaceholder: 'Filtern nach',
    filterByCreatedAtEndPlaceholder: 'Erstellt am',
    userReplication: 'Benutzer-Replikation',
    userReplicationDescription:
      'Repliziert alle aktuellen und zukünftigen Buckets dieses Nutzers.',
    filterTypeBucket: 'Bucket-Replikation',
    filterTypeUser: 'Benutzer-Replikation',
    filterByTypePlaceholder: 'Nach Typ filtern',
  },
};
