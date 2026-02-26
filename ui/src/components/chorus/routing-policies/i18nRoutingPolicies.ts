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
    routingPoliciesTitle: 'Routing Policies',
    columnUser: 'User',
    columnBucket: 'Bucket',
    columnStorage: 'Storage',
    columnStatus: 'Status',
    statusAllowed: 'Allowed',
    statusBlocked: 'Blocked',
    routingPolicyStatusAllowed:
      'Access to the configured routing policy target allowed',
    routingPolicyStatusBlocked:
      'Access to the configured routing policy target blocked',
    routingPolicyStorage:
      'The storage the routing policy configuration is related to',
    allBuckets: 'All Buckets',
    filterByUserPlaceholder: 'Filter by User',
    filterByBucketPlaceholder: 'Search by Bucket',
    filterByStoragePlaceholder: 'Filter by Storage',
    filterByStorageNoResult: 'No storage matches your search criteria',
    filterStatusAllowed: 'Allowed',
    filterStatusBlocked: 'Blocked',
    filterByStatusPlaceholder: 'Filter by Status',
    columnActions: 'Actions',
    actionDelete: 'Delete',
    routingPolicyDeletionConfirmTitle: 'Delete Routing Policy',
    routingPolicyDeletionConfirmContent:
      'You are about to delete the routing policy as follows:',
    routingPolicyDeletionConfirmQuestion: 'Are you sure you want to continue?',
    routingPolicyDeletionConfirmAction: 'Confirm Deletion',
    routingPolicyDeletionCancelAction: 'Cancel',
    deleteSuccessTitle: 'Deleted!',
    deleteSuccessContent: 'The following routing policies have been deleted:',
    deleteErrorTitle: 'Deletion failed!',
    deleteErrorContent:
      'An error occurred when deleting the following routing policies:',
    deleteErrorAction: 'Retry',
    deleteRoutingPolicyErrorUnknown: 'Unknown error',
    deleteErrorDetailsLabel: 'Details',
    cancel: 'Cancel',
    actionSelectedDeleteTitle: 'Delete Routing Policies',
    actionSelectedDeleteContent:
      'You are about to delete the following routing policies:',
    actionSelectedDeleteQuestion: 'Are you sure you want to continue?',
    deleteSelectedErrorContent:
      'An error occurred while deleting the following {total} routing policies:',
    deleteSelectedSuccessContent:
      'The following {total} routing policies have been deleted:',
  },
  [I18nLocale.DE]: {
    routingPoliciesTitle: 'Routing-Richtlinien',
    columnUser: 'Benutzer',
    columnBucket: 'Bucket',
    columnStorage: 'Speicher',
    columnStatus: 'Status',
    statusAllowed: 'Zugelassen',
    statusBlocked: 'Blockiert',
    routingPolicyStatusAllowed:
      'Zugriff auf das konfigurierte Ziel der Routing-Richtlinie erlaubt',
    routingPolicyStatusBlocked:
      'Zugriff auf das konfigurierte Ziel der Routing-Richtlinie blockiert',
    routingPolicyStorage:
      'Der Storage, auf den sich die Routing-Richtlinie bezieht',
    allBuckets: 'Alle Buckets',
    filterByUserPlaceholder: 'Nach Benutzer filtern',
    filterByBucketPlaceholder: 'Nach Bucket suchen',
    filterByStoragePlaceholder: 'Nach Speicherort filtern',
    filterByStorageNoResult: 'Kein Speicher entspricht Ihren Suchkriterien',
    filterStatusAllowed: 'Zugelassen',
    filterStatusBlocked: 'Blockiert',
    filterByStatusPlaceholder: 'Nach Status filtern',
    columnActions: 'Aktionen',
    actionDelete: 'Löschen',
    routingPolicyDeletionConfirmTitle: 'Routing-Richtlinie Löschen',
    routingPolicyDeletionConfirmContent:
      'Sie löschen folgende Routing-Richtlinie:',
    routingPolicyDeletionConfirmQuestion: 'Möchten Sie fortfahren?',
    routingPolicyDeletionConfirmAction: 'Löschen bestätigen',
    routingPolicyDeletionCancelAction: 'Abbrechen',
    deleteSuccessTitle: 'Gelöscht!',
    deleteSuccessContent: 'Die folgenden Routing-Richtlinien wurde gelöscht:',
    deleteErrorTitle: 'Löschen fehlgeschlagen!',
    deleteErrorContent:
      'Beim Löschen der folgenden Routing-Richtlinien ist ein Fehler aufgetreten:',
    deleteErrorAction: 'Erneut versuchen',
    deleteRoutingPolicyErrorUnknown: 'Unbekannter Fehler',
    deleteErrorDetailsLabel: 'Details',
    cancel: 'Abbrechen',
    actionSelectedDeleteTitle: 'Routing-Richtlinien Löschen',
    actionSelectedDeleteContent: 'Sie löschen folgende Routing-Richtlinien:',
    actionSelectedDeleteQuestion: 'Möchten Sie fortfahren?',
    deleteSelectedErrorContent:
      'Es ist ein Fehler beim Löschen der folgenden {total} Routing-Richtlinien aufgetreten:',
    deleteSelectedSuccessContent:
      'Die folgenden {total} Routing-Richtlinien wurden gelöscht:',
  },
};
