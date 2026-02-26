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
    allBucketsDescription: 'Apply to all buckets (including future ones)',
    filterByUserPlaceholder: 'Filter by User',
    filterByBucketPlaceholder: 'Search by Bucket',
    filterByStoragePlaceholder: 'Filter by Storage',
    filterStatusAllowed: 'Allowed',
    filterStatusBlocked: 'Blocked',
    filterByStatusPlaceholder: 'Filter by Status',
    columnActions: 'Actions',
    actionDelete: 'Delete',
    actionDeleteBlocked: 'Unblock the routing policy before deleting it',
    actionBlock: 'Block',
    actionUnblock: 'Allow',
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
    blockSuccessTitle: 'Blocked!',
    blockSuccessContent: 'The following routing policy has been blocked:',
    blockErrorTitle: 'Blocking failed!',
    blockErrorContent:
      'An error occurred while blocking the following routing policy:',
    blockErrorAction: 'Retry',
    unblockSuccessTitle: 'Allowed!',
    unblockSuccessContent: 'The following routing policy has been unblocked:',
    unblockErrorTitle: 'Unblocking failed!',
    unblockErrorContent:
      'An error occurred while unblocking the following routing policy:',
    unblockErrorAction: 'Retry',
    actionSelectedBlockTitle: 'Block Routing Policies',
    actionSelectedBlockContent:
      'You are about to block the following {total} routing policies:',
    actionSelectedBlockQuestion: 'Are you sure you want to continue?',
    blockSelectedSuccessContent:
      'The following {total} routing policies have been blocked:',
    blockSelectedErrorContent:
      'An error occurred while blocking the following {total} routing policies:',
    actionSelectedUnblockTitle: 'Allow Routing Policies',
    actionSelectedUnblockContent:
      'You are about to allow the following {total} routing policies:',
    actionSelectedUnblockQuestion: 'Are you sure you want to continue?',
    unblockSelectedSuccessContent:
      'The following {total} routing policies have been allowed:',
    unblockSelectedErrorContent:
      'An error occurred while allowing the following {total} routing policies:',
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
    allBucketsDescription:
      'Auf alle Buckets anwenden (einschließlich zukünftiger)',
    filterByUserPlaceholder: 'Nach Benutzer filtern',
    filterByBucketPlaceholder: 'Nach Bucket suchen',
    filterByStoragePlaceholder: 'Nach Speicherort filtern',
    filterByStorageNoResult: 'Kein Speicher entspricht Ihren Suchkriterien',
    filterStatusAllowed: 'Zugelassen',
    filterStatusBlocked: 'Blockiert',
    filterByStatusPlaceholder: 'Nach Status filtern',
    columnActions: 'Aktionen',
    actionDelete: 'Löschen',
    actionDeleteBlocked: 'Routing-Richtlinie vor dem Löschen entsperren',
    actionBlock: 'Blockieren',
    actionUnblock: 'Entsperren',
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
    blockSuccessTitle: 'Blockiert!',
    blockSuccessContent: 'Die folgende Routing-Richtlinie wurde blockiert:',
    blockErrorTitle: 'Blockieren fehlgeschlagen!',
    blockErrorContent:
      'Beim Blockieren der folgenden Routing-Richtlinie ist ein Fehler aufgetreten:',
    blockErrorAction: 'Erneut versuchen',
    unblockSuccessTitle: 'Entsperrt!',
    unblockSuccessContent: 'Die folgende Routing-Richtlinie wurde entsperrt:',
    unblockErrorTitle: 'Entsperren fehlgeschlagen!',
    unblockErrorContent:
      'Beim Entsperren der folgenden Routing-Richtlinie ist ein Fehler aufgetreten:',
    unblockErrorAction: 'Erneut versuchen',
    actionSelectedBlockTitle: 'Routing-Richtlinien Blockieren',
    actionSelectedBlockContent: 'Sie blockieren folgende Routing-Richtlinien:',
    actionSelectedBlockQuestion: 'Möchten Sie fortfahren?',
    blockSelectedSuccessContent:
      'Die folgenden {total} Routing-Richtlinien wurden blockiert:',
    blockSelectedErrorContent:
      'Es ist ein Fehler beim Blockieren der folgenden {total} Routing-Richtlinien aufgetreten:',
    actionSelectedUnblockTitle: 'Routing-Richtlinien Entsperren',
    actionSelectedUnblockContent:
      'Sie entsperren folgende Routing-Richtlinien:',
    actionSelectedUnblockQuestion: 'Möchten Sie fortfahren?',
    unblockSelectedSuccessContent:
      'Die folgenden {total} Routing-Richtlinien wurden entsperrt:',
    unblockSelectedErrorContent:
      'Es ist ein Fehler beim Entsperren der folgenden {total} Routing-Richtlinien aufgetreten:',
  },
};
