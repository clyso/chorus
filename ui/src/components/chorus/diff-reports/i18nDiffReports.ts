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
    cliCheckWarning:
      'This check was created via CLI. For further details or actions, please use the CLI.',
    filterByDirectionPlaceholder: 'Filter by direction',
    filterByBucketPlaceholder: 'Filter by bucket',
    filterByStatusPlaceholder: 'Filter by status',
    filterStatusChecking: 'Checking',
    filterStatusConsistent: 'Consistent',
    filterStatusInconsistent: 'Inconsistent',
    actionCreateDiffReport: 'Create Diff Report',
    noResultsTitle: 'No Diff Reports yet',
    noResultsText: 'There are no diff reports available at the moment.',
    errorTitle: 'Error',
    errorText:
      'An error occurred while getting the diff report list.\nPlease try one more time.',
    errorAction: 'Retry',

    deleteDiffReportSuccessTitle: 'Deleted!',
    deleteDiffReportSuccessContent: 'Diff report deleted:',
    deleteDiffReportErrorTitle: 'Deletion failed!',
    deleteDiffReportErrorAction: 'Retry',
    deleteDiffReportErrorContent:
      'An error occurred while deleting the diff report:',
    deleteDiffReportConfirmTitle: 'Delete Diff Report',
    deleteDiffReportConfirmContent: 'You are about to delete the diff report:',
    deleteDiffReportConfirmQuestion: 'Are you sure you want to proceed?',
    deleteDiffReportConfirmAction: 'Delete',
    deleteDiffReportCancelAction: 'Cancel',
    deleteDiffReportDeleteAction: 'Delete',

    deleteDiffReportsSuccessTitle: 'Deleted!',
    deleteDiffReportsSuccessContent:
      'The following {total} diff reports have been deleted:',
    deleteDiffReportsErrorTitle: 'Deletion failed!',
    deleteDiffReportsErrorAction: 'Retry',
    deleteDiffReportsErrorContent:
      'An error occurred while deleting the following {total} diff reports:',
    deleteDiffReportsConfirmTitle: 'Delete Diff Reports',
    deleteDiffReportsConfirmContent:
      'You are about to delete the following diff reports:',
    deleteDiffReportsConfirmQuestion: 'Are you sure you want to proceed?',
    deleteDiffReportsConfirmAction: 'Delete',
    deleteDiffReportsCancelAction: 'Cancel',
    deleteDiffReportsDeleteAction: 'Delete',
    deleteDiffReportsSelected: 'Delete {total} selected',

    restartDiffReportSuccessTitle: 'Restarted!',
    restartDiffReportSuccessContent: 'Diff report restarted:',
    restartDiffReportErrorTitle: 'Restart failed!',
    restartDiffReportErrorAction: 'Retry',
    restartDiffReportErrorContent:
      'An error occurred while restarting the diff report:',
    restartDiffReportConfirmTitle: 'Restart Diff Report',
    restartDiffReportConfirmContent:
      'You are about to restart the diff report generation for the following diff report:',
    restartDiffReportConfirmQuestion: 'Are you sure you want to proceed?',
    restartDiffReportConfirmAction: 'Restart',
    restartDiffReportCancelAction: 'Cancel',
    restartDiffReportRestartAction: 'Restart',

    restartDiffReportsSuccessTitle: 'Restarted!',
    restartDiffReportsSuccessContent:
      'The following {total} diff reports have been restarted:',
    restartDiffReportsErrorTitle: 'Restart failed!',
    restartDiffReportsErrorAction: 'Retry',
    restartDiffReportsErrorContent:
      'An error occurred while restarting the following {total} diff reports:',
    restartDiffReportsConfirmTitle: 'Restart Diff Reports',
    restartDiffReportsConfirmContent:
      'You are about to restart the diff report generation for the following {total} diff reports:',
    restartDiffReportsConfirmQuestion: 'Are you sure you want to proceed?',
    restartDiffReportsConfirmAction: 'Restart',
    restartDiffReportsCancelAction: 'Cancel',
    restartDiffReportsRestartAction: 'Restart',
    restartDiffReportsSelected: 'Restart {total} selected',
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
    cliCheckWarning:
      'Dieser Check wurde über die CLI erstellt. Für weitere Details oder Aktionen verwenden Sie bitte die CLI.',
    filterByDirectionPlaceholder: 'Nach Richtung filtern',
    filterByBucketPlaceholder: 'Nach Bucket filtern',
    filterByStatusPlaceholder: 'Nach Status filtern',
    filterStatusChecking: 'Prüfung läuft',
    filterStatusConsistent: 'Konsistent',
    filterStatusInconsistent: 'Inkonsistent',
    actionCreateDiffReport: 'Diff-Report erstellen',
    noResultsTitle: 'Noch keine Diff-Reports',
    noResultsText: 'Derzeit sind keine Diff-Reports verfügbar.',
    errorTitle: 'Fehler',
    errorText:
      'Beim Abrufen der Diff-Report-Liste ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    errorAction: 'Erneut versuchen',

    deleteDiffReportSuccessTitle: 'Gelöscht!',
    deleteDiffReportSuccessContent: 'Diff-Report gelöscht:',
    deleteDiffReportErrorTitle: 'Löschen fehlgeschlagen!',
    deleteDiffReportErrorAction: 'Erneut versuchen',
    deleteDiffReportErrorContent:
      'Beim Löschen des Diff-Reports ist ein Fehler aufgetreten:',
    deleteDiffReportConfirmTitle: 'Diff-Report löschen',
    deleteDiffReportConfirmContent:
      'Sie sind dabei, den folgenden Diff-Report zu löschen:',
    deleteDiffReportConfirmQuestion:
      'Sind Sie sicher, dass Sie fortfahren möchten?',
    deleteDiffReportConfirmAction: 'Löschen',
    deleteDiffReportCancelAction: 'Abbrechen',
    deleteDiffReportDeleteAction: 'Löschen',

    deleteDiffReportsSuccessTitle: 'Gelöscht!',
    deleteDiffReportsSuccessContent:
      'Die folgenden {total} Diff-Reports wurden gelöscht:',
    deleteDiffReportsErrorTitle: 'Löschen fehlgeschlagen!',
    deleteDiffReportsErrorAction: 'Erneut versuchen',
    deleteDiffReportsErrorContent:
      'Beim Löschen der folgenden {total} Diff-Reports ist ein Fehler aufgetreten:',
    deleteDiffReportsConfirmTitle: 'Diff-Reports löschen',
    deleteDiffReportsConfirmContent:
      'Sie sind dabei, die folgenden Diff-Reports zu löschen:',
    deleteDiffReportsConfirmQuestion:
      'Sind Sie sicher, dass Sie fortfahren möchten?',
    deleteDiffReportsConfirmAction: 'Löschen',
    deleteDiffReportsCancelAction: 'Abbrechen',
    deleteDiffReportsDeleteAction: 'Löschen',
    deleteDiffReportsSelected: '{total} ausgewählte löschen',

    restartDiffReportSuccessTitle: 'Neugestartet!',
    restartDiffReportSuccessContent: 'Diff-Report neugestartet:',
    restartDiffReportErrorTitle: 'Neustart fehlgeschlagen!',
    restartDiffReportErrorAction: 'Erneut versuchen',
    restartDiffReportErrorContent:
      'Beim Neustart des Diff-Reports ist ein Fehler aufgetreten:',
    restartDiffReportConfirmTitle: 'Diff-Report neustarten',
    restartDiffReportConfirmContent:
      'Sie sind dabei, die Diff-Report-Generierung für den folgenden Diff-Report neu zu starten:',
    restartDiffReportConfirmQuestion:
      'Sind Sie sicher, dass Sie fortfahren möchten?',
    restartDiffReportConfirmAction: 'Neustarten',
    restartDiffReportCancelAction: 'Abbrechen',
    restartDiffReportRestartAction: 'Neustarten',

    restartDiffReportsSuccessTitle: 'Neugestartet!',
    restartDiffReportsSuccessContent:
      'Die folgenden {total} Diff-Reports wurden neugestartet:',
    restartDiffReportsErrorTitle: 'Neustart fehlgeschlagen!',
    restartDiffReportsErrorAction: 'Erneut versuchen',
    restartDiffReportsErrorContent:
      'Beim Neustart der folgenden {total} Diff-Reports ist ein Fehler aufgetreten:',
    restartDiffReportsConfirmTitle: 'Diff-Reports neustarten',
    restartDiffReportsConfirmContent:
      'Sie sind dabei, die Diff-Report-Generierung für die folgenden {total} Diff-Reports neu zu starten:',
    restartDiffReportsConfirmQuestion:
      'Sind Sie sicher, dass Sie fortfahren möchten?',
    restartDiffReportsConfirmAction: 'Neustarten',
    restartDiffReportsCancelAction: 'Abbrechen',
    restartDiffReportsRestartAction: 'Neustarten',
    restartDiffReportsSelected: '{total} ausgewählte neustarten',
  },
};
