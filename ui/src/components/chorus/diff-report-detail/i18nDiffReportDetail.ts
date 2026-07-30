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
    breadcrumbDiffReports: 'Diff Reports',
    errorTitle: 'Error',
    errorText:
      'An error occurred while loading the diff report.\nPlease try again.',
    errorAction: 'Retry',
    notFoundTitle: 'Report Not Found',
    notFoundText: 'The requested diff report could not be found.',
    invalidLocationsTitle: 'Invalid Locations',
    invalidLocationsText: 'Diff reports require exactly two locations.',
    invalidLocationsAction: 'Back to Diff Reports',
    notFoundAction: 'Back to Diff Reports',
    // Header
    detailTitle: 'Diff Report:',
    detailSubtitle:
      'Diff report comparing objects between the source and destination locations.',
    // Overview
    overviewTitle: 'Report Overview',
    overviewStatus: 'Status',
    overviewConsistency: 'Consistency',
    overviewVersioned: 'Versioned',
    overviewEtags: 'ETags',
    overviewSizes: 'Sizes',
    statusReady: 'Ready',
    statusChecking: 'Checking',
    statusConsistent: 'Consistent',
    statusInconsistent: 'Inconsistent',
    configVersioned: 'Versioned',
    configNotVersioned: 'Not versioned',
    configConsidersEtags: 'Considers ETags',
    configIgnoresEtags: 'Ignores ETags',
    configConsidersSizes: 'Considers Sizes',
    configIgnoresSizes: 'Ignores Sizes',
    // Progress
    diffProgressTitle: 'Diff Report progress',
    fixProgressTitle: 'Fix progress',
    progressLabel: 'Progress',
    progressQueued: 'Queued',
    progressCompleted: 'Completed',
    progressInconsistentObjects: 'Inconsistent objects',
    fixReady: 'Fix ready',
    fixQueued: 'Fix queued',
    fixCompleted: 'Fix completed',
    fixReadyYes: 'Yes',
    fixReadyNo: 'No',
    // Restart
    restartAction: 'Restart',
    restartConfirmTitle: 'Restart Diff Report',
    restartConfirmContent:
      'You are about to restart this diff report. Are you sure you want to proceed?',
    restartConfirmAction: 'Restart',
    restartCancelAction: 'Cancel',
    restartSuccessTitle: 'Restarted!',
    restartSuccessContent: 'Diff report restarted.',
    restartErrorTitle: 'Restart failed!',
    restartErrorContent:
      'An error occurred while restarting the diff report. You may see outdated report information.',
    errorDetailsLabel: 'Error details: ',
    restartErrorAction: 'Retry',
    // Entries
    entriesTitle: 'Inconsistent Objects',
    entriesSearchPlaceholder: 'Search by object name...',
    columnObject: 'Object',
    columnSize: 'Size',
    columnEtag: 'ETag',
    columnObjectPlacement: 'Object Placement',
    versionIdx: 'version idx {idx}',
  },
  [I18nLocale.DE]: {
    breadcrumbDiffReports: 'Diff-Reports',
    errorTitle: 'Fehler',
    errorText:
      'Beim Laden des Diff-Reports ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    errorAction: 'Erneut versuchen',
    notFoundTitle: 'Report nicht gefunden',
    notFoundText: 'Der angeforderte Diff-Report wurde nicht gefunden.',
    invalidLocationsTitle: 'Ungültige Speicherorte',
    invalidLocationsText: 'Diff-Reports erfordern genau zwei Speicherorte.',
    invalidLocationsAction: 'Zurück zu Diff-Reports',
    notFoundAction: 'Zurück zu Diff-Reports',
    // Header
    detailTitle: 'Diff-Report:',
    detailSubtitle:
      'Diff-Report zum Vergleich von Objekten zwischen Quell- und Ziel-Standorten.',
    // Overview
    overviewTitle: 'Report-Übersicht',
    overviewStatus: 'Status',
    overviewConsistency: 'Konsistenz',
    overviewVersioned: 'Versioniert',
    overviewEtags: 'ETags',
    overviewSizes: 'Dateigrößen',
    statusReady: 'Bereit',
    statusChecking: 'Prüfung läuft',
    statusConsistent: 'Konsistent',
    statusInconsistent: 'Inkonsistent',
    configVersioned: 'Versioniert',
    configNotVersioned: 'Nicht versioniert',
    configConsidersEtags: 'Berücksichtigt ETags',
    configIgnoresEtags: 'Ignoriert ETags',
    configConsidersSizes: 'Berücksichtigt Dateigrößen',
    configIgnoresSizes: 'Ignoriert Dateigrößen',
    // Progress
    diffProgressTitle: 'Diff-Report Fortschritt',
    fixProgressTitle: 'Fix-Fortschritt',
    progressLabel: 'Fortschritt',
    progressQueued: 'In Warteschlange',
    progressCompleted: 'Abgeschlossen',
    progressInconsistentObjects: 'Inkonsistente Objekte',
    fixReady: 'Fix bereit',
    fixQueued: 'Fix in Warteschlange',
    fixCompleted: 'Fix abgeschlossen',
    fixReadyYes: 'Ja',
    fixReadyNo: 'Nein',
    // Restart
    restartAction: 'Neustarten',
    restartConfirmTitle: 'Diff-Report neustarten',
    restartConfirmContent:
      'Sie sind dabei, diesen Diff-Report neu zu starten. Sind Sie sicher, dass Sie fortfahren möchten?',
    restartConfirmAction: 'Neustarten',
    restartCancelAction: 'Abbrechen',
    restartSuccessTitle: 'Neugestartet!',
    restartSuccessContent: 'Diff-Report neugestartet.',
    restartErrorTitle: 'Neustart fehlgeschlagen!',
    restartErrorContent:
      'Beim Neustart des Diff-Reports ist ein Fehler aufgetreten. Die angezeigten Report Details könnten möglicherweise nicht aktuell sein.',
    errorDetailsLabel: 'Error Details: ',
    restartErrorAction: 'Neustarten',
    // Entries
    entriesTitle: 'Inkonsistente Objekte',
    entriesSearchPlaceholder: 'Nach Objektname suchen...',
    columnObject: 'Objekt',
    columnSize: 'Größe',
    columnEtag: 'ETag',
    columnObjectPlacement: 'Objekt-Platzierung',
    versionIdx: 'Version-Index {idx}',
  },
};
