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
    mainStorage: 'Main',
    followerStorage: 'Follower',
    address: 'Address',
    allGood: 'all good',
    downAt: 'down at',
    errorTitle: 'Error',
    successTitle: 'Success',
    errorAction: 'Retry',
    direction: 'Direction',
    user: 'User',
    buckets: 'Buckets',
    objectBrowserTitle: 'Objects',
    storage: 'Storage',
    bucket: 'Bucket',
    selectBucket: 'Select a bucket',
    prefix: 'Prefix',
    loadObjects: 'Load objects',
    objectName: 'Object',
    size: 'Size',
    destinationModified: 'Destination modified',
    originalTimestamp: 'Original timestamp',
    loadMore: 'Load more',
    noObjectsFound: 'No objects found.',
    noS3Storages: 'No configured S3 storage is available.',
    timestampNotRecorded: 'Not recorded',
    loading: 'Loading...',
    loadError: 'Unable to load objects.',
  },
  [I18nLocale.DE]: {
    mainStorage: 'Hauptspeicher', // Consistent with your suggestion
    followerStorage: 'Follower',
    address: 'Adresse',
    allGood: 'Alles in Ordnung',
    downAt: 'Abgesenkt um',
    errorTitle: 'Fehler',
    successTitle: 'Erfolg',
    errorAction: 'Wiederholen',
    direction: 'Richtung',
    user: 'Benutzer',
    buckets: 'Buckets',
    objectBrowserTitle: 'Objekte',
    storage: 'Speicher',
    bucket: 'Bucket',
    selectBucket: 'Bucket auswählen',
    prefix: 'Präfix',
    loadObjects: 'Objekte laden',
    objectName: 'Objekt',
    size: 'Größe',
    destinationModified: 'Im Ziel geändert',
    originalTimestamp: 'Ursprünglicher Zeitstempel',
    loadMore: 'Mehr laden',
    noObjectsFound: 'Keine Objekte gefunden.',
    noS3Storages: 'Kein konfigurierter S3-Speicher verfügbar.',
    timestampNotRecorded: 'Nicht aufgezeichnet',
    loading: 'Wird geladen...',
    loadError: 'Objekte konnten nicht geladen werden.',
  },
};
