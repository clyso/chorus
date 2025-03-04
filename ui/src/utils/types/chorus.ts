/*
 * Copyright © 2025 Clyso GmbH
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

export interface ChorusMigrationListResponse {
  migrations: ChorusMigrationItem[];
}

export interface ChorusMigrationItem {
  fromStorage: string;
  toStorage: string;
  started: boolean;
  done: boolean;
  paused: boolean;
  objListed: number;
  objDone: number;
  objBytesListed: number;
  objBytesDone: number;
  buckets: ChorusBucketMigration[];
  startedAt: string;
  doneAt: string;
}

export interface ChorusBucketMigration {
  done: boolean;
  taggingDone: boolean;
  lifecycleDone: boolean;
  policyDone: boolean;
  versioningDone: boolean;
  objListed: number;
  objDone: number;
  objBytesListed: number;
  objBytesDone: number;
  startedAt: string;
  doneAt: string;
  name: string;
}

export interface ChorusMigrationCostListResponse {
  costs: ChorusMigrationCost[];
}

export interface ChorusMigrationCost {
  fromStorage: string;
  toStorage: string;
  status: ChorusMigrationCostStatus;
  startedAt: string;
  buckets: number;
  objects: number;
  bytes: number;
  fromApiCalls: number;
  toApiCalls: string;
}

export enum ChorusMigrationCostStatus {
  NOT_STARTED = 'NOT_STARTED',
  IN_PROGRESS = 'IN_PROGRESS',
  DONE = 'DONE',
}

export interface ChorusProxyCredentials {
  address: string;
  credentials: ChorusCredential[];
}

export interface ChorusCredential {
  alias: string;
  accessKey: string;
  secretKey: string;
}

export interface ChorusStorageListResponse {
  storages: ChorusStorage[];
}

export interface ChorusStorage {
  name: string;
  isMain: boolean;
  address: string;
  provider: StorageProvider;
  credentials: ChorusCredential[];
}

export enum StorageProvider {
  Other = 'Other',
  Ceph = 'Ceph',
  Minio = 'Minio',
  AWS = 'AWS',
  GCS = 'GCS',
  Alibaba = 'Alibaba',
  Cloudflare = 'Cloudflare',
  DigitalOcean = 'DigitalOcean',
}

export interface ChorusCompareBucketRequest {
  bucket: string;
  fromStorage: string;
  toStorage: string;
  user: string;
  showMatch: boolean; // set true to get list of matching files (match property) in response
}

export interface ChorusCompareBucketResponse {
  isMatch: true; // true if storage's buckets have the same content
  missFrom: string[]; // list of missing files in 'from storage' bucket
  missTo: string[]; // list of missing files in 'to storage' bucket
  differ: string[]; // list of files with different content
  error: string[]; // list of errors occurred during comparison
  match: string[]; // list matched files in storages bucket. Empty if request parameter showMatch set to false.
}

export interface ChorusMigrationStreamResponse {
  result: ChorusMigrationListResponse;
  error?: {
    code: number;
    message: string;
  };
}

// Replication
export interface ChorusReplication {
  user: string;
  bucket: string;
  from: string;
  to: string;
  createdAt: string; // when replication was started
  isPaused: boolean;
  isInitDone: boolean;
  initObjListed: string; // number of objected to replicate during the initial migration
  initObjDone: string; // number of replicated objects during the initial migration
  initBytesListed: string; // same but for bytes
  initBytesDone: string; // same but for bytes
  events: string; // events triggered after initial migration was initiated (live replication)
  eventsDone: string; // number of processed events
  lastEmittedAt?: string; // the date when the last event was emitted (live replication)
  lastProcessedAt?: string; // the date when the last event was processed
  // (lastProcessedAt > lastEmittedAt ? up to date : behind)
}

export interface ChorusReplicationListResponse {
  replications: ChorusReplication[];
}

export interface ChorusAddReplicationsRequest {
  user: string;
  from: string;
  to: string;
  buckets: string[];
  isForAllBuckets: boolean;
}

export type ChorusReplicationBase = Pick<
  ChorusReplication,
  'user' | 'bucket' | 'from' | 'to'
>;

export interface ChorusBucketListRequest {
  user: string;
  from: string;
  to: string;
  showReplicated: boolean;
}

export interface ChorusBucketListResponse {
  buckets: string[];
  replicatedBuckets: string[];
}

export interface ChorusUserReplicationListResponse {
  replications: ChorusUserReplication[];
}

export interface ChorusUserReplication {
  to: string;
  from: string;
  user: string;
}

export interface ChorusDeleteUserReplicationRequest {
  user: string;
  from: string;
  to: string;
  deleteBucketReplications: boolean;
}

export enum ReplicationStatusFilter {
  ACTIVE = 'ACTIVE',
  PAUSED = 'PAUSED',
  INITIAL_IN_PROGRESS = 'INITIAL_IN_PROGRESS',
  INITIAL_DONE = 'INITIAL_DONE',
  LIVE_BEHIND = 'LIVE_BEHIND',
  LIVE_UP_TO_DATE = 'LIVE_UP_TO_DATE',
}

export enum AddReplicationStepName {
  'FROM_STORAGE' = 1,
  'TO_STORAGE' = 2,
  'USER' = 3,
  'BUCKETS' = 4,
}
