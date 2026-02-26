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
  S3 = 'S3',
  SWIFT = 'SWIFT',
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
export interface ChorusReplicationId {
  user: string;
  fromStorage: string;
  toStorage: string;
  fromBucket?: string;
  toBucket?: string;
}

export interface ChorusReplicationOpts {
  agentUrl: string;
}

export interface ChorusReplicationDowntimeOpts {
  startOnInitDone: boolean;
  cron: string;
  startAt: string;
  maxDuration: string;
  maxEventLag: number;
  skipBucketCheck: boolean;
  continueReplication: boolean;
}

export interface ChorusReplicationSwitchInfo {
  lastStatus: string;
  zeroDowntime: boolean;
  multipartTtl: string;
  downtimeOpts: ChorusReplicationDowntimeOpts;
  lastStartedAt: string;
  doneAt: string;
  history: string[];
  replicationId: ChorusReplicationId;
}

export interface ChorusReplication {
  id: ChorusReplicationId;
  idStr: string;
  opts: ChorusReplicationOpts;
  createdAt: string;
  isPaused: boolean;
  isInitDone: boolean;
  initObjListed: string;
  initObjDone: string;
  events: string;
  eventsDone: string;
  eventLag: string;
  hasSwitch: boolean;
  isArchived: boolean;
  archivedAt: string;
  switchInfo: ChorusReplicationSwitchInfo;
  replicationType: ReplicationType;
}

export interface ChorusReplicationListResponse {
  replications: ChorusReplication[];
}

export interface ChorusAddReplicationsRequest {
  id: ChorusReplicationId;
  opts?: ChorusReplicationOpts;
}

export interface ChorusBucketListRequest {
  user: string;
  fromStorage: string;
  toStorage: string;
  showReplicated: boolean;
}

export interface ChorusBucketListResponse {
  buckets: string[];
  replicatedBuckets: string[];
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

export enum ReplicationType {
  'USER' = 'User Replication',
  'BUCKET' = 'Bucket Replication',
}
