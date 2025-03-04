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

import { apiClient } from '@/http';
import { ApiHelper } from '@/utils/helpers/ApiHelper';
import type {
  ChorusAddReplicationsRequest,
  ChorusBucketListRequest,
  ChorusBucketListResponse,
  ChorusCompareBucketRequest,
  ChorusCompareBucketResponse,
  ChorusDeleteUserReplicationRequest,
  ChorusProxyCredentials,
  ChorusReplicationBase,
  ChorusReplicationListResponse,
  ChorusStorageListResponse,
  ChorusUserReplicationListResponse,
} from '@/utils/types/chorus';

export abstract class ChorusService {
  static async getProxyCredentials(): Promise<ChorusProxyCredentials> {
    const { data } = await apiClient.get<ChorusProxyCredentials>(
      ApiHelper.getChorusAPIUrl('/proxy'),
    );

    return data;
  }

  static async getStorages(): Promise<ChorusStorageListResponse> {
    const { data } = await apiClient.get<ChorusStorageListResponse>(
      ApiHelper.getChorusAPIUrl('/storage'),
    );

    return data;
  }

  static async compareBucket(
    payload: ChorusCompareBucketRequest,
  ): Promise<ChorusCompareBucketResponse> {
    const { data } = await apiClient.post<ChorusCompareBucketResponse>(
      ApiHelper.getChorusAPIUrl('/replication/compare-bucket'),
      payload,
    );

    return data;
  }

  static async getReplications(): Promise<ChorusReplicationListResponse> {
    const { data } = await apiClient.get<ChorusReplicationListResponse>(
      ApiHelper.getChorusAPIUrl('/replication'),
    );

    return data;
  }

  static async addReplication(
    payload: ChorusAddReplicationsRequest,
  ): Promise<void> {
    await apiClient.post(
      ApiHelper.getChorusAPIUrl('/replication/add'),
      payload,
    );
  }

  static async deleteBucketReplication(
    payload: ChorusReplicationBase,
  ): Promise<void> {
    await apiClient.put(
      ApiHelper.getChorusAPIUrl('/replication/delete'),
      payload,
    );
  }

  static async pauseBucketReplication(
    payload: ChorusReplicationBase,
  ): Promise<void> {
    await apiClient.put(
      ApiHelper.getChorusAPIUrl('/replication/pause'),
      payload,
    );
  }

  static async resumeBucketReplication(
    payload: ChorusReplicationBase,
  ): Promise<void> {
    await apiClient.put(
      ApiHelper.getChorusAPIUrl('/replication/resume'),
      payload,
    );
  }

  static async getBucketsForReplication(
    payload: ChorusBucketListRequest,
  ): Promise<ChorusBucketListResponse> {
    const { data } = await apiClient.post<ChorusBucketListResponse>(
      ApiHelper.getChorusAPIUrl('/replication/list-buckets'),
      payload,
    );

    return data;
  }

  static async getUserReplications(): Promise<ChorusUserReplicationListResponse> {
    const { data } = await apiClient.get<ChorusUserReplicationListResponse>(
      ApiHelper.getChorusAPIUrl('/replication/user'),
    );

    return data;
  }

  static async deleteUserReplication(
    payload: ChorusDeleteUserReplicationRequest,
  ): Promise<void> {
    await apiClient.put(
      ApiHelper.getChorusAPIUrl('/replication/user/delete'),
      payload,
    );
  }
}
