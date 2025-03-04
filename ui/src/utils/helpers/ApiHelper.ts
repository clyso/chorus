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

import { ApiVersion } from '../types/api';
import { API_BASE_URL } from '@/utils/constants/env';

export abstract class ApiHelper {
  static getChorusAPIUrl(resourcePath: string): string {
    return `${API_BASE_URL}${resourcePath}`;
  }

  static getPrometheusAPIUrl(
    resourcePath: string,
    version = ApiVersion.V1Beta1,
  ): string {
    return `${API_BASE_URL}/prometheus/api/${version}${resourcePath}`;
  }

  static getApiAuthUrl(resourcePath: string): string {
    return `${API_BASE_URL}${resourcePath}`;
  }
}
