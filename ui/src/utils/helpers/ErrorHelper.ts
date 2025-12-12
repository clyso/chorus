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

import axios from 'axios';

export abstract class ErrorHelper {
  static getReason(error: unknown): string | null {
    if (!axios.isAxiosError(error)) return null;

    const details = error.response?.data.details;

    if (!Array.isArray(details)) return null;

    const errorInfo = details.find(
      (d) => d['@type'] === 'type.googleapis.com/google.rpc.ErrorInfo',
    );

    return errorInfo?.reason || null;
  }
}
