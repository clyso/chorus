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

/*
 * Identifier for the standard Google RPC ErrorInfo detail type.
 * Since the API uses Protobuf, rich error metadata (like the specific 'reason' code)
 * is encapsulated in this message type within the error details array, see
 * https://cloud.google.com/apis/design/errors#error_info.
 */
const GOOGLE_RPC_ERROR_TYPE = 'type.googleapis.com/google.rpc.ErrorInfo';

export abstract class ErrorHelper {
  static getReason(error: unknown): string | null {
    if (!axios.isAxiosError(error)) return null;

    const details = error.response?.data.details;

    if (!Array.isArray(details)) return null;

    const errorInfo = details.find((d) => d['@type'] === GOOGLE_RPC_ERROR_TYPE);

    return errorInfo?.reason ?? null;
  }
}
