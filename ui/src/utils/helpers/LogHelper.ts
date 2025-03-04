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

import { IS_DEV_ENV } from '@/utils/constants/env';

export abstract class LogHelper {
  static log = (data: unknown, isForced: boolean = false) => {
    if (!isForced && !IS_DEV_ENV) {
      return;
    }

    // eslint-disable-next-line no-console
    console.log(data);
  };

  static dir = (data: unknown, isForced: boolean = false) => {
    if (!isForced && !IS_DEV_ENV) {
      return;
    }

    // eslint-disable-next-line no-console
    console.dir(data);
  };

  static error = (data: unknown, isForced: boolean = false) => {
    if (!isForced && !IS_DEV_ENV) {
      return;
    }

    // eslint-disable-next-line no-console
    console.error(data);
  };
}
