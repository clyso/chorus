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

export default class LocalStorageHelper {
  static get<T>(itemName: string): T | null {
    const item = window.localStorage.getItem(itemName);
    const numPattern = new RegExp(/^\d+$/);
    const jsonPattern = new RegExp(/[[{].*[}\]]/);

    if (!item) {
      return null;
    }

    if (jsonPattern.test(item)) {
      return JSON.parse(item);
    }

    if (numPattern.test(item)) {
      return parseFloat(item) as T;
    }

    return item as T;
  }

  static set<T>(itemName: string, item: T) {
    const stringifiedValue =
      typeof item === 'string' ? item : JSON.stringify(item);

    window.localStorage.setItem(itemName, stringifiedValue);
  }

  static remove(itemName: string) {
    window.localStorage.removeItem(itemName);
  }
}
