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

export const IS_DEV_ENV = import.meta.env.DEV;
export const HAS_PROMETHEUS = import.meta.env.VITE_HAS_PROMETHEUS === 'TRUE';
export const HAS_LANG_SELECT = import.meta.env.VITE_HAS_LANG_SELECT === 'TRUE';
export const API_BASE_URL: string = import.meta.env.VITE_API_BASE_URL ?? '';
export const API_PREFIX: string = import.meta.env.VITE_API_PREFIX ?? '';
