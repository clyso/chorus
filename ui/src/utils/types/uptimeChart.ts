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

interface BaseUptimeChartDataItem {
  x: number | string;
  y: UptimeStatus;
  meta: {
    downTimestamps: (number | string)[];
  };
}

export interface RawUptimeChartDataItem extends BaseUptimeChartDataItem {
  x: number; // timestamp
  meta: {
    downTimestamps: number[]; // timestamp
  };
}

export interface UptimeChartDataItem extends BaseUptimeChartDataItem {
  x: string; // label
  meta: {
    downTimestamps: string[]; // timeString
  };
}

export type PartialRawUptimeChartDataItem = Omit<RawUptimeChartDataItem, 'x'>;

export enum UptimeStatus {
  'UP' = 1,
  'DOWN' = 0.5,
}
