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

import type { ChartConfiguration } from 'chart.js';
import type { PrometheusUptimeDataItem } from '@/utils/types/prometheus';
import type {
  PartialRawUptimeChartDataItem,
  RawUptimeChartDataItem,
  UptimeChartDataItem,
} from '@/utils/types/uptimeChart';
import { UptimeStatus } from '@/utils/types/uptimeChart';
import { BASE_UPTIME_CHART_CONFIG } from '@/utils/constants/uptimeChart';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';

export abstract class UptimeChartHelper {
  static isPrometheusDataItemUp(item: PrometheusUptimeDataItem) {
    return +item[1] > 0;
  }

  static isUptimeChartItemUp(item: Pick<UptimeChartDataItem, 'y'>) {
    return item.y === UptimeStatus.UP;
  }

  static getTimestamp(value: Date): number {
    return value.getTime();
  }

  static getRawChartDataMap(
    data: PrometheusUptimeDataItem[],
  ): Map<number, PartialRawUptimeChartDataItem> {
    return data.reduce<Map<number, PartialRawUptimeChartDataItem>>(
      (resultMap, item) => {
        const rawDate = new Date(item[0] * 1000);
        const date = new Date(
          rawDate.getFullYear(),
          rawDate.getMonth(),
          rawDate.getDate(),
        );
        const rawTimestamp = this.getTimestamp(rawDate);
        const timestamp = this.getTimestamp(date);

        const isUp = this.isPrometheusDataItemUp(item);
        const y = isUp ? UptimeStatus.UP : UptimeStatus.DOWN;
        const meta = {
          downTimestamps: isUp ? [] : [rawTimestamp],
        };

        const matchedItem = resultMap.get(timestamp);

        if (!matchedItem) {
          resultMap.set(timestamp, { y, meta });

          return resultMap;
        }

        const isMatchedItemUp = this.isUptimeChartItemUp(matchedItem);

        if (isUp) {
          return resultMap;
        }

        if (isMatchedItemUp) {
          resultMap.set(timestamp, { y, meta });

          return resultMap;
        }

        resultMap.set(timestamp, {
          y,
          meta: {
            downTimestamps: [
              ...matchedItem.meta.downTimestamps,
              ...meta.downTimestamps,
            ],
          },
        });

        return resultMap;
      },
      new Map(),
    );
  }

  static getRawChartData(
    data: PrometheusUptimeDataItem[],
  ): RawUptimeChartDataItem[] {
    const dataMap = this.getRawChartDataMap(data);

    return Array.from(dataMap.entries()).map((entry) => {
      const [x, itemR] = entry;

      return { ...itemR, x };
    });
  }

  static getMergedUptimeChartConfiguration(
    partialConfig: Partial<ChartConfiguration>,
  ): ChartConfiguration {
    return GeneralHelper.merge({}, BASE_UPTIME_CHART_CONFIG, partialConfig);
  }
}
