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

import {
  debounce,
  merge,
  mergeWith,
  orderBy,
  throttle,
  uniqueId,
} from 'lodash-es';
import {
  formatDistanceStrict,
  formatDuration,
  intervalToDuration,
} from 'date-fns';
import { de, enGB } from 'date-fns/locale';
import { storeToRefs } from 'pinia';
import { useI18nStore } from '@/stores/i18nStore';

export abstract class GeneralHelper {
  static merge = merge;

  static mergeWith = mergeWith;

  static orderBy = orderBy;

  static throttle = throttle;

  static debounce = debounce;

  static uniqueId = uniqueId;

  static mergeWithCustom<T>(sources: T[]): T {
    const [object, ...restSources] = sources;

    return GeneralHelper.mergeWith(
      object,
      ...restSources,
      (objValue: unknown, srcValue: unknown) =>
        objValue instanceof Array ? objValue.concat(srcValue) : undefined,
    );
  }

  static formatBytes(bytes: number, decimals = 2): string {
    if (bytes === 0) return '0 Bytes';

    const bytesInKb = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    const sizeIndex = Math.floor(Math.log(bytes) / Math.log(bytesInKb));

    return `${parseFloat((bytes / bytesInKb ** sizeIndex).toFixed(dm))} ${sizes[sizeIndex]}`;
  }

  static formatDate(date: string | Date): string {
    const { locale } = storeToRefs(useI18nStore());
    const dateToFormat = typeof date === 'string' ? new Date(date) : date;

    return dateToFormat.toLocaleDateString(
      locale.value === 'en' ? 'en-Gb' : 'de',
      { day: 'numeric', month: 'short', year: 'numeric' },
    );
  }

  static formatDateTime(date: string | Date, hasTimezone = false): string {
    const { locale } = storeToRefs(useI18nStore());
    const dateToFormat = typeof date === 'string' ? new Date(date) : date;

    return dateToFormat.toLocaleDateString(
      locale.value === 'en' ? 'en-Gb' : 'de',
      {
        day: 'numeric',
        month: 'short',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: hasTimezone ? 'short' : undefined,
      },
    );
  }

  static formatDateDistance(date: string | Date, baseDate: string | Date) {
    const { locale } = storeToRefs(useI18nStore());
    const dateComputed = typeof date === 'string' ? new Date(date) : date;
    const baseDateComputed =
      typeof baseDate === 'string' ? new Date(baseDate) : baseDate;

    return formatDistanceStrict(dateComputed, baseDateComputed, {
      locale: locale.value === 'en' ? enGB : de,
    });
  }

  static formatDurationSeconds(duration: string): string {
    const { locale } = storeToRefs(useI18nStore());
    const totalSeconds = Number(duration.replace('s', ''));

    if (isNaN(totalSeconds)) return '-';

    return formatDuration(
      intervalToDuration({ start: 0, end: totalSeconds * 1000 }),
      {
        format: ['days', 'hours', 'minutes', 'seconds'],
        locale: locale.value === 'en' ? enGB : de,
      },
    );
  }
}
