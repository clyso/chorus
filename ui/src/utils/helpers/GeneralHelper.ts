import {
  debounce,
  merge,
  mergeWith,
  orderBy,
  throttle,
  uniqueId,
} from 'lodash-es';
import { formatDistanceStrict } from 'date-fns';
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
}
