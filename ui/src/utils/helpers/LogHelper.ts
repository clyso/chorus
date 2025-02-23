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
