import { ApiVersion } from '../types/api';
import { API_BASE_URL, API_PREFIX } from '@/utils/constants/env';

export abstract class ApiHelper {
  static getChorusAPIUrl(resourcePath: string): string {
    return `${API_BASE_URL}${API_PREFIX}${resourcePath}`;
  }

  static getPrometheusAPIUrl(
    resourcePath: string,
    version = ApiVersion.V1Beta1,
  ): string {
    return `${API_BASE_URL}/prometheus/api/${version}${resourcePath}`;
  }

  static getApiAuthUrl(resourcePath: string): string {
    return `${API_BASE_URL}${resourcePath}`;
  }
}
