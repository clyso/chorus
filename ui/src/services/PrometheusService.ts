import { apiClient } from '@/http';
import { ApiHelper } from '@/utils/helpers/ApiHelper';
import type { PrometheusUptimeDataItem } from '@/utils/types/prometheus';
import { ApiVersion } from '@/utils/types/api';

const MAX_ALLOWED_TICKS = 11000;

function getUptimeConfig(): { start: string; end: string; step: string } {
  const endDate = new Date();
  const startDate = new Date();

  startDate.setMonth(startDate.getMonth() - 1);
  startDate.setHours(0, 0, 0, 0);

  const stepSeconds = Math.ceil(
    (endDate.getTime() - startDate.getTime()) / (MAX_ALLOWED_TICKS * 1000),
  );

  return {
    start: startDate.toISOString(),
    end: endDate.toISOString(),
    step: `${stepSeconds}s`,
  };
}

export abstract class PrometheusService {
  static async getProxyUptimeData(): Promise<PrometheusUptimeDataItem[]> {
    const { data } = await apiClient.post<{
      data: { result: [{ values: PrometheusUptimeDataItem[] }] };
    }>(
      ApiHelper.getPrometheusAPIUrl('/query_range', ApiVersion.V1),
      {
        query: 'sum(up{job="proxy"})',
        ...getUptimeConfig(),
      },
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        },
      },
    );

    return data.data.result[0].values.reverse();
  }

  static async getWorkerUptimeData(): Promise<PrometheusUptimeDataItem[]> {
    const { data } = await apiClient.post<{
      data: { result: [{ values: PrometheusUptimeDataItem[] }] };
    }>(
      ApiHelper.getPrometheusAPIUrl('/query_range', ApiVersion.V1),
      {
        query: 'sum(up{job="worker"})',
        ...getUptimeConfig(),
      },
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        },
      },
    );

    return data.data.result[0].values.reverse();
  }
}
