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
