import { type ChartConfiguration } from 'chart.js';

export const BASE_UPTIME_CHART_CONFIG: ChartConfiguration = {
  type: 'bar',
  data: {
    datasets: [
      {
        normalized: true,
        data: [],
        // parsing: {
        //   xAxisKey: '0',
        //   yAxisKey: '1',
        // },
        barThickness: 'flex',
        barPercentage: 0.7,
        // categoryPercentage: 1,
        // maxBarThickness: 8,
      },
    ],
  },
  options: {
    scales: {
      y: {
        beginAtZero: true,
        display: false,
        min: 0,
        max: 1,
      },
      x: {
        grid: {
          drawOnChartArea: false,
          display: false,
        },
        ticks: {
          autoSkip: true,
          autoSkipPadding: 24,
          maxRotation: 0,
          minRotation: 0,
          align: 'inner',
          font: {
            size: 12,
          },
        },
      },
    },
    responsive: true,
    maintainAspectRatio: false,
    // aspectRatio: 4,
    plugins: {
      legend: {
        display: false,
      },
    },
  },
};
