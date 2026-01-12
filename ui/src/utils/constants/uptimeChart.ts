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
