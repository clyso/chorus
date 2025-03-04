<!--
  - Copyright © 2025 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { computed, onMounted, ref, watch } from 'vue';
  import {
    ArcElement,
    Chart,
    type ChartComponentLike,
    type ChartConfiguration,
    type ChartOptions,
    Colors,
    DoughnutController,
    PieController,
    PolarAreaController,
    RadialLinearScale,
    Tooltip,
  } from 'chart.js';
  import { storeToRefs } from 'pinia';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';

  type HomeChartType = 'pie' | 'doughnut' | 'polarArea';

  const props = withDefaults(
    defineProps<{
      type: HomeChartType;
      labels: string[];
      colors: string[];
      data: number[];
      isClickable?: boolean;
      isHalf?: boolean; // for pie, doughnut
    }>(),
    {
      isClickable: true,
      isHalf: false,
    },
  );
  const emit = defineEmits<{
    (e: 'clickElement', index: number): void;
  }>();

  const CHART_CONTROLLER_MAP: Record<HomeChartType, ChartComponentLike[]> = {
    doughnut: [DoughnutController, Colors, ArcElement, Tooltip],
    pie: [PieController, Colors, ArcElement, Tooltip],
    polarArea: [
      PolarAreaController,
      Colors,
      ArcElement,
      Tooltip,
      RadialLinearScale,
    ],
  };

  Chart.register(...CHART_CONTROLLER_MAP[props.type]);

  Chart.defaults.font.family = 'Poppins';

  const { isDark } = storeToRefs(useColorSchemeStore());

  function getCssVar(name: string): string {
    // same return to recalculate color when isDark value changes
    if (isDark.value) {
      return getComputedStyle(document.body).getPropertyValue(name) ?? name;
    }

    return getComputedStyle(document.body).getPropertyValue(name) ?? name;
  }

  const canvas = ref<HTMLCanvasElement | null>(null);
  let chart: Chart<typeof props.type, number[], string> | null = null;
  const isElementHovered = ref(false);

  const calculatedColors = computed(() =>
    props.colors.map((color) => getCssVar(color)),
  );

  const chartData = computed(() => ({
    labels: props.labels,
    datasets: [
      {
        data: props.data,
        borderWidth: 4,
        borderColor: getCssVar('--card-color'),
        backgroundColor: calculatedColors.value,
        circumference: props.isHalf ? 180 : undefined,
        rotation: props.isHalf ? -90 : undefined,
      },
    ],
  }));

  const chartConfig = computed<
    ChartConfiguration<typeof props.type, number[], string>
  >(() => ({
    type: props.type,
    data: chartData.value,
    options: {
      cutout: props.type === 'doughnut' ? '75%' : undefined,
      scales:
        props.type === 'polarArea'
          ? {
              r: {
                beginAtZero: true,
                grid: {
                  display: false,
                },
                ticks: {
                  display: false,
                },
              },
            }
          : undefined,
      responsive: true,
      onHover: (_, activeEls) => {
        if (!props.isClickable) {
          return;
        }

        isElementHovered.value = activeEls.length > 0;
      },
      onClick: (evt) => {
        if (!chart || !props.isClickable) {
          return;
        }

        const elements = chart.getElementsAtEventForMode(
          evt as unknown as Event,
          'index',
          { intersect: true },
          false,
        );
        const { index } = elements[0];

        emit('clickElement', index);
      },
      plugins: {
        tooltip: {
          boxPadding: 8,
        },
      },
    } as ChartOptions<typeof props.type>,
  }));

  function mountChart() {
    if (!canvas.value) {
      return;
    }

    chart = new Chart(canvas.value, chartConfig.value);
  }

  function updateChart() {
    if (!chart) {
      return;
    }

    chart.data = chartConfig.value.data;
    chart.options = chartConfig.value.options!;
    chart.update('none');
  }

  onMounted(mountChart);

  watch(() => chartConfig.value, updateChart);
</script>

<template>
  <canvas
    ref="canvas"
    class="home-circular-chart"
    :style="{ cursor: isElementHovered ? 'pointer' : 'default' }"
  ></canvas>
</template>
