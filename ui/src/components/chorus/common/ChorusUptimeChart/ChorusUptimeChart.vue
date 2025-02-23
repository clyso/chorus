<script setup lang="ts">
  import { computed, onMounted, ref, watch } from 'vue';
  import { storeToRefs } from 'pinia';
  import {
    BarController,
    BarElement,
    CategoryScale,
    Chart,
    type ChartConfiguration,
    LinearScale,
    type Point,
    Tooltip,
  } from 'chart.js';
  import { useI18n } from 'vue-i18n';
  import { CAspectRatio } from '@clyso/clyso-ui-kit';
  import type { PrometheusUptimeDataItem } from '@/utils/types/prometheus';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import { UptimeChartHelper } from '@/utils/helpers/UptimeChartHelper';
  import type {
    RawUptimeChartDataItem,
    UptimeChartDataItem,
  } from '@/utils/types/uptimeChart';

  Chart.register(
    BarController,
    BarElement,
    CategoryScale,
    LinearScale,
    Tooltip,
  );

  Chart.defaults.font.family = 'Poppins';

  const props = defineProps<{
    data: PrometheusUptimeDataItem[];
  }>();

  const { locale } = storeToRefs(useI18nStore());
  const { t } = useI18n();
  const { isDark } = storeToRefs(useColorSchemeStore());

  function getCssVar(name: string): string {
    // TODO: refactor
    if (isDark.value) {
      return getComputedStyle(document.body).getPropertyValue(name);
    }

    return getComputedStyle(document.body).getPropertyValue(name);
  }

  const rawChartData = computed<RawUptimeChartDataItem[]>(() =>
    UptimeChartHelper.getRawChartData(props.data),
  );
  const chartData = computed<UptimeChartDataItem[]>(() =>
    rawChartData.value.map((rawItem) => ({
      ...rawItem,
      x: new Date(rawItem.x).toLocaleDateString(locale.value, {
        day: 'numeric',
        month: 'short',
      }),
      meta: {
        downTimestamps: rawItem.meta.downTimestamps.map((downTimestamp) =>
          new Date(downTimestamp).toLocaleString(locale.value, {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          }),
        ),
      },
    })),
  );

  const canvas = ref<HTMLCanvasElement | null>(null);
  let chart: Chart | null = null;

  const chartConfig = computed<ChartConfiguration>(() => {
    const barColors = chartData.value.map((item) =>
      UptimeChartHelper.isUptimeChartItemUp(item)
        ? getCssVar('--success-color')
        : getCssVar('--error-color'),
    );
    const ticksColor = getCssVar('--text-color-1');
    const computedConfig: Partial<ChartConfiguration> = {
      data: {
        datasets: [
          {
            data: chartData.value as unknown as Point[],
            backgroundColor: barColors,
          },
        ],
      },
      options: {
        scales: {
          x: {
            ticks: {
              color: ticksColor,
            },
          },
        },
        plugins: {
          tooltip: {
            callbacks: {
              label(context) {
                const item = context.raw as UptimeChartDataItem;
                const {
                  meta: { downTimestamps },
                } = item;
                const downTimeString = `${
                  downTimestamps.length <= 3
                    ? downTimestamps.join(', ')
                    : `${downTimestamps.slice(0, 3).join(', ')}...`
                }`;

                return UptimeChartHelper.isUptimeChartItemUp(item)
                  ? t('allGood')
                  : `${t('downAt')} ${downTimeString}`;
              },
            },
          },
        },
      },
    } as Partial<ChartConfiguration>;

    return UptimeChartHelper.getMergedUptimeChartConfiguration(computedConfig);
  });

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

  onMounted(() => {
    mountChart();
  });

  watch(
    () => chartConfig.value,
    () => updateChart(),
  );
</script>

<template>
  <CAspectRatio
    class="chorus-uptime-chart"
    ratio="4:1"
  >
    <canvas
      ref="canvas"
      class="chorus-uptime-chart__canvas"
    >
    </canvas>
  </CAspectRatio>
</template>
