<script setup lang="ts">
  import {
    CAspectRatio,
    CResult,
    CSkeleton,
    CTile,
    I18nLocale,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { ref } from 'vue';
  import ChorusUptimeChart from '@/components/chorus/common/ChorusUptimeChart/ChorusUptimeChart.vue';
  import { PrometheusService } from '@/services/PrometheusService';
  import type { PrometheusUptimeDataItem } from '@/utils/types/prometheus';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        workerUptimeTitle: 'Worker Uptime',
        notEnabledMessage: 'Prometheus is not available',
        errorMessage: 'An error occurred while getting the chart data.',
      },
      [I18nLocale.DE]: {
        workerUptimeTitle: 'Arbeiter Uptime',
        notEnabledMessage: 'Prometheus ist nicht verfügbar',
        errorMessage:
          'Beim Abrufen der Diagrammdaten ist ein Fehler aufgetreten.',
      },
    },
  });

  const props = withDefaults(
    defineProps<{
      isInitializing?: boolean;
      isEnabled?: boolean;
    }>(),
    {
      isInitializing: false,
      isEnabled: true,
    },
  );

  const emit = defineEmits<{
    (e: 'init', value: boolean): void;
  }>();

  const workerUptimeData = ref<PrometheusUptimeDataItem[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  async function getWorkerUptimeData() {
    if (!props.isEnabled) {
      return;
    }

    isLoading.value = true;
    hasError.value = false;

    try {
      workerUptimeData.value = await PrometheusService.getWorkerUptimeData();
    } catch {
      hasError.value = true;
    } finally {
      isLoading.value = false;
    }
  }

  async function initWorkerUptimeData() {
    emit('init', true);

    try {
      await getWorkerUptimeData();
    } finally {
      emit('init', false);
    }
  }

  initWorkerUptimeData();
</script>

<template>
  <CTile
    class="worker-uptime-widget"
    :is-loading="isLoading || isInitializing"
  >
    <template #title>
      {{ t('workerUptimeTitle') }}
    </template>

    <template #loading-content>
      <CAspectRatio ratio="4:1">
        <CSkeleton
          class="worker-uptime-widget__skeleton"
          type="chart"
          chart-group-height="100%"
          chart-height="100%"
          chart-width="100%"
        />
      </CAspectRatio>
    </template>

    <div class="worker-uptime-widget__content">
      <CAspectRatio
        v-if="!isEnabled"
        key="not-enabled"
        ratio="4:1"
      >
        <CResult
          type="empty"
          :icon-name="IconName.BASE_BAR_CHART"
          :has-content="false"
          size="tiny"
          class="worker-uptime-widget__not-enabled-result"
        >
          <template #title>
            {{ t('notEnabledMessage') }}
          </template>
        </CResult>
      </CAspectRatio>
      <CAspectRatio
        v-else-if="hasError"
        key="error"
        ratio="4:1"
      >
        <CResult
          type="error"
          size="tiny"
          class="worker-uptime-widget__error-result"
          @positive-click="getWorkerUptimeData"
        >
          <template #title>
            {{ t('errorTitle') }}
          </template>
          {{ t('errorMessage') }}
        </CResult>
      </CAspectRatio>
      <ChorusUptimeChart
        v-else
        key="chart"
        class="worker-uptime-widget__chart"
        :data="workerUptimeData"
      />
    </div>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .worker-uptime-widget {
    &__skeleton {
      @include utils.absolute-fit;
    }
  }
</style>
