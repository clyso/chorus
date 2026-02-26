<!--
  - Copyright © 2026 Clyso GmbH
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
  import { useI18n } from 'vue-i18n';
  import { onBeforeMount, ref } from 'vue';
  import {
    I18nLocale,
    CSkeleton,
    CAspectRatio,
    CResult,
  } from '@clyso/clyso-ui-kit';
  import HomeWidget from '@/components/chorus/common/HomeWidget/HomeWidget.vue';
  import type { PrometheusUptimeDataItem } from '@/utils/types/prometheus';
  import { PrometheusService } from '@/services/PrometheusService';
  import { IconName } from '@/utils/types/icon';
  import ChorusUptimeChart from '@/components/chorus/common/ChorusUptimeChart/ChorusUptimeChart.vue';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        proxyUptimeTitle: 'S3 Proxy Uptime',
        notEnabledMessage: 'Prometheus is not available',
      },
      [I18nLocale.DE]: {
        proxyUptimeTitle: 'S3-Proxy Uptime',
        notEnabledMessage: 'Prometheus ist nicht verfügbar',
      },
    },
  });

  const props = withDefaults(
    defineProps<{
      isPageLoading?: boolean;
      isEnabled?: boolean;
    }>(),
    {
      isPageLoading: false,
      isEnabled: true,
    },
  );

  const emit = defineEmits<{
    (e: 'loading', value: boolean): void;
  }>();

  const proxyUptimeData = ref<PrometheusUptimeDataItem[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  async function getProxyUptimeData() {
    if (!props.isEnabled) {
      return;
    }

    isLoading.value = true;
    hasError.value = false;

    try {
      proxyUptimeData.value = await PrometheusService.getProxyUptimeData();
    } catch {
      hasError.value = true;
    } finally {
      isLoading.value = false;
    }
  }

  async function initProxyUptimeData() {
    emit('loading', true);

    try {
      await getProxyUptimeData();
    } finally {
      emit('loading', false);
    }
  }

  onBeforeMount(initProxyUptimeData);
</script>

<template>
  <HomeWidget
    :is-loading="isLoading || isPageLoading"
    :has-error="hasError"
    class="proxy-uptime-widget"
    @retry="getProxyUptimeData"
  >
    <template #title>
      {{ t('proxyUptimeTitle') }}
    </template>

    <template #loading-content>
      <CAspectRatio ratio="4:1">
        <CSkeleton
          class="proxy-uptime-widget__skeleton"
          type="chart"
          chart-group-height="100%"
          chart-height="100%"
          chart-width="100%"
        />
      </CAspectRatio>
    </template>

    <div class="proxy-uptime-widget__content">
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
          class="proxy-uptime-widget__not-enabled-result"
        >
          <template #title>
            {{ t('notEnabledMessage') }}
          </template>
        </CResult>
      </CAspectRatio>
      <ChorusUptimeChart
        v-else
        key="chart"
        class="proxy-uptime-widget__chart"
        :data="proxyUptimeData"
      />
    </div>
  </HomeWidget>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .proxy-uptime-widget {
    overflow: hidden;
    grid-column: span 5;

    @media screen and (min-width: utils.$viewport-desktop) and (max-width: 1300px) {
      grid-column: span 4;
    }

    @include utils.touch {
      grid-column: auto;
      width: 100%;
    }

    &__skeleton {
      @include utils.absolute-fit;
    }
  }
</style>
