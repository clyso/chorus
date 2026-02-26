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
  import { computed } from 'vue';
  import { CAspectRatio, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import HomeCircularChart from '@/components/chorus/common/HomeCircularChart/HomeCircularChart.vue';
  import { useReplicationsWidget } from '@/components/chorus/home/ReplicationsWidget/useReplicationsWidget';

  enum StatusItemIndex {
    ACTIVE = 0,
    PAUSED = 1,
  }

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        paused: 'Status: Paused',
        active: 'Status: Active',
      },
      [I18nLocale.DE]: {
        paused: 'Status: Pausiert',
        active: 'Status: Aktiv',
      },
    },
  });

  const props = defineProps<{
    activeCount: number;
    pausedCount: number;
  }>();

  const { showActiveReplications, showPausedReplications } =
    useReplicationsWidget();

  const chartColors = ['--info-color', '--warning-color'];

  const chartLabels = computed(() => [t('active'), t('paused')]);

  const chartData = computed(() => {
    return [props.activeCount, props.pausedCount];
  });

  function handleArcElementClick(index: number) {
    if (index === StatusItemIndex.PAUSED) {
      showPausedReplications();

      return;
    }

    if (index === StatusItemIndex.ACTIVE) {
      showActiveReplications();
    }
  }
</script>

<template>
  <div class="replications-widget-status-chart">
    <CAspectRatio ratio="1:1">
      <HomeCircularChart
        :data="chartData"
        type="doughnut"
        :colors="chartColors"
        :labels="chartLabels"
        @click-element="handleArcElementClick"
      />
    </CAspectRatio>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-widget-status-chart {
    margin-top: auto;
  }
</style>
