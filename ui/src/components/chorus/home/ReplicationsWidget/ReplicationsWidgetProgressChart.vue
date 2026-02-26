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

  enum ProgressItemIndex {
    UP_TO_DATE = 0,
    IN_PROGRESS = 1,
    BEHIND = 2,
  }

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        inProgress: 'Progress: In progress',
        upToDate: 'Progress: Up to date',
        behind: 'Progress: Behind',
      },
      [I18nLocale.DE]: {
        inProgress: 'Fortschritt: In Bearbeitung',
        upToDate: 'Fortschritt: Aktuell',
        behind: 'Fortschritt: Hinterher',
      },
    },
  });

  const props = defineProps<{
    upToDateCount: number;
    inProgressCount: number;
    behindCount: number;
  }>();

  const {
    showUpToDateReplications,
    showInProgressReplications,
    showBehindReplications,
  } = useReplicationsWidget();

  const chartColors = ['--success-color', '--info-color', '--behind-color'];

  const chartLabels = computed(() => [
    t('upToDate'),
    t('inProgress'),
    t('behind'),
  ]);

  const chartData = computed(() => {
    return [props.upToDateCount, props.inProgressCount, props.behindCount];
  });

  function handleArcElementClick(index: number) {
    if (index === ProgressItemIndex.UP_TO_DATE) {
      showUpToDateReplications();

      return;
    }

    if (index === ProgressItemIndex.IN_PROGRESS) {
      showInProgressReplications();

      return;
    }

    if (index === ProgressItemIndex.BEHIND) {
      showBehindReplications();
    }
  }
</script>

<template>
  <div class="replications-widget-progress-chart">
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

  .replications-widget-progress-chart {
    margin-top: auto;
  }
</style>
