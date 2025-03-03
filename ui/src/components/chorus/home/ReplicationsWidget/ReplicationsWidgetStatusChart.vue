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
