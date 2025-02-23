<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import { CSelect } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { ReplicationStatusFilter } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { filterStatuses, page } = storeToRefs(useChorusReplicationsStore());

  const statusOptions = computed<
    {
      label: string;
      value: ReplicationStatusFilter;
    }[]
  >(() => [
    {
      value: ReplicationStatusFilter.ACTIVE,
      label: t('filterStatusActive'),
    },
    {
      value: ReplicationStatusFilter.PAUSED,
      label: t('filterStatusPaused'),
    },
    {
      value: ReplicationStatusFilter.INITIAL_IN_PROGRESS,
      label: t('filterStatusInitialInProgress'),
    },
    {
      value: ReplicationStatusFilter.INITIAL_DONE,
      label: t('filterStatusInitialDone'),
    },
    {
      value: ReplicationStatusFilter.LIVE_BEHIND,
      label: t('filterStatusLiveBehind'),
    },
    {
      value: ReplicationStatusFilter.LIVE_UP_TO_DATE,
      label: t('filterStatusLiveUpToDate'),
    },
  ]);
</script>

<template>
  <CSelect
    v-model:value="filterStatuses"
    class="replications-filter-by-status"
    clearable
    multiple
    :placeholder="t('filterByStatusPlaceholder')"
    :options="statusOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  />
</template>
