<script setup lang="ts">
  import { storeToRefs } from 'pinia';
  import { CDateRangePicker } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';

  const { filterCreatedAtRange, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const { t } = useI18n({
    messages: i18nReplications,
  });
</script>

<template>
  <CDateRangePicker
    v-model:value="filterCreatedAtRange"
    class="replications-filter-by-created-at"
    format="d MMM yyyy"
    :start-placeholder="t('filterByCreatedAtStartPlaceholder')"
    :end-placeholder="t('filterByCreatedAtEndPlaceholder')"
    clearable
    @update:value="page = 1"
  />
</template>
