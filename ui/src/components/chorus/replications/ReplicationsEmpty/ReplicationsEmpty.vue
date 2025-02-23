<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CResult, CIcon, CButton } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useRouter } from 'vue-router';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { IconName } from '@/utils/types/icon';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { RouteName } from '@/utils/types/router';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { isFiltered } = storeToRefs(useChorusReplicationsStore());

  const router = useRouter();

  const { clearFilters } = useChorusReplicationsStore();

  function handleAction() {
    if (!isFiltered.value) {
      router.push({ name: RouteName.CHORUS_ADD_REPLICATION });

      return;
    }

    clearFilters();
  }
</script>

<template>
  <CResult
    class="replications-empty"
    type="empty"
    size="large"
    :icon-name="IconName.BASE_SWAP_HORIZONTAL"
    :max-width="600"
  >
    <template #title>
      {{ t(isFiltered ? 'filterNoResultsTitle' : 'noResultsTitle') }}
    </template>

    <p>{{ t(isFiltered ? 'filterNoResultsText' : 'noResultsText') }}</p>

    <template #actions>
      <CButton
        type="primary"
        size="large"
        @click="handleAction"
      >
        <template #icon>
          <CIcon
            :is-inline="true"
            :name="isFiltered ? 'close' : IconName.BASE_ADD"
          />
        </template>

        {{ t(isFiltered ? 'filterNoResultsAction' : 'addReplicationAction') }}
      </CButton>
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-empty {
    p {
      white-space: pre-line;
    }
  }
</style>
