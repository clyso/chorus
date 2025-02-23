<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CResult } from '@clyso/clyso-ui-kit';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { initReplicationsPage } = useChorusReplicationsStore();
</script>

<template>
  <CResult
    class="replications-error"
    type="error"
    size="large"
    :max-width="600"
    @positive-click="initReplicationsPage"
  >
    <template #title>
      {{ t('errorTitle') }}
    </template>

    <p>{{ t('errorText') }}</p>

    <template #positive-text>
      {{ t('errorAction') }}
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-error {
    p {
      white-space: pre-line;
    }
  }
</style>
