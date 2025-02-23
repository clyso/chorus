<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CResult } from '@clyso/clyso-ui-kit';
  import i18nStorages from '@/components/chorus/storages/i18nStorages';
  import { useChorusStoragesStore } from '@/stores/chorusStoragesStore';

  const { t } = useI18n({
    messages: i18nStorages,
  });

  const { initStorages } = useChorusStoragesStore();
</script>

<template>
  <CResult
    class="storages-error"
    type="error"
    size="large"
    :max-width="600"
    @positive-click="initStorages"
  >
    <template #title>
      {{ t('storagesErrorTitle') }}
    </template>

    <p>{{ t('storagesErrorText') }}</p>

    <template #positive-text>
      {{ t('storagesErrorAction') }}
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages-error {
    min-height: 600px;

    p {
      white-space: pre-line;
    }
  }
</style>
