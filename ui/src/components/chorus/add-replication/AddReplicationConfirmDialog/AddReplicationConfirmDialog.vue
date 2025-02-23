<script setup lang="ts">
  import { CDialog } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import AddReplicationSummary from '@/components/chorus/add-replication/AddReplicationSummary/AddReplicationSummary.vue';

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const { isConfirmDialogOpen } = storeToRefs(useChorusAddReplicationStore());
  const { createReplication } = useChorusAddReplicationStore();
</script>

<template>
  <CDialog
    class="add-replication-confirm-dialog"
    type="confirm"
    :is-shown="isConfirmDialogOpen"
    :width="500"
    :positive-handler="createReplication"
    @update:is-shown="
      (value) => {
        isConfirmDialogOpen = value;
      }
    "
  >
    <template #title>
      {{ t('confirmReplicationTitle') }}
    </template>

    <div class="confirmation-details">
      <p class="confirmation-details__description">
        {{ t('confirmReplicationDescription') }}
      </p>

      <AddReplicationSummary />
    </div>

    <template #positive-text>
      {{ t('confirmReplicationPositive') }}
    </template>
    <template #negative-text>
      {{ t('confirmReplicationNegative') }}
    </template>
  </CDialog>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .confirmation-details {
    &__description {
      margin-bottom: utils.unit(4);
    }
  }
</style>
