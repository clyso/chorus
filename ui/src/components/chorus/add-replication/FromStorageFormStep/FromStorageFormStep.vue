<script setup lang="ts">
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import { CCollapseTransition, CSkeleton, CAlert } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';

  const { storages, selectedFromStorage, isMainAsSourceAlertShown, isLoading } =
    storeToRefs(useChorusAddReplicationStore());

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const fromStoragesOptions = computed(() =>
    GeneralHelper.orderBy(storages.value, 'isMain', 'desc'),
  );
</script>

<template>
  <div class="from-storage-form-step">
    <div
      v-if="isLoading"
      key="loading"
      class="from-storage-form-step__loading"
    >
      <CSkeleton
        type="text"
        width="39.5%"
        :margin-bottom="8"
      />

      <div class="from-storage-form-step__storages-list">
        <CSkeleton
          v-for="(_, index) in Array(4)"
          :key="index"
          :margin-bottom="12"
          :border-radius="8"
          :height="106"
        />
      </div>
    </div>

    <div
      v-else
      key="content"
      class="from-storage-form-step__content"
    >
      <p class="from-storage-form-step__title">
        {{ t('fromStorageStepTitle') }}
      </p>

      <div class="from-storage-form-step__storages-list">
        <ChorusStorageCard
          v-for="(storage, index) in fromStoragesOptions"
          :key="index"
          :storage="storage"
          :is-selectable="true"
          :type="storage.isMain ? 'success' : 'warning'"
          :is-disabled="!storage.isMain"
          :is-selected="storage.name === selectedFromStorage?.name"
          @select="selectedFromStorage = storage"
        />
      </div>

      <CCollapseTransition
        :show="isMainAsSourceAlertShown"
        class="from-storage-form-step__alert-wrapper"
        appear
      >
        <CAlert
          type="info"
          class="from-storage-form-step__alert"
          closable
          @close="isMainAsSourceAlertShown = false"
        >
          <template #header>
            {{ t('alertTitle') }}
          </template>

          {{ t('alertDescription') }}
        </CAlert>
      </CCollapseTransition>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .from-storage-form-step {
    &__title {
      margin-bottom: utils.unit(2);
    }

    &__storages-list {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: utils.unit(3);
    }

    &__alert-wrapper {
      margin-top: utils.unit(6);
      display: flex;
    }
  }
</style>
