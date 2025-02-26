<script setup lang="ts">
  import { computed, ref } from 'vue';
  import { CResult, CSkeleton, CTile, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { type ChorusStorage } from '@/utils/types/chorus';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import { ChorusService } from '@/services/ChorusService';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        storagesOverviewTitle: 'Storages Overview',
        actionText: 'See Replications',
        errorMessage: 'An error occurred while getting the storages data.',
      },
      [I18nLocale.DE]: {
        storagesOverviewTitle: 'Speicherübersicht',
        actionText: 'Replikationen anzeigen',
        errorMessage:
          'Beim Abrufen der Speicherdaten ist ein Fehler aufgetreten.',
      },
    },
  });

  withDefaults(
    defineProps<{
      isInitializing?: boolean;
    }>(),
    {
      isInitializing: false,
    },
  );

  const emit = defineEmits<{
    (e: 'init', value: boolean): void;
  }>();

  const storages = ref<ChorusStorage[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  const mainStorage = computed<ChorusStorage | undefined>(() => {
    console.log(storages.value);

    return storages.value.find((storage) => storage.isMain);
  });
  const followerStorages = computed<ChorusStorage[]>(() =>
    storages.value.filter((storage) => !storage.isMain),
  );

  async function getStorages() {
    isLoading.value = true;
    hasError.value = false;

    try {
      const { storages: storagesValue } = await ChorusService.getStorages();

      storages.value = storagesValue ?? [];
    } catch {
      hasError.value = true;
    } finally {
      isLoading.value = false;
    }
  }

  async function initStorages() {
    emit('init', true);

    try {
      await getStorages();
    } finally {
      emit('init', false);
    }
  }

  initStorages();
</script>

<template>
  <CTile
    class="storages-widget"
    :is-loading="isLoading || isInitializing"
  >
    <template #title>
      {{ t('storagesOverviewTitle') }}
    </template>

    <div class="storages-widget__content">
      <CResult
        v-if="hasError"
        type="error"
        size="tiny"
        class="storages-widget__error-result"
        @positive-click="getStorages"
      >
        <template #title>
          {{ t('errorTitle') }}
        </template>
        {{ t('errorMessage') }}
      </CResult>
      <div
        v-else
        class="storages"
      >
        <ChorusStorageCard
          v-if="mainStorage"
          type="success"
          :storage="mainStorage"
        />

        <ChorusStorageCard
          v-for="follower in followerStorages"
          :key="follower.name"
          type="warning"
          :storage="follower"
        />
      </div>
    </div>

    <template #loading-content>
      <div class="storages">
        <CSkeleton
          :height="106"
          :border-radius="8"
        />
        <CSkeleton
          :height="106"
          :border-radius="8"
        />
        <CSkeleton
          :height="106"
          :border-radius="8"
        />
      </div>
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages-widget {
    &__error-result {
      height: 106px;
    }
  }

  .storages {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: utils.unit(3);
    padding-bottom: utils.unit(3);

    &__main {
      margin-bottom: utils.unit(4);
    }
  }
</style>
