<script setup lang="ts">
  import { computed, ref } from 'vue';
  import { CSkeleton, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { type ChorusStorage } from '@/utils/types/chorus';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import { ChorusService } from '@/services/ChorusService';
  import HomeWidget from '@/components/chorus/common/HomeWidget/HomeWidget.vue';
  import { IconName } from '@/utils/types/icon';
  import { RouteName } from '@/utils/types/router';
  import HomeWidgetAction from '@/components/chorus/common/HomeWidgetAction/HomeWidgetAction.vue';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        storagesOverviewTitle: 'Storages Overview',
        storagesActionLink: 'Go to Storages',
        actionText: 'See Replications',
        errorMessage: 'An error occurred while getting the storages data.',
      },
      [I18nLocale.DE]: {
        storagesOverviewTitle: 'Speicherübersicht',
        storagesActionLink: 'Zu Speichern gehen',
        actionText: 'Replikationen anzeigen',
        errorMessage:
          'Beim Abrufen der Speicherdaten ist ein Fehler aufgetreten.',
      },
    },
  });

  withDefaults(
    defineProps<{
      isPageLoading?: boolean;
    }>(),
    {
      isPageLoading: false,
    },
  );

  const emit = defineEmits<{
    (e: 'loading', value: boolean): void;
  }>();

  const storages = ref<ChorusStorage[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  const mainStorage = computed<ChorusStorage | undefined>(() =>
    storages.value.find((storage) => storage.isMain),
  );
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
    emit('loading', true);

    try {
      await getStorages();
    } finally {
      emit('loading', false);
    }
  }

  initStorages();
</script>

<template>
  <HomeWidget
    :is-loading="isLoading || isPageLoading"
    :has-error="hasError"
    class="storages-widget"
    @retry="initStorages"
  >
    <template #title>
      {{ t('storagesOverviewTitle') }}
    </template>

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

    <template #actions>
      <HomeWidgetAction
        :icon-name="IconName.BASE_SERVER"
        :to="{ name: RouteName.CHORUS_STORAGES }"
        :tooltip-text="t('storagesActionLink')"
      />
    </template>

    <div class="storages-widget__content storages">
      <ChorusStorageCard
        v-if="mainStorage"
        type="success"
        :to="{
          name: RouteName.CHORUS_STORAGE_DETAILS,
          params: { storageName: mainStorage.name },
        }"
        :storage="mainStorage"
      />

      <ChorusStorageCard
        v-for="follower in followerStorages"
        :key="follower.name"
        :to="{
          name: RouteName.CHORUS_STORAGE_DETAILS,
          params: { storageName: follower.name },
        }"
        type="warning"
        :storage="follower"
      />
    </div>
  </HomeWidget>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages-widget {
    grid-column: span 8;

    @media screen and (min-width: utils.$viewport-desktop) and (max-width: 1300px) {
      grid-column: span 8;
    }

    @include utils.touch {
      grid-column: auto;
      width: 100%;
    }
  }

  .storages {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(264px, 1fr));
    gap: utils.unit(3);
    padding-bottom: utils.unit(3);

    &__main {
      margin-bottom: utils.unit(4);
    }
  }
</style>
