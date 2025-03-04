<!--
  - Copyright © 2025 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { onBeforeMount, onUnmounted } from 'vue';
  import { storeToRefs } from 'pinia';
  import {
    CBreadcrumb,
    CBreadcrumbItem,
    CDashboardPage,
    CSkeleton,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import StorageDetailsError from '@/components/chorus/storage-details/StorageDetailsError/StorageDetailsError.vue';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';
  import { RouteName } from '@/utils/types/router';
  import i18nStorageDetails from '@/components/chorus/storage-details/i18nStorageDetails';
  import StorageGeneral from '@/components/chorus/storage-details/StorageGeneral/StorageGeneral.vue';
  import ChorusStorageProvider from '@/components/chorus/common/ChorusStorageProvider/ChorusStorageProvider.vue';

  const props = defineProps<{
    storageName: string;
  }>();

  const { t } = useI18n({
    messages: i18nStorageDetails,
  });

  const { hasError, isLoading, storage } = storeToRefs(
    useChorusStorageDetailsStore(),
  );
  const { initStorageDetails, $reset } = useChorusStorageDetailsStore();

  onBeforeMount(() => {
    initStorageDetails(props.storageName);
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <CDashboardPage class="storage-details-page">
    <template #breadcrumbs>
      <CBreadcrumb class="storage-details-page__breadcrumb">
        <CBreadcrumbItem :to="{ name: RouteName.CHORUS_STORAGES }">
          {{ t('storagesBreadcrumb') }}
        </CBreadcrumbItem>
        <CBreadcrumbItem :is-active="true">
          {{ t('storageDetailsBreadcrumb') }}
        </CBreadcrumbItem>
      </CBreadcrumb>
    </template>

    <template #title>
      <Transition
        name="opacity"
        mode="out-in"
      >
        <div
          v-if="isLoading"
          key="loading"
          class="storage-details-page__title storage-details-page__title--loading"
        >
          <CSkeleton
            :height="24"
            :width="24"
            :border-radius="24"
          />
          <CSkeleton
            :height="17"
            :width="220"
            :padding-block="10"
            :border-radius="4"
          />
        </div>
        <div
          v-else
          key="name"
          class="storage-details-page__title"
        >
          <ChorusStorageProvider
            v-if="storage?.provider"
            :storage-provider="storage.provider"
            :has-tooltip="false"
          />
          {{ storageName }}
        </div>
      </Transition>
    </template>

    <StorageDetailsError
      v-if="hasError"
      key="error"
    />
    <div
      v-else
      key="content"
      class="storage-details-page__content"
    >
      <StorageGeneral class="storage-details-page__general" />
    </div>
  </CDashboardPage>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storage-details-page {
    padding: 0;

    &__title {
      display: flex;
      align-items: center;
      gap: utils.unit(3);
    }

    &__general {
      margin-bottom: utils.unit(3);
    }
  }
</style>
