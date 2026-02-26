<!--
  - Copyright © 2026 Clyso GmbH
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
  import {
    CTile,
    CSkeleton,
    CDescriptionList,
    CDescriptionItem,
    DASHBOARD_NAV_META_INJECT_KEY,
    getDefaultNavMeta,
    CTag,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { inject, readonly, ref } from 'vue';
  import i18nStorages from '@/components/chorus/storages/i18nStorages';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';

  const { t } = useI18n({
    messages: i18nStorages,
  });

  const { isLoading, storage } = storeToRefs(useChorusStorageDetailsStore());

  const navMeta = inject(
    DASHBOARD_NAV_META_INJECT_KEY,
    readonly(ref(getDefaultNavMeta())),
  );
</script>

<template>
  <CTile
    :is-loading="isLoading"
    class="storage-details-tile"
  >
    <template #title>
      {{ t('storageGeneralTitle') }}
    </template>

    <div class="storage-details-tile__content">
      <CDescriptionList
        v-if="storage"
        class="storage-field-list"
        :label-placement="navMeta.isMobile ? 'top' : 'left'"
        :columns="1"
      >
        <CDescriptionItem class="storage-field-list__item">
          <template #label> {{ t('nameLabel') }}: </template>

          {{ storage.name }}
        </CDescriptionItem>
        <CDescriptionItem class="storage-field-list__item">
          <template #label> {{ t('addressLabel') }}: </template>

          {{ storage.address }}
        </CDescriptionItem>
        <CDescriptionItem class="storage-field-list__item">
          <template #label> {{ t('providerLabel') }}: </template>

          {{ storage.provider }}
        </CDescriptionItem>
        <CDescriptionItem class="storage-field-list__item">
          <template #label> {{ t('typeLabel') }}: </template>

          <CTag
            size="small"
            :type="storage.isMain ? 'success' : 'warning'"
          >
            {{ $t(storage.isMain ? 'mainStorage' : 'followerStorage') }}
          </CTag>
        </CDescriptionItem>
      </CDescriptionList>
    </div>

    <template #loading-content>
      <div
        v-for="(_, index) in Array(4)"
        :key="index"
        class="loading-row"
        :class="{ 'loading-row--mobile': navMeta.isMobile }"
      >
        <CSkeleton
          :padding-block="11"
          :height="12"
          :width="130"
        />
        <CSkeleton
          :padding-block="11"
          :height="12"
        />
      </div>
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storage-field-list {
    ::v-deep(.c-description-item--left) {
      grid-template-columns: auto minmax(0, 1fr);
    }

    ::v-deep(.c-description-item__label) {
      line-height: 34px;
      min-width: 130px;
    }

    ::v-deep(.c-description-item__value) {
      min-width: 0;
      align-self: center;
    }
  }

  .loading-row {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: center;
    gap: utils.unit(3);
    margin-bottom: utils.unit(3);

    &--mobile {
      grid-template-columns: initial;
      gap: 0;
    }

    &:last-child {
      margin-bottom: 0;
    }

    > * {
      flex-grow: 1;
    }
  }
</style>
