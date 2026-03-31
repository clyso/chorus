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
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
  import type { ChorusStorage } from '@/utils/types/chorus';

  const store = useChorusAddRoutingPolicyStore();
  const { selectedToStorage, storages, isBlockOnly, validator } =
    storeToRefs(store);

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  const storageOptions = computed(() =>
    GeneralHelper.orderBy(storages.value, 'isMain', 'asc'),
  );

  function selectStorage(storage: ChorusStorage) {
    if (selectedToStorage.value?.name === storage.name) {
      selectedToStorage.value = null;
    } else {
      selectedToStorage.value = storage;
    }
  }
</script>

<template>
  <div class="storage-selection">
    <div class="storage-selection__header">
      <h5 class="storage-selection__title">{{ t('storageSelectionTitle') }}</h5>
      <p class="storage-selection__description">
        {{ t('storageSelectionDescription') }}
      </p>
    </div>

    <div class="storage-selection__content">
      <ChorusStorageCard
        v-for="storage in storageOptions"
        v-model="selectedToStorage"
        :key="storage.name"
        :storage="storage"
        is-selectable
        is-deselectable
        :type="storage.isMain ? 'success' : 'warning'"
        :is-selected="storage.name === selectedToStorage?.name"
        size="small"
        @select="selectStorage(storage)"
        @deselect="selectStorage(storage)"
      />
    </div>
    <span
      v-if="validator.selectedToStorage.$error"
      class="storage-selection__error-text"
    >
      {{ t('storageSelectionRequired') }}
    </span>
    <span
      v-if="isBlockOnly"
      class="storage-selection__hint-text"
    >
      {{ t('storageSelectionBlockedOnlyHint') }}
    </span>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storage-selection {
    &__title,
    &__description {
      margin-bottom: utils.unit(1);
    }

    &__content {
      display: flex;
      gap: utils.unit(2);
    }

    &__error-text {
      color: var(--error-color);
      font-size: 12px;
    }

    &__hint-text {
      color: var(--warning-color);
      font-size: 12px;
    }
  }
</style>
