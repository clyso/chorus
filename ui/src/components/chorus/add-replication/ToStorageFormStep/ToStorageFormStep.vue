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
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';

  const { storages, selectedToStorage } = storeToRefs(
    useChorusAddReplicationStore(),
  );
  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const fromStoragesOptions = computed(() =>
    GeneralHelper.orderBy(storages.value, 'isMain', 'asc'),
  );
</script>

<template>
  <div class="to-storage-form-step">
    <p class="to-storage-form-step__title">
      {{ t('toStorageStepTitle') }}
    </p>

    <div class="to-storage-form-step__storages-list">
      <ChorusStorageCard
        v-for="(storage, index) in fromStoragesOptions"
        :key="index"
        :storage="storage"
        :is-selectable="true"
        :type="storage.isMain ? 'success' : 'warning'"
        :is-disabled="storage.isMain"
        :is-selected="storage.name === selectedToStorage?.name"
        @select="selectedToStorage = storage"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .to-storage-form-step {
    &__title {
      margin-bottom: utils.unit(2);
    }

    &__storages-list {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(264px, 1fr));
      gap: utils.unit(3);
    }
  }
</style>
