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
  import { CSkeleton, CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import i18nStorages from '@/components/chorus/storages/i18nStorages';
  import { useChorusStoragesStore } from '@/stores/chorusStoragesStore';
  import { RouteName } from '@/utils/types/router';

  const { t } = useI18n({
    messages: i18nStorages,
  });

  const { isLoading, mainStorage, followerStorages } = storeToRefs(
    useChorusStoragesStore(),
  );
</script>

<template>
  <CTile
    :is-loading="isLoading"
    class="storages-tile"
  >
    <template #title>
      {{ t('storagesTitle') }}
    </template>

    <div class="storages-tile__content">
      <div class="storages-blocks-list">
        <div
          v-if="mainStorage"
          class="storage-block storage-block--main"
        >
          <h2 class="storage-block__title">
            {{ t('mainStorageTitle') }}
          </h2>

          <div class="storage-block__storages">
            <RouterLink
              :to="{
                name: RouteName.CHORUS_STORAGE_DETAILS,
                params: { storageName: mainStorage.name },
              }"
            >
              <ChorusStorageCard
                v-if="mainStorage"
                type="success"
                :storage="mainStorage"
                :is-selectable="true"
              />
            </RouterLink>
          </div>
        </div>

        <div class="storage-block storage-block--followers">
          <h2 class="storage-block__title">
            {{ t('followersStoragesTitle') }}
          </h2>

          <div class="storage-block__storages">
            <RouterLink
              v-for="follower in followerStorages"
              :key="follower.name"
              :to="{
                name: RouteName.CHORUS_STORAGE_DETAILS,
                params: { storageName: follower.name },
              }"
            >
              <ChorusStorageCard
                type="warning"
                :is-selectable="true"
                :storage="follower"
              />
            </RouterLink>
          </div>
        </div>
      </div>
    </div>

    <template #loading-content>
      <div class="storage-block">
        <CSkeleton
          class="storage-block__title"
          :height="12"
          :width="200"
          :padding-block="4"
          :border-radius="8"
        />

        <div class="storage-block__storages">
          <CSkeleton
            :height="106"
            :border-radius="8"
          />
        </div>
      </div>
      <div class="storage-block">
        <CSkeleton
          class="storage-block__title"
          :height="12"
          :width="200"
          :padding-block="4"
          :border-radius="8"
        />

        <div class="storage-block__storages">
          <CSkeleton
            v-for="(_, index) in Array(3)"
            :key="index"
            :height="106"
            :border-radius="8"
          />
        </div>
      </div>
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages-tile {
    padding-bottom: utils.unit(10);
  }

  .storage-block {
    margin-bottom: utils.unit(10);

    &:last-child {
      margin-bottom: 0;
    }

    &__title {
      @include utils.apply-styles(utils.$text-h5);
      font-family: utils.$font-primary;
      margin-bottom: utils.unit(2);
    }

    &__storages {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(264px, 1fr));
      gap: utils.unit(3);
    }
  }
</style>
