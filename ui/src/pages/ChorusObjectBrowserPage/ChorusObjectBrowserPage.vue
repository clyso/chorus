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
  import { computed, onBeforeMount, ref } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { ChorusService } from '@/services/ChorusService';
  import type {
    ChorusBrowserObject,
    ChorusStorage,
  } from '@/utils/types/chorus';
  import { StorageProvider } from '@/utils/types/chorus';

  const { t } = useI18n();
  const storages = ref<ChorusStorage[]>([]);
  const selectedStorage = ref('');
  const selectedUser = ref('');
  const buckets = ref<string[]>([]);
  const selectedBucket = ref('');
  const prefix = ref('');
  const objects = ref<ChorusBrowserObject[]>([]);
  const nextAfter = ref('');
  const loadedPrefix = ref('');
  const hasMore = ref(false);
  const isLoadingStorages = ref(false);
  const isLoadingBuckets = ref(false);
  const isLoadingObjects = ref(false);
  const errorMessage = ref('');
  let objectRequestId = 0;

  const availableStorages = computed(() =>
    storages.value.filter(
      (storage) =>
        storage.provider === StorageProvider.S3 &&
        storage.credentials.length > 0,
    ),
  );

  const selectedStorageInfo = computed(() =>
    availableStorages.value.find(
      (storage) => storage.name === selectedStorage.value,
    ),
  );

  onBeforeMount(loadStorages);

  async function loadStorages() {
    isLoadingStorages.value = true;
    errorMessage.value = '';
    try {
      const response = await ChorusService.getStorages();
      storages.value = response.storages;
      const firstStorage = availableStorages.value[0];
      if (firstStorage) {
        selectedStorage.value = firstStorage.name;
        selectedUser.value = firstStorage.credentials[0].alias;
        await loadBuckets();
      }
    } catch (error) {
      setError(error);
    } finally {
      isLoadingStorages.value = false;
    }
  }

  async function loadBuckets() {
    objectRequestId += 1;
    isLoadingObjects.value = false;
    buckets.value = [];
    selectedBucket.value = '';
    objects.value = [];
    nextAfter.value = '';
    hasMore.value = false;
    if (!selectedStorage.value || !selectedUser.value) {
      return;
    }

    isLoadingBuckets.value = true;
    errorMessage.value = '';
    try {
      const response = await ChorusService.getBrowserBuckets({
        storage: selectedStorage.value,
        user: selectedUser.value,
      });
      buckets.value = response.buckets;
      selectedBucket.value = '';
    } catch (error) {
      setError(error);
    } finally {
      isLoadingBuckets.value = false;
    }
  }

  async function loadObjects(reset: boolean) {
    if (!selectedStorage.value || !selectedUser.value || !selectedBucket.value) {
      return;
    }

    isLoadingObjects.value = true;
    errorMessage.value = '';
    const requestId = ++objectRequestId;
    const requestedPrefix = reset ? prefix.value.trim() : loadedPrefix.value;
    try {
      const response = await ChorusService.getBrowserObjects({
        storage: selectedStorage.value,
        user: selectedUser.value,
        bucket: selectedBucket.value,
        prefix: requestedPrefix,
        after: reset ? '' : nextAfter.value,
        limit: 50,
      });
      if (requestId !== objectRequestId) {
        return;
      }
      objects.value = reset
        ? response.objects
        : [...objects.value, ...response.objects];
      if (reset) {
        loadedPrefix.value = requestedPrefix;
      }
      nextAfter.value = response.nextAfter;
      hasMore.value = response.hasMore;
    } catch (error) {
      if (requestId === objectRequestId) {
        setError(error);
      }
    } finally {
      if (requestId === objectRequestId) {
        isLoadingObjects.value = false;
      }
    }
  }

  async function loadMore() {
    await loadObjects(prefix.value.trim() !== loadedPrefix.value);
  }

  async function handleStorageChange() {
    selectedUser.value = selectedStorageInfo.value?.credentials[0]?.alias ?? '';
    await loadBuckets();
  }

  async function handleUserChange() {
    await loadBuckets();
  }

  async function handleBucketChange() {
    await loadObjects(true);
  }

  function setError(error: unknown) {
    errorMessage.value = error instanceof Error ? error.message : t('loadError');
  }

  function formatTimestamp(value: string) {
    if (!value) {
      return t('timestampNotRecorded');
    }
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short',
    }).format(new Date(value));
  }

  function formatSize(value: string) {
    const size = Number(value);
    if (size < 1024) {
      return `${size} B`;
    }
    const units = ['KB', 'MB', 'GB', 'TB'];
    let displaySize = size;
    let unitIndex = -1;
    do {
      displaySize /= 1024;
      unitIndex += 1;
    } while (displaySize >= 1024 && unitIndex < units.length - 1);
    return `${displaySize.toFixed(1)} ${units[unitIndex]}`;
  }
</script>

<template>
  <section class="object-browser">
    <h1 class="object-browser__title">{{ t('objectBrowserTitle') }}</h1>

    <form
      class="object-browser__filters"
      @submit.prevent="loadObjects(true)"
    >
      <label class="object-browser__field">
        <span>{{ t('storage') }}</span>
        <select
          v-model="selectedStorage"
          :disabled="isLoadingStorages || availableStorages.length === 0"
          @change="handleStorageChange"
        >
          <option
            v-for="storage in availableStorages"
            :key="storage.name"
            :value="storage.name"
          >
            {{ storage.name }}
          </option>
        </select>
      </label>

      <label class="object-browser__field">
        <span>{{ t('user') }}</span>
        <select
          v-model="selectedUser"
          :disabled="!selectedStorageInfo || isLoadingBuckets"
          @change="handleUserChange"
        >
          <option
            v-for="credential in selectedStorageInfo?.credentials ?? []"
            :key="credential.alias"
            :value="credential.alias"
          >
            {{ credential.alias }}
          </option>
        </select>
      </label>

      <label class="object-browser__field">
        <span>{{ t('bucket') }}</span>
        <select
          v-model="selectedBucket"
          :disabled="buckets.length === 0 || isLoadingBuckets"
          @change="handleBucketChange"
        >
          <option value="">{{ t('selectBucket') }}</option>
          <option
            v-for="bucket in buckets"
            :key="bucket"
            :value="bucket"
          >
            {{ bucket }}
          </option>
        </select>
      </label>

      <label class="object-browser__field object-browser__field--prefix">
        <span>{{ t('prefix') }}</span>
        <input
          v-model="prefix"
          type="text"
          autocomplete="off"
        />
      </label>

      <button
        class="object-browser__button"
        type="submit"
        :disabled="!selectedBucket || isLoadingObjects"
      >
        {{ isLoadingObjects ? t('loading') : t('loadObjects') }}
      </button>
    </form>

    <p
      v-if="errorMessage"
      class="object-browser__error"
      role="alert"
    >
      {{ errorMessage }}
    </p>
    <p
      v-else-if="!isLoadingStorages && availableStorages.length === 0"
      class="object-browser__empty"
    >
      {{ t('noS3Storages') }}
    </p>

    <div class="object-browser__table-wrap">
      <table class="object-browser__table">
        <thead>
          <tr>
            <th>{{ t('objectName') }}</th>
            <th>{{ t('size') }}</th>
            <th>{{ t('destinationModified') }}</th>
            <th>{{ t('originalTimestamp') }}</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="object in objects"
            :key="object.key"
          >
            <td class="object-browser__key">{{ object.key }}</td>
            <td>{{ formatSize(object.size) }}</td>
            <td>{{ formatTimestamp(object.lastModified) }}</td>
            <td>{{ formatTimestamp(object.sourceLastModified) }}</td>
          </tr>
          <tr v-if="!isLoadingObjects && objects.length === 0 && selectedBucket">
            <td
              class="object-browser__empty-row"
              colspan="4"
            >
              {{ t('noObjectsFound') }}
            </td>
          </tr>
          <tr v-if="isLoadingStorages || isLoadingBuckets || isLoadingObjects">
            <td
              class="object-browser__empty-row"
              colspan="4"
            >
              {{ t('loading') }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <button
      v-if="hasMore"
      class="object-browser__button object-browser__button--more"
      type="button"
      :disabled="isLoadingObjects"
      @click="loadMore"
    >
      {{ isLoadingObjects ? t('loading') : t('loadMore') }}
    </button>
  </section>
</template>

<style lang="scss" scoped>
  .object-browser {
    min-width: 0;
    color: #242b29;

    &__title {
      margin: 0 0 24px;
      font-size: 22px;
      font-weight: 600;
    }

    &__filters {
      display: flex;
      align-items: end;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 20px;
      padding-bottom: 18px;
      border-bottom: 1px solid #d5dbd8;
    }

    &__field {
      display: grid;
      gap: 6px;
      min-width: 150px;
      color: #38423e;
      font-size: 13px;

      select,
      input {
        box-sizing: border-box;
        height: 38px;
        min-width: 0;
        padding: 0 10px;
        border: 1px solid #aeb8b3;
        border-radius: 4px;
        background: #fff;
        color: #242b29;
        font: inherit;
      }

      &--prefix {
        flex: 1 1 180px;
      }
    }

    &__button {
      min-height: 38px;
      padding: 0 14px;
      border: 1px solid #176b58;
      border-radius: 4px;
      background: #176b58;
      color: #fff;
      font: inherit;
      font-weight: 600;
      cursor: pointer;

      &:disabled {
        cursor: not-allowed;
        opacity: 0.55;
      }

      &--more {
        margin-top: 16px;
      }
    }

    &__table-wrap {
      max-width: 100%;
      overflow-x: auto;
    }

    &__table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;

      th,
      td {
        padding: 11px 12px;
        border-bottom: 1px solid #e0e5e2;
        vertical-align: top;
      }

      th {
        color: #38423e;
        font-size: 12px;
        font-weight: 600;
        white-space: nowrap;
      }

      td {
        font-size: 13px;
      }
    }

    &__key {
      min-width: 220px;
      overflow-wrap: anywhere;
    }

    &__error {
      color: #a12622;
    }

    &__empty {
      padding: 18px 0;
      color: #56615c;
    }

    &__empty-row {
      padding-block: 24px !important;
      color: #56615c;
      text-align: center;
    }
  }

  @media (max-width: 640px) {
    .object-browser {
      &__filters {
        align-items: stretch;
        flex-direction: column;
      }

      &__field,
      &__button {
        width: 100%;
      }
    }
  }
</style>