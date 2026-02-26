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
  import { onBeforeMount, onBeforeUnmount } from 'vue';
  import { storeToRefs } from 'pinia';
  import StoragesTile from '@/components/chorus/storages/StoragesTile/StoragesTile.vue';
  import { useChorusStoragesStore } from '@/stores/chorusStoragesStore';
  import StoragesError from '@/components/chorus/storages/StoragesError/StoragesError.vue';

  const { hasError } = storeToRefs(useChorusStoragesStore());
  const { initStorages, $reset } = useChorusStoragesStore();

  onBeforeMount(() => initStorages());
  onBeforeUnmount($reset);
</script>

<template>
  <div class="storages-page">
    <StoragesError
      v-if="hasError"
      key="error"
    />
    <StoragesTile
      v-else
      key="tile"
    />
  </div>
</template>
