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
  import { storeToRefs } from 'pinia';
  import { CSkeleton } from '@clyso/clyso-ui-kit';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import ReplicationsFilterByUser from '@/components/chorus/replications/ReplicationsFilterByUser/ReplicationsFilterByUser.vue';
  import ReplicationsFilterByBucket from '@/components/chorus/replications/ReplicationsFilterByBucket/ReplicationsFilterByBucket.vue';
  import ReplicationsFilterByToStorage from '@/components/chorus/replications/ReplicationsFilterByToStorage/ReplicationsFilterByToStorage.vue';
  import ReplicationsFilterByStatus from '@/components/chorus/replications/ReplicationsFilterByStatus/ReplicationsFilterByStatus.vue';
  import ReplicationsFilterByCreatedAt from '@/components/chorus/replications/ReplicationsFilterByCreatedAt/ReplicationsFilterByCreatedAt.vue';

  const { isLoading } = storeToRefs(useChorusReplicationsStore());
</script>

<template>
  <div class="replications-filters">
    <div
      v-if="isLoading"
      key="loading"
      class="replications-filters__list"
    >
      <CSkeleton
        v-for="(_, index) in Array(5)"
        :key="index"
        :height="34"
        :border-radius="4"
      />
    </div>
    <div
      v-else
      key="filters"
      class="replications-filters__list"
    >
      <ReplicationsFilterByUser class="replications-filters__user" />

      <ReplicationsFilterByBucket class="replications-filters__bucket" />

      <ReplicationsFilterByCreatedAt class="replications-filters__created-at" />

      <ReplicationsFilterByToStorage class="replications-filters__to-storage" />

      <ReplicationsFilterByStatus class="replications-filters__bucket" />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-filters {
    padding: 24px 16px;
    border-radius: 12px;
    background-color: var(--filters-card-color);
    border: 1px solid var(--border-color);

    @include utils.mobile {
      padding: 0;
      border-radius: 0;
      background-color: unset;
      border: 0;
    }

    &__list {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: utils.unit(5) utils.unit(3);
      align-items: start;
    }
  }
</style>
