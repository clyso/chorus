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
  import { CSkeleton } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import RoutingPoliciesFilterByUser from '../RoutingPoliciesFilterByUser/RoutingPoliciesFilterByUser.vue';
  import RoutingPoliciesFilterByBucket from '../RoutingPoliciesFilterByBucket/RoutingPoliciesFilterByBucket.vue';
  import RoutingPoliciesFilterByStorage from '../RoutingPoliciesFilterByStorage/RoutingPoliciesFilterByStorage.vue';
  import RoutingPoliciesFilterByStatus from '../RoutingPoliciesFilterByStatus/RoutingPoliciesFilterByStatus.vue';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';

  const { isLoading } = storeToRefs(useChorusRoutingPoliciesStore());
</script>

<template>
  <div class="routing-policies-filters">
    <div
      v-if="isLoading"
      key="loading"
      class="routing-policies-filters__list"
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
      class="routing-policies-filters__list"
    >
      <RoutingPoliciesFilterByUser class="routing-policies-filters__user" />

      <RoutingPoliciesFilterByBucket class="routing-policies-filters__bucket" />

      <RoutingPoliciesFilterByStorage
        class="routing-policies-filters__storage"
      />

      <RoutingPoliciesFilterByStatus class="routing-policies-filters__status" />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .routing-policies-filters {
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
