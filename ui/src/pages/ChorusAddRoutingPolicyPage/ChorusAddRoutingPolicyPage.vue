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
  import { CBreadcrumb, CBreadcrumbItem } from '@clyso/clyso-ui-kit';
  import { onBeforeMount, onUnmounted } from 'vue';
  import { RouteName } from '@/utils/types/router';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';
  import AddRoutingPolicyTile from '@/components/chorus/add-routing-policies/AddRoutingPolicyTile/AddRoutingPolicyTile.vue';

  const { initAddRoutingPolicyPage, $reset } = useChorusAddRoutingPolicyStore();

  onBeforeMount(() => {
    initAddRoutingPolicyPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <div class="chorus-add-routing-policy-page">
    <CBreadcrumb class="chorus-add-routing-policy-page__breadcrumb">
      <CBreadcrumbItem :to="{ name: RouteName.CHORUS_ROUTING_POLICIES }">
        Routing Policies
      </CBreadcrumbItem>
      <CBreadcrumbItem :is-active="true"> Add Routing Policy </CBreadcrumbItem>
    </CBreadcrumb>

    <AddRoutingPolicyTile />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-add-routing-policy-page {
    &__breadcrumb {
      margin-bottom: utils.unit(2);
    }
  }
</style>
