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
  import { CBreadcrumb, CBreadcrumbItem } from '@clyso/clyso-ui-kit';
  import { onBeforeMount, onUnmounted } from 'vue';
  import { RouteName } from '@/utils/types/router';
  import AddReplicationTile from '@/components/chorus/add-replication/AddReplicationTile/AddReplicationTile.vue';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';

  const { initAddReplicationPage, $reset } = useChorusAddReplicationStore();

  onBeforeMount(() => {
    initAddReplicationPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <div class="chorus-add-replication-page">
    <CBreadcrumb class="chorus-add-replication-page__breadcrumb">
      <CBreadcrumbItem :to="{ name: RouteName.CHORUS_REPLICATION }">
        Replication
      </CBreadcrumbItem>
      <CBreadcrumbItem :is-active="true"> Add Replication </CBreadcrumbItem>
    </CBreadcrumb>

    <AddReplicationTile />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-add-replication-page {
    &__breadcrumb {
      margin-bottom: utils.unit(2);
    }
  }
</style>
