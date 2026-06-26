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
  import { CShortList } from '@clyso/clyso-ui-kit';
  import type { AddId } from '@/utils/types/helper';
  import type { DiffReport } from '@/utils/types/chorus';

  withDefaults(
    defineProps<{
      reports: AddId<DiffReport>[];
      max?: number;
      size?: 'small' | 'medium';
    }>(),
    {
      max: 5,
      size: 'small',
    },
  );
</script>

<template>
  <CShortList
    :size="size"
    :max="max"
    :list="reports"
    :item-key="(item) => item.idStr"
    class="diff-reports-short-list"
  >
    <template #default="{ item }: { item: AddId<DiffReport> }">
      <template
        v-bind:key="index"
        v-for="(location, index) in item.locations"
      >
        <template v-if="index > 0"> → </template>
        {{ location.storage }}/<span class="diff-reports-short-list__bucket">{{
          location.bucket
        }}</span>
      </template>
    </template>
  </CShortList>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-short-list {
    &__bucket {
      font-weight: utils.$font-weight-medium;
    }
  }
</style>
