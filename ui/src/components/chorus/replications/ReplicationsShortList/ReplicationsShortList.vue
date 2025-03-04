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
  import { CShortList } from '@clyso/clyso-ui-kit';
  import type { AddId } from '@/utils/types/helper';
  import type { ChorusReplication } from '@/utils/types/chorus';

  withDefaults(
    defineProps<{
      replications: AddId<ChorusReplication>[];
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
    :list="replications"
    :item-key="(item) => item.id"
    class="replications-short-list"
  >
    <template #default="{ item }: { item: AddId<ChorusReplication> }">
      <span class="replications-short-list__bucket">{{ item.bucket }}</span>
      (<span class="replications-short-list__user">{{ item.user }}</span
      >, <span>{{ item.from }} → {{ item.to }}</span
      >)
    </template>
  </CShortList>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-short-list {
    &__bucket,
    &__user {
      font-weight: utils.$font-weight-medium;
    }
  }
</style>
