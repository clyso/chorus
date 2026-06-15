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
  import { CResult } from '@clyso/clyso-ui-kit';

  withDefaults(
    defineProps<{
      title: string;
      text: string;
      actionText: string;
      maxWidth?: number;
    }>(),
    {
      maxWidth: 600,
    },
  );

  const emit = defineEmits<{
    (e: 'retry'): void;
  }>();
</script>

<template>
  <CResult
    class="chorus-list-error"
    type="error"
    size="large"
    :max-width="maxWidth"
    @positive-click="emit('retry')"
  >
    <template #title>
      {{ title }}
    </template>

    <p>{{ text }}</p>

    <template #positive-text>
      {{ actionText }}
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  .chorus-list-error {
    p {
      white-space: pre-line;
    }
  }
</style>
