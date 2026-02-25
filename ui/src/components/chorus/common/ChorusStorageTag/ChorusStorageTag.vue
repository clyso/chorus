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
  import { CTag, CTooltip } from '@clyso/clyso-ui-kit';

  type CTagProps = InstanceType<typeof CTag>['$props'];

  const props = withDefaults(
    defineProps<{
      storageName: string;
      tooltip?: string;
      size?: CTagProps['size'];
      type?: CTagProps['type'];
    }>(),
    {
      size: 'small',
      type: 'success',
      tooltip: undefined,
    },
  );
</script>

<template>
  <div class="chorus-storage-tag">
    <CTooltip
      :delay="500"
      :disabled="!props.tooltip"
    >
      <template #trigger>
        <CTag
          :bordered="false"
          round
          class="chorus-storage-tag__storage-name"
          :size="props.size"
          :type="props.type"
        >
          {{ props.storageName }}
        </CTag>
      </template>
      <span class="chorus-storage-tag__storage-tooltip">
        <strong>{{ props.storageName }}</strong
        >: {{ props.tooltip }}
      </span>
    </CTooltip>
  </div>
</template>
