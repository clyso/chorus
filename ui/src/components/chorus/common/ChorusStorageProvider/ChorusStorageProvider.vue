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
  import { computed } from 'vue';
  import { CIcon, CTooltip } from '@clyso/clyso-ui-kit';
  import { StorageProvider } from '@/utils/types/chorus';
  import { IconName } from '@/utils/types/icon';

  const props = withDefaults(
    defineProps<{
      storageProvider: StorageProvider;
      hasTooltip?: boolean;
    }>(),
    {
      hasTooltip: true,
    },
  );

  const STORAGE_PROVIDER_ICON_MAP = {
    [StorageProvider.S3]: IconName.PROVIDER_OTHER,
    [StorageProvider.SWIFT]: IconName.PROVIDER_OTHER,
  };

  const storageProviderIconName = computed(
    () => STORAGE_PROVIDER_ICON_MAP[props.storageProvider],
  );
</script>

<template>
  <div class="chorus-storage-provider">
    <CTooltip :disabled="!hasTooltip">
      <template #trigger>
        <CIcon
          class="chorus-storage-provider__icon"
          :name="storageProviderIconName"
          tabindex="-1"
          :is-inline="true"
        />
      </template>
      <span class="chorus-storage-provider__text">
        {{ storageProvider }}
      </span>
    </CTooltip>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-storage-provider {
    position: relative;

    &__text {
      @include utils.apply-styles(utils.$text-small);
    }

    &__icon {
      width: 24px;
      height: 24px;
      color: var(--primary-color);
    }
  }
</style>
