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
  import { useI18n } from 'vue-i18n';
  import { CResult } from '@clyso/clyso-ui-kit';
  import i18nStorages from '@/components/chorus/storages/i18nStorages';
  import { useChorusStoragesStore } from '@/stores/chorusStoragesStore';

  const { t } = useI18n({
    messages: i18nStorages,
  });

  const { initStorages } = useChorusStoragesStore();
</script>

<template>
  <CResult
    class="storages-error"
    type="error"
    size="large"
    :max-width="600"
    @positive-click="initStorages"
  >
    <template #title>
      {{ t('storagesErrorTitle') }}
    </template>

    <p>{{ t('storagesErrorText') }}</p>

    <template #positive-text>
      {{ t('storagesErrorAction') }}
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages-error {
    min-height: 600px;

    p {
      white-space: pre-line;
    }
  }
</style>
