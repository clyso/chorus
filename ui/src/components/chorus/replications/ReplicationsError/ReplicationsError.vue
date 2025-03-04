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
  import { useI18n } from 'vue-i18n';
  import { CResult } from '@clyso/clyso-ui-kit';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { initReplicationsPage } = useChorusReplicationsStore();
</script>

<template>
  <CResult
    class="replications-error"
    type="error"
    size="large"
    :max-width="600"
    @positive-click="initReplicationsPage"
  >
    <template #title>
      {{ t('errorTitle') }}
    </template>

    <p>{{ t('errorText') }}</p>

    <template #positive-text>
      {{ t('errorAction') }}
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-error {
    p {
      white-space: pre-line;
    }
  }
</style>
