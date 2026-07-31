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
  import { CTag } from '@clyso/clyso-ui-kit';
  import type { DiffReportStorageEntry } from '@/utils/types/chorus';

  defineProps<{
    storageEntries: DiffReportStorageEntry[];
  }>();
</script>

<template>
  <div class="diff-report-detail-entries-storage-cell">
    <CTag
      v-for="(entry, index) in storageEntries"
      :key="index"
      :bordered="false"
      round
      size="small"
      type="success"
    >
      {{ entry.storage }} · {{ entry.bucket }}
    </CTag>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-entries-storage-cell {
    display: flex;
    flex-wrap: wrap;
    gap: utils.unit(1);
  }
</style>
