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
  import { onBeforeMount, onUnmounted } from 'vue';
  import DiffReportsTile from '@/components/chorus/diff-reports/DiffReportsTile/DiffReportsTile.vue';
  import { useChorusDiffReportsStore } from '@/stores/chorusDiffReportsStore';

  const { initDiffReportPage, $reset } = useChorusDiffReportsStore();

  onBeforeMount(() => {
    initDiffReportPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <div class="diff-reports-page">
    <DiffReportsTile />
  </div>
</template>

<style lang="scss" scoped>
  .diff-reports-page {
    height: 100%;
  }
</style>
