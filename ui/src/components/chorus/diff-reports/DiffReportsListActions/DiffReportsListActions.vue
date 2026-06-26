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

<script setup lang="ts"></script>

<template>
  <div class="diff-reports-list-actions">
    <div class="diff-reports-list-actions__selection-actions"></div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-list-actions {
    display: flex;
    justify-content: space-between;
    gap: utils.unit(2);

    &__selection-actions {
      display: inline-flex;
      align-items: center;
      gap: utils.unit(3);

      ::v-deep(.c-badge-sup) {
        pointer-events: none;
      }
    }
  }
</style>
