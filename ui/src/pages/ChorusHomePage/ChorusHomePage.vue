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
  import { computed, ref } from 'vue';
  import ProxyUptimeWidget from '@/components/chorus/home/ProxyUptimeWidget/ProxyUptimeWidget.vue';
  import { HAS_PROMETHEUS } from '@/utils/constants/env';
  import WorkerUptimeWidget from '@/components/chorus/home/WorkerUptimeWidget/WorkerUptimeWidget.vue';
  import StoragesWidget from '@/components/chorus/home/StoragesWidget/StoragesWidget.vue';
  import ProxyWidget from '@/components/chorus/home/ProxyWidget/ProxyWidget.vue';
  import ReplicationsWidget from '@/components/chorus/home/ReplicationsWidget/ReplicationsWidget.vue';

  const loadingState = ref<number>(0);
  const isLoadingFinished = ref(false);
  const isPageLoading = computed(
    () => !isLoadingFinished.value || loadingState.value > 0,
  );

  function setLoadingState(value: boolean) {
    if (value) {
      loadingState.value += 1;

      return;
    }

    loadingState.value -= 1;

    if (loadingState.value === 0) {
      isLoadingFinished.value = true;
    }
  }
</script>

<template>
  <div class="chorus-home-page">
    <div class="chorus-home-page__widgets">
      <ProxyUptimeWidget
        v-if="HAS_PROMETHEUS"
        :is-page-loading="isPageLoading"
        :is-enabled="HAS_PROMETHEUS"
        @loading="setLoadingState"
      />
      <WorkerUptimeWidget
        v-if="HAS_PROMETHEUS"
        :is-page-loading="isPageLoading"
        :is-enabled="HAS_PROMETHEUS"
        @loading="setLoadingState"
      />

      <StoragesWidget
        :is-page-loading="isPageLoading"
        @loading="setLoadingState"
      />
      <ProxyWidget
        :is-page-loading="isPageLoading"
        @loading="setLoadingState"
      />
      <ReplicationsWidget
        :is-page-loading="isPageLoading"
        @loading="setLoadingState"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-home-page {
    max-width: 1800px;

    &__widgets {
      display: grid;
      grid-template-columns: repeat(12, minmax(50px, 1fr));
      gap: utils.unit(3);
      margin-bottom: utils.unit(3);

      @media screen and (min-width: utils.$viewport-desktop) and (max-width: 1300px) {
        grid-template-columns: repeat(8, minmax(100px, 1fr));
      }

      @include utils.touch {
        grid-template-columns: auto;
      }

      &:last-child {
        margin-bottom: 0;
      }
    }
  }
</style>
