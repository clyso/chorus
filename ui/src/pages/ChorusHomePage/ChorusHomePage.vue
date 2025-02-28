<script setup lang="ts">
  import { computed, ref } from 'vue';
  import ProxyUptimeWidget from '@/components/chorus/home/widgets/ProxyUptimeWidget/ProxyUptimeWidget.vue';
  import { HAS_PROMETHEUS } from '@/utils/constants/env';
  import WorkerUptimeWidget from '@/components/chorus/home/widgets/WorkerUptimeWidget/WorkerUptimeWidget.vue';
  import StoragesWidget from '@/components/chorus/home/widgets/StoragesWidget/StoragesWidget.vue';

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
        :is-initializing="isPageLoading"
        :is-enabled="HAS_PROMETHEUS"
        @loading="setLoadingState"
      />
      <WorkerUptimeWidget
        :is-initializing="isPageLoading"
        :is-enabled="HAS_PROMETHEUS"
        @loading="setLoadingState"
      />

      <StoragesWidget
        :is-initializing="isPageLoading"
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
