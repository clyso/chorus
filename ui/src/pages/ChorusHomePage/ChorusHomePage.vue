<script setup lang="ts">
  import { computed, ref } from 'vue';
  import ProxyUptimeWidget from '@/components/chorus/home/ProxyUptimeWidget/ProxyUptimeWidget.vue';
  import { HAS_PROMETHEUS } from '@/utils/constants/env';
  import WorkerUptimeWidget from '@/components/chorus/home/WorkerUptimeWidget/WorkerUptimeWidget.vue';
  import StoragesWidget from '@/components/chorus/home/StoragesWidget/StoragesWidget.vue';
  import ProxyWidget from '@/components/chorus/home/ProxyWidget/ProxyWidget.vue';
  import ReplicationsWidget from '@/components/chorus/home/ReplicationsWidget/ReplicationsWidget.vue';

  const initState = ref<number>(0);
  const isInitFinished = ref(false);
  const isPageInitializing = computed(
    () => !isInitFinished.value || initState.value > 0,
  );

  function setInitState(value: boolean) {
    if (value) {
      initState.value += 1;

      return;
    }

    initState.value -= 1;

    if (initState.value === 0) {
      isInitFinished.value = true;
    }
  }
</script>

<template>
  <div class="home-page">
    <div class="home-page__row home-page__row--1">
      <ProxyUptimeWidget
        :is-initializing="isPageInitializing"
        :is-enabled="HAS_PROMETHEUS"
        @init="setInitState"
      />
      <WorkerUptimeWidget
        :is-initializing="isPageInitializing"
        :is-enabled="HAS_PROMETHEUS"
        @init="setInitState"
      />
    </div>

    <div class="home-page__row home-page__row--2">
      <StoragesWidget
        class="home-page__storages"
        :is-initializing="isPageInitializing"
        @init="setInitState"
      />
      <ReplicationsWidget
        class="home-page__replications"
        :is-initializing="isPageInitializing"
        @init="setInitState"
      />
      <ProxyWidget
        class="home-page__proxy"
        :is-initializing="isPageInitializing"
        @init="setInitState"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .home-page {
    display: grid;
    gap: 12px;

    &__row {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
      gap: 12px;

      @include utils.mobile {
        grid-template-columns: auto;
      }

      &--2 {
        @include utils.touch {
          grid-template-columns: auto;
        }
      }
    }

    &__storages {
      grid-column: span 2;

      @include utils.touch {
        grid-column: span 1;
      }
    }

    &__proxy {
      grid-column: span 1;
      align-self: start;
    }

    &__replications {
      grid-column: span 1;
      align-self: start;
    }
  }
</style>
