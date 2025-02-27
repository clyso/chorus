<script setup lang="ts">
  import { storeToRefs } from 'pinia';
  import {
    CColorSchemeToggle,
    CDashboardHeader,
    CDashboardLayout,
    CLanguageSelect,
  } from '@clyso/clyso-ui-kit';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import DashboardNav from '@/components/dashboard/DashboardNav/DashboardNav.vue';
  import { IS_DEV_ENV } from '@/utils/constants/env';
  import DashboardLogo from '@/components/dashboard/DashboardLogo/DashboardLogo.vue';

  const { locale } = storeToRefs(useI18nStore());
  const { setLocale } = useI18nStore();

  const { colorScheme } = storeToRefs(useColorSchemeStore());
  const { setColorScheme } = useColorSchemeStore();
</script>

<template>
  <CDashboardLayout
    class="dashboard-view"
    :has-side-menu="false"
  >
    <template #header>
      <CDashboardHeader
        :options="[]"
        :has-side-menu="false"
        :has-user-menu="false"
      >
        <template #start>
          <DashboardLogo />
        </template>

        <template #end>
          <CLanguageSelect
            v-if="IS_DEV_ENV"
            :value="locale"
            @update:value="setLocale"
          />

          <CColorSchemeToggle
            :value="colorScheme"
            @update:value="setColorScheme"
          />
        </template>
      </CDashboardHeader>
    </template>

    <main class="dashboard-view__main">
      <DashboardNav class="dashboard-view__nav" />
      <div class="dashboard-view__render-view">
        <RouterView />
      </div>
    </main>
  </CDashboardLayout>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .dashboard-view {
    &__main {
      height: 100%;
      max-width: 100vw;
      padding: utils.unit(5) utils.unit(8);
      display: grid;
      grid-template-rows: auto 1fr;

      @include utils.tablet-only {
        padding: utils.unit(5) utils.unit(6);
      }

      @include utils.mobile {
        padding: utils.unit(3) utils.unit(3);
      }
    }

    &__nav {
      margin-bottom: utils.unit(8);
      min-width: 0;
    }

    &__render-view {
      min-width: 0;
    }

    ::v-deep(.c-dashboard-header) {
      z-index: 2;
    }
  }
</style>
