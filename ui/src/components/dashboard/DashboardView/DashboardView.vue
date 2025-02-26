<script setup lang="ts">
  import { storeToRefs } from 'pinia';
  import {
    CColorSchemeToggle,
    CDashboardHeader,
    CDashboardLayout,
    CIcon,
    CLanguageSelect,
  } from '@clyso/clyso-ui-kit';
  import { RouteName } from '@/utils/types/router';
  import { IconName } from '@/utils/types/icon';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import DashboardNav from '@/components/dashboard/DashboardNav/DashboardNav.vue';
  import { IS_DEV_ENV } from '@/utils/constants/env';

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
          <div class="logo-wrapper">
            <RouterLink :to="{ name: RouteName.CHORUS_HOME }">
              <div class="logo">
                <CIcon
                  :is-inline="true"
                  class="logo__icon"
                  :name="IconName.CUSTOM_CHORUS_LOGO"
                />
                <span class="logo__text">Chorus</span>
              </div>
            </RouterLink>
            <a
              href="https://clyso.com"
              rel="noopener"
              target="_blank"
              class="clyso-logo"
            >
              <span class="clyso-logo__text">by</span>
              <CIcon
                class="clyso-logo__icon"
                :name="IconName.BASE_LOGO_CLYSO"
              />
            </a>
          </div>
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
  }

  .logo {
    @include utils.apply-styles(utils.$text-h1);
    line-height: 0.8;
    display: flex;
    align-items: center;
    gap: utils.unit(2);
    height: 40px;
    font-family: utils.$font-highlight;

    @include utils.mobile {
      @include utils.apply-styles(utils.$text-h2);
    }

    &__icon {
      width: 40px;
      height: 40px;
      color: var(--primary-color);

      @include utils.mobile {
        width: 30px;
        height: 30px;
      }
    }

    &__text {
      z-index: 1;
      letter-spacing: -0.08em;
    }
  }

  .logo-wrapper {
    display: flex;
    align-items: center;
    gap: utils.unit(3);
  }

  .clyso-logo {
    font-size: 6px;
    line-height: 1;
    font-family: utils.$font-highlight;
    display: flex;
    align-items: center;
    gap: utils.unit(1);
    opacity: 0.5;
    transition: opacity utils.$transition-duration;

    &:hover {
      opacity: 0.6;
    }

    &:active {
      opacity: 0.45;
    }

    &__icon {
      width: 50px;
      height: auto;
      position: relative;
    }

    &__text {
      position: relative;
      top: 2px;
    }
  }
</style>
