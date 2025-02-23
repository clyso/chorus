<script setup lang="ts">
  import {
    CColorSchemeToggle,
    CIcon,
    CLanguageSelect,
  } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { IconName } from '@/utils/types/icon';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import { IS_DEV_ENV } from '@/utils/constants/env';

  const { locale } = storeToRefs(useI18nStore());
  const { setLocale } = useI18nStore();

  const { colorScheme } = storeToRefs(useColorSchemeStore());
  const { setColorScheme } = useColorSchemeStore();
</script>

<template>
  <header class="auth-header">
    <div class="container">
      <div class="auth-header__inner">
        <a
          target="_blank"
          href="https://www.clyso.com/"
          rel="noopener noreferrer"
          class="auth-header__logo-link"
        >
          <CIcon
            class="auth-header__logo-icon"
            :name="IconName.BASE_LOGO_CLYSO"
          />
          Clyso
        </a>

        <div class="auth-header__actions">
          <CLanguageSelect
            v-if="IS_DEV_ENV"
            :value="locale"
            @update:value="setLocale"
          />
          <CColorSchemeToggle
            :value="colorScheme"
            @update:value="setColorScheme"
          />
        </div>
      </div>
    </div>
  </header>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .auth-header {
    &__inner {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-top: utils.unit(10);
      padding-bottom: utils.unit(10);

      @include utils.mobile {
        padding-top: utils.unit(6);
        padding-bottom: utils.unit(6);
      }
    }

    &__logo-link {
      display: inline-flex;
      font-size: 0;
    }

    &__logo-icon {
      width: 170px;
      fill: var(--primary-color);
      opacity: 0.3;
      transition: opacity utils.$transition-duration;

      @include utils.mobile {
        width: 100px;
      }

      &:hover {
        opacity: 0.2;
      }

      &:active {
        opacity: 0.25;
      }
    }

    &__actions {
      display: flex;
      align-items: center;
      gap: utils.unit(3);
    }
  }
</style>
