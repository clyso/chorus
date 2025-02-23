<script setup lang="ts">
  import { RouterView } from 'vue-router';
  import {
    CConfigProvider,
    CDialogProvider,
    CNotificationProvider,
  } from '@clyso/clyso-ui-kit';
  import { toRefs } from 'vue';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';

  const { locale } = toRefs(useI18nStore());
  const { colorScheme } = toRefs(useColorSchemeStore());
</script>

<template>
  <CConfigProvider
    class="app"
    :color-scheme="colorScheme"
    :locale="locale"
  >
    <RouterView />

    <CDialogProvider />
    <CNotificationProvider />
  </CConfigProvider>
</template>

<style lang="scss" scoped>
  .app {
    min-height: 100vh;
  }
</style>
