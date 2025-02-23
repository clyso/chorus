<script setup lang="ts">
  import { storeToRefs } from 'pinia';
  import { toRefs, watch } from 'vue';
  import AuthPage from '@/components/auth/AuthPage/AuthPage.vue';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import {
    useVantaBackground,
    VantaType,
  } from '@/utils/composables/vantaBackground';
  import LoginForm from '@/components/auth/LoginForm/LoginForm.vue';

  const { isDark } = storeToRefs(useColorSchemeStore());
  const { vantaEffect } = toRefs(
    useVantaBackground(VantaType.DOTS, {
      el: '.login-page',
      mouseControls: true,
      touchControls: false,
      gyroControls: false,
      minHeight: 200.0,
      minWidth: 200.0,
      scale: 1.0,
      scaleMobile: 1.0,
      color: 0x84cddf,
      backgroundColor: isDark.value ? 0x101014 : 0xffffff,
      showLines: false,
    }),
  );

  watch(isDark, () => {
    vantaEffect.value?.setOptions({
      backgroundColor: isDark.value ? 0x101014 : 0xffffff,
    });
  });
</script>

<template>
  <AuthPage class="login-page">
    <LoginForm />
  </AuthPage>
</template>
