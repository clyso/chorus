<script setup lang="ts">
  import AuthHeader from '@/components/auth/AuthHeader/AuthHeader.vue';
  import AuthPageContent from '@/components/auth/AuthPageContent/AuthPageContent.vue';
</script>

<template>
  <div class="auth-page">
    <AuthHeader class="auth-page__header" />

    <AuthPageContent class="auth-page__content" />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .auth-page {
    min-height: 100vh;
    display: grid;
    grid-template-rows: auto 1fr;
  }
</style>
