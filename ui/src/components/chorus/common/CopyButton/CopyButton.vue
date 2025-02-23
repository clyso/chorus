<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CCopyButton, I18nLocale } from '@clyso/clyso-ui-kit';

  withDefaults(
    defineProps<{
      text: string;
      size?: 'tiny' | 'small' | 'medium' | 'large';
    }>(),
    {
      size: 'tiny',
    },
  );

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        copied: 'Copied!',
        failed: 'Copy failed!',
      },
      [I18nLocale.DE]: {
        copied: 'Copied!',
        failed: 'Copy failed!',
      },
    },
  });
</script>

<template>
  <CCopyButton
    class="copy-button"
    :size="size"
    :text="text"
    :success-text="t('copied')"
    :error-text="t('failed')"
  />
</template>
