<script setup lang="ts">
  import { onBeforeMount, ref } from 'vue';
  import {
    CButton,
    CDescriptionItem,
    CDescriptionList,
    CSkeleton,
    I18nLocale,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import ProxyCredentialsModal from './ProxyCredentialsModal.vue';
  import { type ChorusProxyCredentials } from '@/utils/types/chorus';
  import { ChorusService } from '@/services/ChorusService';
  import HomeWidget from '@/components/chorus/common/HomeWidget/HomeWidget.vue';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        credentials: 'Credentials',
        seeCredentials: 'See Credentials',
        proxyCredentialsTitle: 'S3 Proxy Credentials',
        errorMessage: 'An error occurred while getting the proxy data.',
      },
      [I18nLocale.DE]: {
        credentials: 'Anmeldeinformationen',
        seeCredentials: 'Anzeigen',
        proxyCredentialsTitle: 'S3-Proxy-Anmeldeinformationen',
        errorMessage:
          'Beim Abrufen der Proxy-Daten ist ein Fehler aufgetreten.',
      },
    },
  });

  withDefaults(
    defineProps<{
      isPageLoading?: boolean;
    }>(),
    {
      isPageLoading: false,
    },
  );

  const emit = defineEmits<{
    (e: 'loading', value: boolean): void;
  }>();

  const proxy = ref<ChorusProxyCredentials | null>(null);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);
  const isCredentialsModalShown = ref<boolean>(false);

  async function getProxy() {
    isLoading.value = true;
    hasError.value = false;

    try {
      proxy.value = await ChorusService.getProxyCredentials();
    } catch {
      hasError.value = true;
    } finally {
      isLoading.value = false;
    }
  }

  async function initProxy() {
    emit('loading', true);

    try {
      await getProxy();
    } finally {
      emit('loading', false);
    }
  }

  onBeforeMount(initProxy);
</script>

<template>
  <HomeWidget
    class="proxy-widget"
    :is-loading="isLoading || isPageLoading"
    :has-error="hasError"
    @retry="initProxy"
  >
    <template #title>
      {{ t('proxyCredentialsTitle') }}
    </template>

    <template #loading-content>
      <CSkeleton
        type="text"
        :repeat="2"
      />
      <CSkeleton
        type="text"
        width="60%"
      />
    </template>

    <div
      v-if="proxy"
      class="proxy-widget__content proxy"
    >
      <CDescriptionList
        class="proxy-widget__content-list"
        label-placement="left"
        :columns="1"
      >
        <CDescriptionItem>
          <template #label> {{ t('address') }}: </template>
          {{ proxy.address }}
        </CDescriptionItem>

        <CDescriptionItem>
          <template #label> {{ t('credentials') }}: </template>
          <CButton
            text
            size="small"
            class="proxy-widget__credentials-button"
            type="primary"
            @click="isCredentialsModalShown = true"
          >
            {{ t('seeCredentials') }}
          </CButton>
        </CDescriptionItem>
      </CDescriptionList>

      <ProxyCredentialsModal
        v-model:is-shown="isCredentialsModalShown"
        :proxy="proxy"
      />
    </div>
  </HomeWidget>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .proxy-widget {
    grid-column: span 4;
    align-self: start;

    @media screen and (min-width: utils.$viewport-desktop) and (max-width: 1300px) {
      grid-column: span 3;
    }

    @include utils.touch {
      grid-column: auto;
      width: 100%;
    }

    &__credentials-button {
      &:focus-visible {
        opacity: 0.5;
      }
    }
  }
</style>
