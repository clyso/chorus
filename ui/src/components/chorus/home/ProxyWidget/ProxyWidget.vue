<script setup lang="ts">
  import { ref } from 'vue';
  import {
    CButton,
    CDescriptionItem,
    CDescriptionList,
    CResult,
    CSkeleton,
    CTile,
    I18nLocale,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { type ChorusProxyCredentials } from '@/utils/types/chorus';
  import { ChorusService } from '@/services/ChorusService';
  import ProxyCredentialsModal from '@/components/chorus/home/ProxyWidget/ProxyCredentialsModal.vue';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        credentials: 'Credentials',
        seeCredentials: 'See Credentials',
        proxyCredentialsTitle: 'S3 Proxy Credentials',
        errorMessage: 'An error occurred while getting the proxy data.',
      },
      [I18nLocale.DE]: {
        credentials: 'Credentials',
        seeCredentials: 'See Credentials',
        proxyCredentialsTitle: 'S3 Proxy Credentials',
        errorMessage: 'An error occurred while getting the proxy data.',
      },
    },
  });

  withDefaults(
    defineProps<{
      isInitializing?: boolean;
    }>(),
    {
      isInitializing: false,
    },
  );

  const emit = defineEmits<{
    (e: 'init', value: boolean): void;
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
    emit('init', true);

    try {
      await getProxy();
    } finally {
      emit('init', false);
    }
  }

  initProxy();
</script>

<template>
  <CTile
    class="proxy-widget"
    :is-loading="isLoading || isInitializing"
  >
    <template #title>
      {{ t('proxyCredentialsTitle') }}
    </template>

    <div class="proxy-widget__content">
      <CResult
        v-if="hasError"
        type="error"
        size="tiny"
        class="proxy-widget__error-result"
        @positive-click="getProxy"
      >
        <template #title>
          {{ t('errorTitle') }}
        </template>
        {{ t('errorMessage') }}
      </CResult>

      <div
        v-else-if="proxy"
        class="proxy"
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
    </div>

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
  </CTile>
</template>
