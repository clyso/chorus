<script setup lang="ts">
  import { computed } from 'vue';
  import { CIcon, CTooltip } from '@clyso/clyso-ui-kit';
  import { StorageProvider } from '@/utils/types/chorus';
  import { IconName } from '@/utils/types/icon';

  const props = withDefaults(
    defineProps<{
      storageProvider: StorageProvider;
      hasTooltip?: boolean;
    }>(),
    {
      hasTooltip: true,
    },
  );

  const STORAGE_PROVIDER_ICON_MAP = {
    [StorageProvider.AWS]: IconName.PROVIDER_AWS,
    [StorageProvider.Other]: IconName.PROVIDER_OTHER,
    [StorageProvider.Ceph]: IconName.PROVIDER_CEPH,
    [StorageProvider.Minio]: IconName.PROVIDER_MINIO,
    [StorageProvider.GCS]: IconName.PROVIDER_GOOGLE,
    [StorageProvider.Alibaba]: IconName.PROVIDER_ALIBABA,
    [StorageProvider.Cloudflare]: IconName.PROVIDER_CLOUDFLARE,
    [StorageProvider.DigitalOcean]: IconName.PROVIDER_DIGITALOCEAN,
  };

  const storageProviderIconName = computed(
    () => STORAGE_PROVIDER_ICON_MAP[props.storageProvider],
  );
</script>

<template>
  <div class="chorus-storage-provider">
    <CTooltip :disabled="!hasTooltip">
      <template #trigger>
        <CIcon
          class="chorus-storage-provider__icon"
          :class="{
            'chorus-storage-provider__icon--other':
              storageProvider === StorageProvider.Other,
          }"
          :name="storageProviderIconName"
          tabindex="-1"
          :is-inline="true"
        />
      </template>
      <span class="chorus-storage-provider__text">
        {{ storageProvider }}
      </span>
    </CTooltip>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-storage-provider {
    position: relative;

    &__text {
      @include utils.apply-styles(utils.$text-small);
    }

    &__icon {
      width: 24px;
      height: 24px;

      &--other {
        color: var(--primary-color);
      }
    }
  }
</style>
