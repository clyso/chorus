<!--
  - Copyright © 2026 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { CResult, CSkeleton, CTile, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        widgetErrorTitle: 'Error',
        widgetErrorText:
          'There was an issue loading the widget. Please try again later.',
        actionRetry: 'Retry',
      },
      [I18nLocale.DE]: {
        widgetErrorTitle: 'Fehler',
        widgetErrorText:
          'Beim Laden des Widgets ist ein Problem aufgetreten. Bitte versuchen Sie es später erneut.',
        actionRetry: 'Wiederholen',
      },
    },
  });

  withDefaults(
    defineProps<{
      isLoading?: boolean;
      hasError?: boolean;
      isEmpty?: boolean;
      iconName?: IconName;
    }>(),
    {
      isLoading: false,
      hasError: false,
      isEmpty: false,
      iconName: undefined,
    },
  );

  const emit = defineEmits<{
    (e: 'retry'): void;
  }>();
</script>

<template>
  <CTile
    :is-loading="isLoading"
    class="home-widget"
  >
    <template #title>
      <slot name="title"></slot>
    </template>

    <template
      v-if="$slots.actions"
      #header-extra
    >
      <div class="home-widget__actions">
        <slot name="actions"></slot>
      </div>
    </template>

    <CResult
      v-if="hasError"
      type="error"
      size="small"
      class="home-widget__error-result"
      @positive-click="
        () => {
          emit('retry');
        }
      "
    >
      <template #title>
        <slot name="error-title">
          {{ t('widgetErrorTitle') }}
        </slot>
      </template>

      <slot name="error-text">
        {{ t('widgetErrorText') }}
      </slot>

      <template #positive-text>
        <slot name="error-retry">
          {{ t('actionRetry') }}
        </slot>
      </template>
    </CResult>
    <CResult
      v-else-if="isEmpty"
      key="empty"
      type="empty"
      size="small"
      :icon-name="iconName"
      class="home-widget__empty-result"
    >
      <template #title>
        <slot name="empty-title"> Title </slot>
      </template>

      <slot name="empty-text"> Text </slot>

      <template #actions>
        <slot name="empty-actions"> </slot>
      </template>
    </CResult>
    <div
      v-else
      class="home-widget__content"
    >
      <slot></slot>
    </div>

    <template #loading-header>
      <CSkeleton
        v-if="$slots.title"
        :padding-block="3"
        :height="17"
        :width="200"
      />

      <div
        v-if="$slots.actions"
        class="home-widget__actions"
      >
        <CSkeleton
          v-for="(_, index) in $slots.actions()"
          :key="index"
          :height="28"
          :width="28"
          :border-radius="14"
        />
      </div>
    </template>

    <template #loading-content>
      <slot name="loading-content"></slot>
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .home-widget {
    ::v-deep(.c-tile__content) {
      padding-top: 0;
    }

    &__actions {
      display: flex;
      align-items: center;
      gap: utils.unit(2);
    }

    &__content {
      height: 100%;
    }
  }
</style>
