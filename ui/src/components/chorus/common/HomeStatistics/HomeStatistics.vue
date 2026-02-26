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
  import { CButton, CSkeleton } from '@clyso/clyso-ui-kit';

  withDefaults(
    defineProps<{
      type?: 'default' | 'info' | 'success' | 'warning' | 'error' | 'primary';
      size?: 'small' | 'medium' | 'large';
      align?: 'left' | 'center' | 'right';
      isLoading?: boolean;
      labelWhiteSpace?: 'wrap' | 'nowrap';
      isClickable?: boolean;
    }>(),
    {
      type: 'default',
      size: 'medium',
      align: 'left',
      isLoading: false,
      labelWhiteSpace: 'wrap',
      isClickable: false,
    },
  );

  const emit = defineEmits<{
    (e: 'click'): void;
  }>();
</script>

<template>
  <div
    class="home-statistics"
    :class="`home-statistics--${type} home-statistics--${size} home-statistics--${align}`"
  >
    <div
      v-if="isLoading"
      key="loading"
      class="home-statistics__label home-statistics__label--loading"
    >
      <CSkeleton
        key="loading"
        class="home-statistics__label-loading"
        :padding-block="4"
        :height="11"
      />
    </div>
    <div
      v-else
      key="label"
      :class="{
        'home-statistics__label--no-wrap': labelWhiteSpace === 'nowrap',
      }"
      class="home-statistics__label"
    >
      <slot name="label"> Label </slot>
    </div>

    <CButton
      v-if="isClickable && !isLoading"
      text
      class="home-statistics__value"
      :class="{ 'home-statistics__value--clickable': isClickable }"
      @click="
        () => {
          emit('click');
        }
      "
    >
      <slot></slot>
    </CButton>
    <div
      v-else-if="isLoading"
      class="home-statistics__value home-statistics__value--loading"
    >
      <CSkeleton
        v-if="isLoading"
        key="loading"
        class="home-statistics__value-loading"
        :padding-block="2"
        height="100%"
        :border-radius="4"
      />
    </div>
    <div
      v-else
      class="home-statistics__value"
    >
      <slot key="value"></slot>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  $small-size: (
    font-size: 24px,
    height: 24px,
  );
  $small-size-mobile: (
    font-size: 18px,
    height: 18px,
  );
  $medium-size: (
    font-size: 32px,
    height: 36px,
  );
  $medium-size-mobile: (
    font-size: 24px,
    height: 24px,
  );
  $large-size: (
    font-size: 42px,
    height: 42px,
  );
  $large-size-mobile: (
    font-size: 32px,
    height: 32px,
  );

  .home-statistics {
    display: inline-grid;

    &--info {
      .home-statistics__value {
        color: var(--info-color);
      }
    }

    &--success {
      .home-statistics__value {
        color: var(--success-color);
      }
    }

    &--warning {
      .home-statistics__value {
        color: var(--warning-color);
      }
    }

    &--error {
      .home-statistics__value {
        color: var(--error-color);
      }
    }

    &--primary {
      .home-statistics__value {
        color: var(--primary-color);
      }
    }

    &--small {
      .home-statistics__value {
        @include utils.apply-styles($small-size);

        @include utils.mobile {
          @include utils.apply-styles($small-size-mobile);
        }
      }
    }

    &--large {
      .home-statistics__value {
        @include utils.apply-styles($large-size);

        @include utils.mobile {
          @include utils.apply-styles($large-size-mobile);
        }
      }
    }

    &--center {
      justify-items: center;

      .home-statistics__label,
      .home-statistics__value {
        text-align: center;
      }

      .home-statistics__label-loading,
      .home-statistics__value-loading {
        margin: 0 auto;
      }
    }

    &--left {
      justify-items: start;

      .home-statistics__label,
      .home-statistics__value {
        text-align: left;
      }
    }

    &--right {
      justify-items: end;

      .home-statistics__label,
      .home-statistics__value {
        text-align: right;
      }

      .home-statistics__label-loading,
      .home-statistics__value-loading {
        margin-left: auto;
        margin-right: 0;
      }
    }

    &__label {
      @include utils.apply-styles(utils.$text-caption);
      color: var(--text-color-3);
      line-height: 1.3;

      &--loading {
        width: 100%;
      }

      &--no-wrap {
        white-space: nowrap;
      }
    }

    &__label-loading {
      width: 60%;
    }

    &__value {
      display: inline;
      line-height: 1;
      font-weight: utils.$font-weight-medium;
      @include utils.apply-styles($medium-size);

      &--loading {
        width: 100%;
      }

      @include utils.mobile {
        @include utils.apply-styles($medium-size-mobile);
      }

      &--clickable {
        transition: opacity utils.$transition-duration;

        &:hover {
          opacity: 0.7;
        }

        &:active {
          opacity: 0.9;
        }

        ::v-deep(.c-button__content) {
          font-weight: utils.$font-weight-medium;
        }
      }
    }

    &__value-loading {
      height: 100%;
      width: 100%;

      ::v-deep(.c-skeleton__type-group) {
        height: 100%;

        .c-skeleton__item > * {
          height: 100%;
        }
      }
    }
  }
</style>
