<script setup lang="ts">
  import {
    CDescriptionItem,
    CDescriptionList,
    CTag,
  } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import type { RouteLocationRaw } from 'vue-router';
  import type { ChorusStorage } from '@/utils/types/chorus';
  import ChorusStorageProvider from '@/components/chorus/common/ChorusStorageProvider/ChorusStorageProvider.vue';

  const props = withDefaults(
    defineProps<{
      storage: ChorusStorage;
      type?: 'success' | 'warning' | 'info' | 'error';
      to?: RouteLocationRaw; // to be used as link
      isSelectable?: boolean;
      isSelected?: boolean;
      isDisabled?: boolean;
      size?: 'medium' | 'small';
    }>(),
    {
      type: 'warning',
      isSelectable: false,
      isSelected: false,
      isDisabled: false,
      to: undefined,
      size: 'medium',
    },
  );

  const storageCardClasses = computed(() => [
    `chorus-storage-card--${props.type}`,
    `chorus-storage-card--${props.size}`,
    {
      'chorus-storage-card--link': !!props.to,
      'chorus-storage-card--selectable': props.isSelectable && !props.to,
      'chorus-storage-card--selected': props.isSelected && !props.to,
      'chorus-storage-card--disabled': props.isDisabled && !props.to,
    },
  ]);

  const tabindex = computed<number | undefined>(() =>
    !!props.to || (props.isSelectable && !props.isSelected && !props.isDisabled)
      ? 0
      : undefined,
  );

  const emit = defineEmits<{
    (e: 'select'): void;
  }>();

  function handleClick() {
    if (
      !!props.to ||
      !props.isSelectable ||
      props.isSelected ||
      props.isDisabled
    ) {
      return;
    }

    emit('select');
  }
</script>

<template>
  <component
    :is="to ? 'RouterLink' : 'div'"
    :to="to"
    :class="storageCardClasses"
    class="chorus-storage-card"
    :tabindex="tabindex"
    @click="handleClick"
    @keydown.enter="handleClick"
  >
    <div class="chorus-storage-card__inner">
      <div class="chorus-storage-card__header">
        <div class="chorus-storage-card__header-left">
          <ChorusStorageProvider :storage-provider="storage.provider" />
        </div>
        <div class="chorus-storage-card__header-right">
          <CTag
            size="tiny"
            class="chorus-storage-card__storage-tag"
            :type="storage.isMain ? 'success' : 'warning'"
          >
            {{ $t(storage.isMain ? 'mainStorage' : 'followerStorage') }}
          </CTag>
        </div>
      </div>

      <h4 class="chorus-storage-card__title">
        {{ storage.name }}
      </h4>

      <div class="chorus-storage-card__content">
        <CDescriptionList
          size="small"
          class="chorus-storage-card__content-list"
          label-placement="left"
          :columns="1"
        >
          <CDescriptionItem>
            <template #label> {{ $t('address') }}: </template>
            {{ storage.address }}
          </CDescriptionItem>
        </CDescriptionList>
      </div>
    </div>
  </component>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-storage-card {
    padding: utils.unit(3);
    border-radius: 8px;
    position: relative;
    overflow: hidden;

    &--success {
      &::before {
        background-color: var(--success-color);
      }
    }

    &--warning {
      &::before {
        background-color: var(--warning-color);
      }
    }

    &--info {
      &::before {
        background-color: var(--info-color);
      }
    }

    &--error {
      &::before {
        background-color: var(--error-color);
      }
    }

    &--small {
      padding: utils.unit(2) utils.unit(3);

      .chorus-storage-card__header {
        margin-bottom: 0;
      }

      .chorus-storage-card__title {
        margin-bottom: 0;
        @include utils.apply-styles(utils.$text);
        font-family: utils.$font-highlight;
        font-weight: utils.$font-weight-semibold;
      }

      ::v-deep(.c-description-item) {
        @include utils.apply-styles(utils.$text-tiny);
        font-weight: utils.$font-weight-semibold;
      }

      ::v-deep(.chorus-storage-provider__icon) {
        width: 16px;
        height: 16px;
      }
    }

    &--selectable {
      cursor: pointer;
      outline: 2px solid transparent;
      transition: outline-color utils.$transition-duration;

      &:not(.chorus-storage-card--selected):not(
          .chorus-storage-card--disabled
        ) {
        &:focus-visible {
          outline-color: var(--info-color);
        }

        &:hover {
          &::before {
            opacity: 0.15;
          }
        }

        &:active {
          &::before {
            opacity: 0.25;
          }
        }
      }
    }

    &--selected {
      outline-color: var(--primary-color);
      cursor: default;
    }

    &--disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }

    &--link {
      cursor: pointer;
      outline: 2px solid transparent;
      transition: outline-color utils.$transition-duration;

      &:focus-visible {
        outline-color: var(--primary-color);
      }

      &:hover {
        &::before {
          opacity: 0.15;
        }
      }

      &:active {
        &::before {
          opacity: 0.25;
        }
      }
    }

    &::before {
      content: '';
      @include utils.absolute-fit;
      opacity: 0.2;
      z-index: 0;
      transition: opacity utils.$transition-duration;
    }

    &__title {
      @include utils.apply-styles(utils.$text-h4);
      font-weight: utils.$font-weight-medium;
      margin-bottom: utils.unit(2);
    }

    &__storage-tag {
      @include utils.apply-styles(utils.$text-tiny);
      pointer-events: none;
    }

    &__inner {
      position: relative;
      z-index: 1;
    }

    &__header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: utils.unit(2);
    }
  }
</style>
