<script setup lang="ts">
  import { CAvatar } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';

  const props = withDefaults(
    defineProps<{
      user: string;
      isSelectable?: boolean;
      isSelected?: boolean;
      isDisabled?: boolean;
    }>(),
    {
      isSelectable: false,
      isSelected: false,
      isDisabled: false,
    },
  );

  const userCardClasses = computed(() => ({
    'chorus-user-card--selectable': props.isSelectable,
    'chorus-user-card--selected': props.isSelected,
    'chorus-user-card--disabled': props.isDisabled,
  }));

  const tabindex = computed<number | undefined>(() =>
    props.isSelectable && !props.isSelected && !props.isDisabled
      ? 0
      : undefined,
  );

  const emit = defineEmits<{
    (e: 'select'): void;
  }>();

  function handleClick() {
    if (!props.isSelectable || props.isSelected || props.isDisabled) {
      return;
    }

    emit('select');
  }
</script>

<template>
  <div
    :tabindex="tabindex"
    class="chorus-user-card"
    :class="userCardClasses"
    @click="handleClick"
    @keydown.enter="handleClick"
  >
    <div class="chorus-user-card__inner">
      <CAvatar
        round
        :name="user"
        class="chorus-user-card__avatar"
      />

      <span class="chorus-user-card__name">{{ user }}</span>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-user-card {
    display: inline-flex;
    position: relative;
    border-radius: utils.$border-radius;
    overflow: hidden;
    cursor: pointer;
    outline: 2px solid transparent;
    transition: outline-color utils.$transition-duration;

    &:focus-visible:not(.user-list__option--selected) {
      outline: 2px solid var(--info-color);
    }

    &--selected {
      outline-color: var(--primary-color);
      cursor: default;
    }

    &--disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }

    &::before {
      content: '';
      @include utils.absolute-fit;
      z-index: 0;
      background: var(--button-color-2);
    }

    &__inner {
      position: relative;
      z-index: 1;
      padding: utils.unit(2) utils.unit(3);
      display: flex;
      align-items: center;
    }

    &__avatar {
      background-color: var(--info-color);
      margin-right: utils.unit(3);
    }
  }
</style>
