<!--
  - Copyright © 2025 Clyso GmbH
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
  import { computed, ref } from 'vue';
  import { CButton, CCollapseTransition, CIcon } from '@clyso/clyso-ui-kit';

  const props = withDefaults(
    defineProps<{
      buckets: string[];
      max?: number;
      size?: 'small' | 'medium';
    }>(),
    {
      max: 5,
      size: 'small',
    },
  );

  const visibleBuckets = computed(() => props.buckets.slice(0, props.max));
  const collapsedBuckets = computed(() =>
    props.buckets.slice(props.max, props.buckets.length),
  );

  const isCollapsedShown = ref(false);
</script>

<template>
  <div
    class="summary-buckets-list"
    :class="{ 'summary-buckets-list--small': size === 'small' }"
  >
    <ul class="summary-buckets-list__list">
      <li
        v-for="bucket in visibleBuckets"
        :key="bucket"
        class="summary-buckets-list__item"
      >
        <span>{{ bucket }}</span>
      </li>

      <CCollapseTransition :show="isCollapsedShown">
        <li
          v-for="bucket in collapsedBuckets"
          :key="bucket"
          class="summary-buckets-list__item"
        >
          <span>{{ bucket }}</span>
        </li>
      </CCollapseTransition>

      <li
        v-if="!isCollapsedShown && collapsedBuckets.length"
        class="summary-buckets-list__item"
      >
        ...
      </li>
    </ul>

    <div
      v-if="collapsedBuckets.length"
      class="summary-buckets-list__show"
      :class="{ 'summary-buckets-list__show--shown': isCollapsedShown }"
    >
      <CButton
        quaternary
        class="summary-buckets-list__show-button"
        size="tiny"
        @click="isCollapsedShown = !isCollapsedShown"
      >
        <template #icon>
          <CIcon
            class="summary-buckets-list__show-icon"
            :is-inline="true"
            name="chevron-down"
          />
        </template>
      </CButton>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .summary-buckets-list {
    &--small {
      .summary-buckets-list__item {
        @include utils.apply-styles(utils.$text-small);
      }
    }

    &__item {
      span {
        font-weight: utils.$font-weight-medium;
      }

      &::before {
        content: '- ';
      }
    }

    &__show {
      margin-top: - utils.unit(1);
      display: flex;
      justify-content: center;

      &--shown {
        .summary-buckets-list__show-icon {
          transform: rotate(180deg);
        }
      }
    }
  }
</style>
