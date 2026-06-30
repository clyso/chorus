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
  import { CCollapseTransition, CTag, I18nLocale } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        filtersApplied: 'Filters applied',
        elementsSelected: '{total} elements selected',
      },
      [I18nLocale.DE]: {
        filtersApplied: 'Filter angewendet',
        elementsSelected: '{total} Elemente ausgewählt',
      },
    },
  });

  const props = defineProps<{
    selectedCount: number;
    isFiltered: boolean;
  }>();

  const emit = defineEmits<{
    (e: 'clear-selection'): void;
    (e: 'clear-filters'): void;
  }>();

  const isAnySelected = computed(() => props.selectedCount > 0);

  const isTagsShown = computed(() => isAnySelected.value || props.isFiltered);
</script>

<template>
  <CCollapseTransition :show="isTagsShown">
    <div
      v-if="isTagsShown"
      class="chorus-list-tags tag-list"
    >
      <CTag
        v-if="isAnySelected"
        round
        closable
        type="info"
        class="tag-list__selected-tag"
        @close="emit('clear-selection')"
      >
        {{ t('elementsSelected', { total: selectedCount }) }}
      </CTag>
      <CTag
        v-if="isFiltered"
        round
        closable
        type="warning"
        class="tag-list__filters-tag"
        @close="emit('clear-filters')"
      >
        {{ t('filtersApplied') }}
      </CTag>
    </div>
  </CCollapseTransition>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-list-tags {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: utils.unit(2);
  }
</style>
