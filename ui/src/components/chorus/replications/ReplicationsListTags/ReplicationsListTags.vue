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
  import { useI18n } from 'vue-i18n';
  import { CCollapseTransition, CTag } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const {
    selectedReplicationIds,
    isAnyReplicationsSelected,
    selectedReplicationsCount,
    isFiltered,
  } = storeToRefs(useChorusReplicationsStore());

  const { clearFilters } = useChorusReplicationsStore();

  const isTagsShown = computed(
    () => isAnyReplicationsSelected.value || isFiltered.value,
  );
</script>

<template>
  <CCollapseTransition :show="isTagsShown">
    <div
      v-if="isTagsShown"
      class="replications-list-tags tag-list"
    >
      <CTag
        v-if="isAnyReplicationsSelected"
        round
        closable
        type="info"
        class="tag-list__selected-tag"
        @close="selectedReplicationIds = []"
      >
        {{ t('tagSelectedReplications', { total: selectedReplicationsCount }) }}
      </CTag>
      <CTag
        v-if="isFiltered"
        round
        closable
        type="warning"
        class="tag-list__filters-tag"
        @close="clearFilters"
      >
        {{ t('tagClearFilters') }}
      </CTag>
    </div>
  </CCollapseTransition>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-list-tags {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: utils.unit(2);
  }
</style>
