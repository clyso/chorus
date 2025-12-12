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
  import { CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import ReplicationsList from '@/components/chorus/replications/ReplicationsList/ReplicationsList.vue';
  import ReplicationsListActions from '@/components/chorus/replications/ReplicationsListActions/ReplicationsListActions.vue';
  import ReplicationsListTags from '@/components/chorus/replications/ReplicationsListTags/ReplicationsListTags.vue';
  import ReplicationsFilters from '@/components/chorus/replications/ReplicationsFilters/ReplicationsFilters.vue';

  const { t } = useI18n({
    messages: i18nReplications,
  });
</script>

<template>
  <CTile class="replications-tile">
    <template #title>
      {{ t('replicationsTitle') }}
    </template>

    <ReplicationsFilters class="replications-tile__filters" />

    <ReplicationsListTags class="replications-tile__tags" />

    <ReplicationsListActions class="replications-tile__actions" />

    <ReplicationsList class="replications-tile__replications" />
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-tile {
    &__filters {
      margin-bottom: utils.unit(6);
    }

    &__tags {
      margin-bottom: utils.unit(4);
    }

    &__actions {
      margin-bottom: utils.unit(4);
    }
  }
</style>
