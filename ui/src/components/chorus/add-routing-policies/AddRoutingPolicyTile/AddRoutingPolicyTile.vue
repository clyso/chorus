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
  import { CTile, CButton } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import UserSelection from '../UserSelection/UserSelection.vue';
  import BucketSelection from '../BucketSelection/BucketSelection.vue';
  import StorageSelection from '../StorageSelection/StorageSelection.vue';
  import StatusSelection from '../StatusSelection/StatusSelection.vue';
  import AddRoutingPolicyConfirmDialog from '../AddRoutingPolicyConfirmDialog/AddRoutingPolicyConfirmDialog.vue';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';

  const { isConfirmDialogOpen, validator } = storeToRefs(
    useChorusAddRoutingPolicyStore(),
  );

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  async function openConfirmDialog() {
    validator.value.$touch();

    const isValid = await validator.value.$validate();

    if (isValid) {
      isConfirmDialogOpen.value = true;
    }
  }
</script>

<template>
  <CTile class="add-routing-policy-tile">
    <template #title>
      {{ t('addRoutingPolicyTitle') }}
    </template>
    <template #header>
      {{ t('addRoutingPolicyHeader') }}
    </template>
    <div class="add-routing-policy-tile__content">
      <UserSelection />
      <StorageSelection />
      <BucketSelection />
      <StatusSelection />
      <div class="add-routing-policy-tile__actions">
        <CButton
          type="primary"
          size="large"
          @click="openConfirmDialog"
        >
          {{ t('actionAddRoutingPolicy') }}
        </CButton>
      </div>
    </div>
  </CTile>
  <AddRoutingPolicyConfirmDialog v-model:is-shown="isConfirmDialogOpen" />
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-routing-policy-tile {
    &__content {
      display: flex;
      flex-direction: column;
      gap: utils.unit(8);
    }

    &__actions {
      display: flex;
      justify-content: flex-start;
      padding-top: utils.unit(2);
    }
  }
</style>
