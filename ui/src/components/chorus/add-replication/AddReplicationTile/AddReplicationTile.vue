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
  import {
    CResult,
    CSteps,
    CTile,
    DASHBOARD_NAV_META_INJECT_KEY,
    getDefaultNavMeta,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { inject, readonly, ref } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import UserFormStep from '@/components/chorus/add-replication/UserFormStep/UserFormStep.vue';
  import BucketsFormStep from '@/components/chorus/add-replication/BucketsFormStep/BucketsFormStep.vue';
  import FromStorageFormStep from '@/components/chorus/add-replication/FromStorageFormStep/FromStorageFormStep.vue';
  import ToStorageFormStep from '@/components/chorus/add-replication/ToStorageFormStep/ToStorageFormStep.vue';
  import { AddReplicationStepName } from '@/utils/types/chorus';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import AddReplicationActions from '@/components/chorus/add-replication/AddReplicationActions/AddReplicationActions.vue';
  import AddReplicationConfirmDialog from '@/components/chorus/add-replication/AddReplicationConfirmDialog/AddReplicationConfirmDialog.vue';

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const navMeta = inject(
    DASHBOARD_NAV_META_INJECT_KEY,
    readonly(ref(getDefaultNavMeta())),
  );

  const { hasError, currentStep, steps } = storeToRefs(
    useChorusAddReplicationStore(),
  );
  const { initAddReplicationPage } = useChorusAddReplicationStore();
</script>

<template>
  <CTile class="add-replication-tile">
    <template #title>
      {{ t('addReplicationTitle') }}
    </template>

    <CResult
      v-if="hasError"
      key="error"
      class="add-replication-tile__error"
      type="error"
      size="large"
      :max-width="600"
      @positive-click="initAddReplicationPage"
    >
      <template #title>
        {{ t('errorTitle') }}
      </template>

      <p>{{ t('addReplicationError') }}</p>

      <template #positive-text>
        {{ t('errorAction') }}
      </template>
    </CResult>

    <div
      v-else
      key="content"
      class="add-replication-tile__content"
    >
      <CSteps
        class="add-replication-tile__steps"
        :current="currentStep"
        :size="navMeta.isMobile ? 'small' : 'medium'"
        status="process"
        :steps="steps"
        :vertical="navMeta.isMobile ?? false"
      />

      <div class="add-replication-tile__form">
        <Transition
          name="opacity"
          mode="out-in"
        >
          <FromStorageFormStep
            v-if="currentStep === AddReplicationStepName.FROM_STORAGE"
            :key="AddReplicationStepName.FROM_STORAGE"
          />
          <ToStorageFormStep
            v-else-if="currentStep === AddReplicationStepName.TO_STORAGE"
            :key="AddReplicationStepName.TO_STORAGE"
          />
          <UserFormStep
            v-else-if="currentStep === AddReplicationStepName.USER"
            :key="AddReplicationStepName.USER"
          />
          <BucketsFormStep
            v-else-if="currentStep === AddReplicationStepName.BUCKETS"
            :key="AddReplicationStepName.BUCKETS"
          />
        </Transition>
      </div>

      <AddReplicationActions />
    </div>

    <AddReplicationConfirmDialog />
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-replication-tile {
    &__steps {
      margin-bottom: utils.unit(10);
    }

    &__form {
      min-height: 300px;
      margin-bottom: utils.unit(10);
    }

    &__error {
      min-height: 500px;
    }
  }
</style>
