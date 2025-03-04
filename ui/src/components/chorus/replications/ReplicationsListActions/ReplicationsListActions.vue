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
  import { useI18n } from 'vue-i18n';
  import {
    CButton,
    CIcon,
    CTooltip,
    useDialog,
    CBadge,
  } from '@clyso/clyso-ui-kit';
  import { computed, h } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { IconName } from '@/utils/types/icon';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import ReplicationsShortList from '@/components/chorus/replications/ReplicationsShortList/ReplicationsShortList.vue';
  import { RouteName } from '@/utils/types/router';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { createDialog } = useDialog();

  const {
    selectedReplicationsCount,
    isAnyReplicationsSelected,
    selectedReplications,
    isResumeSelectedProcessing,
    isPauseSelectedProcessing,
    isDeleteSelectedProcessing,
  } = storeToRefs(useChorusReplicationsStore());
  const { resumeReplications, pauseReplications, deleteReplications } =
    useChorusReplicationsStore();

  const selectedReplicationsForResume = computed(() =>
    selectedReplications.value.filter((replication) => replication.isPaused),
  );
  const selectedReplicationsForPause = computed(() =>
    selectedReplications.value.filter((replication) => !replication.isPaused),
  );

  const isResumeDisabled = computed(
    () =>
      selectedReplicationsForResume.value.length === 0 ||
      isPauseSelectedProcessing.value ||
      isDeleteSelectedProcessing.value,
  );
  const isPauseDisabled = computed(
    () =>
      selectedReplicationsForPause.value.length === 0 ||
      isResumeSelectedProcessing.value ||
      isDeleteSelectedProcessing.value,
  );
  const isDeleteDisabled = computed(
    () =>
      !isAnyReplicationsSelected.value ||
      isPauseSelectedProcessing.value ||
      isResumeSelectedProcessing.value,
  );

  function openResumeConfirmation() {
    const replications = selectedReplicationsForResume.value;

    createDialog({
      type: 'info',
      iconName: IconName.BASE_PLAY_CIRCLE,
      title: t('actionSelectedResumeTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('actionSelectedResumeContent', { total: replications.length }),
        ),
        h(ReplicationsShortList, {
          replications,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('actionSelectedResumeQuestion'),
      ],
      positiveText: t('actionResume'),
      negativeText: t('cancel'),
      positiveHandler: () => resumeReplications(replications),
    });
  }

  function openPauseConfirmation() {
    const replications = selectedReplicationsForPause.value;

    createDialog({
      type: 'warning',
      iconName: IconName.BASE_PAUSE_CIRCLE,
      title: t('actionSelectedPauseTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('actionSelectedPauseContent', { total: replications.length }),
        ),
        h(ReplicationsShortList, {
          replications,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('actionSelectedPauseQuestion'),
      ],
      positiveText: t('actionPause'),
      negativeText: t('cancel'),
      positiveHandler: () => pauseReplications(replications),
    });
  }

  function openDeleteConfirmation() {
    const replications = selectedReplications.value;

    createDialog({
      type: 'error',
      iconName: IconName.BASE_TRASH,
      title: t('actionSelectedDeleteTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('actionSelectedDeleteContent', { total: replications.length }),
        ),
        h(ReplicationsShortList, {
          replications,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('actionSelectedDeleteQuestion'),
      ],
      positiveText: t('actionDelete'),
      negativeText: t('cancel'),
      positiveHandler: () => deleteReplications(replications),
    });
  }
</script>

<template>
  <div class="replications-list-actions">
    <div class="replications-list-actions__creation">
      <RouterLink :to="{ name: RouteName.CHORUS_ADD_REPLICATION }">
        <CButton
          type="primary"
          size="medium"
          ghost
          class="add-replication-button"
          tag="div"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="IconName.BASE_ADD"
            />
          </template>

          {{ t('addReplicationAction') }}
        </CButton>
      </RouterLink>
    </div>

    <div class="replications-list-actions__selection-actions">
      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedReplicationsForResume.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isResumeDisabled"
              :loading="isResumeSelectedProcessing"
              size="medium"
              type="info"
              @click="openResumeConfirmation"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_PLAY"
                />
              </template>
            </CButton>
          </CBadge>
        </template>

        <template v-if="isResumeDisabled">
          {{ t('actionResume') }}
        </template>
        <template v-else>
          {{
            t('actionResumeSelected', {
              total: selectedReplicationsForResume.length,
            })
          }}
        </template>
      </CTooltip>

      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedReplicationsForPause.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isPauseDisabled"
              :loading="isPauseSelectedProcessing"
              size="medium"
              type="warning"
              @click="openPauseConfirmation"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_PAUSE"
                />
              </template>
            </CButton>
          </CBadge>
        </template>

        <template v-if="isPauseDisabled">
          {{ t('actionPause') }}
        </template>
        <template v-else>
          {{
            t('actionPauseSelected', {
              total: selectedReplicationsForPause.length,
            })
          }}
        </template>
      </CTooltip>

      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedReplications.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isDeleteDisabled"
              :loading="isDeleteSelectedProcessing"
              size="medium"
              type="error"
              @click="openDeleteConfirmation"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TRASH"
                />
              </template>
            </CButton>
          </CBadge>
        </template>

        <template v-if="isDeleteDisabled">
          {{ t('actionDelete') }}
        </template>
        <template v-else>
          {{ t('actionDeleteSelected', { total: selectedReplicationsCount }) }}
        </template>
      </CTooltip>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-list-actions {
    display: flex;
    flex-direction: row-reverse;
    justify-content: space-between;
    gap: utils.unit(2);

    &__selection-actions {
      display: inline-flex;
      align-items: center;
      gap: utils.unit(3);

      ::v-deep(.c-badge-sup) {
        pointer-events: none;
      }
    }
  }
</style>
