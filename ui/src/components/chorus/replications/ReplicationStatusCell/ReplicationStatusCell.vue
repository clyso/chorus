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
    CTag,
    CDescriptionList,
    CDescriptionItem,
    CTooltip,
    CIcon,
    CProgress,
  } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import type { ChorusReplication } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';
  import { IconName } from '@/utils/types/icon';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
  import { ReplicationsHelper } from '@/utils/helpers/ReplicationsHelper';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const props = defineProps<{
    replication: AddId<ChorusReplication>;
  }>();

  const initPercentage = computed<number>(() => {
    const { isInitDone, initBytesListed, initBytesDone } = props.replication;

    if (+initBytesListed === 0) {
      return isInitDone ? 100 : 0;
    }

    const percentage = Math.ceil((+initBytesDone / +initBytesListed) * 100);

    if (percentage >= 100) {
      return 99;
    }

    return percentage;
  });

  const replicationBytesString = computed<string>(() => {
    const { initBytesListed, initBytesDone } = props.replication;

    return `${GeneralHelper.formatBytes(+initBytesDone)} / ${GeneralHelper.formatBytes(+initBytesListed)}`;
  });

  const lastEmittedDateString = computed<string>(() => {
    const { lastEmittedAt } = props.replication;

    return lastEmittedAt ? GeneralHelper.formatDateTime(lastEmittedAt) : '—';
  });

  const isLiveReplicationBehind = computed<boolean>(() =>
    ReplicationsHelper.isLiveReplicationBehind(props.replication),
  );

  const liveReplicationDistance = computed<number>(() =>
    ReplicationsHelper.getLiveReplicationEventsDifference(props.replication),
  );
</script>

<template>
  <div class="replication-status-cell">
    <CDescriptionList
      class="status-list"
      label-placement="left"
      :columns="1"
      size="small"
    >
      <CDescriptionItem class="status-list__item status-list__item--status">
        <template #label> {{ t('columnStatus') }}: </template>

        <CTag
          :bordered="false"
          size="small"
          :type="replication.isPaused ? 'warning' : 'info'"
        >
          {{ t(replication.isPaused ? 'statusPaused' : 'statusActive') }}
        </CTag>
      </CDescriptionItem>

      <CDescriptionItem class="status-list__item status-list__item--initial">
        <template #label> {{ t('initialReplication') }}: </template>

        <CTooltip>
          <template #trigger>
            <CTag
              v-if="replication.isInitDone"
              :bordered="false"
              size="small"
              round
              type="success"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_CHECKMARK"
                />
              </template>
              {{ t('statusDone') }}
            </CTag>
            <CProgress
              v-else
              type="line"
              class="test"
              :percentage="initPercentage"
              size="small"
              :height="2"
              :processing="initPercentage !== 100"
              indicator-placement="outside"
            />
          </template>

          <div class="details-popup">
            <span class="details-popup__title">
              {{ t('replicationDetails') }}
            </span>
            <CDescriptionList
              class="details-popup__list"
              label-placement="left"
              :columns="1"
              size="small"
            >
              <CDescriptionItem class="details-popup__item">
                <template #label> {{ t('labelObjects') }}: </template>

                {{ replication.initObjDone }} / {{ replication.initObjListed }}
              </CDescriptionItem>
              <CDescriptionItem class="details-popup__item">
                <template #label> {{ t('labelBytes') }}: </template>

                {{ replicationBytesString }}
              </CDescriptionItem>
            </CDescriptionList>
          </div>
        </CTooltip>
      </CDescriptionItem>

      <CDescriptionItem
        v-if="replication.isInitDone"
        class="status-list__item status-list__item--live"
      >
        <template #label> {{ t('liveReplication') }}: </template>

        <CTooltip>
          <template #trigger>
            <CTag
              v-if="isLiveReplicationBehind"
              :bordered="false"
              size="small"
              round
              type="warning"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TIMER"
                />
              </template>
              {{ liveReplicationDistance }} {{ t('statusBehind') }}
            </CTag>
            <CTag
              v-else
              :bordered="false"
              size="small"
              round
              type="success"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_CHECKMARK"
                />
              </template>

              {{ t('statusUptodate') }}
            </CTag>
          </template>

          <div class="details-popup">
            <span class="details-popup__title">
              {{ t('replicationDetails') }}
            </span>
            <CDescriptionList
              class="details-popup__list"
              label-placement="left"
              :columns="1"
              size="small"
            >
              <CDescriptionItem class="details-popup__item">
                <template #label> {{ t('labelEvents') }}: </template>

                {{ replication.eventsDone }} / {{ replication.events }}
              </CDescriptionItem>
              <CDescriptionItem class="details-popup__item">
                <template #label> {{ t('labelLastEmitted') }}: </template>

                {{ lastEmittedDateString }}
              </CDescriptionItem>
            </CDescriptionList>
          </div>
        </CTooltip>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replication-status-cell {
    display: block;

    ::v-deep(
      .c-progress.c-progress--line .c-progress-icon.c-progress-icon--as-text
    ) {
      @include utils.apply-styles(utils.$text-caption);
    }
  }

  .status-list {
    gap: utils.unit(1);

    &__item {
      align-items: center;
    }

    ::v-deep(.c-description-item__label) {
      font-weight: 400;
      white-space: nowrap;
    }

    ::v-deep(.c-icon) {
      width: 12px;
      height: 12px;
    }
  }

  .details-popup {
    &__title {
      @include utils.apply-styles(utils.$text-small);
      font-weight: utils.$font-weight-semibold;
      margin-bottom: utils.unit(1);
    }

    &__list {
      gap: 0;
    }

    ::v-deep(.c-description-item__label) {
      font-weight: 400;
    }
  }
</style>
