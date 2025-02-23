<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import {
    CDescriptionItem,
    CDescriptionList,
    CIcon,
    CTag,
  } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import { IconName } from '@/utils/types/icon';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import ChorusUserCard from '@/components/chorus/common/ChorusUserCard/ChorusUserCard.vue';
  import SummaryBucketsList from '@/components/chorus/add-replication/SummaryBucketsList/SummaryBucketsList.vue';

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const {
    selectedUser,
    selectedFromStorage,
    selectedToStorage,
    selectedBuckets,
    isForAllBuckets,
  } = storeToRefs(useChorusAddReplicationStore());

  const orderedSelectedBuckets = computed<string[]>(() =>
    [...selectedBuckets.value].sort(),
  );
</script>

<template>
  <div class="add-replication-summary">
    <CDescriptionList
      size="medium"
      class="summary-list"
      label-placement="top"
      :columns="1"
    >
      <CDescriptionItem
        v-if="selectedFromStorage && selectedToStorage"
        class="summary-list__item"
      >
        <template #label> {{ t('direction') }}: </template>

        <div class="replication-direction">
          <ChorusStorageCard
            type="success"
            size="small"
            class="replication-direction__storage"
            :storage="selectedFromStorage"
          />
          <CIcon
            class="replication-direction__arrow"
            :is-inline="true"
            :name="IconName.BASE_ARROW_FORWARD"
          />
          <ChorusStorageCard
            type="warning"
            size="small"
            class="replication-direction__storage"
            :storage="selectedToStorage"
          />
        </div>
      </CDescriptionItem>

      <CDescriptionItem
        v-if="selectedUser"
        class="summary-list__item"
      >
        <template #label> {{ t('user') }}: </template>

        <ChorusUserCard
          class="user-card"
          :user="selectedUser"
        />
      </CDescriptionItem>

      <CDescriptionItem
        v-if="isForAllBuckets || selectedBuckets.length"
        class="summary-list__item"
      >
        <template #label>
          <span v-if="isForAllBuckets"> {{ t('buckets') }}: </span>
          <span v-else>
            {{ t('buckets') }} <span>({{ selectedBuckets.length }})</span>:
          </span>
        </template>

        <div class="buckets-list">
          <CTag
            v-if="isForAllBuckets"
            key="all"
            round
            type="info"
          >
            {{ t('summaryAllBuckets') }}
          </CTag>
          <div v-else>
            <SummaryBucketsList :buckets="orderedSelectedBuckets" />
          </div>
        </div>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replication-direction {
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    gap: utils.unit(2);

    &__storage {
      flex-grow: 1;
      align-self: stretch;
    }
  }

  .user-card {
    min-width: 218px;

    @include utils.mobile {
      width: 100%;
    }
  }
</style>
