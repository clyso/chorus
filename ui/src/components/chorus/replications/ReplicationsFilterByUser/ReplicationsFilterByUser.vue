<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { computed, watch } from 'vue';
  import { CSelect, CResult } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { replications, filterUsers, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const usersOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() => {
    const sortedUniqueUsers = [
      ...new Set(replications.value.map((replication) => replication.user)),
    ].sort();

    return sortedUniqueUsers.map((user) => ({ label: user, value: user }));
  });

  function sanitizeFilterUsers() {
    filterUsers.value = filterUsers.value.filter((filterUser) =>
      usersOptions.value.map((user) => user.label).includes(filterUser),
    );
  }

  watch(usersOptions, sanitizeFilterUsers);
</script>

<template>
  <CSelect
    v-model:value="filterUsers"
    class="replications-filter-by-user"
    multiple
    filterable
    clearable
    :placeholder="t('filterByUserPlaceholder')"
    :options="usersOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
      >
        <template v-if="usersOptions.length">
          {{ t('filterByUserNoResult') }}
        </template>
      </CResult>
    </template>
  </CSelect>
</template>
