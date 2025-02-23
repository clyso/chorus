<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { computed, ref, watch } from 'vue';
  import { CAutoComplete } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const searchString = ref('');
  const select = ref<InstanceType<typeof CAutoComplete> | null>(null);

  const { replications, filterBucket, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const bucketOptions = computed<string[]>(() => {
    const uniqueBucketOptions = [
      ...new Set(replications.value.map((replication) => replication.bucket)),
    ];

    return uniqueBucketOptions
      .filter((bucket) =>
        bucket
          .trim()
          .toLowerCase()
          .includes(searchString.value.trim().toLowerCase()),
      )
      .sort();
  });

  const handleValueUpdate = GeneralHelper.debounce(() => {
    page.value = 1;
    filterBucket.value = searchString.value;
  }, 1000);

  watch(filterBucket, () => {
    if (filterBucket.value === '') {
      searchString.value = '';
    }
  });
</script>

<template>
  <CAutoComplete
    ref="select"
    v-model:value="searchString"
    class="replications-filter-by-bucket"
    :input-props="{
      autocomplete: 'disabled',
    }"
    :options="bucketOptions"
    :placeholder="t('filterByBucketPlaceholder')"
    clearable
    blur-after-select
    @update:value="handleValueUpdate"
  />
</template>
