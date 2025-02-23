<script setup lang="ts">
  import { CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import ReplicationsList from '@/components/chorus/replications/ReplicationsList/ReplicationsList.vue';
  import UserReplicationDeletionDialog from '@/components/chorus/replications/UserReplicationDeletionDialog/UserReplicationDeletionDialog.vue';
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

    <UserReplicationDeletionDialog />
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
