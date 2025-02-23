<script setup lang="ts">
  import { CBreadcrumb, CBreadcrumbItem } from '@clyso/clyso-ui-kit';
  import { onBeforeMount, onUnmounted } from 'vue';
  import { RouteName } from '@/utils/types/router';
  import AddReplicationTile from '@/components/chorus/add-replication/AddReplicationTile/AddReplicationTile.vue';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';

  const { initAddReplicationPage, $reset } = useChorusAddReplicationStore();

  onBeforeMount(() => {
    initAddReplicationPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <div class="chorus-add-replication-page">
    <CBreadcrumb class="chorus-add-replication-page__breadcrumb">
      <CBreadcrumbItem :to="{ name: RouteName.CHORUS_REPLICATION }">
        Replication
      </CBreadcrumbItem>
      <CBreadcrumbItem :is-active="true"> Add Replication </CBreadcrumbItem>
    </CBreadcrumb>

    <AddReplicationTile />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-add-replication-page {
    &__breadcrumb {
      margin-bottom: utils.unit(2);
    }
  }
</style>
