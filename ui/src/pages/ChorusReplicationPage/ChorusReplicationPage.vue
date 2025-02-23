<script setup lang="ts">
  import { onBeforeMount, onUnmounted } from 'vue';
  import ReplicationsTile from '@/components/chorus/replications/ReplicationsTile/ReplicationsTile.vue';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { initReplicationsPage, $reset } = useChorusReplicationsStore();

  onBeforeMount(() => {
    initReplicationsPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <div class="replication-page">
    <ReplicationsTile />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replication-page {
    height: 100%;
  }
</style>
