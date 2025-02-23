<script setup lang="ts">
  import { onBeforeMount, onBeforeUnmount } from 'vue';
  import { storeToRefs } from 'pinia';
  import StoragesTile from '@/components/chorus/storages/StoragesTile/StoragesTile.vue';
  import { useChorusStoragesStore } from '@/stores/chorusStoragesStore';
  import StoragesError from '@/components/chorus/storages/StoragesError/StoragesError.vue';

  const { hasError } = storeToRefs(useChorusStoragesStore());
  const { initStorages, $reset } = useChorusStoragesStore();

  onBeforeMount(() => initStorages());
  onBeforeUnmount($reset);
</script>

<template>
  <div class="storages-page">
    <StoragesError
      v-if="hasError"
      key="error"
    />
    <StoragesTile
      v-else
      key="tile"
    />
  </div>
</template>
