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
  import { useRoute, useRouter } from 'vue-router';
  import { computed, ref, watch } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { CTabs } from '@clyso/clyso-ui-kit';
  import { RouteName } from '@/utils/types/router';

  const { t } = useI18n();

  const route = useRoute();
  const router = useRouter();
  const routeName = ref<RouteName>(route.name as RouteName);

  const tabs = computed(() => [
    {
      name: RouteName.CHORUS_HOME,
      label: t(RouteName.CHORUS_HOME),
    },
    {
      name: RouteName.CHORUS_REPLICATION,
      label: t(RouteName.CHORUS_REPLICATION),
    },
    {
      name: RouteName.CHORUS_STORAGES,
      label: t(RouteName.CHORUS_STORAGES),
    },
    {
      name: RouteName.CHORUS_MONITORING,
      label: t(RouteName.CHORUS_MONITORING),
    },
  ]);

  const selectedTab = computed<RouteName>(() => {
    const matchedTab = tabs.value.find((tab) => {
      if (tab.name === RouteName.CHORUS_REPLICATION) {
        return (
          routeName.value === RouteName.CHORUS_REPLICATION ||
          routeName.value === RouteName.CHORUS_ADD_REPLICATION
        );
      }

      if (tab.name === RouteName.CHORUS_STORAGES) {
        return (
          routeName.value === RouteName.CHORUS_STORAGES ||
          routeName.value === RouteName.CHORUS_STORAGE_DETAILS
        );
      }

      return tab.name === routeName.value;
    });

    return matchedTab?.name ?? RouteName.CHORUS_HOME;
  });

  watch(
    () => route.name as RouteName,
    (value) => {
      routeName.value = value;
    },
  );

  function handleTabChange(value: string | number) {
    if (typeof value !== 'string' || routeName.value === value) {
      return;
    }

    router.push({ name: value });
  }
</script>

<template>
  <nav class="dashboard-nav">
    <CTabs
      :value="selectedTab"
      :tabs="tabs"
      type="card"
      size="small"
      @click="handleTabChange"
    />
  </nav>
</template>
