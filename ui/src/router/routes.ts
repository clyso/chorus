import type { RouteRecordRaw } from 'vue-router';
import { RouteName } from '@/utils/types/router';
import ChorusHomePage from '@/pages/ChorusHomePage/ChorusHomePage.vue';
import DashboardView from '@/components/dashboard/DashboardView/DashboardView.vue';
import ChorusStoragesPage from '@/pages/ChorusStoragesPage/ChorusStoragesPage.vue';
import ChorusMonitoringPage from '@/pages/ChorusMonitoringPage/ChorusMonitoringPage.vue';
import ChorusReplicationPage from '@/pages/ChorusReplicationPage/ChorusReplicationPage.vue';
import ChorusAddReplicationPage from '@/pages/ChorusAddReplicationPage/ChorusAddReplicationPage.vue';
import ChorusStorageDetailsPage from '@/pages/ChorusStorageDetailsPage/ChorusStorageDetailsPage.vue';

export const routes: RouteRecordRaw[] = [
  {
    path: '/',
    component: DashboardView,
    redirect: { name: RouteName.CHORUS_HOME },
    children: [
      {
        path: 'home',
        name: RouteName.CHORUS_HOME,
        component: ChorusHomePage,
      },
      {
        path: 'replication',
        name: RouteName.CHORUS_REPLICATION,
        component: ChorusReplicationPage,
      },
      {
        path: 'replication/add',
        name: RouteName.CHORUS_ADD_REPLICATION,
        component: ChorusAddReplicationPage,
      },
      {
        path: 'storages',
        name: RouteName.CHORUS_STORAGES,
        component: ChorusStoragesPage,
      },
      {
        path: 'storages/:storageName',
        name: RouteName.CHORUS_STORAGE_DETAILS,
        component: ChorusStorageDetailsPage,
        props: true,
      },
      {
        path: 'monitoring',
        name: RouteName.CHORUS_MONITORING,
        component: ChorusMonitoringPage,
      },
    ],
  },
  {
    path: '/:pathMatch(.*)*',
    redirect: { name: RouteName.CHORUS_HOME },
  },
];
