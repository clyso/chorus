import type { RouteRecordRaw } from 'vue-router';
import { RouteName } from '@/utils/types/router';
import LoginPage from '@/components/auth/LoginPage/LoginPage.vue';
import ChorusHomePage from '@/pages/ChorusHomePage/ChorusHomePage.vue';
import DashboardView from '@/components/dashboard/DashboardView/DashboardView.vue';
import ChorusStoragesPage from '@/pages/ChorusStoragesPage/ChorusStoragesPage.vue';
import ChorusMonitoringPage from '@/pages/ChorusMonitoringPage/ChorusMonitoringPage.vue';
import ChorusReplicationPage from '@/pages/ChorusReplicationPage/ChorusReplicationPage.vue';
import ChorusAddReplicationPage from '@/pages/ChorusAddReplicationPage/ChorusAddReplicationPage.vue';
import ChorusStorageDetailsPage from '@/pages/ChorusStorageDetailsPage/ChorusStorageDetailsPage.vue';

export const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: RouteName.LOGIN,
    component: LoginPage,
    meta: {
      isPublic: () => true,
      isAuth: () => true,
    },
  },
  {
    path: '/',
    component: DashboardView,
    meta: {
      isPublic: () => false,
    },
    redirect: { name: RouteName.CHORUS_HOME },
    children: [
      {
        path: 'home',
        name: RouteName.CHORUS_HOME,
        component: ChorusHomePage,
        meta: {
          isPublic: () => false,
        },
      },
      {
        path: 'replication',
        name: RouteName.CHORUS_REPLICATION,
        component: ChorusReplicationPage,
        meta: {
          isPublic: () => false,
        },
      },
      {
        path: 'replication/add',
        name: RouteName.CHORUS_ADD_REPLICATION,
        component: ChorusAddReplicationPage,
        meta: {
          isPublic: () => false,
        },
      },
      {
        path: 'storages',
        name: RouteName.CHORUS_STORAGES,
        component: ChorusStoragesPage,
        meta: {
          isPublic: () => false,
        },
      },
      {
        path: 'storages/:storageName',
        name: RouteName.CHORUS_STORAGE_DETAILS,
        component: ChorusStorageDetailsPage,
        meta: {
          isPublic: () => false,
        },
        props: true,
      },
      {
        path: 'monitoring',
        name: RouteName.CHORUS_MONITORING,
        component: ChorusMonitoringPage,
        meta: {
          isPublic: () => false,
        },
      },
    ],
  },
  {
    path: '/:pathMatch(.*)*',
    redirect: { name: RouteName.CHORUS_HOME },
  },
];
