import type { NavigationGuard } from 'vue-router';
import { useAuthStore } from '@/stores/authStore';
import { RouteName } from '@/utils/types/router';
import { LogHelper } from '@/utils/helpers/LogHelper';

const accessGuard: NavigationGuard = (to, _from, next) => {
  const { isAuthenticated } = useAuthStore();

  if (isAuthenticated && to.meta.isAuth?.()) {
    next({ name: RouteName.CHORUS_HOME });

    return;
  }

  if (!isAuthenticated && !to.meta.isPublic?.()) {
    next({
      name: RouteName.LOGIN,
      query: {
        redirect: to.fullPath,
      },
    });

    return;
  }

  if (to.meta.isAuthorized?.() === false) {
    LogHelper.error('Access denied', true);
    next({ name: RouteName.CHORUS_HOME });

    return;
  }

  next();
};

export default accessGuard;
