import type { NavigationHookAfter } from 'vue-router';
import { ROUTE_TITLE_PROJECT_PREFIX } from '@/utils/constants/router';
import { i18n } from '@/i18n';

const { t } = i18n.global;

const metaHook: NavigationHookAfter = (to) => {
  document.title = `${ROUTE_TITLE_PROJECT_PREFIX} | ${t(String(to.name))}`;
};

export default metaHook;
