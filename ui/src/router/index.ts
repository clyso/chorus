import { createRouter, createWebHistory } from 'vue-router';
import metaHook from '@/router/guards/meta-hook';
import { routes } from '@/router/routes';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  scrollBehavior(to, from, savedPosition) {
    if (savedPosition) {
      return savedPosition;
    }

    if (to.params.savePosition === 'TRUE') {
      return {};
    }

    if (to.hash) {
      return {
        selector: to.hash,
        behavior: 'smooth',
      };
    }

    return { x: 0, y: 0 };
  },
  routes,
});

router.afterEach(metaHook);

export default router;
