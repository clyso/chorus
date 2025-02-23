import 'vue-router';
import 'vue';
import type {
  VantaEffect,
  VantaType,
} from '@/utils/composables/vantaBackground';

declare module 'vue-router' {
  interface RouteMeta {
    isPublic?: () => boolean;
    isAuth?: () => boolean;
    isAuthorized?: () => boolean;
  }
}

declare global {
  interface Window {
    VANTA:
      | {
          [k in VantaType]: (options: VantaOptions) => VantaEffect;
        }
      | undefined;
  }
}
