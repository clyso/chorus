export const IS_DEV_ENV = import.meta.env.DEV;
export const API_BASE_URL: string = IS_DEV_ENV
  ? ''
  : import.meta.env.VITE_API_BASE_URL;
export const API_PREFIX: string = import.meta.env.VITE_API_PREFIX;
export const HAS_AUTH = import.meta.env.VITE_HAS_AUTH === 'TRUE';
export const HAS_PROMETHEUS = import.meta.env.VITE_HAS_PROMETHEUS === 'TRUE';
