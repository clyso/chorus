export const IS_DEV_ENV = import.meta.env.DEV;
export const API_BASE_URL: string = import.meta.env.VITE_API_BASE_URL;
export const HAS_PROMETHEUS = import.meta.env.VITE_HAS_PROMETHEUS === 'TRUE';
export const HAS_LANG_SELECT = import.meta.env.VITE_HAS_LANG_SELECT === 'TRUE';
