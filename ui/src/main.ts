import { createApp } from 'vue';
import { createPinia } from 'pinia';
import 'virtual:svg-icons-register';
import '@/styles/styles.scss';
import { ColorScheme } from '@clyso/clyso-ui-kit';
import { useI18nStore } from '@/stores/i18nStore';
import { useColorSchemeStore } from '@/stores/colorSchemeStore';
import router from '@/router';
import App from '@/App.vue';
import { i18n } from '@/i18n';

const app = createApp(App);

app.use(createPinia());
app.use(router);
app.use(i18n);

app.mount('#app');

const i18nStore = useI18nStore();
const colorSchemeStore = useColorSchemeStore();

i18nStore.initLocale();
colorSchemeStore.initColorScheme(ColorScheme.DARK);
