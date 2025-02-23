import { createI18n } from 'vue-i18n';
import { I18N_DEFAULT_LOCALE } from '@clyso/clyso-ui-kit';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
import formErrors from '@/i18n/messages/formErrors';
import common from '@/i18n/messages/common';
import routes from '@/i18n/messages/routes';

const messages = GeneralHelper.merge(common, formErrors, routes);

export const i18n = createI18n({
  legacy: false,
  locale: I18N_DEFAULT_LOCALE,
  fallbackLocale: I18N_DEFAULT_LOCALE,
  messages,
});
