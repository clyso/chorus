import { type I18nMessages, I18nLocale } from '@clyso/clyso-ui-kit';
import { RouteName } from '@/utils/types/router';

type Routes = {
  [k in RouteName]: string;
};

const routes: I18nMessages<Routes> = {
  [I18nLocale.EN]: {
    [RouteName.LOGIN]: 'Login',
    [RouteName.CHORUS_HOME]: 'Home',
    [RouteName.CHORUS_STORAGES]: 'Storages',
    [RouteName.CHORUS_STORAGE_DETAILS]: 'Storage Details',
    [RouteName.CHORUS_REPLICATION]: 'Replication',
    [RouteName.CHORUS_ADD_REPLICATION]: 'Add Replication',
    [RouteName.CHORUS_MONITORING]: 'Monitoring',
  },
  [I18nLocale.DE]: {
    [RouteName.LOGIN]: 'Login',
    [RouteName.CHORUS_HOME]: 'Startseite',
    [RouteName.CHORUS_STORAGES]: 'Speicher',
    [RouteName.CHORUS_STORAGE_DETAILS]: 'Speicherdetails',
    [RouteName.CHORUS_REPLICATION]: 'Replikation',
    [RouteName.CHORUS_ADD_REPLICATION]: 'Replikation hinzufügen',
    [RouteName.CHORUS_MONITORING]: 'Monitoring',
  },
};

export default routes;
