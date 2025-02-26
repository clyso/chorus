import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    mainStorage: 'Main',
    followerStorage: 'Follower',
    address: 'Address',
    allGood: 'all good',
    downAt: 'down at',
    errorTitle: 'Error',
    successTitle: 'Success',
    errorAction: 'Retry',
    direction: 'Direction',
    user: 'User',
    buckets: 'Buckets',
  },
  [I18nLocale.DE]: {
    mainStorage: 'Hauptspeicher', // Consistent with your suggestion
    followerStorage: 'Follower',
    address: 'Adresse',
    allGood: 'Alles in Ordnung',
    downAt: 'Abgesenkt um',
    errorTitle: 'Fehler',
    successTitle: 'Erfolg',
    errorAction: 'Wiederholen',
    direction: 'Richtung',
    user: 'Benutzer',
    buckets: 'Buckets',
  },
};
