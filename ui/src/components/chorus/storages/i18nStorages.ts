import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    storagesTitle: 'Storages',
    mainStorageTitle: 'Main',
    followersStoragesTitle: 'Followers',
    storagesErrorTitle: 'Storages Error',
    storagesErrorText:
      'An error occurred while retrieving storages.\nPlease try again.',
    storagesErrorAction: 'Retry',

    storageGeneralTitle: 'General Information',
    mainStorage: 'Main',
    followerStorage: 'Follower',
    nameLabel: 'Name',
    addressLabel: 'Address',
    providerLabel: 'Provider',
    typeLabel: 'Type',

    storageBrowserTitle: 'Storage Explorer',
  },
  [I18nLocale.DE]: {
    replicationsTitle: 'Storages',
    mainStorageTitle: 'Main',
    followersStoragesTitle: 'Followers',
    storagesErrorTitle: 'Fehler bei Storages',
    storagesErrorText:
      'Beim Abrufen der Speicher ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    storagesErrorAction: 'Erneut versuchen',

    storageGeneralTitle: 'Allgemeine Informationen',
    mainStorage: 'Main',
    followerStorage: 'Follower',
    nameLabel: 'Name',
    addressLabel: 'Adresse',
    providerLabel: 'Anbieter',
    typeLabel: 'Typ',

    storageBrowserTitle: 'Storage Explorer',
  },
};
