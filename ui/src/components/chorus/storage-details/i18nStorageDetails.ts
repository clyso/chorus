import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    storageDetailsErrorTitle: 'Storage Details Error',
    storageDetailsErrorText:
      'An error occurred while retrieving storage details.\nPlease try again.',
    storageDetailsErrorAction: 'Retry',
    storagesBreadcrumb: 'Storages',
    storageDetailsBreadcrumb: 'Storage Details',
  },
  [I18nLocale.DE]: {
    storageDetailsErrorTitle: 'Fehler bei Speicherdetails',
    storageDetailsErrorText:
      'Beim Abrufen der Speicherdetails ist ein Fehler aufgetreten.\nBitte versuchen Sie es erneut.',
    storageDetailsErrorAction: 'Erneut versuchen',
    storagesBreadcrumb: 'Storages',
    storageDetailsBreadcrumb: 'Storage Details',
  },
};
