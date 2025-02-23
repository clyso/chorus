import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    slogan: 'Success needs a system',
    greeting: 'Hello! Welcome back',
    username: 'Username',
    password: 'Password',
    login: 'Login',
    forgot: 'Forgot password?',
    wrongCredentialsTitle: 'Wrong credentials!',
    wrongCredentialsSubtitle:
      'Please verify your username or password and try to login again. Thank you!',
  },
  [I18nLocale.DE]: {
    slogan: 'Erfolg braucht System',
    greeting: 'Hallo! Willkommen zurück',
    username: 'Username',
    password: 'Passwort',
    login: 'Login',
    forgot: 'Passwort vergessen?',
    wrongCredentialsTitle: 'Falsche Anmeldedaten!',
    wrongCredentialsSubtitle:
      'Bitte überprüfen Sie Ihren Benutzernamen oder Ihr Passwort und versuchen Sie erneut, sich anzumelden. Danke schön!',
  },
};
