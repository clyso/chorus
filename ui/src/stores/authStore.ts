import { defineStore } from 'pinia';
import { computed, reactive } from 'vue';
import { useRouter } from 'vue-router';
import { createLeaderElection, BroadcastChannel } from 'broadcast-channel';
import { AuthService } from '@/services/AuthService';
import LocalStorageHelper from '@/utils/helpers/LocalStorageHelper';
import {
  IDLE_MESSAGE_TIMEOUT_MINUTES,
  IDLE_TIMEOUT_MINUTES,
  TOKEN_REFRESH_TIMEOUT_MINUTES,
} from '@/utils/constants/auth';
import {
  AuthChannelMessage,
  type AuthCredentials,
  type AuthMeta,
  type AuthResponse,
} from '@/utils/types/auth';
import { LogHelper } from '@/utils/helpers/LogHelper';
import { apiClient } from '@/http';
import { RouteName } from '@/utils/types/router';
import { LocalStorageItem } from '@/utils/types/localStorage';
import { HAS_AUTH } from '@/utils/constants/env';

const TOKEN_REFRESH_TIMEOUT_MS = TOKEN_REFRESH_TIMEOUT_MINUTES * 60 * 1000;
const IDLE_TIMEOUT_MS = IDLE_TIMEOUT_MINUTES * 60 * 1000;
const IDLE_MESSAGE_TIMEOUT_MS = IDLE_MESSAGE_TIMEOUT_MINUTES * 60 * 1000;

const broadcastChannel = new BroadcastChannel<AuthChannelMessage>(
  'session-updater',
);
const broadcastChannelElector = createLeaderElection(broadcastChannel);

const postChannelMessage = (message: AuthChannelMessage) =>
  broadcastChannel.postMessage(message);
const parseJwt = (
  token: string,
): { familyName: string; givenName: string; email: string } => {
  const base64Url = token.split('.')[1];
  const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
  const jsonPayload = decodeURIComponent(
    window
      .atob(base64)
      .split('')
      .map((c) => `%${`00${c.charCodeAt(0).toString(16)}`.slice(-2)}`)
      .join(''),
  );

  const {
    family_name: familyName,
    given_name: givenName,
    email,
  } = JSON.parse(jsonPayload);

  return {
    familyName,
    givenName,
    email,
  };
};
const convertAuthResponseToAuthMeta = ({
  access_token: token,
  expires_in: expiresIn,
  refresh_token: refreshToken,
  refresh_expires_in: refreshExpiresIn,
}: AuthResponse): AuthMeta => ({
  token,
  expiresAt: Date.now() + expiresIn * 1000,
  refreshToken,
  refreshExpiresAt: Date.now() + refreshExpiresIn * 1000,
});

interface AuthState {
  authMeta: AuthMeta | null;
  refreshTimer: number | null;
  idleTimer: number | null;
  idleMessageTimer: number | null;
  isInactivityPopupShown: boolean;
}

export const useAuthStore = defineStore('auth', () => {
  const state = reactive<AuthState>({
    authMeta: null,
    refreshTimer: null,
    idleTimer: null,
    idleMessageTimer: null,
    // TODO: implement later
    isInactivityPopupShown: false,
  });
  const router = useRouter();

  const token = computed<string | null>(() => state.authMeta?.token ?? null);
  const userInfo = computed(() => {
    if (!token.value) {
      return null;
    }

    return parseJwt(token.value);
  });
  const isAuthenticated = computed<boolean>(() => state.authMeta !== null);
  const isTokenExpired = computed<boolean>(
    () => !state.authMeta || state.authMeta.expiresAt <= Date.now(),
  );

  function initAuth() {
    if (!HAS_AUTH) {
      return;
    }

    setAuthMetaFromLocalStorage();

    subscribeToLeadership();
    subscribeToChannelUpdates();

    notifyIdleMessageClosed();

    if (!isAuthenticated.value || isTokenExpired.value) {
      resetAuthState();

      return;
    }

    setIdleListener();
    postChannelMessage(AuthChannelMessage.RESET_IDLE);
  }

  async function login({ username, password }: AuthCredentials) {
    const authResponse = await AuthService.login({ username, password });
    const authMeta = convertAuthResponseToAuthMeta(authResponse);

    setAuthMeta(authMeta);

    initLeaderActions();
    setIdleListener();
    postChannelMessage(AuthChannelMessage.LOGIN);
    redirectToDashboard();
  }

  function logout({
    isChannelMessageForced,
    isRedirectRouteSaved,
  }: {
    isChannelMessageForced: boolean;
    isRedirectRouteSaved: boolean;
  }) {
    if (isChannelMessageForced) {
      postChannelMessage(AuthChannelMessage.LOGOUT);
    }

    resetAuthState();
    redirectToLoginPage(isRedirectRouteSaved);
  }

  async function initTokenAutoRefresh() {
    if (
      !isAuthenticated.value ||
      !broadcastChannelElector.isLeader ||
      !state.authMeta
    ) {
      return;
    }

    setRefreshTimer(null);

    const isTokenExpirationBeforeTimeout =
      state.authMeta.expiresAt - Date.now() <= TOKEN_REFRESH_TIMEOUT_MS;

    if (isTokenExpirationBeforeTimeout) {
      await refreshToken();
    }

    const refreshTimer = window.setTimeout(async () => {
      await refreshToken();
      initTokenAutoRefresh();
    }, TOKEN_REFRESH_TIMEOUT_MS);

    setRefreshTimer(refreshTimer);
  }

  async function refreshToken() {
    if (!isAuthenticated.value || !state.authMeta) {
      logout({ isChannelMessageForced: true, isRedirectRouteSaved: true });

      return;
    }

    const authResponse = await AuthService.refresh(state.authMeta.refreshToken);
    const authMeta = convertAuthResponseToAuthMeta(authResponse);

    if (!isAuthenticated.value) {
      return;
    }

    setAuthMeta(authMeta);
    postChannelMessage(AuthChannelMessage.REFRESH_TOKEN);
    LogHelper.log('AUTH: token refreshed');
  }

  function initLeaderActions() {
    initTokenAutoRefresh();
    setIdleTimeouts();
  }

  async function subscribeToLeadership() {
    await broadcastChannelElector.awaitLeadership();

    LogHelper.log('AUTH: This tab is leader now');
    initLeaderActions();
  }

  function subscribeToChannelUpdates() {
    broadcastChannel.addEventListener(
      'message',
      async (message: AuthChannelMessage) => {
        if (message === AuthChannelMessage.LOGIN) {
          setAuthMetaFromLocalStorage();
          initLeaderActions();
          setIdleListener();
          // TODO: init user info here
          redirectToDashboard();

          return;
        }

        if (message === AuthChannelMessage.LOGOUT) {
          logout({ isChannelMessageForced: false, isRedirectRouteSaved: true });

          return;
        }

        if (message === AuthChannelMessage.REFRESH_TOKEN) {
          setAuthMetaFromLocalStorage();

          return;
        }

        if (message === AuthChannelMessage.RESET_IDLE) {
          if (!broadcastChannelElector.isLeader) {
            return;
          }

          setIdleTimeouts();

          return;
        }

        if (message === AuthChannelMessage.SHOW_IDLE_MESSAGE) {
          state.isInactivityPopupShown = true;
        }

        if (message === AuthChannelMessage.HIDE_IDLE_MESSAGE) {
          state.isInactivityPopupShown = false;
        }
      },
    );
  }

  function setIdleTimeouts() {
    if (!isAuthenticated.value || !broadcastChannelElector.isLeader) {
      return;
    }

    setIdleTimer(null);
    setIdleMessageTimer(null);

    const idleTimer = window.setTimeout(() => {
      logout({ isChannelMessageForced: true, isRedirectRouteSaved: true });
    }, IDLE_TIMEOUT_MS);

    const idleMessageTimer = window.setTimeout(() => {
      postChannelMessage(AuthChannelMessage.SHOW_IDLE_MESSAGE);
      state.isInactivityPopupShown = true;
    }, IDLE_MESSAGE_TIMEOUT_MS);

    setIdleTimer(idleTimer);
    setIdleMessageTimer(idleMessageTimer);
  }

  function setIdleListener() {
    if (!isAuthenticated.value) {
      return;
    }

    document.addEventListener('click', handleIdleReset);
  }

  function removeIdleListener() {
    document.removeEventListener('click', handleIdleReset);
  }

  function handleIdleReset() {
    if (state.isInactivityPopupShown) {
      return;
    }

    resetIdle();
  }

  function resetIdle() {
    setIdleTimeouts();
    postChannelMessage(AuthChannelMessage.RESET_IDLE);
  }

  function setAuthMetaFromLocalStorage() {
    state.authMeta = LocalStorageHelper.get(LocalStorageItem.AUTH_META_KEY);
    setAuthorizationHeader();
  }

  function setAuthMeta(value: AuthMeta | null) {
    state.authMeta = value;
    saveAuthMetaToLocalStorage();
    setAuthorizationHeader();
  }

  function saveAuthMetaToLocalStorage() {
    if (!state.authMeta) {
      LocalStorageHelper.remove(LocalStorageItem.AUTH_META_KEY);

      return;
    }

    LocalStorageHelper.set(LocalStorageItem.AUTH_META_KEY, state.authMeta);
  }

  function setAuthorizationHeader() {
    apiClient.defaults.headers.common.Authorization = token.value
      ? `Bearer ${token.value}`
      : '';
  }

  function redirectToDashboard() {
    const { meta, query } = router.currentRoute.value;

    if (meta?.isPublic?.() && !meta?.isAuth?.()) {
      return;
    }

    router.push((query.redirect as string) ?? { name: RouteName.CHORUS_HOME });
  }

  function redirectToLoginPage(isRedirectRouteSaved: boolean) {
    const { meta, fullPath } = router.currentRoute.value;

    if (meta?.isPublic?.() && !meta?.isAuth?.()) {
      return;
    }

    router.push({
      name: RouteName.LOGIN,
      query: {
        redirect:
          isRedirectRouteSaved && fullPath !== '/' ? fullPath : undefined,
      },
    });
  }

  function setRefreshTimer(value: number | null) {
    if (state.refreshTimer !== null) {
      window.clearTimeout(state.refreshTimer);
    }

    state.refreshTimer = value;
  }

  function setIdleTimer(value: number | null) {
    if (state.idleTimer !== null) {
      window.clearTimeout(state.idleTimer);
    }

    state.idleTimer = value;
  }

  function setIdleMessageTimer(value: number | null) {
    if (state.idleMessageTimer !== null) {
      window.clearTimeout(state.idleMessageTimer);
    }

    state.idleMessageTimer = value;
  }

  function notifyIdleMessageClosed() {
    postChannelMessage(AuthChannelMessage.HIDE_IDLE_MESSAGE);
  }

  function resetAuthState() {
    setRefreshTimer(null);
    setIdleTimer(null);
    setIdleMessageTimer(null);
    removeIdleListener();
    setAuthMeta(null);
    // TODO: reset user info here
  }

  return {
    state,
    userInfo,
    isAuthenticated,
    initAuth,
    login,
    logout,
  };
});
