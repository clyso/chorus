export interface AuthResponse {
  access_token: string;
  expires_in: number;
  refresh_expires_in: number;
  refresh_token: string;
  token_type: string;
  id_token: string;
  session_state: string;
  scope: string;
}

export interface AuthMeta {
  token: string;
  refreshToken: string;
  expiresAt: number;
  refreshExpiresAt: number;
}

export interface AuthCredentials {
  username: string;
  password: string;
}

export enum AuthGrantType {
  PASSWORD = 'password',
  REFRESH_TOKEN = 'refresh_token',
}

export enum AuthChannelMessage {
  LOGIN = 'LOGIN',
  LOGOUT = 'LOGOUT',
  REFRESH_TOKEN = 'REFRESH_TOKEN',
  RESET_IDLE = 'RESET_IDLE',
  SHOW_IDLE_MESSAGE = 'SHOW_IDLE_MESSAGE',
  HIDE_IDLE_MESSAGE = 'HIDE_IDLE_MESSAGE',
}
