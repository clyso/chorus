import { apiClient } from '@/http';
import type { AuthCredentials, AuthResponse } from '@/utils/types/auth';
import { ApiHelper } from '@/utils/helpers/ApiHelper';
import { AuthGrantType } from '@/utils/types/auth';
import { AUTH_CLIENT_ID } from '@/utils/constants/auth';

export abstract class AuthService {
  static async login({
    username,
    password,
  }: AuthCredentials): Promise<AuthResponse> {
    const { data } = await apiClient.post<AuthResponse>(
      ApiHelper.getApiAuthUrl('/token'),
      {
        username,
        password,
        grant_type: AuthGrantType.PASSWORD,
        client_id: AUTH_CLIENT_ID,
      },
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        },
      },
    );

    return data;
  }

  static async refresh(refreshToken: string): Promise<AuthResponse> {
    const { data } = await apiClient.post<AuthResponse>(
      ApiHelper.getApiAuthUrl('/token'),
      {
        refresh_token: refreshToken,
        grant_type: AuthGrantType.REFRESH_TOKEN,
        client_id: AUTH_CLIENT_ID,
      },
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        },
      },
    );

    return data;
  }
}
