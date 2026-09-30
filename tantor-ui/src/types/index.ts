export interface UserResponse {
  id: string;
  username: string;
  role: string;
  is_active: boolean;
  created_at: string;
  last_login?: string;
  auth_source: string;
  ldap_dn?: string;
}
