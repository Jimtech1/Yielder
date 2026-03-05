export const ADMIN_PERMISSION_KEYS = [
  'users.read',
  'users.update_role',
  'users.update_tier',
  'users.suspend',
  'users.session_revoke',
  'users.password_reset',
  'users.two_factor_reset',
  'users.promote_owner',
  'audit.read',
  'audit.clear',
  'announcements.manage',
  'notifications.manage',
  'feature_flags.manage',
  'billing.read',
  'platform_fees.read',
  'platform_fees.collect',
  'timeline.read',
  'permissions.read',
] as const;

export type AdminPermissionKey = (typeof ADMIN_PERMISSION_KEYS)[number];

export type AdminPermissionMatrix = Record<AdminPermissionKey, boolean>;

const buildAllAllowed = (): AdminPermissionMatrix => {
  return ADMIN_PERMISSION_KEYS.reduce((acc, key) => {
    acc[key] = true;
    return acc;
  }, {} as AdminPermissionMatrix);
};

const OWNER_PERMISSIONS: AdminPermissionMatrix = buildAllAllowed();

const ADMIN_PERMISSIONS: AdminPermissionMatrix = {
  ...OWNER_PERMISSIONS,
  'users.promote_owner': false,
  'audit.clear': false,
  'platform_fees.collect': false,
};

const EMPTY_PERMISSIONS: AdminPermissionMatrix = ADMIN_PERMISSION_KEYS.reduce((acc, key) => {
  acc[key] = false;
  return acc;
}, {} as AdminPermissionMatrix);

export const normalizeAdminRole = (role: unknown): 'user' | 'admin' | 'owner' => {
  const normalized = typeof role === 'string' ? role.trim().toLowerCase() : '';
  if (normalized === 'owner' || normalized === 'admin') {
    return normalized;
  }
  return 'user';
};

export const getAdminPermissionMatrix = (role: unknown): AdminPermissionMatrix => {
  const normalizedRole = normalizeAdminRole(role);
  if (normalizedRole === 'owner') {
    return { ...OWNER_PERMISSIONS };
  }
  if (normalizedRole === 'admin') {
    return { ...ADMIN_PERMISSIONS };
  }
  return { ...EMPTY_PERMISSIONS };
};

export const hasAdminPermission = (
  role: unknown,
  permission: AdminPermissionKey,
): boolean => {
  const matrix = getAdminPermissionMatrix(role);
  return matrix[permission] === true;
};

