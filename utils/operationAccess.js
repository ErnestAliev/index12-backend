// One workspace policy for HTTP mutations and MCP; never trust a client role.
function workspaceAccess(workspace, actorId) {
    if (String(workspace.userId) === String(actorId)) return { role: 'admin', accountIds: null };
    const share = workspace.sharedWith?.find(item => String(item.userId) === String(actorId));
    if (!share || !['admin', 'manager', 'analyst'].includes(share.role)) return null;
    return { role: share.role, accountIds: share.role === 'manager' ? (share.accessibleAccountIds || []).map(String) : null };
}
module.exports = { workspaceAccess };
