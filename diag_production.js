/**
 * Production Database Diagnostic Script
 * Connects to production DB and shows all users with their data counts
 */

require('dotenv').config();
const mongoose = require('mongoose');

const DB_URL = process.env.DB_URL;
if (!DB_URL) {
    console.error('❌ DB_URL not found in .env');
    process.exit(1);
}

console.log('🔍 Connecting to Production DB...');
console.log('🔗 DB:', DB_URL.replace(/:[^:@]+@/, ':***@'));

mongoose.connect(DB_URL)
    .then(async () => {
        console.log('✅ Connected to MongoDB\n');

        const db = mongoose.connection.db;

        // Get all users
        const users = await db.collection('users').find({}).toArray();
        console.log(`👥 Found ${users.length} users in database\n`);

        for (const user of users) {
            console.log('━'.repeat(80));
            console.log(`📧 User: ${user.email || 'NO EMAIL'}`);
            console.log(`🆔 _id: ${user._id}`);
            console.log(`🔑 currentWorkspaceId: ${user.currentWorkspaceId || 'NONE'}`);

            // Count accounts
            const accountsWithObjectId = await db.collection('accounts').countDocuments({
                userId: user._id
            });
            const accountsWithString = await db.collection('accounts').countDocuments({
                userId: String(user._id)
            });

            console.log(`💰 Accounts (ObjectId userId): ${accountsWithObjectId}`);
            console.log(`💰 Accounts (String userId): ${accountsWithString}`);

            // Show account names
            const accounts = await db.collection('accounts').find({
                $or: [
                    { userId: user._id },
                    { userId: String(user._id) }
                ]
            }).toArray();

            if (accounts.length > 0) {
                console.log(`   Accounts: ${accounts.map(a => `${a.name} (${typeof a.userId})`).join(', ')}`);
            }

            // Count events
            const eventsWithObjectId = await db.collection('events').countDocuments({
                userId: user._id
            });
            const eventsWithString = await db.collection('events').countDocuments({
                userId: String(user._id)
            });

            console.log(`📝 Operations (ObjectId userId): ${eventsWithObjectId}`);
            console.log(`📝 Operations (String userId): ${eventsWithString}`);

            // Count companies
            const companies = await db.collection('companies').countDocuments({
                $or: [
                    { userId: user._id },
                    { userId: String(user._id) }
                ]
            });
            console.log(`🏢 Companies: ${companies}`);

            // Count contractors
            const contractors = await db.collection('contractors').countDocuments({
                $or: [
                    { userId: user._id },
                    { userId: String(user._id) }
                ]
            });
            console.log(`🤝 Contractors: ${contractors}`);

            // Count workspaces
            const workspaces = await db.collection('workspaces').find({
                userId: user._id
            }).toArray();
            console.log(`🗂️  Workspaces: ${workspaces.length}`);
            if (workspaces.length > 0) {
                workspaces.forEach(ws => {
                    console.log(`   - ${ws.name} (${ws._id}) ${ws.isDefault ? '[DEFAULT]' : ''}`);
                });
            }

            console.log('');
        }

        console.log('━'.repeat(80));
        console.log('✅ Diagnostic complete');

        await mongoose.connection.close();
        process.exit(0);
    })
    .catch(err => {
        console.error('❌ Connection error:', err);
        process.exit(1);
    });
