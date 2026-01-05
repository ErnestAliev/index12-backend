/**
 * Migration script to update old operations with workspace data
 * 
 * This script:
 * 1. Finds all users
 * 2. For each user, finds or creates default workspace
 * 3. Updates all user's operations to have correct workspaceId
 * 4. Ensures userId is consistent (ObjectId format)
 * 
 * Run: node migrate-workspace-data.js
 */

require('dotenv').config();
const mongoose = require('mongoose');

// Connect to MongoDB
const DB_URL = process.env.DB_URL;
if (!DB_URL) {
    console.error('❌ DB_URL not found in .env');
    process.exit(1);
}

console.log('🔄 Connecting to MongoDB...');
mongoose.connect(DB_URL)
    .then(() => console.log('✅ Connected to MongoDB'))
    .catch(err => {
        console.error('❌ MongoDB connection error:', err);
        process.exit(1);
    });

// Define schemas
const userSchema = new mongoose.Schema({}, { strict: false });
const User = mongoose.model('User', userSchema);

const workspaceSchema = new mongoose.Schema({}, { strict: false });
const Workspace = mongoose.model('Workspace', workspaceSchema);

const eventSchema = new mongoose.Schema({}, { strict: false });
const Event = mongoose.model('Event', eventSchema);

const accountSchema = new mongoose.Schema({}, { strict: false });
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({}, { strict: false });
const Company = mongoose.model('Company', companySchema);

const individualSchema = new mongoose.Schema({}, { strict: false });
const Individual = mongoose.model('Individual', individualSchema);

const contractorSchema = new mongoose.Schema({}, { strict: false });
const Contractor = mongoose.model('Contractor', contractorSchema);

const categorySchema = new mongoose.Schema({}, { strict: false });
const Category = mongoose.model('Category', categorySchema);

const projectSchema = new mongoose.Schema({}, { strict: false });
const Project = mongoose.model('Project', projectSchema);

async function migrateUserData(userId) {
    console.log(`\n📦 Migrating user: ${userId}`);

    // 1. Find or create default workspace
    let defaultWorkspace = await Workspace.findOne({ userId, isDefault: true });

    if (!defaultWorkspace) {
        console.log('  ➕ Creating default workspace...');
        defaultWorkspace = await Workspace.create({
            userId,
            name: 'Основной проект',
            isDefault: true
        });
        console.log(`  ✅ Created workspace: ${defaultWorkspace._id}`);
    } else {
        console.log(`  ✅ Found default workspace: ${defaultWorkspace._id}`);
    }

    const workspaceId = defaultWorkspace._id;

    // 2. Normalize userId format in all operations (convert ObjectId to string)
    const userIdString = userId.toString();
    const eventsWithObjectId = await Event.find({
        userId: { $type: 'objectId' },
        $or: [
            { userId: userId },
            { userId: new mongoose.Types.ObjectId(userId) }
        ]
    });

    if (eventsWithObjectId.length > 0) {
        console.log(`  🔧 Found ${eventsWithObjectId.length} operations with ObjectId userId, converting to string...`);
        await Event.updateMany(
            {
                userId: { $type: 'objectId' },
                $or: [
                    { userId: userId },
                    { userId: new mongoose.Types.ObjectId(userId) }
                ]
            },
            { $set: { userId: userIdString } }
        );
        console.log(`  ✅ Converted userId to string format`);
    }

    // 3. Update all operations with workspaceId
    const eventsResult = await Event.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  📝 Updated ${eventsResult.modifiedCount} operations with workspaceId`);

    // 4. Update all entities with string userId
    const accountsResult = await Account.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  💰 Updated ${accountsResult.modifiedCount} accounts`);

    const companiesResult = await Company.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  🏢 Updated ${companiesResult.modifiedCount} companies`);

    const individualsResult = await Individual.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  👤 Updated ${individualsResult.modifiedCount} individuals`);

    const contractorsResult = await Contractor.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  🤝 Updated ${contractorsResult.modifiedCount} contractors`);

    const categoriesResult = await Category.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  📁 Updated ${categoriesResult.modifiedCount} categories`);

    const projectsResult = await Project.updateMany(
        { userId: userIdString, workspaceId: { $exists: false } },
        { $set: { workspaceId } }
    );
    console.log(`  📊 Updated ${projectsResult.modifiedCount} projects`);

    // 5. Set currentWorkspaceId for user
    await User.updateOne(
        { _id: userId },
        { $set: { currentWorkspaceId: workspaceId } }
    );
    console.log(`  👤 Updated user's currentWorkspaceId`);
}

async function main() {
    try {
        console.log('\n🚀 Starting workspace migration...\n');

        // Get all users
        const users = await User.find({}).select('_id email').lean();
        console.log(`📊 Found ${users.length} users\n`);

        // Migrate each user
        for (const user of users) {
            await migrateUserData(user._id);
        }

        console.log('\n✅ Migration completed successfully!');
        console.log('\n📊 Summary:');
        console.log(`   - Users processed: ${users.length}`);
        console.log(`   - All operations and entities updated with workspaceId`);

    } catch (error) {
        console.error('\n❌ Migration failed:', error);
        process.exit(1);
    } finally {
        await mongoose.connection.close();
        console.log('\n👋 Disconnected from MongoDB');
        process.exit(0);
    }
}

// Run migration
main();
