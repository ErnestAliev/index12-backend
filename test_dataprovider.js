/**
 * Test dataProvider with production data
 */

require('dotenv').config();
const mongoose = require('mongoose');

// Import models
const DB_URL = process.env.DB_URL;
mongoose.connect(DB_URL).then(async () => {
    console.log('✅ Connected to MongoDB');

    // Define minimal schemas
    const Event = mongoose.model('Event', new mongoose.Schema({}, { strict: false, collection: 'events' }));
    const Account = mongoose.model('Account', new mongoose.Schema({}, { strict: false, collection: 'accounts' }));
    const Company = mongoose.model('Company', new mongoose.Schema({}, { strict: false, collection: 'companies' }));
    const Contractor = mongoose.model('Contractor', new mongoose.Schema({}, { strict: false, collection: 'contractors' }));
    const Individual = mongoose.model('Individual', new mongoose.Schema({}, { strict: false, collection: 'individuals' }));
    const Project = mongoose.model('Project', new mongoose.Schema({}, { strict: false, collection: 'projects' }));
    const Category = mongoose.model('Category', new mongoose.Schema({}, { strict: false, collection: 'categories' }));

    const models = { Event, Account, Company, Contractor, Individual, Project, Category, mongoose };

    // Create dataProvider
    const createDataProvider = require('./ai/dataProvider');
    const dataProvider = createDataProvider(models);

    // Test with production userId
    const testUserId = '696d554bff8f70383f56896e';
    console.log(`\n🔍 Testing dataProvider.buildDataPacket for userId: ${testUserId}\n`);

    const result = await dataProvider.buildDataPacket(testUserId, {
        includeHidden: true,
        visibleAccountIds: null
    });

    console.log('━'.repeat(80));
    console.log('📊 RESULTS:');
    console.log('━'.repeat(80));
    console.log(`💰 Accounts: ${result.accounts?.length || 0}`);
    if (result.accounts && result.accounts.length > 0) {
        result.accounts.forEach(acc => {
            console.log(`   - ${acc.name}: current=${acc.currentBalance}₸, future=${acc.futureBalance}₸, hidden=${acc.isHidden}`);
        });
    }

    console.log(`\n📝 Operations: ${result.operations?.length || 0}`);
    if (result.operations && result.operations.length > 0) {
        console.log(`   Sample operations:`);
        result.operations.slice(0, 5).forEach(op => {
            console.log(`   - ${op.date}: ${op.type} ${op.amount}₸ ${op.description || ''}`);
        });
    }

    console.log(`\n📈 Totals:`);
    console.log(`   Open: current=${result.totals?.open?.current || 0}₸, future=${result.totals?.open?.future || 0}₸`);
    console.log(`   Hidden: current=${result.totals?.hidden?.current || 0}₸, future=${result.totals?.hidden?.future || 0}₸`);
    console.log(`   All: current=${result.totals?.all?.current || 0}₸, future=${result.totals?.all?.future || 0}₸`);

    console.log(`\n🏢 Companies: ${result.catalogs?.companies?.length || 0}`);
    console.log(`🤝 Contractors: ${result.catalogs?.contractors?.length || 0}`);
    console.log(`📁 Categories: ${result.catalogs?.categories?.length || 0}`);
    console.log(`📊 Projects: ${result.catalogs?.projects?.length || 0}`);

    console.log('\n✅ Test complete');
    process.exit(0);
}).catch(err => {
    console.error('❌ Error:', err);
    process.exit(1);
});
