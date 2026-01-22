
const mongoose = require('mongoose');
require('dotenv').config();

async function run() {
    if (!process.env.DB_URL) {
        console.error('DB_URL not found in .env');
        process.exit(1);
    }

    await mongoose.connect(process.env.DB_URL);
    console.log('Connected to MongoDB');

    const Account = mongoose.model('Account', new mongoose.Schema({ userId: mongoose.Schema.Types.Mixed, name: String, isExcluded: Boolean, hidden: Boolean, isHidden: Boolean }));
    const Company = mongoose.model('Company', new mongoose.Schema({ userId: mongoose.Schema.Types.Mixed, name: String }));
    const Individual = mongoose.model('Individual', new mongoose.Schema({ userId: mongoose.Schema.Types.Mixed, name: String }));
    const Contractor = mongoose.model('Contractor', new mongoose.Schema({ userId: mongoose.Schema.Types.Mixed, name: String }));
    const Workspace = mongoose.model('Workspace', new mongoose.Schema({ userId: String, name: String, isDefault: Boolean }));

    const accounts = await Account.find({}).lean();
    const companies = await Company.find({}).lean();
    const individuals = await Individual.find({}).lean();
    const contractors = await Contractor.find({}).lean();
    const workspaces = await Workspace.find({}).lean();

    console.log(`Total accounts: ${accounts.length}`);
    console.log(`Total companies: ${companies.length}`);
    console.log(`Total individuals: ${individuals.length}`);
    console.log(`Total contractors: ${contractors.length}`);
    console.log(`Total workspaces: ${workspaces.length}`);

    console.log('\n--- ACCOUNTS ---');
    accounts.forEach(a => {
        const status = [];
        if (a.isExcluded) status.push('excluded');
        if (a.hidden) status.push('hidden');
        if (a.isHidden) status.push('isHidden');
        console.log(`- ${a.name} (ID: ${a._id}, UID: ${a.userId}, status: ${status.join(',') || 'visible'})`);
    });

    console.log('\n--- COMPANIES ---');
    companies.forEach(a => console.log(`- ${a.name} (ID: ${a._id}, UID: ${a.userId})`));

    console.log('\n--- INDIVIDUALS ---');
    individuals.forEach(a => console.log(`- ${a.name} (ID: ${a._id}, UID: ${a.userId})`));

    console.log('\n--- CONTRACTORS ---');
    contractors.forEach(a => console.log(`- ${a.name} (ID: ${a._id}, UID: ${a.userId})`));

    console.log('\n--- WORKSPACES ---');
    workspaces.forEach(a => console.log(`- ${a.name} (ID: ${a._id}, UID: ${a.userId}, isDefault: ${a.isDefault})`));

    await mongoose.disconnect();
}

run().catch(console.error);
