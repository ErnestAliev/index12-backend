
const mongoose = require('mongoose');
require('dotenv').config();

async function run() {
    await mongoose.connect(process.env.DB_URL);
    const db = mongoose.connection.db;
    const collections = await db.listCollections().toArray();
    console.log('Collections:', collections.map(c => c.name));

    const accountsCol = db.collection('accounts');
    const allAccounts = await accountsCol.find({}).toArray();
    console.log(`Total documents in 'accounts' collection: ${allAccounts.length}`);
    allAccounts.forEach(a => {
        console.log(`- ${a.name} (userId: ${a.userId})`);
    });

    await mongoose.disconnect();
}
run().catch(console.error);
