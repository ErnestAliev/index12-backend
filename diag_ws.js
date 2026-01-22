
const mongoose = require('mongoose');
require('dotenv').config();

async function run() {
    await mongoose.connect(process.env.DB_URL);
    const db = mongoose.connection.db;

    const projects = await db.collection('projects').find({ name: /Пушкина/i }).toArray();
    console.log('Projects matching "Пушкина":');
    projects.forEach(p => console.log(JSON.stringify(p, null, 2)));

    const workspaces = await db.collection('workspaces').find({}).toArray();
    console.log('\nWorkspaces:');
    workspaces.forEach(w => console.log(JSON.stringify(w, null, 2)));

    await mongoose.disconnect();
}
run().catch(console.error);
