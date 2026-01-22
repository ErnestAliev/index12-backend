
const mongoose = require('mongoose');
require('dotenv').config();

async function run() {
    await mongoose.connect(process.env.DB_URL);
    const db = mongoose.connection.db;
    const collections = await db.listCollections().toArray();

    const searchTerms = ['Kaspi Pay', 'Пушкина', 'Наличные', 'BCC [8349]'];

    for (const colInfo of collections) {
        const col = db.collection(colInfo.name);
        for (const term of searchTerms) {
            const results = await col.find({
                $or: [
                    { name: { $regex: term, $options: 'i' } },
                    { description: { $regex: term, $options: 'i' } }
                ]
            }).toArray();

            if (results.length > 0) {
                console.log(`Found "${term}" in collection "${colInfo.name}":`, results.length, 'matches');
                results.forEach(r => console.log(` - ${r.name || r.description || r._id}`));
            }
        }
    }

    await mongoose.disconnect();
}
run().catch(console.error);
