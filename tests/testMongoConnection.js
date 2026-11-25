/**
 * @file testMongoConnection.js
 * @description Diagnostic script to verify connection to Source MongoDB,
 *              check the database, verify the collection, and inspect sample records.
 *
 * Usage:
 *   npm run test:mongo
 *   # or: node tests/testMongoConnection.js
 */

import 'dotenv/config';
import { MongoClient } from 'mongodb';
import { config } from '../src/config/index.js';

async function testMongo() {
  console.log('================================================================');
  console.log('🔍 TESTING MONGODB SOURCE CONNECTION');
  console.log('================================================================');
  console.log(`📡 URI:        ${config.sourceMongo.uri}`);
  console.log(`🗄️  Database:   ${config.sourceMongo.peopleDb}`);
  console.log(`📦 Collection: ${config.sourceMongo.peopleCollection}`);
  console.log('----------------------------------------------------------------\n');

  const startTime = Date.now();
  const client = new MongoClient(config.sourceMongo.uri, {
    serverSelectionTimeoutMS: 5000,
    connectTimeoutMS: 5000
  });

  try {
    console.log('1️⃣  Connecting to MongoDB cluster...');
    await client.connect();
    console.log(`   ✅ Connected successfully in ${Date.now() - startTime}ms!\n`);

    console.log('2️⃣  Running ping command...');
    const adminDb = client.db('admin');
    const pingResult = await adminDb.command({ ping: 1 });
    console.log(`   ✅ Ping response: ${JSON.stringify(pingResult)}\n`);

    console.log(`3️⃣  Checking Database '${config.sourceMongo.peopleDb}'...`);
    const db = client.db(config.sourceMongo.peopleDb);
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(c => c.name);
    console.log(`   Available collections: ${collectionNames.join(', ') || '(none)'}`);

    const colExists = collectionNames.includes(config.sourceMongo.peopleCollection);
    if (!colExists) {
      console.warn(`   ⚠️ WARNING: Collection '${config.sourceMongo.peopleCollection}' was not found in database '${config.sourceMongo.peopleDb}'.`);
      console.warn(`      Check SOURCE_PEOPLE_DB and SOURCE_PEOPLE_COLLECTION in your .env.\n`);
    } else {
      console.log(`   ✅ Collection '${config.sourceMongo.peopleCollection}' found!\n`);
    }

    const col = db.collection(config.sourceMongo.peopleCollection);

    console.log('4️⃣  Counting total documents in collection...');
    const totalDocs = await col.estimatedDocumentCount();
    console.log(`   📊 Estimated total documents: ${totalDocs.toLocaleString()}\n`);

    console.log('5️⃣  Checking query filter (first_name and website present)...');
    const validCount = await col.countDocuments({
      first_name: { $exists: true, $nin: ['', null] },
      website: { $exists: true, $nin: ['', null] }
    }, { limit: 1000 });
    console.log(`   ✅ Found at least ${validCount} eligible contacts with first_name & website in the first 1000 docs scanned.\n`);

    console.log('6️⃣  Fetching 2 sample documents:');
    const samples = await col.find({
      first_name: { $exists: true, $nin: ['', null] },
      website: { $exists: true, $nin: ['', null] }
    }).limit(2).toArray();

    samples.forEach((doc, idx) => {
      console.log(`   Sample #${idx + 1}:`);
      console.log(`     - _id:          ${doc._id}`);
      console.log(`     - first_name:   ${doc.first_name}`);
      console.log(`     - last_name:    ${doc.last_name || '(empty)'}`);
      console.log(`     - company_name: ${doc.company_name || doc.company || '(empty)'}`);
      console.log(`     - website:      ${doc.website}`);
    });

    console.log('\n================================================================');
    console.log('🎉 MONGODB CONNECTION VERIFIED & READY FOR PRODUCTION!');
    console.log('================================================================');
    process.exit(0);

  } catch (err) {
    console.error('\n❌ MongoDB Connection Failed!');
    console.error(`   Error: ${err.message}`);
    if (err.name === 'MongoServerSelectionError') {
      console.error('\n💡 Troubleshooting Tips:');
      console.error('   1. Check if the IP is correct and MongoDB is running on the target server.');
      console.error('   2. Ensure /etc/mongod.conf has "bindIp: 0.0.0.0" on the DB server.');
      console.error('   3. Ensure port 27017 is open in the firewall on the DB server.');
      console.error('   4. If MongoDB requires authentication, use: mongodb://user:pass@host:27017/db?authSource=admin');
    }
    console.error('\n================================================================');
    process.exit(1);
  } finally {
    await client.close();
  }
}

testMongo();
