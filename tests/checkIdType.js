import 'dotenv/config';
import { MongoClient, Long, ObjectId } from 'mongodb';
import { config } from '../src/config/index.js';

async function checkId() {
  const client = new MongoClient(config.sourceMongo.uri);
  await client.connect();
  const db = client.db(config.sourceMongo.peopleDb);
  const col = db.collection(config.sourceMongo.peopleCollection);

  const doc = await col.findOne({
    first_name: { $exists: true, $nin: ['', null] },
    website: { $exists: true, $nin: ['', null] }
  });

  if (!doc) {
    console.log('No doc found');
    process.exit(1);
  }

  console.log('Doc _id:', doc._id);
  console.log('Type of doc._id:', typeof doc._id);
  console.log('Constructor:', doc._id?.constructor?.name);
  console.log('Is Long?:', Long.isLong(doc._id));
  console.log('Is ObjectId?:', doc._id instanceof ObjectId);

  // Check count greater than this id using raw doc._id:
  const countRaw = await col.countDocuments({ _id: { $gt: doc._id } });
  console.log('Count _id > raw doc._id:', countRaw);

  // Check count greater than this id using String(doc._id):
  const countStr = await col.countDocuments({ _id: { $gt: String(doc._id) } });
  console.log('Count _id > String(doc._id):', countStr);

  // Check count greater than this id using Long:
  try {
    const countLong = await col.countDocuments({ _id: { $gt: Long.fromString(String(doc._id)) } });
    console.log('Count _id > Long:', countLong);
  } catch (e) {
    console.log('Long error:', e.message);
  }

  // Check checkpoint collection:
  const cpCol = client.db(config.sourceMongo.checkpointDb).collection(config.sourceMongo.checkpointCollection);
  const cp = await cpCol.findOne({ key: 'mongo_stream_parquet_checkpoint' });
  console.log('Current Checkpoint in DB:', cp);

  await client.close();
}

checkId().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
