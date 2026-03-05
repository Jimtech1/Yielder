const mongoose = require('mongoose');

// Constructed based on standard Atlas patterns and the resolved shard name
const uri = "mongodb://aslambhai93164_db_user:n5DlCVfjlXtzBv5i@mento0-shard-00-00.6i4vpk2.mongodb.net:27017,mento0-shard-00-01.6i4vpk2.mongodb.net:27017,mento0-shard-00-02.6i4vpk2.mongodb.net:27017/yielder?ssl=true&authSource=admin&replicaSet=atlas-6i4vpk2-shard-0";

console.log("Testing Standard Connection String (No SRV)...");
console.log(`URI: ${uri.replace(/:[^:]*@/, ':****@')}`); // Hide password in log

mongoose.connect(uri)
  .then(() => {
    console.log("✅ Connection Successful!");
    process.exit(0);
  })
  .catch((err) => {
    console.error("❌ Connection Failed:");
    console.error(err.message);
    process.exit(1);
  });
