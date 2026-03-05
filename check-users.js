const mongoose = require('mongoose');

const uri = "mongodb://localhost:27017/yielder";

mongoose.connect(uri)
  .then(async () => {
    console.log("Connected to MongoDB");
    
    // Define minimal schema to query
    const UserSchema = new mongoose.Schema({}, { strict: false });
    const User = mongoose.model('User', UserSchema);
    
    const count = await User.countDocuments();
    console.log(`Total Users in DB: ${count}`);
    
    const users = await User.find();
    console.log('Users:', JSON.stringify(users, null, 2));

    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
