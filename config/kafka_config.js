// config/kafka_config.js
const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'bridge-migration',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

// User Register
const userRegisterConsumer = kafka.consumer({ groupId: 'migration_user_register_group' });

// User Booking
const userBookingConsumer = kafka.consumer({ groupId: 'migration_user_booking_group' });

// B2B Booking
const b2bBookingConsumer = kafka.consumer({ groupId: 'migration_b2b_booking_group' });

// Feedback Track / Service Buddy
const feedbackConsumer = kafka.consumer({ groupId: 'migration_feedback_track_group' });

// Admin Comments
const adminCommentsConsumer = kafka.consumer({ groupId: 'migration_admin_comments_group' });

// user reg
const uservechicleConsumer =  kafka.consumer({ groupId: 'user_vechicle_bridge_migration'});




module.exports = {
  kafka,     // optional, for admin/debug
  producer,
  userRegisterConsumer,
  userBookingConsumer,
  b2bBookingConsumer,
  feedbackConsumer,
  adminCommentsConsumer,
  uservechicleConsumer
};
