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
const userRegisterConsumer = kafka.consumer({ groupId: 'user-register-migration-group' });

// User Booking
const userBookingConsumer = kafka.consumer({ groupId: 'user-booking-migration-group' });

// B2B Booking
const b2bBookingConsumer = kafka.consumer({ groupId: 'b2b-booking-migration-group' });

// Feedback Track / Service Buddy
const feedbackConsumer = kafka.consumer({ groupId: 'feedback-track-migration-group' });

// Admin Comments
const adminCommentsConsumer = kafka.consumer({ groupId: 'admin-comments-migration-group' });

// user reg
const uservechicleConsumer =  kafka.consumer({ groupId: 'user-vechile-migration-group'});




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
  