const { Kafka, Partitioners, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'bridge-migration',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  compression: CompressionTypes.GZIP, // ✅ compress messages
});

module.exports = {
  kafka,
  producer,
  userRegisterConsumer: kafka.consumer({ groupId: 'migration_user_register_group' }),
  userBookingConsumer: kafka.consumer({ groupId: 'migration_user_booking_group' }),
  b2bBookingConsumer: kafka.consumer({ groupId: 'migration_b2b_booking_group' }),
  feedbackConsumer: kafka.consumer({ groupId: 'migration_feedback_track_group' }),
  adminCommentsConsumer: kafka.consumer({ groupId: 'migration_admin_comments_group' }),
  uservechicleConsumer: kafka.consumer({ groupId: 'user_vechicle_bridge_migration' }),
};
