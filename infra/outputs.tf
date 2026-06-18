output "kafka_brokers" {
  value = module.eventhubs.kafka_brokers
}

output "eventhub_connection_string" {
  value     = module.eventhubs.connection_string
  sensitive = true
}

output "rds_endpoint" {
  value = module.rds.endpoint
}

output "s3_bucket" {
  value = module.s3.bucket_name
}

output "ec2_public_ip" {
  value = module.ec2.public_ip
}
