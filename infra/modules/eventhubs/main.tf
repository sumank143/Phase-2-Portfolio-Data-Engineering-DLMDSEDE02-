variable "project"              { type = string }
variable "azure_location"       { type = string }
variable "resource_group_name"  { type = string }
variable "eventhub_sku"         { type = string }

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.azure_location
}

resource "azurerm_eventhub_namespace" "ns" {
  name                = "${var.project}-ehns"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = var.eventhub_sku
  capacity            = 1
  tags                = { project = var.project }
}

resource "azurerm_eventhub" "accidents" {
  name                = "accident-raw"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub" "vehicles" {
  name                = "vehicles-raw"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub_namespace_authorization_rule" "kafka" {
  name                = "kafka-access"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = true
  manage              = false
}

# Store connection string in AWS Secrets Manager
resource "aws_secretsmanager_secret" "eventhub" {
  name        = "${var.project}/eventhub-connection"
  description = "Azure Event Hubs Kafka connection details"
}

resource "aws_secretsmanager_secret_version" "eventhub" {
  secret_id = aws_secretsmanager_secret.eventhub.id
  secret_string = jsonencode({
    connection_string = azurerm_eventhub_namespace_authorization_rule.kafka.primary_connection_string
    brokers           = "${azurerm_eventhub_namespace.ns.name}.servicebus.windows.net:9093"
  })
}

output "kafka_brokers"     { value = "${azurerm_eventhub_namespace.ns.name}.servicebus.windows.net:9093" }
output "connection_string" { value = azurerm_eventhub_namespace_authorization_rule.kafka.primary_connection_string; sensitive = true }
output "secret_arn"        { value = aws_secretsmanager_secret.eventhub.arn }
output "secret_name"       { value = aws_secretsmanager_secret.eventhub.name }
