### Namespace Transformation
This project is to solve the issue that jdbc connect doesn't provide native support to change the namespace of schema.
The jdbc connect will override the predefined schema with default name and namespace which cause deserialization exception when java ingestor consumes the data with predefined schema.
ref: https://github.com/confluentinc/kafka-connect-jdbc/issues/123# 

### How to deploy
run mvn package and it will generate the jar and deploy it to the share folder where jdbc connect jar stays

### How to configure
After deployment, append below configuration to where jdbc connect configuration which is a json file.
"transforms.addKeyNameSapce.type" : "kafka.connect.transform.Namespacefy",
"transforms.addKeyNameSapce.record.namespace" : "<your namespace>"