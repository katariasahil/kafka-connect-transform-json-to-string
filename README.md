Kafka Connect SMT to transform nested java objects and arrays to string



Example on how to add to your connector:
```
transforms=jsontostring
transforms.jsontostring.type=com.github.katariasahil.kafka.connect.smt.JsonToString$Value
```
