package com.github.katariasahil.kafka.connect.smt;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.errors.ConnectException;


public abstract class JsonToString<R extends ConnectRecord<R>> implements Transformation<R> {
  
  public static final String OVERVIEW_DOC =
    "Convert Nested objects and arrays to JSON strings. ";
  private static final ObjectMapper mapper = new ObjectMapper();

     public static final ConfigDef CONFIG_DEF = new ConfigDef();
   private static final String PURPOSE = "Transforming Nested objects and arrays to JSON strings.";

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) { 
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    // Work In Progress: Schemaless handling not implemented yet
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
    final Map<String, Object> updatedValue = new HashMap<>(value);
    return newRecord(record, null, updatedValue);
  }
private static Map<String, Object> structToMap(Struct struct) {
    Map<String, Object> result = new HashMap<>();
    for (Field f : struct.schema().fields()) {
        result.put(f.name(), struct.get(f));
    }
    return result;
}

private Object convertComplexObject(Object obj) {
    if (obj instanceof Struct) {
        Struct struct = (Struct) obj;
        Map<String, Object> map = new HashMap<>();
        for (Field f : struct.schema().fields()) {
            map.put(f.name(), convertComplexObject(struct.get(f)));
        }
        return map;
    } else if (obj instanceof List) {
        List<?> list = (List<?>) obj;
        List<Object> newList = new ArrayList<>();
        for (Object item : list) {
            newList.add(convertComplexObject(item));
        }
        return newList;
    } else {
        return obj;
    }
}


private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
        updatedSchema = makeUpdatedSchema(value.schema());
        schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
        Object fieldValue = value.get(field);

        try {
            if (fieldValue instanceof Struct || fieldValue instanceof List) {
    Object converted = convertComplexObject(fieldValue);
    String json = mapper.writeValueAsString(converted);
    updatedValue.put(field.name(), json);
} else {
    updatedValue.put(field.name(), fieldValue);
}
        } catch (JsonProcessingException e) {
            throw new ConnectException("Failed to convert field " + field.name() + " to JSON string", e);
        }
    }

    return newRecord(record, updatedSchema, updatedValue);
}

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
        Schema fieldSchema = field.schema();

        if (fieldSchema.type() == Schema.Type.STRUCT || fieldSchema.type() == Schema.Type.ARRAY) {
            builder.field(field.name(), Schema.STRING_SCHEMA);
        } else {
            builder.field(field.name(), fieldSchema);
        }
    }

    return builder.build();
}

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends JsonToString<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends JsonToString<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
}
