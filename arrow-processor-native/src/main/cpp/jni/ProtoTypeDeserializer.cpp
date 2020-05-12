#include "ProtoTypeDeserializer.h"
#include "gandiva/projector.h"
#include "Types.pb.h"

using gandiva::ConditionPtr;
using gandiva::DataTypePtr;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::SchemaPtr;
using gandiva::Status;

bool ParseProtobuf(uint8_t *buf, int bufLen, google::protobuf::Message *msg) {
    google::protobuf::io::CodedInputStream cis(buf, bufLen);
    cis.SetRecursionLimit(1000);
    return msg->ParseFromCodedStream(&cis);
}

DataTypePtr ProtoTypeToTime32(const types::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types::SEC:
            return arrow::time32(arrow::TimeUnit::SECOND);
        case types::MILLISEC:
            return arrow::time32(arrow::TimeUnit::MILLI);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time32\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToTime64(const types::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types::MICROSEC:
            return arrow::time64(arrow::TimeUnit::MICRO);
        case types::NANOSEC:
            return arrow::time64(arrow::TimeUnit::NANO);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time64\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToTimestamp(const types::ExtGandivaType &ext_type) {
    switch (ext_type.timeunit()) {
        case types::SEC:
            return arrow::timestamp(arrow::TimeUnit::SECOND);
        case types::MILLISEC:
            return arrow::timestamp(arrow::TimeUnit::MILLI);
        case types::MICROSEC:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case types::NANOSEC:
            return arrow::timestamp(arrow::TimeUnit::NANO);
        default:
            std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for timestamp\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToInterval(const types::ExtGandivaType &ext_type) {
    switch (ext_type.intervaltype()) {
        case types::YEAR_MONTH:
            return arrow::month_interval();
        case types::DAY_TIME:
            return arrow::day_time_interval();
        default:
            std::cerr << "Unknown interval type: " << ext_type.intervaltype() << "\n";
            return nullptr;
    }
}

DataTypePtr ProtoTypeToDataType(const types::ExtGandivaType &ext_type) {
    switch (ext_type.type()) {
        case types::NONE:
            return arrow::null();
        case types::BOOL:
            return arrow::boolean();
        case types::UINT8:
            return arrow::uint8();
        case types::INT8:
            return arrow::int8();
        case types::UINT16:
            return arrow::uint16();
        case types::INT16:
            return arrow::int16();
        case types::UINT32:
            return arrow::uint32();
        case types::INT32:
            return arrow::int32();
        case types::UINT64:
            return arrow::uint64();
        case types::INT64:
            return arrow::int64();
        case types::HALF_FLOAT:
            return arrow::float16();
        case types::FLOAT:
            return arrow::float32();
        case types::DOUBLE:
            return arrow::float64();
        case types::UTF8:
            return arrow::utf8();
        case types::BINARY:
            return arrow::binary();
        case types::DATE32:
            return arrow::date32();
        case types::DATE64:
            return arrow::date64();
        case types::DECIMAL:
            // TODO: error handling
            return arrow::decimal(ext_type.precision(), ext_type.scale());
        case types::TIME32:
            return ProtoTypeToTime32(ext_type);
        case types::TIME64:
            return ProtoTypeToTime64(ext_type);
        case types::TIMESTAMP:
            return ProtoTypeToTimestamp(ext_type);
        case types::INTERVAL:
            return ProtoTypeToInterval(ext_type);
        case types::FIXED_SIZE_BINARY:
        case types::LIST:
        case types::STRUCT:
        case types::UNION:
        case types::DICTIONARY:
        case types::MAP:
            std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
            return nullptr;

        default:
            std::cerr << "Unknown data type: " << ext_type.type() << "\n";
            return nullptr;
    }
}

FieldPtr ProtoTypeToField(const types::Field &f) {
    const std::string &name = f.name();
    DataTypePtr type = ProtoTypeToDataType(f.type());
    bool nullable = true;
    if (f.has_nullable()) {
        nullable = f.nullable();
    }

    return field(name, type, nullable);
}


SchemaPtr ProtoTypeToSchema(const types::Schema &schema) {
    std::vector<FieldPtr> fields;

    for (int i = 0; i < schema.columns_size(); i++) {
        FieldPtr field = ProtoTypeToField(schema.columns(i));
        if (field == nullptr) {
            std::cerr << "Unable to extract arrow field from schema\n";
            return nullptr;
        }

        fields.push_back(field);
    }

    return arrow::schema(fields);
}

std::shared_ptr<arrow::Schema> ReadSchemaFromProtobufBytes(jbyte *schema_bytes, jsize schema_len) {
    types::Schema proto_schema;
    if (!ParseProtobuf(reinterpret_cast<uint8_t *>(schema_bytes), schema_len, &proto_schema)) {
        std::cout << "Unable to parse schema protobuf\n";
        return nullptr;
    }

    return ProtoTypeToSchema(proto_schema);
}
