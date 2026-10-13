export interface SchemaFieldChange {
  kind: 'added' | 'removed' | 'type' | 'required' | 'default';
  field: string;
  before?: string;
  after?: string;
}

export interface SchemaFieldComparison {
  supported: boolean;
  identical: boolean;
  changes: SchemaFieldChange[];
  reason?: string;
}

interface FieldDefinition {
  type: string;
  required?: boolean;
  defaultValue?: string;
}

type JsonObject = Record<string, unknown>;

const isObject = (value: unknown): value is JsonObject =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

const hasOwn = (value: JsonObject, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

const decodeSchema = (source: string): unknown => {
  let value: unknown = source;
  for (let attempt = 0; attempt < 3 && typeof value === 'string'; attempt += 1) {
    value = JSON.parse(value);
  }
  return value;
};

const avroType = (value: unknown): string => {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.map(avroType).join(' | ');
  if (!isObject(value)) return 'unknown';
  const type = value.type;
  if (type === 'array') return `array<${avroType(value.items)}>`;
  if (type === 'map') return `map<${avroType(value.values)}>`;
  if (type === 'record' || type === 'enum' || type === 'fixed') {
    return typeof value.name === 'string' ? `${type} ${value.name}` : type;
  }
  const base = avroType(type);
  return typeof value.logicalType === 'string' ? `${base} (${value.logicalType})` : base;
};

const nestedAvroRecord = (value: unknown): JsonObject | null => {
  if (Array.isArray(value)) {
    const records = value.map(nestedAvroRecord).filter((item): item is JsonObject => item !== null);
    return records.length === 1 ? records[0] : null;
  }
  return isObject(value) && value.type === 'record' ? value : null;
};

const avroFields = (schema: JsonObject): Map<string, FieldDefinition> | null => {
  if (schema.type !== 'record' || !Array.isArray(schema.fields)) return null;
  const result = new Map<string, FieldDefinition>();
  const visit = (record: JsonObject, prefix: string, depth: number) => {
    if (depth > 20 || !Array.isArray(record.fields)) return;
    for (const rawField of record.fields) {
      if (!isObject(rawField) || typeof rawField.name !== 'string') continue;
      const field = prefix ? `${prefix}.${rawField.name}` : rawField.name;
      result.set(field, {
        type: avroType(rawField.type),
        defaultValue: hasOwn(rawField, 'default') ? JSON.stringify(rawField.default) : undefined
      });
      const nested = nestedAvroRecord(rawField.type);
      if (nested) visit(nested, field, depth + 1);
    }
  };
  visit(schema, '', 0);
  return result;
};

const jsonType = (value: JsonObject): string => {
  if (typeof value.$ref === 'string') return `reference ${value.$ref}`;
  if (Array.isArray(value.type)) return value.type.map(String).join(' | ');
  if (typeof value.type === 'string') return value.type;
  for (const option of ['oneOf', 'anyOf', 'allOf']) {
    if (Array.isArray(value[option])) {
      return `${option}(${value[option].map(item => isObject(item) ? jsonType(item) : 'unknown').join(', ')})`;
    }
  }
  return isObject(value.properties) ? 'object' : 'unspecified';
};

const jsonFields = (schema: JsonObject): Map<string, FieldDefinition> | null => {
  if (!isObject(schema.properties) && schema.type !== 'object') return null;
  const result = new Map<string, FieldDefinition>();
  const visit = (object: JsonObject, prefix: string, depth: number) => {
    if (depth > 20 || !isObject(object.properties)) return;
    const required = new Set(Array.isArray(object.required) ? object.required : []);
    for (const [name, rawField] of Object.entries(object.properties)) {
      if (!isObject(rawField)) continue;
      const field = prefix ? `${prefix}.${name}` : name;
      result.set(field, {
        type: jsonType(rawField),
        required: required.has(name),
        defaultValue: hasOwn(rawField, 'default') ? JSON.stringify(rawField.default) : undefined
      });
      if (isObject(rawField.properties)) visit(rawField, field, depth + 1);
      if (isObject(rawField.items) && isObject(rawField.items.properties)) {
        visit(rawField.items, `${field}[]`, depth + 1);
      }
    }
  };
  visit(schema, '', 0);
  return result;
};

/** Compares the older version to the newer version, regardless of selector order. */
export const compareSchemaFields = (
  olderSchema: string,
  newerSchema: string,
  olderType: string,
  newerType: string
): SchemaFieldComparison => {
  const type = (olderType || 'AVRO').toUpperCase();
  if (type !== (newerType || 'AVRO').toUpperCase()) {
    return { supported: false, identical: false, changes: [], reason: 'Schema format changed; compare the full definitions below.' };
  }
  if (type !== 'AVRO' && type !== 'JSON' && type !== 'JSON_SCHEMA') {
    return { supported: false, identical: olderSchema === newerSchema, changes: [], reason: 'Field-level comparison is available for Avro and JSON Schema. Compare the full definitions below.' };
  }

  let older: unknown;
  let newer: unknown;
  try {
    older = decodeSchema(olderSchema);
    newer = decodeSchema(newerSchema);
  } catch {
    return { supported: false, identical: olderSchema === newerSchema, changes: [], reason: 'A schema could not be parsed. Compare the full definitions below.' };
  }
  if (!isObject(older) || !isObject(newer)) {
    return { supported: false, identical: false, changes: [], reason: 'A schema could not be parsed. Compare the full definitions below.' };
  }
  const olderFields = type === 'AVRO' ? avroFields(older) : jsonFields(older);
  const newerFields = type === 'AVRO' ? avroFields(newer) : jsonFields(newer);
  if (!olderFields || !newerFields) {
    return { supported: false, identical: false, changes: [], reason: 'This schema structure has no comparable record fields. Compare the full definitions below.' };
  }

  const changes: SchemaFieldChange[] = [];
  const fields = new Set([...olderFields.keys(), ...newerFields.keys()]);
  for (const field of fields) {
    const before = olderFields.get(field);
    const after = newerFields.get(field);
    // A parent field already describes a whole added or removed subtree.
    if ([...changes].some(change => (change.kind === 'added' || change.kind === 'removed') && field.startsWith(`${change.field}.`))) continue;
    if (!before && after) changes.push({ kind: 'added', field, after: after.type });
    else if (before && !after) changes.push({ kind: 'removed', field, before: before.type });
    else if (before && after) {
      if (before.type !== after.type) changes.push({ kind: 'type', field, before: before.type, after: after.type });
      if (before.required !== after.required) {
        changes.push({ kind: 'required', field, before: before.required ? 'required' : 'optional', after: after.required ? 'required' : 'optional' });
      }
      if (before.defaultValue !== after.defaultValue) {
        changes.push({ kind: 'default', field, before: before.defaultValue ?? 'none', after: after.defaultValue ?? 'none' });
      }
    }
  }
  return { supported: true, identical: JSON.stringify(older) === JSON.stringify(newer), changes };
};
