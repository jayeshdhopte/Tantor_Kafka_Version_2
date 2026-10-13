interface SchemaSubmissionFailure {
  message?: unknown;
  validationReason?: unknown;
  affectedField?: unknown;
}

export function schemaSubmissionError(data: unknown, fallback: string): string {
  if (!data || typeof data !== 'object') return fallback;
  const failure = data as SchemaSubmissionFailure;
  const reason = typeof failure.validationReason === 'string' ? failure.validationReason.trim() : '';
  const field = typeof failure.affectedField === 'string' ? failure.affectedField.trim() : '';
  const message = typeof failure.message === 'string' ? failure.message.trim() : '';
  if (reason) return field ? `${reason} Affected field: ${field}.` : reason;
  if (field) return `${fallback} Affected field: ${field}.`;
  return message || fallback;
}
