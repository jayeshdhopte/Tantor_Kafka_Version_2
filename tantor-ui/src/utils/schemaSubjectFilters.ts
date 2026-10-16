export interface FilterableSchemaSubject {
  subject: string;
  id: number | null;
  schemaType: string;
  compatibility?: string;
  deleted?: boolean;
}

export const schemaTypes = ['AVRO', 'PROTOBUF', 'JSON'];
export const schemaCompatibilityLevels = ['DEFAULT', 'NONE', 'BACKWARD', 'BACKWARD_TRANSITIVE',
  'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE'];
export const subjectCompatibilityLevel = (subject: FilterableSchemaSubject): string =>
  subject.compatibility?.toUpperCase() || 'UNKNOWN';

export function filterSchemaSubjects<T extends FilterableSchemaSubject>(subjects: T[], search: string,
  types: string[], compatibility: string[]): T[] {
  const query = search.trim().toLowerCase();
  return subjects.filter(subject => (!query || subject.subject.toLowerCase().includes(query)
      || (subject.id != null && String(subject.id).includes(query)))
    && (!types.length || types.includes(subject.schemaType.toUpperCase()))
    && (!compatibility.length || compatibility.includes(subjectCompatibilityLevel(subject))));
}
