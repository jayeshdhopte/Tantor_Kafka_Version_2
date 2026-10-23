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

export function filterSchemaVersions<T extends { deleted?: boolean }>(versions: T[], deletedOnly: boolean): T[] {
  return versions.filter(version => Boolean(version.deleted) === deletedOnly);
}

export function filterSchemaSubjects<T extends FilterableSchemaSubject>(subjects: T[], search: string,
  types: string[], compatibility: string[], deletedOnly?: boolean): T[] {
  const query = search.trim().toLowerCase();
  return subjects.filter(subject => (!query || subject.subject.toLowerCase().includes(query)
      || (subject.id != null && String(subject.id).includes(query)))
    && (!types.length || types.includes(subject.schemaType.toUpperCase()))
    && (!compatibility.length || compatibility.includes(subjectCompatibilityLevel(subject)))
    && (deletedOnly === undefined || Boolean(subject.deleted) === deletedOnly));
}
