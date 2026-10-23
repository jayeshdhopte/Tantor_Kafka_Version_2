export async function bulkSoftDeleteSubjects(
  subjects: string[],
  urlForSubject: (subject: string) => string,
  request: typeof fetch = fetch,
) {
  const deleted: string[] = [];
  const failed: { subject: string; message: string }[] = [];
  for (const subject of new Set(subjects)) {
    try {
      const response = await request(urlForSubject(subject), { method: 'DELETE' });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.message || 'Soft delete failed.');
      }
      deleted.push(subject);
    } catch (error) {
      failed.push({ subject, message: error instanceof Error ? error.message : 'Soft delete failed.' });
    }
  }
  return { deleted, failed };
}
