// Keep relative filter labels and the request window tied to the same instant.
export function relativeLogRange(hours: string, now: number) {
  const duration = Number(hours) * 3_600_000;
  if (!Number.isFinite(duration) || duration <= 0 || !Number.isFinite(now)) {
    throw new Error('Choose a valid relative log range.');
  }
  const input = (instant: number) => new Date(instant + 19_800_000).toISOString().slice(0, 16);
  return { from: input(now - duration), to: input(now), fromIso: new Date(now - duration).toISOString() };
}
