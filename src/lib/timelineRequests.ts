// A closed drawer or a tab change can destroy a TimelinePanel while its native
// call is still running. Keep one shared slot so a new instance cannot start a
// second full aggregation before that call settles.
let tail: Promise<void> = Promise.resolve();

export async function requestTimelineHistogram<T>(
  isCurrent: () => boolean,
  request: () => Promise<T>,
): Promise<T | undefined> {
  const previous = tail;
  let release!: () => void;
  tail = new Promise<void>((resolve) => { release = resolve; });
  await previous;
  try {
    return isCurrent() ? await request() : undefined;
  } finally {
    release();
  }
}
