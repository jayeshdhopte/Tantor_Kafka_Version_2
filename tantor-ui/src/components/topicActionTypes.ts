export type TopicActionKind = 'clear' | 'recreate' | 'remove';

export const topicActionCopy: Record<TopicActionKind, { title: string; description: string; button: string }> = {
  clear: {
    title: 'Clear all messages?',
    description: 'Kafka will advance the low watermark for every partition. This cannot be undone and requires a DELETE cleanup policy.',
    button: 'Clear messages'
  },
  recreate: {
    title: 'Recreate this topic?',
    description: 'All messages will be deleted. Partition assignments and explicit settings will be restored.',
    button: 'Recreate topic'
  },
  remove: {
    title: 'Remove this topic?',
    description: 'The topic and all associated data will be permanently deleted.',
    button: 'Remove topic'
  }
};
